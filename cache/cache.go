package cache
import(
	"github.com/zaddone/operate/oanda"
	"github.com/zaddone/analog/request"
	"github.com/zaddone/analog/config"
	"github.com/zaddone/analog/cluster"
	//"github.com/boltdb/bolt"
	"time"
	"fmt"
	"log"
	"net/url"
	"sync"
	"os"
	"math"
	"io"
	//"encoding/binary"
	"encoding/json"
	//"encoding/gob"
	//"bytes"
	"path/filepath"
	"bufio"
	//"strings"
)
const(
	Scale int64 = 5
	Count = 100
	OutTime int64 = 604800
)

var (
	Bucket  = []byte{1}
)

type CacheList interface{
	//FindAllDur( int64,func(int,float64,bool))
	Read(func(int,interface{}))
	Len() int
	HandMap([]byte,func(interface{}))
}

type Cache struct {

	Ins *oanda.Instrument
	part *level

	CandlesChan chan *Candles
	EleChan chan config.Element
	stop chan bool
	//wait chan bool
	lastKey [8]byte
	//CacheAll []*Cache
	Cshow [6]float64

	Cl CacheList

	pool *cluster.Pool
	//tmpSample map[string]*cluster.Sample
	tmpSample *sync.Map

}

func (self *Cache) syncAddPrice(){
	for{
		select{
		case p := <-self.EleChan:
			if e := self.GetLastElement();(e!= nil) && ((p.DateTime() - e.DateTime()) >100) {
				self.part = NewLevel(0,self,nil)
			}
			self.part.add(p,self.Ins)
		case <-self.stop:
			return
		}
	}
}

func (self *Cache) ShowPoolNum() int {
	return self.pool.ShowPoolNum()
}

func (self *Cache) FindLevelWithSame(dur int64) *level {

	le := self.part
	if le == nil {
		return nil
	}
	var minLe *level
	var min,max int64
	for{
		max = le.duration()
		if ( max < dur) {
			min = max
			minLe = le
			le = le.par
			if le == nil {
				return nil
			}
		}else{
			break
		}
	}
	if min == 0{
		return le
	}
	if (dur-min) > (max-dur) {
		return minLe
	}else{
		return le
	}

}

func NewCache(ins *oanda.Instrument) (c *Cache) {
	c = &Cache {
		Ins:ins,
		//tmpSample:make(map[string]*cluster.tmpSample),
		tmpSample:new(sync.Map),
		CandlesChan:make(chan *Candles,Count),
		EleChan:make(chan config.Element,5),
		stop:make(chan bool),
		//pool:cluster.NewPool(ins.Name),
	}
	c.part = NewLevel(0,c,nil)
	//go c.syncAddPrice()
	return c
}

func (self *Cache) FindDur(dur int64) float64 {
	//begin := self.FindLastTime()
	end := self.GetLastElement()
	if end == nil {
		return 0
	}
	var begin config.Element
	self.part.readf(func(e config.Element) bool{
		if e.DateTime() < dur {
		//if (end.DateTime() - e.DateTime()) >= dur {
			return false
		}
		begin = e
		return true
	})
	return end.Middle() - begin.Middle()

}

func (self *Cache) syncSaveCandles(){

	var file *os.File = nil
	SaveToFile := func(can_ *Candles){
		df := time.Unix(can_.DateTime(),0).In(Loc)
		fname := df.Format("20060102")
		var err error
		if file != nil {
			if file.Name() == fname {

				if err = json.NewEncoder(file).Encode(can_);err != nil {
					panic(err)
				}
				return
			}else{
				file.Close()
			}
		}
		p := filepath.Join(config.Conf.LogPath,self.Ins.Name,df.Format("200601"))
		if _,err = os.Stat(p); err != nil {
			if err = os.MkdirAll(p,0700);err != nil {
				panic(err)
			}
		}
		p = filepath.Join(p,fname)
		if file,err = os.OpenFile(p,os.O_APPEND|os.O_CREATE|os.O_RDWR,0700); err != nil {
			panic(err)
		}else{
			if err = json.NewEncoder(file).Encode(can_);err != nil {
				fmt.Println(can_)
				panic(err)
			}
		}
	}
	for{
	select{
	case <-self.stop:
		return
	case c := <-self.CandlesChan:
		SaveToFile(c)
	}
	}
	if file != nil {
		file.Close()
		file = nil
	}

}

func (self *Cache) FindLastTime() int64 {
	var lastfile string
	err := filepath.Walk(filepath.Join(config.Conf.LogPath,self.Ins.Name),func(p string,info os.FileInfo,er error)error{
		if er != nil {
			return er
		}
		if info.IsDir() {
			return nil
		}
		lastfile = p
		return nil
	})
	if err != nil {
		fmt.Println(err)
		return 0
	}
	if len(lastfile) == 0 {
		return 0
	}
	f,err := os.Open(lastfile)
	if err != nil {
		return 0
		//panic(err)
	}
	//fmt.Println(lastfile)
	buf := bufio.NewReader(f)
	var li,endli []byte
	for {
		li,err = buf.ReadSlice('\n')
		if len(li) >1 {
			endli = li
			//fmt.Println()
				//fmt.Println(lastfile)
				//f.Close()
				//err = os.Remove(lastfile)
				//if err != nil {
				//	panic(err)
				//}
				//return self.FindLastTime()
		}
		if err != nil {
			if err != io.EOF {
				panic(err)
			}
			break
		}
	}
	f.Close()

	endCandles := &Candles{}
	err = endCandles.load(endli)
	if err != nil {
		panic(err)
	}
	return endCandles.DateTime()

}

func (self *Cache) downCan (h func(*Candles)bool){


	from := self.FindLastTime()
	if from == 0 {
		from = config.GetFromTime()
	}
	var err error
	var begin int64
	//fmt.Println(self.Ins.Name,time.Unix(from,0),"down")
	for{
		u :=url.Values{
				"granularity": []string{"S5"},
				"price": []string{"AB"},
				"count": []string{fmt.Sprintf("%d", Count)},
				"from": []string{fmt.Sprintf("%d", from)},
				//"dailyAlignment":[]string{"3"},
			}.Encode()
		err = request.ClientHttp(
		0,
		"GET",
		fmt.Sprintf(
			"%s/instruments/%s/candles?%s",
			config.Host,
			self.Ins.Name,
			u,
		),
		nil,
		func(statusCode int,body io.Reader)(er error){
			var da interface{}
			er = json.NewDecoder(body).Decode(&da)
			if er != nil {
				return er
			}
			if statusCode != 200 {
				fmt.Println(u)
				return fmt.Errorf("%d %v",statusCode,da)
			}
			for _,c := range da.(map[string]interface{})["candles"].([]interface{}) {
				can := NewCandles(c.(map[string]interface{}))
				begin = can.Time
				if h != nil && !h(can) {
					return io.EOF
				}
			}
			return nil
		})
		if (err != nil) {
			if (err == io.EOF) {
				return
			}
			from = begin
			kill := time.Now().Unix() - from
			if kill >0 {
				<-time.After(time.Second*time.Duration(kill))
			}else{
				log.Println(self.Ins.Name,err)
			}
		}else{
			from = begin + Scale
		}
	}
}

func (self *Cache) readAndDownCandles(h func(*Candles) bool){
	self.readCandles(h)
	//self.downCan(h)
	go self.syncSaveCandles()
	self.downCan(func(can *Candles)bool{
		self.CandlesChan <- can
		return h(can)
	})

}

func (self *Cache) readCandles(h func(*Candles) bool){

	var from,beginU int64
	if self.pool != nil{
		beginU = self.pool.GetLastTime()
	}
	if beginU == 0 {
		beginT,err := time.Parse(config.TimeFormat,config.Conf.BeginTime)
		beginU = beginT.Unix()
		if err != nil {
			panic(err)
		}
	}

	fmt.Println("begin",self.Ins.Name,time.Unix(beginU,0))

	if err := filepath.Walk(filepath.Join(config.Conf.LogPath,self.Ins.Name),func(p string,info os.FileInfo,er error)error{
		if er != nil {
			return io.EOF
		}
		if info.IsDir() {
			return nil
		}
		//fmt.Println(p)
		if file,er := os.Open(p);er == nil {
			defer file.Close()
			buf := bufio.NewReader(file)
			for {
				li,err := buf.ReadSlice('\n')
				if len(li) >1 {
					c := &Candles{}
					if err = c.load(li); err != nil {
						fmt.Println(p)
						panic(err)
					}
					if beginU > c.DateTime() {
						continue
					}
					if from >= c.DateTime() {
						continue
					}
					//self.Cshow[0]++
					from = c.DateTime()
					if !h(c) {
						return io.EOF
					}
				}
				if err != nil {
					if err != io.EOF {
						panic(err)
					}
					break
				}
			}
		}else{
			return er
		}
		return nil
	});err != nil {
		if err != io.EOF {
		panic(err)
		}
	}
}
func (self *Cache) SetPool(){
	self.pool = cluster.NewPool(self.Ins.Name)
	//self.setPool = snap.NewSetPool(self.Ins.Name)
}
func (self *Cache) RunDown(){
	//self.SetCacheDB()
	go self.syncSaveCandles()
	self.downCan(func(can *Candles)bool{
		select{
		case <-self.stop:
			return false
		default:
			self.CandlesChan <- can
			//self.SaveCandles(can)
			return true
		}
	})
	self.Close()
}
func (self *Cache) Close(){
	if self.stop != nil {
		fmt.Println(self.Ins.Name,"close")
		close(self.stop)
		self.stop = nil

		if self.pool != nil {
			self.pool.Close()
		}
	}
}

func (self *Cache) Read(hand func(t int64)){

	xin := self.Ins.Integer()
	var from,begin int64
	//var from int64
	self.readAndDownCandles(func(c *Candles) bool {
		from = c.DateTime()
		if hand != nil {
			hand(from)
		}
		self.AddPrice(&eNode{
			middle:c.Middle()*xin,
			diff:c.Diff()*xin,
			dateTime:from,
			duration:c.Duration(),
		})

		if (self.pool != nil) && ((from - begin) >= 604800) {
			go func(){
				if f,err := os.OpenFile(
				filepath.Join(config.Conf.ClusterPath,self.Ins.Name,"log"),
				os.O_APPEND|os.O_CREATE|os.O_RDWR,
				0700,);
				err == nil {
				f.WriteString(
					fmt.Sprintf(
						"%s %s %.2f %.2f %.2f %.0f %d\r\n",
						time.Now().Format(config.TimeFormat),
						time.Unix(from,0).Format(config.TimeFormat),
						self.Cshow[0]/self.Cshow[1],
						self.Cshow[2]/self.Cshow[3],
						self.Cshow[4]/self.Cshow[5],
						self.Cshow,
						self.ShowPoolNum(),
					))
					f.Close()
				}else{
					panic(err)
				}
				self.Cshow = [6]float64{0,0,0,0,0,0}
			}()

			begin = from
		}
		return true
	})
}

func (self *Cache) GetLastElement() config.Element {

	le := len(self.part.list)
	if le == 0 {
		return nil
	}
	return self.part.list[le-1]

}

func (self *Cache) AddPrice(p config.Element) {

	self.tmpSample.Range(func(k interface{},s_ interface{})bool{
		s := s_.(*cluster.Sample)
		d := p.Middle() - s.GetEndElement().Middle()
		n := int((s.KeyName()[8] &^ 2) <<1)
		if math.Abs(d) > math.Abs(s.GetDiff()) {
			if (d>0) == (s.GetDiff()>0) {
				self.Cshow[n]++
			}else{
				self.Cshow[n+1]++
			}
			self.tmpSample.Delete(k)
		}
		return true
	})
	//self.EleChan <- p

	if e := self.GetLastElement();
	(e!= nil) && ((p.DateTime() - e.DateTime()) >100) {
		self.part = NewLevel(0,self,nil)
	}
	self.part.add(p,self.Ins)

}
