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
	//"sync"
	"os"
	"math"
	"io"
	//"encoding/binary"
	"encoding/json"
	//"encoding/gob"
	//"bytes"
	"path/filepath"
	"bufio"
	"strings"
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
	FindAllDur( int64,func(int,float64,bool))
}

type Cache struct {

	Ins *oanda.Instrument
	part *level

	CandlesChan chan *Candles
	stop chan bool
	//wait chan bool
	lastKey [8]byte
	//CacheAll []*Cache
	Cshow [6]float64

	Cl CacheList

	pool *cluster.Pool
	tmpSample map[string]*cluster.Sample

}
func (self *Cache) ShowPoolNum() int64 {
	return self.pool.SetCount
}

func NewCache(ins *oanda.Instrument) (c *Cache) {
	c = &Cache {
		Ins:ins,
		tmpSample:make(map[string]*cluster.Sample),
		CandlesChan:make(chan *Candles,Count),
		stop:make(chan bool),
		//pool:cluster.NewPool(ins.Name),
	}
	c.part = NewLevel(0,c,nil)
	return c
}

func (self *Cache) FindDur(dur int64) (float64,bool) {
	//begin := self.FindLastTime()
	end := self.GetLastElement()
	if end == nil {
		return 0,false
	}
	var begin config.Element
	var sum float64
	var count float64
	self.part.readf(func(e config.Element) bool{
		sum += math.Abs(e.Diff())
		if (end.DateTime() - e.DateTime()) >= dur {
			begin = e
			return false
		}
		return true
	})
	diff := begin.Middle() - end.Middle()
	return math.Abs(diff) - sum/count,diff > 0

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
			if strings.HasPrefix(err.Error(),"400") {
				log.Println(self.Ins.Name,err)
				return
			}
			from = begin
			//time.After(from)
			//time.Sleep(time.Second*5)

			//return
		}else{
			from = begin + Scale
		}
		//N := from - time.Now().Unix()
		//if N>0 {
		//	time.After(time.Second*time.Duration(N))
		//}
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

	var from int64
	beginT,err := time.Parse(config.TimeFormat,config.Conf.BeginTime)
	beginU := beginT.Unix()
	if err != nil {
		panic(err)
	}
	if err = filepath.Walk(filepath.Join(config.Conf.LogPath,self.Ins.Name),func(p string,info os.FileInfo,er error)error{
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
					err = c.load(li)
					if err != nil {
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
	//var from,begin int64
	var from int64
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
		//if (self.pool != nil) && ((from - begin) >= 604800) {
			//if f,err := os.OpenFile(
			//filepath.Join(config.Conf.ClusterPath,self.Ins.Name,"log"),
			//os.O_APPEND|os.O_CREATE|os.O_RDWR,
			//0700,);
			//err == nil {
			//f.WriteString(
			//	fmt.Sprintf(
			//		"%s %s %.2f %.2f,%.0f\r\n",
			//		time.Now().Format(config.TimeFormat),
			//		time.Unix(from,0).Format(config.TimeFormat),
			//		self.Cshow[4]/self.Cshow[3],
			//		self.Cshow[1]/self.Cshow[0],
			//		self.Cshow,
			//	))
			//f.Close()
			//}else{
			//	panic(err)
			//}
			//self.Cshow = [6]float64{0,0,0,0,0,0}
			//begin = from
		//}
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
	for k,e :=range self.tmpSample {
		d := p.Middle() - e.GetEndElement().Middle()
		if math.Abs(d) > math.Abs(e.GetDiff()) {
			if (d>0) == (e.GetDiff()>0) {
				self.Cshow[1]++
			}else{
				self.Cshow[2]++
			}
			delete(self.tmpSample,k)
		}
	}


	if e := self.GetLastElement(); (e!= nil) && ((p.DateTime() - e.DateTime()) >100) {
		self.part = NewLevel(0,self,nil)
	}
	self.part.add(p,self.Ins)

}
