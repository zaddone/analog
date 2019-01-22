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

type Cache struct {

	Ins *oanda.Instrument
	part *level

	//priceChan chan config.Element
	CandlesChan chan *Candles
	stop chan bool

	//LastE config.Element
	lastKey [8]byte
	CacheAll []*Cache
	//InsCaches sync.Map
	Cshow [5]float64

	pool *cluster.Pool
	tmpSample map[string]*cluster.Sample

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
	fmt.Println(self.Ins.Name,time.Unix(from,0),"down")
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
	if err := filepath.Walk(filepath.Join(config.Conf.LogPath,self.Ins.Name),func(p string,info os.FileInfo,er error)error{
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
					if from >= c.DateTime() {
						//fmt.Println(p,from,c.DateTime(),time.Unix(from,0).In(Loc),time.Unix(c.DateTime(),0).In(Loc))
						//panic(0)
						//self.Cshow[1]++
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

func (self *Cache) findDurationSame (dur int64) (l *level,min int64) {

	var v int64
	self.part.readUp(func(_l *level){
		v = func(_v int64 ) int64{
			if _v<0 {
				return -_v
			}
			return _v
		}(dur - _l.duration())
		if  (min == 0) ||
			(v < min) {
			min = v
			l = _l
		}
	})
	return

}

//func (self *Cache) Follow(t int64,w *sync.WaitGroup){
//
//	xin := self.Ins.Integer()
//	var dt int64
//	self.loadCandlesFile(0,t,func(c *Candles) bool{
//		if c == nil {
//			return true
//		}
//		dt = c.DateTime()
//		if dt<= t {
//			self.AddPrice(&eNode{
//				middle:c.Middle()*xin,
//				diff:c.Diff()*xin,
//				dateTime:dt,
//				duration:c.Duration(),
//			})
//
//		}else{
//			binary.BigEndian.PutUint64(self.lastKey[:],uint64(dt))
//			return false
//		}
//		return true
//	})
//	w.Done()
//
//}

func (self *Cache) Read(hand func(t int64)){

	xin := self.Ins.Integer()
	var from int64
	self.readAndDownCandles(func(c *Candles) bool {

		select{
		case <-self.stop:
			return false
		default:
			from = c.DateTime()
			self.AddPrice(&eNode{
				middle:c.Middle()*xin,
				diff:c.Diff()*xin,
				dateTime:from,
				duration:c.Duration(),
			})
			if hand != nil {
				hand(from)
			}
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
