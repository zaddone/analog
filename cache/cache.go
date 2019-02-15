package cache
import(
	"github.com/zaddone/operate/oanda"
	"github.com/zaddone/analog/request"
	"github.com/zaddone/analog/config"
	"github.com/zaddone/analog/cluster"
	"github.com/boltdb/bolt"
	"time"
	"fmt"
	"log"
	"net/url"
	"sync"
	"os"
	"math"
	"io"
	"io/ioutil"
	"encoding/binary"
	"encoding/json"
	//"encoding/gob"
	//"bytes"
	"path/filepath"
	//"bufio"
	//"strings"
)
const(
	Scale int64 = 5
	Count = 500
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
	db *bolt.DB

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
		//CandlesChan:make(chan *Candles,Count),
		EleChan:make(chan config.Element,5),
		stop:make(chan bool),
		//pool:cluster.NewPool(ins.Name),
	}
	c.part = NewLevel(0,c,nil)

	var err error
	if _,err = os.Stat(config.Conf.LogPath);err != nil {
		if err = os.MkdirAll(config.Conf.LogPath,0700);err != nil {
			panic(err)
		}
	}

	if c.db,err = bolt.Open(
		filepath.Join(
			config.Conf.LogPath,
			c.Ins.Name),
			0600,
			nil);err != nil {
		panic(err)
	}
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

func (self *Cache) syncSaveToDB(){

	self.CandlesChan = make(chan *Candles,Count)
	err := self.db.Update(func(t *bolt.Tx)error{
		b,er := t.CreateBucketIfNotExists([]byte{1})
		if er != nil {
			return er
		}
		for{
			c := <-self.CandlesChan
			er = b.Put(c.Key(),c.toByte())
			if er != nil {
				return er
			}

		}
	})
	if err != nil {
		panic(err)
	}

}
//func (self *Cache) saveToDB(can *Candles){
//	if len(self.CandlesChan)== 0 {
//		return
//	}
//	err := self.db.Batch(func(t *bolt.Tx)error{
//		b,er := t.CreateBucketIfNotExists([]byte{1})
//		if er != nil {
//			return er
//		}
//		for{
//			select{
//			case c := <-self.CandlesChan:
//				er = b.Put(c.Key(),c.toByte())
//				if er != nil {
//					return er
//				}
//
//			default:
//				if can != nil {
//					er = b.Put(can.Key(),can.toByte())
//					if er != nil {
//						return er
//					}
//				}
//				return nil
//			}
//		}
//	})
//	if err != nil {
//		panic(err)
//	}
//
//
//}

//func (self *Cache) syncSaveCandles(){
//
//	var file *os.File = nil
//	SaveToFile := func(can_ *Candles){
//		df := time.Unix(can_.DateTime(),0).In(Loc)
//		fname := df.Format("20060102")
//		var err error
//		if file != nil {
//			if file.Name() == fname {
//
//				if err = json.NewEncoder(file).Encode(can_);err != nil {
//					panic(err)
//				}
//				return
//			}else{
//				file.Close()
//			}
//		}
//		p := filepath.Join(config.Conf.LogPath,self.Ins.Name,df.Format("200601"))
//		if _,err = os.Stat(p); err != nil {
//			if err = os.MkdirAll(p,0700);err != nil {
//				panic(err)
//			}
//		}
//		p = filepath.Join(p,fname)
//		if file,err = os.OpenFile(p,os.O_APPEND|os.O_CREATE|os.O_RDWR,0700); err != nil {
//			panic(err)
//		}else{
//			if err = json.NewEncoder(file).Encode(can_);err != nil {
//				fmt.Println(can_)
//				panic(err)
//			}
//		}
//	}
//	for{
//	select{
//	case <-self.stop:
//		return
//	case c := <-self.CandlesChan:
//		SaveToFile(c)
//	}
//	}
//	if file != nil {
//		file.Close()
//		file = nil
//	}
//
//}

func (self *Cache) FindLastTime() (lt int64) {
	err := self.db.View(func(t *bolt.Tx) error{
		b := t.Bucket([]byte{1})
		if b == nil {
			return nil
		}
		k,_ := b.Cursor().Last()
		lt = int64(binary.BigEndian.Uint64(k))+Scale
		return nil
	})
	if err != nil {
		panic(err)
	}
	return

}

func (self *Cache) downCan (h func(*Candles)bool){
	from := self.FindLastTime()
	if from == 0 {
		from = config.GetFromTime()
	}
	var err error
	var begin int64

	go self.syncSaveToDB()
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
			if statusCode != 200 {
				if statusCode == 429 {
					time.Sleep(time.Second)
					return nil
				}
				db,err := ioutil.ReadAll(body)
				if err != nil {
					panic(err)
				}
				//fmt.Println(u)
				return fmt.Errorf("%d %s",statusCode,string(db))
			}
			var da interface{}
			er = json.NewDecoder(body).Decode(&da)
			if er != nil {
				return er
			}
			for _,c := range da.(map[string]interface{})["candles"].([]interface{}) {
				can := NewCandles(c.(map[string]interface{}))

				if self.CandlesChan  != nil {
					self.CandlesChan <- can
				}
				//	select{
				//	case self.CandlesChan <- can:
				//		continue
				//	default:
				//		go self.saveToDB(can)

				//	}
				if (h!=nil) && !h(can) {
					return io.EOF
				}
				begin = can.DateTime() + Scale
			}
			return nil
		})

		//go self.saveToDB(nil)
		if (err != nil) {
			if (err == io.EOF) {
				return
			}else{
				log.Println(err)
			}
		}
		from = begin
		d := from - time.Now().Unix()
		if d >0 {
			<-time.After(time.Second*time.Duration(d))
		}
	}
}

func (self *Cache) tmpRead(begin uint64 ,tmp chan *Candles){
	be := make([]byte,8)
	binary.BigEndian.PutUint64(be,begin)
	self.db.View(func(t *bolt.Tx)error{
		b := t.Bucket([]byte{1})
		if b == nil {
			return nil
		}
		c:= b.Cursor()
		for k,v := c.Seek(be);k != nil;k,v = c.Next(){
			select{
			case tmp <- NewCandlesWithDB(v):
			default:
				return nil
			}
		}
		return nil

	})
	return
}

//func (self *Cache) readCandles(h func(*Candles) bool){
//
//	var from,beginU int64
//	if self.pool != nil{
//		beginU = self.pool.GetLastTime()
//	}
//	fmt.Println("begin",self.Ins.Name,time.Unix(beginU,0))
//	tmpChan := make(chan *Candles,Count)
//	var c *Candles
//	for{
//		self.tmpRead(uint64(beginU),tmpChan)
//		G:
//		for {
//			select{
//			case c = <-tmpChan:
//				if !h(c) {
//					return
//				}
//			default:
//				break G
//			}
//		}
//		if c != nil {
//			beginU = c.DateTime()+Scale
//			d := beginU - time.Now().Unix()
//			if d>0 {
//				<-time.After(time.Second*time.Duration(d))
//			}
//		}else{
//			<-time.After(time.Second*1)
//		}
//	}
//
//}
func (self *Cache) SetPool(){
	self.pool = cluster.NewPool(self.Ins.Name)
	//self.setPool = snap.NewSetPool(self.Ins.Name)
}
func (self *Cache) RunDown(){
	//self.SetCacheDB()
	self.downCan(nil)
	//self.Close()
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
func (self *Cache) SaveTestLog(from int64){

	if (self.pool == nil) {
		return
	}
	f,err := os.OpenFile(
	filepath.Join(config.Conf.ClusterPath,self.Ins.Name,"log"),
	os.O_APPEND|os.O_CREATE|os.O_RDWR,
	0700,)
	if err == nil {
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

}

func (self *Cache) Read(hand func(t int64)){

	var c *Candles
	var from int64
	xin := self.Ins.Integer()
	err := self.db.View(func(t *bolt.Tx)error{
		b := t.Bucket([]byte{1})
		if b == nil {
			return nil
		}
		return b.ForEach(func(k,v []byte)error{
			c = NewCandlesWithDB(v)
			from = int64(binary.BigEndian.Uint64(k))
			c.SetTime(from)
			if hand != nil {
				hand(from)
			}
			self.AddPrice(&eNode{
				middle:c.Middle()*xin,
				diff:c.Diff()*xin,
				dateTime:from,
				duration:c.Duration(),
			})
			return nil
		})
	})
	if err != nil {
		panic(err)
	}
	self.downCan(func(c_ *Candles) bool {
		if c_.DateTime() < c.DateTime() {
			return true
		}
		c = c_
		if hand != nil {
			hand(c.DateTime())
		}
		self.AddPrice(&eNode{
			middle:c.Middle()*xin,
			diff:c.Diff()*xin,
			dateTime:from,
			duration:c.Duration(),
		})
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
