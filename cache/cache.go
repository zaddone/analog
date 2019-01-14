package cache
import(
	"github.com/zaddone/operate/oanda"
	"github.com/zaddone/analog/request"
	"github.com/zaddone/analog/config"
	"github.com/zaddone/analog/snap"
	"github.com/boltdb/bolt"
	"time"
	"fmt"
	"log"
	"net/url"
	//"sync"
	"os"
	//"math"
	"io"
	"encoding/binary"
	"encoding/json"

	"encoding/gob"
	"bytes"

	"path/filepath"
	"bufio"
)
const(
	Scale int64 = 5
	Count = 5000
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
	Cshow [2]float64
	samples map[string]*snap.Sample
	setPool *snap.SetPool
	//TimeList []*TimeFile
	//TimeList []string

	CacheDB *bolt.DB

}

func NewCache(ins *oanda.Instrument) (c *Cache) {
	c = &Cache {
		//InsCaches:insC,
		Ins:ins,
		//priceChan:make(chan config.Element,Count),
		samples:make(map[string]*snap.Sample),
		//setPool:snap.NewSetPool(ins.Name),
		CandlesChan:make(chan *Candles,Count),
	}
	c.part = NewLevel(0,c,nil)
	return c
}

func (self *Cache) SaveCandles(c *Candles){
	select{
	case self.CandlesChan <- c:
		return
	default:

		k:=make([]byte,8)
		//err := self.CacheDB.Batch(func(tx *bolt.Tx)error {
		err := self.CacheDB.Update(func(tx *bolt.Tx)error {
			b,err := tx.CreateBucketIfNotExists(Bucket)
			if err != nil {
				panic(err)
			}
			var endTime int64
			G:
			for{
				select{
				case _c :=<-self.CandlesChan:
					endTime = _c.DateTime()
					binary.BigEndian.PutUint64(k,uint64(endTime))
					var buf bytes.Buffer
					if err=gob.NewEncoder(&buf).Encode(_c); err != nil {
						panic(err)
					}
					b.Put(k,buf.Bytes())
				default:
					break G
				}
			}
			if endTime == 0 {
				return nil
			}
			var file *os.File = nil
			defer func(){
				if file != nil {
					file.Close()
				}
			}()
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
			cu := b.Cursor()
			var can Candles
			for k,v := cu.First();v!=nil;k,v = cu.Next(){
				if (endTime - int64(binary.BigEndian.Uint64(k)))< OutTime {
					break
				}
				err = gob.NewDecoder(bytes.NewBuffer(v)).Decode(&can)
				if err != nil {
					panic(err)
				}

				b.Delete(k)
				SaveToFile(&can)
			}
			return nil
		})
		if err != nil {
			panic(err)
		}
	}
	self.SaveCandles(c)

}

func (self *Cache) DownCan (h func(*Candles)bool){

	var from int64
	var err error

	if err = self.CacheDB.View(func(tx *bolt.Tx)error{
		b := tx.Bucket(Bucket)
		if b == nil {
			return io.EOF
		}
		k,_ := b.Cursor().Last()
		if k == nil {
			return io.EOF
		}
		from  = int64(binary.BigEndian.Uint64(k)) + Scale
		return nil

	});err != nil {
		from = config.GetFromTime()
	}
	var begin int64
	for{
		err = request.ClientHttp(
		0,
		"GET",
		fmt.Sprintf(
			"%s/instruments/%s/candles?%s",
			config.Host,
			self.Ins.Name,
			url.Values{
				"granularity": []string{"S5"},
				"price": []string{"AB"},
				"count": []string{fmt.Sprintf("%d", Count)},
				"from": []string{fmt.Sprintf("%d", from)},
				//"dailyAlignment":[]string{"3"},
			}.Encode(),
		),
		nil,
		func(statusCode int,body io.Reader)(er error){
			if statusCode != 200 {
				return fmt.Errorf("%d",statusCode)
			}
			var da interface{}
			er = json.NewDecoder(body).Decode(&da)
			if er != nil {
				return er
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
			log.Println(err)
			from = begin
		}else{
			from = begin + Scale
		}
	}
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
					c.load(li)
					if from >= c.DateTime() {
						fmt.Println(p,from,c.DateTime(),time.Unix(from,0).In(Loc),time.Unix(c.DateTime(),0).In(Loc))
						panic(0)
						self.Cshow[1]++
						continue
					}
					self.Cshow[0]++
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
func (self *Cache) SetCacheDB() {
	dir := config.Conf.DbPath
	var err error
	if _,err = os.Stat(dir);err != nil {
		err = os.MkdirAll(dir,0700)
		if err != nil {
			panic(err)
		}
	}

	self.CacheDB,err = bolt.Open(filepath.Join(dir,self.Ins.Name),0600,nil)
	if err != nil {
		panic(err)
	}

}


func (self *Cache) SetSnap(){
	self.setPool = snap.NewSetPool(self.Ins.Name)
}

func (self *Cache) RunDown(){
	self.SetCacheDB()
	self.DownCan(func(can *Candles)bool{
		select{
		case <-self.stop:
			return false
		default:
			self.SaveCandles(can)
			return true
		}
	})
}
func (self *Cache) Close(){
	close(self.stop)
	//close(self.priceChan)
	self.setPool.Close()
	if self.CacheDB != nil {
		self.CacheDB.Close()
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
	self.readCandles(func(c *Candles) bool {

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

//func (self *Cache) Run(hand func(t int64)){
//	xin := self.Ins.Integer()
//	var from int64
//	for{
//		select{
//		case <-self.stop:
//			return
//		case <-time.After(time.Second*5):
//			self.loadCandlesFile(0,from,func(c *Candles) bool {
//				if c == nil {
//					return true
//				}
//				select{
//				case <-self.stop:
//					return false
//				default:
//					from = c.DateTime()
//					self.AddPrice(&eNode{
//						middle:c.Middle()*xin,
//						diff:c.Diff()*xin,
//						dateTime:from,
//						duration:c.Duration(),
//					})
//					if hand != nil {
//						hand(from)
//					}
//				}
//				return true
//			})
//		}
//	}
//}

func (self *Cache) GetLastElement() config.Element {

	le := len(self.part.list)
	if le == 0 {
		return nil
	}
	return self.part.list[le-1]

}

func (self *Cache) AddPrice(p config.Element) {
	var diff float64
	for k,sa := range self.samples {
		diff = sa.Check(p)
		if diff != 0 {
			delete(self.samples,k)
			self.setPool.Add(sa)
		}
	}
	if e := self.GetLastElement(); (e!= nil) && ((p.DateTime() - e.DateTime()) >300) {
		self.part = NewLevel(0,self,nil)
	}
	self.part.add(p,self.Ins)
}
