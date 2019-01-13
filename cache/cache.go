package cache
import(
	"github.com/zaddone/operate/oanda"
	"github.com/zaddone/analog/request"
	"github.com/zaddone/analog/config"
	"github.com/zaddone/analog/snap"
	//"github.com/boltdb/bolt"
	"time"
	"fmt"
	"log"
	"net/url"
	"sync"
	"os"
	//"math"
	"io"
	"encoding/binary"
	"encoding/json"

	//"encoding/gob"
	//"bytes"

	"path/filepath"
	"bufio"
)
const(
	Scale int64 = 5
	Count = 5000
)
var (
	Bucket  = []byte{1}
)
//type TimeFile struct{
//	//date int64
//	path string
//}
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
	//Cshow [2]float64
	samples map[string]*snap.Sample
	setPool *snap.SetPool
	//TimeList []*TimeFile
	//TimeList []string
	//CacheDB *bolt.DB

}

//func (self *Cache) SaveCandles (c *Candles) {
//
//	select{
//	case self.CandlesChan <- c:
//	default:
//
//		//var buf bytes.Buffer
//		//encode := gob.NewEncoder(&buf)
//		k:=make([]byte,8)
//
//		//err := self.CacheDB.Batch(func(tx *bolt.Tx)error {
//		err := self.CacheDB.Update(func(tx *bolt.Tx)error {
//			b,err := tx.CreateBucketIfNotExists(Bucket)
//			if err != nil {
//				return err
//			}
//			for{
//				select{
//				case _c :=<-self.CandlesChan:
//
//
//					binary.BigEndian.PutUint64(k,uint64(_c.DateTime()))
//					var buf bytes.Buffer
//					if err=gob.NewEncoder(&buf).Encode(_c); err != nil {
//						panic(err)
//					}
//					b.Put(k,buf.Bytes())
//					//buf.Reset()
//
//					//b.Put(k,_c.GetByte())
//				default:
//					return nil
//				}
//			}
//			return nil
//		})
//		if err != nil {
//			panic(err)
//		}
//		//self.CacheDB.Sync()
//		self.SaveCandles(c)
//	}
//
//}

//func (self *Cache) loadTimeList(){
//
//
//	err := filepath.Walk(filepath.Join(config.Conf.LogPath,self.Ins.Name),func(p string,info os.FileInfo,er error)error{
//		if info.IsDir() {
//			return nil
//		}
//		d,er := time.Parse("20060102",info.Name())
//		if er != nil {
//			return nil
//		}
//		self.TimeList = append(self.TimeList,p)
//	})
//	if err != nil {
//		fmt.Println(err)
//		return
//	}
//}

func (self *Cache) findLastFile() *os.File {

	var pf string
	err := filepath.Walk(filepath.Join(config.Conf.LogPath,self.Ins.Name),func(p string,info os.FileInfo,er error)error{
		if er != nil {
			return er
		}
		if info.IsDir() {
			return nil
		}
		_,er = time.Parse("20060102",info.Name())
		if er != nil {
			return nil
		}
		pf = p
		return nil
	})
	if err != nil {
		fmt.Println(err)
		return nil
	}
	if pf == "" {
		return nil
	}
	f,err := os.Open(pf)
	if err != nil {
		panic(err)
	}
	return f
}

func findLastCandle(f *os.File) (c *Candles) {

	buf := bufio.NewReader(f)
	for {
		li,err := buf.ReadSlice('\n')
		if len(li) >1 {
			c = &Candles{}
			c.load(li)
		}
		if err != nil {
			if err != io.EOF {
				panic(err)
			}
			break
		}
	}

	return
}
func (self *Cache) DownCandles (h func(*Candles)bool){

	var file *os.File = nil
	defer func(){
		if file != nil {
			file.Close()
		}
	}()

	file = self.findLastFile()
	var from int64
	if file != nil {
		lastc := findLastCandle(file)
		if lastc !=nil {
			from = lastc.DateTime()+Scale
		}else{
			from = config.GetFromTime()
		}

	}else{
		from = config.GetFromTime()
	}
	opf := func (df time.Time){
		fname := df.Format("20060102")
		if (file != nil){
			if (file.Name() == fname) {
				return
			}else{
				file.Close()
			}
		}
		var err error
		p := filepath.Join(config.Conf.LogPath,self.Ins.Name,df.Format("200601"))
		if _,err = os.Stat(p); err != nil {
			if err = os.MkdirAll(p,0700);err != nil {
				panic(err)
			}
		}
		p = filepath.Join(p,fname)
		//self.TimeList = append(self.TimeList,p)
		if file,err = os.OpenFile(p,os.O_APPEND|os.O_CREATE|os.O_RDWR,0700); err != nil {
			panic(err)
		}
	}

	//var err error
	var begin int64
	for{
		err := request.ClientHttp(
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
				//can.scale = scale
				begin = can.Time
				opf(time.Unix(begin,0).In(Loc))
				if err := json.NewEncoder(file).Encode(can); err != nil {
					panic(err)
				}
				//self.SaveCandles(can)
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

//func (self *Cache) loadCandles(from int64,h func(*Candles) bool){
//	if from == 0 {
//		from = config.GetFromTime()
//	}
//	bf := time.Unix(from,0).In(Loc)
//
//	p := filepath.Join(config.Conf.LogPath,self.Ins.Name,bf.Format("200601"),bf.Format("20060102"))
//	isR := false
//	for _,fp := range self.TimeList {
//		if !isR && fp == p {
//			isR = true
//		}
//	}
//
//}

func (self *Cache) loadCandlesFile (d int,from int64,h func(*Candles) bool){
	if from == 0 {
		from = config.GetFromTime()
	}
	bf := time.Unix(from,0).In(Loc)
	fname := bf.Format("20060102")
	file,err := os.Open(filepath.Join(config.Conf.LogPath,self.Ins.Name,bf.Format("200601"),bf.Format("20060102")))
	if err != nil {
		if d<2 {
			nd,err := time.Parse("20060102",fname)
			if err != nil {
				panic(err)
			}
			self.loadCandlesFile(d+1,nd.In(Loc).AddDate(0,0,1).Unix(),h)
		}

		return

	}
	defer file.Close()
	buf := bufio.NewReader(file)
	for {
		li,err := buf.ReadSlice('\n')
		if len(li) >1 {
			c := &Candles{}
			c.load(li)
			if from >= c.DateTime() {
				continue
			}
			from = c.DateTime()
			if !h(c) {
				return
			}
		}
		if err != nil {
			if err != io.EOF {
				panic(err)
			}
			break
		}
	}

	nd,err := time.Parse("20060102",fname)
	if err != nil {
		panic(err)
	}
	self.loadCandlesFile(0,nd.In(Loc).AddDate(0,0,1).Unix(),h)

}
//func (self *Cache) loadCandles (k []byte,h func(*Candles) bool){
//
//	var v []byte
//	//var V [300]byte
//	var K [8]byte
//	//copy(K[:],k)
//	//V = make([]byte,1024*8)
//	var err error
//	ca := make(chan *Candles,Count)
//	for{
//
//		//if err = self.CacheDB.Batch(func(tx *bolt.Tx)error{
//		if err = self.CacheDB.View(func(tx *bolt.Tx)error{
//			b := tx.Bucket(Bucket)
//			if b == nil {
//				return nil
//			}
//			c := b.Cursor()
//			if k == nil {
//				k,v = c.First()
//			}else{
//				k,v = c.Seek(K[:])
//			}
//			G:
//			for {
//				if v == nil {
//					break G
//				}
//				//c__ := &Candles{}
//				//c__.load(v)
//
//				//fmt.Println(len(V),len(v))
//				//copy(V[:],v)
//				//fmt.Println(v)
//				select{
//				case ca <- NewCandlesWithDB(v):
//				//case ca <- v:
//					k,v = c.Next()
//				default:
//					break G
//				}
//			}
//			copy(K[:],k)
//			return nil
//		});err != nil {
//			panic(err)
//		}
//		G:
//		for {
//			select{
//			case c_ := <-ca:
//				if !h(c_){
//				//if !h(NewCandlesWithDB(v_)){
//					return
//				}
//			default:
//				break G
//			}
//		}
//	}
//}

func NewCache(ins *oanda.Instrument) (c *Cache) {
	c = &Cache {
		//InsCaches:insC,
		Ins:ins,
		//priceChan:make(chan config.Element,Count),
		samples:make(map[string]*snap.Sample),
		setPool:snap.NewSetPool(ins.Name),
		CandlesChan:make(chan *Candles,Count),
	}

	//db,err := bolt.Open(filepath.Join(config.Conf.LogPath,ins.Name),0600,nil)
	//if err != nil {
	//	panic(err)
	//}
	//c.CacheDB = db
	c.part = NewLevel(0,c,nil)

	//var from int64
	//if err := c.CacheDB.View(func(tx *bolt.Tx)error{
	//	b := tx.Bucket(Bucket)
	//	if b != nil {
	//		k,_ := b.Cursor().Last()
	//		from = int64(binary.BigEndian.Uint64(k))
	//	}
	//	return nil
	//}); err != nil {
	//	panic(err)
	//}
	//go c.DownCandles(0,func(can *Candles)bool{
	//	select{
	//	case <-c.stop:
	//		return false
	//	default:
	//		return true
	//	}
	//})
	return c
}

func (self *Cache) RunDown(){
	self.DownCandles(func(can *Candles)bool{
		select{
		case <-self.stop:
			return false
		default:
			return true
		}
	})
}
func (self *Cache) Close(){
	close(self.stop)
	//close(self.priceChan)
	self.setPool.Close()
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

func (self *Cache) Follow(t int64,w *sync.WaitGroup){

	xin := self.Ins.Integer()
	var dt int64
	self.loadCandlesFile(0,t,func(c *Candles) bool{
		if c == nil {
			return true
		}
		dt = c.DateTime()
		if dt<= t {
			self.AddPrice(&eNode{
				middle:c.Middle()*xin,
				diff:c.Diff()*xin,
				dateTime:dt,
				duration:c.Duration(),
			})

		}else{
			binary.BigEndian.PutUint64(self.lastKey[:],uint64(dt))
			return false
		}
		return true
	})
	w.Done()

}

func (self *Cache) Run(hand func(t int64)){
	xin := self.Ins.Integer()
	var from int64
	for{
		select{
		case <-self.stop:
			return
		case <-time.After(time.Second*5):
			self.loadCandlesFile(0,from,func(c *Candles) bool {
				if c == nil {
					return true
				}
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
	}
}

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
