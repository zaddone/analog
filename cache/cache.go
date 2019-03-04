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
	//"math"
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
	Count = 500
	OutTime int64 = 604800
	ListMaxLen int = 34560
)

var (
	Bucket  = []byte{0}
)

type CacheList interface{
	//FindAllDur( int64,func(int,float64,bool))
	Read(func(int,interface{}))
	Len() int
	HandMap([]byte,func(interface{},byte))
	Show() int
}

type Cache struct {

	Ins *oanda.Instrument
	part *level

	CandlesChan chan *CandlesMin
	EleChan chan config.Element
	//tmpChan chan config.Element
	lastKey [8]byte
	Cshow [8]float64
	Cl CacheList
	pool *cluster.Pool
	tmpSample *sync.Map
	//db *bolt.DB
	dbPath string
	lastDateTime int64
	//m sync.Mutex

}

func (self *Cache) SyncAddPrice(){
	self.syncAddPrice()
}

func (self *Cache) syncAddPrice(){
	for{
		p := <-self.EleChan
		//self.m.Lock()
		if e := self.GetLastElement();(e!= nil) && ((p.DateTime() - e.DateTime()) >100) {
			self.part = NewLevel(0,self,nil)
		}
		self.part.add(p,self.Ins)
		//self.m.Unlock()
	}
}

func (self *Cache) ShowPoolNum() [3]int {
	//if self.Cl != nil {
	//	return self.Cl.Show()
	//}else{
	//	return 0
	//}
	return self.pool.ShowPoolNum()
}

func NewCache(ins *oanda.Instrument) (c *Cache) {
	c = &Cache {
		Ins:ins,
		//tmpChan : make(chan config.Element,Count),
		tmpSample:new(sync.Map),
		CandlesChan:make(chan *CandlesMin,Count),
		EleChan:make(chan config.Element,Count),
		dbPath:filepath.Join(config.Conf.DbPath),
		//stop:make(chan bool),
	}
	c.part = NewLevel(0,c,nil)
	//c.dbPath =filepath.Join(config.Conf.DbPath)
	_,err := os.Stat(c.dbPath)
	if err != nil {
		if err = os.MkdirAll(c.dbPath,0700);err != nil {
			panic(err)
		}
	}
	c.dbPath = filepath.Join(c.dbPath,c.Ins.Name)
	//_,err = os.Stat(c.dbPath)
	//if err != nil {
	//	panic(err)
	//}
	fmt.Println(c.Ins.Name,"New")
	return c

}
func (self *Cache) openDB() *bolt.DB {

	db,err := bolt.Open(self.dbPath,0600,nil);
	if err != nil {
		panic(err)
	}
	return db
}

//func (self *Cache) FindDur(dur int64) float64 {
//	//begin := self.FindLastTime()
//	self.m.RLock()
//	defer self.m.RUnlock()
//	end := self.GetLastElement()
//	if end == nil ||  end.DateTime() > dur {
//		return 0
//	}
//	le := self.part
//	for{
//		if len(le.list) == 0 {
//			return 0
//		}
//		if le.list[0].DateTime() > dur {
//			break
//		}
//		if le.par == nil {
//			return 0
//		}
//		le = le.par
//	}
//
//	var begin config.Element
//	for _,_l := range le.list[1:] {
//		_l.Read(func(_e config.Element)bool{
//			if _e.DateTime()< dur {
//				begin = _e
//				return false
//			}
//			return true
//		})
//	}
//	if begin == nil {
//		panic(0)
//	}
//	return end.Middle() - begin.Middle()
//
//}

func (self *Cache) saveToDB(can *CandlesMin){
	if len(self.CandlesChan)== 0 {
		return
	}
	//t := time.Now().Unix()
	var c *CandlesMin
	db := self.openDB()
	err := db.Batch(func(t *bolt.Tx)error{
		b,er := t.CreateBucketIfNotExists(Bucket)
		if er != nil {
			return er
		}
		if can != nil {
			er = b.Put(can.Key(),can.toByte())
			if er != nil {
				return er
			}
		}
		for{
			select{
			case c = <-self.CandlesChan:
				k,v := config.Zip(c)
				er = b.Put(k,v)
				if er != nil {
					return er
				}

			default:
				return nil
			}
		}
	})
	if err != nil {
		panic(err)
	}
	db.Close()
	//log.Println(time.Now().Unix() - t,time.Unix(c.DateTime(),0))

}

func (self *Cache) FindLastTime() (lt int64) {
	db := self.openDB()
	err := db.View(func(t *bolt.Tx) error{
		b := t.Bucket(Bucket)
		if b == nil {
			return nil
		}
		k,_ := b.Cursor().Last()
		if k != nil {
			lt = int64(binary.BigEndian.Uint64(k))+Scale
		}
		//k_,_ := b.Cursor().First()
		//fmt.Println("b",time.Unix(int64(binary.BigEndian.Uint64(k_)),0),self.Ins.Name)
		return nil
	})
	if err != nil {
		panic(err)
	}
	db.Close()
	fmt.Println("e",time.Unix(lt,0),self.Ins.Name)
	return
}

func (self *Cache) downCan (h func(config.Element)bool){
	from := self.FindLastTime()
	if from == 0 {
		from = config.GetFromTime()
	}
	var err error
	//var begin int64
	begin := from
	xin := self.Ins.Integer()
	//go self.syncSaveToDB()
	//fmt.Println(self.Ins.Name,time.Unix(from,0),"down")
	n := 1
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
					time.Sleep(time.Second*time.Duration(n))
					n++
					return nil
				}
				db,err := ioutil.ReadAll(body)
				if err != nil {
					panic(err)
				}
				return fmt.Errorf("%d %s",statusCode,string(db))
			}
			n = 1
			var da interface{}
			er = json.NewDecoder(body).Decode(&da)
			if er != nil {
				return er
			}
			for _,c := range da.(map[string]interface{})["candles"].([]interface{}) {
				can := NewCandles(c.(map[string]interface{})).toMin(xin)
				begin = can.DateTime() + Scale
				select{
				case self.CandlesChan <- can:
					continue
				default:
					go self.saveToDB(can)
				}
				if (h!=nil) && !h(can) {
					return io.EOF
				}
			}
			return nil
		})

		go self.saveToDB(nil)
		if (err != nil) {
			if (err == io.EOF) {
				return
			}else{
				log.Println(err)
			}
		}
		f := time.Unix(from,0)
		b := time.Unix(begin,0)
		if f.Month() != b.Month() {
			fmt.Println(self.Ins.Name,b)
		}
		from = begin
		//d := from - time.Now().Unix()
		if from > time.Now().Unix() {
			//return
			//<-time.After(time.Second*time.Duration(d))
			//time.Sleep(time.M)
			time.Sleep(time.Minute*5)
		}
	}
}

func (self *Cache) tmpRead(tmp chan config.Element){
	be := make([]byte,8)
	binary.BigEndian.PutUint64(be,uint64(self.lastDateTime))
	var k,v []byte
	db := self.openDB()
	db.View(func(t *bolt.Tx)error{
		b := t.Bucket(Bucket)
		if b == nil {
			return nil
		}
		c:= b.Cursor()
		for k,v = c.Seek(be);k != nil;k,v = c.Next(){
			can := NewCandlesMin(k,v)
			select{
			case tmp <- can:
				self.lastDateTime = can.DateTime()+Scale
			default:
				return nil
			}
		}
		return nil

	})
	db.Close()
	return

}

func (self *Cache) SetPool(){
	self.pool = cluster.NewPool(self.Ins.Name)
	//self.setPool = snap.NewSetPool(self.Ins.Name)
}
func (self *Cache) RunDown(){
	//self.SetCacheDB()
	self.downCan(nil)
	//self.Close()
}
//func (self *Cache) Close(){
//	if self.pool != nil {
//		self.pool.Close()
//	}
//	//self.db.Close()
//	//self.mindb.Close()
//
//}
func (self *Cache) SaveTestLog(from int64){

	p := filepath.Join(config.Conf.ClusterPath,self.Ins.Name)
	_,err := os.Stat(p)
	if err != nil{
		if err = os.MkdirAll(p,0700);err != nil {
			panic(err)
		}
	}

	str := fmt.Sprintf(
			"%s %s %.2f %.2f %.2f %.2f %.0f %d\r\n",
			time.Now().Format(config.TimeFormat),
			time.Unix(from,0).Format(config.TimeFormat),
			self.Cshow[0]/self.Cshow[1],
			self.Cshow[2]/self.Cshow[3],
			self.Cshow[4]/self.Cshow[5],
			self.Cshow[6]/self.Cshow[7],
			self.Cshow,
			self.ShowPoolNum(),
		)
		//return
	f,err := os.OpenFile(
	filepath.Join(p,"log"),
	os.O_APPEND|os.O_CREATE|os.O_RDWR|os.O_SYNC,
	0700,)
	if err != nil {
		fmt.Println(self.Ins.Name,str)
		return
		//panic(err)
	}
	f.WriteString(str)
	f.Close()
	//self.Cshow[7] = 0
	//self.Cshow = [8]float64{self.Cshow[0],self.Cshow[1],0,0,0,0,self.Cshow[6],self.Cshow[7]}

}

func (self *Cache) GetLastTime() int64 {

	if self.pool != nil {
		return self.pool.GetLastTime()
	}
	return 0

}

func (self *Cache) ReadAll(hand func(t int64)){

	go self.syncAddPrice()
	be := make([]byte,8)
	binary.BigEndian.PutUint64(be,uint64(self.GetLastTime()))

	//fmt.Println(time.Unix(self.GetLastTime(),0))
	var k,v []byte
	db := self.openDB()
	//fmt.Println(db.Path())
	err := db.View(func(t *bolt.Tx)error{
		b := t.Bucket(Bucket)
		if b == nil {
			return nil
		}
		c:= b.Cursor()
		for k,v = c.Seek(be);k != nil;k,v = c.Next(){
			can := NewCandlesMin(k,v)
			hand(can.DateTime())
			self.AddPrice(can)
		}
		return nil

	})
	if err != nil {
		panic(err)
	}
	db.Close()

}


func (self *Cache) GetLastElement() config.Element {

	le := len(self.part.list)
	if le == 0 {
		return nil
	}
	return self.part.list[le-1]

}

func (self *Cache) AddPrice(p config.Element) {

	self.EleChan <- p

	//if e := self.GetLastElement();
	//(e!= nil) && ((p.DateTime() - e.DateTime()) >100) {
	//	self.part = NewLevel(0,self,nil)
	//}
	//self.part.add(p,self.Ins)

}

//func (self *Cache) GetCacheMap(b,end config.Element) (caMap []byte) {
//	if self.Cl == nil {
//		return nil
//	}
//	dif := end.Middle() - b.Middle()
//	dur := b.DateTime()
//	absDif := math.Abs(dif)
//	le := self.Cl.Len()
//	sumlen := le/4+1
//	if le%4 >0 {
//		sumlen++
//	}
//	caMap = make([]byte,sumlen)
//	type tmpdb struct{
//		t byte
//		i int
//	}
//	chanTmp := make(chan *tmpdb,le)
//
//	var w,w_ sync.WaitGroup
//	w_.Add(1)
//	go func(_w_ *sync.WaitGroup){
//		for d :=range chanTmp {
//			//fmt.Println(len(caMap),d.i)
//			caMap[d.i] |= d.t
//		}
//		_w_.Done()
//	}(&w_)
//	w.Add(le)
//	self.Cl.Read(func(i int,_c interface{}){
//		go func(I int,c *Cache,_w *sync.WaitGroup){
//			chanTmp <- &tmpdb{
//			t:func()byte{
//				if c == self {
//					return 0
//				}
//				d := c.FindDur(dur)
//				if d == 0 {
//					return 0
//				}
//				if math.Abs(d) < absDif {
//					return 3
//				}
//				if d>0{
//					return 1
//				}else{
//					return 2
//				}
//			}() << uint(I%8),
//			i:I/8,
//			}
//			_w.Done()
//		}(i*2,_c.(*Cache),&w)
//
//	})
//	w.Wait()
//	close(chanTmp)
//	w_.Wait()
//	return caMap
//
//}
