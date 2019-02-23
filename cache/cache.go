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
	Count = 500
	OutTime int64 = 604800
	ListMaxLen int = 34560
)

var (
	Bucket  = []byte{1}
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
	tmpChan chan config.Element
	stop chan bool
	//chanStop chan bool
	//wait chan bool
	lastKey [8]byte
	//CacheAll []*Cache
	Cshow [8]float64

	Cl CacheList

	pool *cluster.Pool
	//tmpSample map[string]*cluster.Sample
	tmpSample *sync.Map
	db *bolt.DB
	mindb *bolt.DB
	lastDateTime int64

	//EleList []config.Element

	//w sync.WaitGroup
	m sync.RWMutex

}


func (self *Cache) SyncAddPrice(){
	self.syncAddPrice()
}
func (self *Cache) syncAddPrice(){
	for{
		p := <-self.EleChan

		//self.m.Lock()
		if e := self.GetLastElement();(e!= nil) && ((p.DateTime() - e.DateTime()) >100) {
			//self.part.ClearPostAll()
			self.part = NewLevel(0,self,nil)
		}
		self.part.add(p,self.Ins)
		//self.m.Unlock()
	}
}

func (self *Cache) ShowPoolNum() int {
	if self.Cl != nil {
		return self.Cl.Show()
	}else{
		return 0
	}
	//return self.pool.ShowPoolNum()
}

func NewCache(ins *oanda.Instrument) (c *Cache) {
	c = &Cache {
		Ins:ins,
		//tmpSample:make(map[string]*cluster.tmpSample),

		tmpChan : make(chan config.Element,Count),
		tmpSample:new(sync.Map),
		CandlesChan:make(chan *CandlesMin,Count),
		EleChan:make(chan config.Element,2),
		stop:make(chan bool),
		//chanStop:make(chan bool,1),
		//EleChan:make([]config.Element,0,),
		//pool:cluster.NewPool(ins.Name),
		//EleList:make([]config.Element,0,ListMaxLen),
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
	//c.SetLastTime()


	if _,err = os.Stat(config.Conf.DbPath);err != nil {
		if err = os.MkdirAll(config.Conf.DbPath,0700);err != nil {
			panic(err)
		}
	}
	if c.mindb,err = bolt.Open(
		filepath.Join(
			config.Conf.DbPath,
			c.Ins.Name),
			0600,
			nil);err != nil {
		panic(err)
	}
	//c.chanStop<-true
	//go c.syncAddPrice()
	return c
}
func (self *Cache) SaveMinDB() {

	last := make([]byte,8)
	err := self.mindb.View(func(t *bolt.Tx)error{
		b := t.Bucket([]byte{0})
		if b == nil {
			return nil
		}
		k,_ := b.Cursor().Last()
		if k != nil {
			copy(last,k)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	tmp := make(chan config.Element,1000)
	go func(){
		err := self.db.View(func(t *bolt.Tx)error{
			b := t.Bucket([]byte{1})
			if b == nil {
				return nil
			}
			c:= b.Cursor()
			for k,v := c.Seek(last);k != nil;k,v = c.Next(){
				tmp <- NewCandlesMin(k,v)
			}
			return nil
		})
		if err != nil {
			panic(err)
		}
		close(tmp)
	}()
	err = self.mindb.Update(func(t *bolt.Tx)error{
		b,er := t.CreateBucketIfNotExists([]byte{0})
		if er != nil {
			return er
		}
		for e := range tmp {
			k,v := config.Zip(e)
			er = b.Put(k,v)
			if er != nil {
				return er
			}
		}
		return nil

	})
	if err != nil {
		panic(err)
	}
}
func (self *Cache) FindDur(dur int64) float64 {
	//begin := self.FindLastTime()
	self.m.RLock()
	defer self.m.RUnlock()
	end := self.GetLastElement()
	if end == nil ||  end.DateTime() > dur {
		return 0
	}
	le := self.part
	for{
		if len(le.list) == 0 {
			return 0
		}
		if le.list[0].DateTime() > dur {
			break
		}
		if le.par == nil {
			return 0
		}
		le = le.par
	}

	var begin config.Element
	for _,_l := range le.list[1:] {
		_l.Read(func(_e config.Element)bool{
			if _e.DateTime()< dur {
				begin = _e
				return false
			}
			return true
		})
	}
	if begin == nil {
		panic(0)
	}
	return end.Middle() - begin.Middle()

}

func (self *Cache) saveToDB(can *CandlesMin){
	if len(self.CandlesChan)== 0 {
		return
	}
	//t := time.Now().Unix()
	var c *CandlesMin
	err := self.db.Batch(func(t *bolt.Tx)error{
		b,er := t.CreateBucketIfNotExists([]byte{1})
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
				er = b.Put(c.Key(),c.toByte())
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
	//log.Println(time.Now().Unix() - t,time.Unix(c.DateTime(),0))


}

func (self *Cache) FindLastTime() (lt int64) {
	err := self.db.View(func(t *bolt.Tx) error{
		b := t.Bucket([]byte{1})
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
					//fmt.Println(self.Ins.Name,statusCode)
					time.Sleep(time.Second*time.Duration(n))
					n++
					return nil
				}
				db,err := ioutil.ReadAll(body)
				if err != nil {
					panic(err)
				}
				//fmt.Println(u)
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
	self.db.View(func(t *bolt.Tx)error{
		b := t.Bucket([]byte{1})
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
	//self.Cshow = [8]float64{0,0,0,0,0,0,0,0}

}

func (self *Cache) SyncReadAll(hand func(config.Element)){
	self.db.View(func(t *bolt.Tx)error{
		b := t.Bucket([]byte{1})
		if b == nil {
			return nil
		}
		return b.ForEach(func(k,v []byte)error{
			hand(NewCandlesMin(k,v))
			//self.tmpChan <- NewCandlesMin(k,v)
			return nil
		})
	})
}


func (self *Cache) GetLastTime() int64 {

	if self.pool != nil {
		return self.pool.GetLastTime()
	}
	return 0

}

func (self *Cache) ReadAll(hand func(t int64)){

	go self.SyncAddPrice()
	be := make([]byte,8)
	binary.BigEndian.PutUint64(be,uint64(self.GetLastTime()))
	var k,v []byte
	self.db.View(func(t *bolt.Tx)error{
		b := t.Bucket([]byte{1})
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

}


func (self *Cache) GetLastElement() config.Element {

	le := len(self.part.list)
	if le == 0 {
		return nil
	}
	return self.part.list[le-1]

}

func (self *Cache) AddPrice(p config.Element) {

	//self.tmpSample.Range(func(k interface{},s_ interface{})bool{
	//	s := s_.(*cluster.Sample)
	//	d := p.Middle() - s.GetEndElement().Middle()
	//	n := int((s.KeyName()[8] &^ 2) <<1)
	//	if math.Abs(d) > math.Abs(s.GetDiff()) {
	//		if (d>0) == (s.GetDiff()>0) {
	//			self.Cshow[n]++
	//		}else{
	//			self.Cshow[n+1]++
	//		}
	//		self.tmpSample.Delete(k)
	//	}
	//	return true
	//})
	//return
	self.EleChan <- p

	//if e := self.GetLastElement();
	//(e!= nil) && ((p.DateTime() - e.DateTime()) >100) {
	//	self.part = NewLevel(0,self,nil)
	//}
	//self.part.add(p,self.Ins)

}

func (self *Cache) GetCacheMap(b config.Element) (caMap []byte) {
	if self.Cl == nil {
		return nil
	}
	dif := self.GetLastElement().Middle() - b.Middle()
	dur := b.DateTime()
	absDif := math.Abs(dif)
	le := self.Cl.Len()
	sumlen := le/4+1
	if le%4 >0 {
		sumlen++
	}
	caMap = make([]byte,sumlen)
	type tmpdb struct{
		t byte
		i int
	}
	chanTmp := make(chan *tmpdb,le)

	var w,w_ sync.WaitGroup
	w_.Add(1)
	go func(_w_ *sync.WaitGroup){
		for d :=range chanTmp {
			//fmt.Println(len(caMap),d.i)
			caMap[d.i] |= d.t
		}
		_w_.Done()
	}(&w_)
	w.Add(le)
	self.Cl.Read(func(i int,_c interface{}){
		go func(I int,c *Cache,_w *sync.WaitGroup){
			chanTmp <- &tmpdb{
			t:func()byte{
				if c == self {
					return 0
				}
				d := c.FindDur(dur)
				if d == 0 {
					return 0
				}
				if math.Abs(d) < absDif {
					return 3
				}
				if d>0{
					return 1
				}else{
					return 2
				}
			}() << uint(I%8),
			i:I/8,
			}
			_w.Done()
		}(i*2,_c.(*Cache),&w)

	})
	w.Wait()
	close(chanTmp)
	w_.Wait()
	return caMap

}
