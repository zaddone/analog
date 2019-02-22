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
	chanStop chan bool
	//wait chan bool
	lastKey [8]byte
	//CacheAll []*Cache
	Cshow [8]float64

	Cl CacheList

	pool *cluster.Pool
	//tmpSample map[string]*cluster.Sample
	tmpSample *sync.Map
	db *bolt.DB
	LastDateTime int64

	//w sync.WaitGroup
	//m sync.Mutex

}

func (self *Cache) syncAddPrice(){
	for{
		p := <-self.EleChan
		if e := self.GetLastElement();(e!= nil) && ((p.DateTime() - e.DateTime()) >100) {
			self.part = NewLevel(0,self,nil)
		}
		self.part.add(p,self.Ins)
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

		tmpChan : make(chan config.Element,Count),
		tmpSample:new(sync.Map),
		CandlesChan:make(chan *CandlesMin,Count),
		EleChan:make(chan config.Element,1),
		stop:make(chan bool),
		chanStop:make(chan bool,1),
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
	c.chanStop<-true
	go c.syncAddPrice()
	return c
}

func (self *Cache) FindDur(dur int64) float64 {
	//begin := self.FindLastTime()
	end := self.GetLastElement()
	if end == nil {
		return 0
	}
	if end.DateTime() <= dur {
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
	if begin ==nil {
		return 0
	}
	return end.Middle() - begin.Middle()

}

//func (self *Cache) syncSaveToDB(){
//
//	self.CandlesChan = make(chan *Candles,Count)
//	err := self.db.Update(func(t *bolt.Tx)error{
//		b,er := t.CreateBucketIfNotExists([]byte{1})
//		if er != nil {
//			return er
//		}
//		for{
//			c := <-self.CandlesChan
//			er = b.Put(c.Key(),c.toByte())
//			if er != nil {
//				return er
//			}
//
//		}
//	})
//	if err != nil {
//		panic(err)
//	}
//
//}
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
		d := from - time.Now().Unix()
		if d >0 {
			//return
			//<-time.After(time.Second*time.Duration(d))
			//time.Sleep(time.M)
			time.Sleep(time.Minute*5)
		}
	}
}

func (self *Cache) tmpRead(begin uint64 ,tmp chan config.Element,stop chan bool){
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
			case tmp <- NewCandlesMin(k,v):
			default:
				return nil
			}
		}
		return nil

	})
	stop <- true
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
			"%s %s %.2f %.2f %.2f %.2f %.0f %d\r\n",
			time.Now().Format(config.TimeFormat),
			time.Unix(from,0).Format(config.TimeFormat),
			self.Cshow[0]/self.Cshow[1],
			self.Cshow[2]/self.Cshow[3],
			self.Cshow[4]/self.Cshow[5],
			self.Cshow[6]/self.Cshow[7],
			self.Cshow,
			self.ShowPoolNum(),
		))
		f.Close()
	}else{
		panic(err)
	}
	//self.Cshow[7] = 0
	//self.Cshow = [8]float64{0,0,0,0,0,0,0,0}

}

func (self *Cache) SyncReadAll(){
//	var beginU int64
//	if self.pool != nil{
//		beginU = self.pool.GetLastTime()
//	}
//
//
//	self.db.View(func(t *bolt.Tx)error{
//		b := t.Bucket([]byte{1})
//		if b == nil {
//			return nil
//		}
//		return b.ForEach(func(k,v []byte)error{
//			self.tmpChan <- NewCandlesMin(k,v)
//			return nil
//		})
//	})
}

func (self *Cache) ReadAll(hand func(t int64)){
	//fmt.Println("begin",self.Ins.Name,time.Unix(beginU,0))
	for{
		select{
		case c := <-self.tmpChan:
			self.AddPrice(c)
			self.LastDateTime = c.DateTime()
			hand(self.LastDateTime)
			return
		case <- self.chanStop:
			if self.LastDateTime == 0 {
				self.LastDateTime = self.pool.GetLastTime()
			}
			go self.tmpRead(uint64(self.LastDateTime),self.tmpChan,self.chanStop)
		}
	}


}

func (self *Cache) Read(hand func(t int64)){
	var beginU int64
	if self.pool != nil{
		beginU = self.pool.GetLastTime()
	}
	//fmt.Println("begin",self.Ins.Name,time.Unix(beginU,0))
	tmpChan := make(chan config.Element,Count*10)
	var c config.Element
	for{
		self.tmpRead(uint64(beginU),tmpChan,self.chanStop)
		G:
		for {
			select{
			case c = <-tmpChan:
				hand(c.DateTime())
				//if (c != nil) && c_.DateTime() < c.DateTime() {
				//	panic(9)
				//}
				//c = c_
				//fmt.Println("add",self.Ins.Name,time.Unix(c.DateTime(),0))
				self.AddPrice(c)
			default:
				break G
			}
		}
		if c != nil {
			beginU = c.DateTime()+Scale
		}else{
			time.Sleep(time.Minute*5)
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
