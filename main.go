package main
import (
	"github.com/zaddone/analog/config"
	"github.com/zaddone/analog/request"
	"github.com/zaddone/analog/cache"
	"github.com/zaddone/operate/oanda"
	"github.com/boltdb/bolt"
	"encoding/json"
	//"fmt"
	"log"
	"time"
	//"sync"
)

type cacheList struct {
	//sync.Mutex
	//sync.Mutex
	cas []*_cache
	Date int64
	//minC chan *_cache
}

func NewCacheList() *cacheList {
	return &cacheList{
		//cas :make(map[string]*_cache)
		//minC:make(chan *_cache,1),
	}
}

func (self *cacheList) HandMap(m []byte,hand func(ca interface{})){

	var v byte = 255
	var t byte
	var j uint
	for i,n := range m {
		if n == v {
			continue
		}
		for j=0;j<4;j++{
			J := j*2
			t = (n&^(^(3<<J)))>>J
			if t == 3 {
				continue
			}
			hand(self.cas[i*4+int(j)].ca)
		}
	}

}

func (self *cacheList) Len() int {
	return len(self.cas)
}
func (self *cacheList) Read(h func(int,interface{})){
	for i,c := range self.cas {
		h(i,c.ca)
	}
}

func (self *cacheList) findMin() {

	var I int
	var minVal int64
	for i,c := range self.cas {
		if (c.val != 0)  && ((minVal==0) || (c.val<minVal)) {
			minVal = c.val
			I = i
		}
	}
	if minVal != 0 {
		if minVal - 3600 > self.Date {
			log.Printf("%s\r",time.Unix(minVal,0))
			self.Date = minVal
		}
		self.cas[I].run()
		return
		//self.minC <- self.cas[I]
	}
	time.Sleep(time.Second)
	self.findMin()
	//panic(0)

}

//func (self *cacheList) run(){
//	for{
//
//		c := <-self.minC
//		fmt.Printf("%s %s\r",c.ca.Ins.Name,time.Unix(c.val,0))
//		c.run()
//	}
//}
type _cache struct {
	cas *cacheList

	ca *cache.Cache
	//index int
	//wait chan int64
	wait chan bool
	val int64
}
func NewCache(ins *oanda.Instrument,cali *cacheList) (c *_cache) {
	c = &_cache{
		ca:cache.NewCache(ins),
		wait:make(chan bool),
		cas:cali,
		//wait:make(chan int64),
	}
	c.ca.SetPool()
	//c.index = len(cali.cas)
	//cali.cas[ins.Name] = append(cali.cas,c)
	cali.cas= append(cali.cas, c)
	go c.ca.Read(func(t int64){
		c.val = t
		<-c.wait

		go c.cas.findMin()
	})
	return c
}

func (self *_cache) run() {
	//self.val = 0
	//fmt.Printf("%s %s\r",self.ca.Ins.Name,time.Unix(self.val,0))
	self.wait<-true
}

var (
	InsList *cacheList = NewCacheList()
)
func main() {

	loadCache()
	//go InsList.run()

	InsList.findMin()


	log.Println("wait")
	t := time.Tick(time.Second * 3600)
	for e := range t {
		log.Println(e)
	}
	//for{
	//	time.Tick(
	//}
}
func loadCache(){
	err := config.HandDB(func(db *bolt.DB)error{
		return db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket(request.Ins_key)
			if b == nil {
				return nil
			}
			return b.ForEach(func(k,v []byte)error{
				_ins := &oanda.Instrument{}
				err := json.Unmarshal(v,_ins)
				if err != nil {
					panic(err)
				}
				NewCache(_ins,InsList)
				return nil
			})
		})
	})
	if err != nil {
		panic(err)
	}
	if len(InsList.cas) == 0 {
		err = request.DownAccountProperties()
		if err != nil {
			panic(err)
		}
		loadCache()
	}
}
