package main
import (
	"github.com/zaddone/analog/config"
	"github.com/zaddone/analog/request"
	"github.com/zaddone/analog/cache"
	"github.com/zaddone/operate/oanda"
	"github.com/boltdb/bolt"
	"encoding/json"
	"fmt"
	"time"
	//"sync"
)
type cacheList struct {
	//sync.Mutex
	//sync.Mutex
	cas []*_cache
	minC chan *_cache
}
func NewCacheList() *cacheList {
	return &cacheList{
		minC:make(chan *_cache),
	}
}

func (self *cacheList) findMin(){

	var C *_cache
	var minVal int64
	for _,c := range self.cas {
		if (minVal==0) || (c.val !=0 && c.val<minVal) {
			minVal = c.val
			C = c
		}
	}
	if C != nil {
		self.minC <- C
	}

}


func (self *cacheList) run(){
	for{

		c := <-self.minC
		fmt.Printf("%s %s\r",c.ca.Ins.Name,time.Unix(c.val,0))
		c.run()
	}
}
type _cache struct {
	cas *cacheList
	ca *cache.Cache
	index int
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
	c.index = len(cali.cas)
	cali.cas = append(cali.cas,c)
	go c.ca.Read(func(t int64){
		c.val = t
		<-c.wait
		c.cas.findMin()
	})
	return c
}

func (self *_cache) run() {
	//self.val = 0
	self.wait<-true
}

var (
	InsList *cacheList = NewCacheList()
)
func main() {
	loadCache()
	go InsList.run()
	InsList.findMin()

	fmt.Println(time.Now())
	t := time.Tick(time.Second * 3600)
	for e := range t {
		fmt.Println(e)
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
				InsList.cas = append(InsList.cas,NewCache(_ins,InsList))
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

