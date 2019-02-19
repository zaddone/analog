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
	"sync"
)

type cacheList struct {
	//sync.Mutex
	//sync.Mutex
	cas []*_cache
	Date int64
	w sync.WaitGroup
	topTree *tree
	//count int
	//lastC *_cache
	//minC chan *_cache
	// minVal int64
}

func NewCacheList() *cacheList {
	return &cacheList{
		//cas :make(map[string]*_cache)
		//minC:make(chan *_cache,1),
	}
}
func (self *cacheList) addTree( c *_cache){
	if self.topTree ==nil {
		self.topTree = NewTree(c)
	}else{
		self.topTree.Add(c)
	}
	//self.count++
	//fmt.Println(c.ca.Ins.Name,time.Unix(c.GetVal(),0),self.count)
	self.w.Done()
}
func (self *cacheList) PopTree() *_cache {
	//self.count--
	self.w.Add(1)
	return self.topTree.PopSmall().(*_cache)
}

func (self *cacheList) HandMap(m []byte,hand func(interface{},byte)){

	var t byte
	var j,J uint
	for i,n := range m {
		if n == 255 {
			continue
		}
		for j=0;j<4;j++{
			J = j*2
			t = (n&^(^(3<<J)))>>J
			if t == 3 || t == 0 {
				continue
			}
			hand(self.cas[i*4+int(j)].ca,t)
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
	// Getfirst
	//for{
		self.w.Wait()
		self.PopTree().run()
	//}
	//time.Sleep(time.Millisecond*1000)
	//self.findMin()
	//return



}
type _cache struct {
	cas *cacheList

	ca *cache.Cache
	//index int
	//wait chan int64
	wait chan bool
	val int64
	begin int64
	w *sync.WaitGroup
}
func (self *_cache) GetVal() int64 {
	return self.val
}
func NewCache(ins *oanda.Instrument,cali *cacheList) (c *_cache) {
	c = &_cache{
		ca:cache.NewCache(ins),
		wait:make(chan bool),
		cas:cali,
		//w:&(cali.w),
		//wait:make(chan int64),
	}
	c.ca.SetPool()
	cali.cas= append(cali.cas, c)
	cali.w.Add(1)
	go c.Read()
	go c.ca.RunDown()
	return c
}
func (self *_cache) Read() {
	self.ca.Read(func(t int64){
		if t - self.begin > 604800 {
			self.ca.SaveTestLog(t)
			self.begin = t
		}
		self.val = t
		self.cas.addTree(self)
		<-self.wait
		go self.cas.findMin()
	})
}

func (self *_cache) run() {
	//self.val = 0
	//fmt.Printf("%s %s\r",self.ca.Ins.Name,time.Unix(self.val,0))
	self.wait<-true
	//self.w.Add(1)
}

var (
	InsList *cacheList = NewCacheList()
)
func main() {

	loadCache()
	//go InsList.run()



	log.Println("wait")
	InsList.findMin()
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
type no interface{
	GetVal() int64
}

type tree struct {
	node no
	big *tree
	small *tree
	top *tree
}
func NewTree(n no) *tree {
	return &tree{
		node:n,
	}
}
func (self *tree) Copy(t *tree) {
	self.node = t.node
	self.big = t.big
	self.small = t.small
	if self.big != nil{
		self.big.top = self
	}
	if self.small != nil {
		self.small.top = self
	}
}
func (self *tree) Add (n no) {
	if self.node.GetVal() >= n.GetVal() {
		if self.small == nil {
			self.small = NewTree(n)
			self.small.top = self
		}else{
			self.small.Add(n)
		}
	}else{
		if self.big == nil {
			self.big = NewTree(n)
			self.big.top = self
		}else{
			self.big.Add(n)
		}
	}
}
func (self *tree) PopSmall() (n no) {

	if self.small != nil {
		return self.small.PopSmall()
	}
	n = self.node
	if self.top != nil {
		if self.big != nil {
			self.big.top = self.top
		}
		self.top.small = self.big
	}else{

		self.Copy(self.big)
	}
	return
}
