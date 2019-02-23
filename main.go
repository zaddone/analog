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
	//"math/rand"
	//"sync"
)

type cacheList struct {
	//sync.Mutex
	//sync.Mutex
	cas []*_cache
	Date int64
	//w sync.WaitGroup
	topTree *tree
	count int
	//countl int
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
func (self *cacheList) Show() (n int) {
	return self.count
	//n =  self.count -self.countl
	//self.countl = self.count
	//return
	//self.count = 0
	//return
}
func (self *cacheList) addTree(c *tree){

	if self.topTree == nil {
		self.topTree = c
	}else{
		self.topTree.Add(c)
	}

}
func (self *cacheList) UpdateTree(t *tree){
	self.topTree = t
}
func (self *cacheList) PopTree() *_cache {
	return self.topTree.PopSmall(self).(*_cache)
}

func (self *cacheList) HandMap(m []byte,hand func(interface{},byte)){
	if m == nil {
		return
	}
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
	for{
		self.PopTree().Read()
	}
}
type _cache struct {
	cas *cacheList

	ca *cache.Cache
	//index int
	//wait chan int64
	//wait chan bool
	val int64
	begin int64
	//w *sync.WaitGroup
	noinfo *tree
	//chanStop chan bool

}
func (self *_cache) GetVal() int64 {
	return self.val
	//return self.ca.LastDateTime
}
func NewCache(ins *oanda.Instrument,cali *cacheList) (c *_cache) {
	c = &_cache{
		ca:cache.NewCache(ins),
		//wait:make(chan bool,1),
		cas:cali,
		//w:&(cali.w),
		//wait:make(chan int64),
	}
	c.noinfo = NewTree(c)
	c.ca.Cl = cali
	cali.cas= append(cali.cas, c)

	c.ca.SetPool()
	//go c.ca.RunDown()
	c.Read()
	//fmt.Println(c.ca.Ins.Name)

	return c
}


func (self *_cache) Read() {
	self.ca.ReadAll(func(t_ int64){
		if t_ - self.begin > 604800 {
			self.ca.SaveTestLog(t_)
			self.begin = t_
		}
		if t_ < self.val {
			panic(0)
		}
		self.val = t_
		self.cas.addTree(self.noinfo)
		//go self.cas.findMin()
	})
}

//func (self *_cache) run() {
//	//self.val = 0
//	//fmt.Printf("%s %s\r",self.ca.Ins.Name,time.Unix(self.val,0))
//	self.wait<-true
//	//self.w.Add(1)
//}

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
type No interface{
	GetVal() int64
}
type NodeTree interface{
	UpdateTree(*tree)
}
type tree struct {
	node No
	big *tree
	small *tree
	//top *tree
}
func NewTree(n No) *tree {
	return &tree{
		node:n,
	}
}
//func (self *tree) GetVal() int64 {
//	return self.node.GetVal()
//}

func (self *tree) Add (n *tree) {
	if self.node.GetVal() > n.node.GetVal() {
		if self.small == nil {
			self.small = n
			//self.small.top = self
		}else{
			self.small.Add(n)
		}
	}else{
		if self.big == nil {
			self.big = n
			//self.big.top = self
		}else{
			self.big.Add(n)
		}
	}
}
func (self *tree) PopSmall(top NodeTree) (No) {

	if self.small != nil {
		return self.small.PopSmall(self)
	}
	top.UpdateTree(self.big)
	self.big = nil
	return self.node

}

func (self *tree) UpdateTree(t *tree){
	self.small = t
}

