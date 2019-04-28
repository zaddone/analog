package cache
import(
	//"github.com/zaddone/operate/oanda"
	"github.com/zaddone/analog/config"
	//"github.com/zaddone/analog/cluster"
	cluster "github.com/zaddone/analog/telecar"
	"math"
	//"log"
	"sync"
	//"bytes"
	//"fmt"
	//"time"
	//"encoding/binary"
)
const(
	MaxTag = 6
	//TimeOut = 14400
)
type LevelInfo struct {
	l *Level
	n byte
}
func NewLevelInfo(l *Level,n byte) *LevelInfo {
	return &LevelInfo{
		l:l,
		n:n,
	}
}

type Level struct {

	list []config.Element
	dis float64
	par *Level
	b config.Element
	child *Level
	tag int

	AbsMax float64
	//max float64
	//maxid int
	//update bool
	//next *part
	//tp config.Element
	//sl config.Element
	//lastOrder *order
	ca *Cache
	//post []*postDB
	Order *sync.Map
	//Or *OrderInfo

	sample *cluster.Sample
	//OtherLevel []*LevelInfo

}
//func (self *Level) NewOr(sumdif float64){
//	if self.Or != nil {
//		return
//	}
//	if self.sample == nil {
//		return
//	}
//	nb := make([]byte,self.ca.GetSumLen())
//	self.sample.GetCaMap(1,func(b []byte){
//		for i,m := range b {
//			nb |= ^m
//		}
//	})
//	isOut := true
//	for i,m := range nb {
//		if m != 255{
//			isOut = false
//			break
//		}
//	}
//	if isOut {
//		return
//	}
//	self.sample.GetCaMap(2,func(b []byte){
//		for i,m := range b {
//			nb |= ^m
//		}
//	})
//	isOut = true
//	for i,m := range nb {
//		if m != 255{
//			isOut = false
//			break
//		}
//	}
//	if isOut {
//		return
//	}
//	node := NewbNode(self.list...)
//	if math.Abs(node.Diff()) < sumdif {
//		return
//	}
//}

func (self *Level)AddOrder(o *OrderInfo){
	self.Order.Store(o,true)
}
func (self *Level) GetCache() CacheInterface {
	return self.ca
}

func NewLevel(tag int,c *Cache,le *Level) (l *Level) {
	l =  &Level{
		tag:tag,
		ca:c,
		child:le,
		list:make([]config.Element,0,1000),
		Order:new(sync.Map),
		//post:make([]*postDB,0,100),
	}
	//if c.Cl != nil {
	//	l.OtherLevel  = make([]*LevelInfo,c.Cl.Len())
	//}
	return
}
//func (self *Level) AddOtherLevel(i int,l *LevelInfo) {
//	self.OtherLevel[i] = l
//}
//func (self *Level) ClearOtherLevel(){
//	for i,_:= range  self.OtherLevel{
//		self.OtherLevel[i] = nil
//	}
//}

func (self *Level) ClearOrder(){
	go self.Order.Range(func(k,v interface{})bool{
		k.(*OrderInfo).Clear()
		self.Order.Delete(k)
		return true
	})
}


func (self *Level) LastTime() int64 {
	le := len(self.list)
	if le == 0 {
		return 0
	}
	last :=self.list[le-1]
	return last.DateTime()+last.Duration()

}

func (self *Level) duration () int64 {
	last :=self.LastTime()
	if last <= 0 {
		return 0
	}
	return last - self.list[0].DateTime()
}
func (self *Level) readf( h func(e config.Element) bool){
	for i :=len(self.list)-1;i> 0;i-- {
		if !self.list[i].Readf(h){
			return
		}
	}
	if self.par != nil {
		self.par.readf(h)
	}
	//self.par
}
//func (self *level) ClearPostAll(){
//	self.ClearPost()
//	if self.par == nil {
//		return
//	}
//	self.par.ClearPostAll()
//}
//func (self *level) ClearPost(){
//	//if len(self.post) == 0 {
//	//	return
//	//}
//	//e := self.ca.getLastElement().DateTime()
//	//for _,p := range self.post{
//	//	//self.ca.Cshow[p.clear(e)+1]++
//	//	//if n==0 {
//	//	//	self.ca.Cshow[4]++
//	//	//}else{
//	//	//	self.ca.Cshow[p.t+1]++
//	//	//}
//	//	//}else if n == p.t {
//	//	//}else if n == 2 {
//	//	//	self.ca.Cshow[2]++
//	//	//}else{
//	//	//	self.ca.Cshow[3]++
//	//	//}
//	//	//self.ca.Cshow[0]++
//	//}
//	////self.post.clear()
//	//self.post = nil
//}

func (self *Level) add(e config.Element) {

	if e.Diff() == 0 {
		return
	}
	//self.update = false
	le := len(self.list)

	self.list =append(self.list,e)
	if le == 0 {
		return
	}
	var sumdif,max,diff,absDiff float64
	var maxid int
	self.AbsMax = 0
	//var _e config.Element
	for i,_e := range self.list[:le]{
		sumdif += math.Abs(_e.Diff())
		diff = e.Middle() - _e.Middle()
		if (diff>0) == (self.dis>0) {
			continue
		}
		absDiff = math.Abs(diff)
		if absDiff > self.AbsMax {
			maxid = i
			max = diff
			self.AbsMax = absDiff
		}
	}

	//PostOrder

	sumdif /= float64(le)

	if (maxid <= 0) ||
	(self.AbsMax == 0) ||
	(self.AbsMax < sumdif) {
		return
	}
	//self.update = true
	self.ClearOrder()
	//node := NewbNode(self.list[:maxid]...)
	node := NewbNode(self.list[:maxid+1]...)
	//fmt.Println(time.Unix(node.duration/config.Conf.DateUnixV,0),node.Diff(),node.Middle())
	if self.par == nil {
		tag := self.tag+1
		self.par = NewLevel(tag,self.ca,self)
	}else{
		if self.ca != nil {
			self.ca.CheckOrder(self,node,sumdif)
		}
		//self.par.add(node,ins)
	}
	self.par.add(node)
	self.list = self.list[maxid:]

	//li := self.list[maxid:]
	//self.list = make([]config.Element,len(li),len(self.list))
	//copy(self.list,li)

	if self.ca != nil {
		self.b = self.ca.getLastElement()
	}
	self.dis = max

}
