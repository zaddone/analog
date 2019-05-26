package cache
import(
	//"github.com/zaddone/operate/oanda"
	"github.com/zaddone/analog/config"
	//"github.com/zaddone/analog/cluster"
	//cluster "github.com/zaddone/analog/telecar"
	cluster "github.com/zaddone/analog/pool"
	"math"
	//"log"
	//"sync"
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

	//max float64
	//maxid int
	//update bool
	//next *part
	//tp config.Element
	//sl config.Element
	//lastOrder *order
	ca *Cache
	//post []*postDB
	//Order *sync.Map
	//Or *OrderInfo

	sample *cluster.Sample
	sampleTmp *cluster.Sample
	maxid int
	addSample []*cluster.Sample

	//AbsMax float64
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


//func (self *Level) ClearOrderInfo(){
//	self.Order.Range(func(k,v interface{})bool{
//		(k.(*OrderInfo)).ClearCs(self.ca)
//		self.Order.Delete(k)
//		return true
//	})
//}
//
//func (self *Level) SetOrderInfo(o *OrderInfo){
//	self.Order.Store(o,true)
//	//o.SetCs(self.ca)
//}
//
//func (self *Level)CheckOrderInfo() bool {
//
//	if self.Or == nil{
//		return true
//	}
//	if self.Or.End {
//		self.Or = nil
//		return true
//	}
//	if self.Or.e == nil {
//		return true
//	}
//	return false
//
//}
//
//func (self *Level)GetOrderInfo() *OrderInfo {
//	//self.Order.Store(o,true)
//	if self.Or == nil || self.Or.End {
//		self.Or = NewOrderInfo(self.ca)
//	}
//	return self.Or
//}
func (self *Level) GetCache() CacheInterface {
	return self.ca
}

func NewLevel(tag int,c *Cache,le *Level) (l *Level) {
	l =  &Level{
		tag:tag,
		ca:c,
		child:le,
		list:make([]config.Element,0,1000),
		//Order:new(sync.Map),
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

//func (self *Level) ClearOrder(diff float64){
//
//	if self.Or == nil{
//		return
//	}
//	if self.Or.End ||
//	self.Or.e == nil {
//		self.Or = nil
//		return
//	}
//	if (self.Or.f>0) != (diff>0){
//		return
//	}
//	self.Or.Clear()
//	self.Or = nil
//
//}


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

func (self *Level) ClearLevel(){
	if self.addSample != nil {
		for _,e := range self.addSample{

			self.ca.pool.Add(e)
		}
		//self.ca.pool.Add(self.addSample)
		self.addSample = nil
	}
	if self.par == nil {
		return
	}
	self.par.ClearLevel()
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

	//return
	if e.Diff() == 0 {
		return
	}

	tmp:=make([]*cluster.Sample,0,len(self.addSample))
	for _,sa := range self.addSample {
		if sa.CheckVal(e){
			self.ca.pool.Add(sa)
		}else{
			tmp = append(tmp,sa)
		}
	}
	self.addSample = tmp
	//self.update = false
	le := len(self.list)

	self.list =append(self.list,e)
	if le == 0 {
		return
	}
	var sumdif,max,diff,absDiff float64
	//var maxid int
	var maxid int = 0
	var max_i float64
	var AbsMax float64 = 0
	//var _e config.Element
	for i,_e := range self.list[:le]{
		sumdif += math.Abs(_e.Diff())
		diff = e.Middle() - _e.Middle()
		absDiff = math.Abs(diff)
		if absDiff > max_i {
			max_i = absDiff
		}
		if (diff>0) == (self.dis>0) {
			continue
		}
		if absDiff > AbsMax {
			maxid = i
			max = diff
			AbsMax = absDiff
		}
	}

	//PostOrder
	if maxid==0{
		self.maxid = maxid
		self.sampleTmp = nil
		//return
	}else{
		if maxid != self.maxid {
			self.maxid = maxid
			if self.par== nil {
				self.sampleTmp = nil
			}else{
				self.sampleTmp = cluster.NewSample(append(self.par.list, NewbNode(self.list[:maxid+1]...)),self.ca.GetSumLen())
			}
		}
	}
	//self.CheckPostOrder()

	//if self.Or != nil {
	//	if self.sampleTmp != nil {
	//		max_i = math.Abs(self.sampleTmp.GetLastElement().Diff())
	//		if  (math.Abs(self.par.list[len(self.par.list)-1].Diff())> max_i) {
	//			self.PostOrder(!(self.dis>0))
	//		}

	//	}
	//	self.Or.ClearOrderCheck()
	//}

	if maxid==0{
		return
	}

	sumdif /= float64(le)
	if (AbsMax == 0) ||
	(AbsMax <= sumdif) {
		return
	}
	//self.update = true
	//self.ClearOrderInfo()
	//node := NewbNode(self.list[:maxid]...)
	//node := NewbNode(self.list[:maxid+1]...)
	var node config.Element
	if self.sampleTmp ==nil {
		node = NewbNode(self.list[:maxid+1]...)
	}else{
		node = self.sampleTmp.GetLastElement()
	}
	//self.ClearOrder(node.Diff())
	//fmt.Println(time.Unix(node.duration/config.Conf.DateUnixV,0),node.Diff(),node.Middle())
	if self.par == nil {
		tag := self.tag+1
		self.par = NewLevel(tag,self.ca,self)
	}else{
		if self.ca != nil {
			self.ca.Unlock()
			self.ca.CheckOrder(self,self.sampleTmp,sumdif)
			self.ca.Lock()
		}
		//self.par.add(node,ins)
	}
	self.par.add(node)
	self.list = self.list[maxid:]
	self.sampleTmp = nil
	self.maxid = 0

	//li := self.list[maxid:]
	//self.list = make([]config.Element,len(li),len(self.list))
	//copy(self.list,li)

	if self.ca != nil {
		self.b = self.ca.getLastElement()
	}
	self.dis = max


	//if len(self.addSample)>0{
	//	self.ca.pool.Add(self.addSample)
	//	self.addSample = nil
	//}

}

//func (self *Level) PostOrder(diff bool){
//
//	if (self.Or == nil) || (self.sample== nil) {
//		return
//	}
//	e_ := self.list[0]
//	if !self.Or.CheckPostOrder(e_,diff) {
//		return
//	}
//	e := self.ca.GetLastElement()
//	self.Or.SetPostOrder(e,e_.Middle()-e.Middle())
//
//}

//func (self *Level) CheckPostOrder(){
//	if self.sampleTmp == nil || self.sample ==nil {
//		return
//	}
//
//	if self.sampleTmp.GetCheckBak() {
//		return
//	}
//	long := math.Abs(self.sampleTmp.GetLastElement().Diff()) > math.Abs(self.sample.GetLastElement().Diff())
//	if long {
//		return
//	}
//	p := self.sample.GetPar()
//	if p == nil {
//		return
//	}
//	if p.Long != long {
//		return
//	}
//	if !self.ca.pool.CheckSample(self.sampleTmp){
//		return
//	}
//	self.sampleTmp.SetCheck(true)
//	//self.ca.SetCShow(5+int(self.sampleTmp.GetTag()&^2) *2,1)
//
//	var j uint
//	I_ := self.ca.GetI()*2
//	t := ^byte(3)
//	self.sample.GetCaMap(1,func(b []byte){
//		G:
//		for i,m := range b{
//			if m == 0 {
//				continue
//			}
//			for j=0;j<8;j+=2{
//				n:=(m>>j) &^ t
//				if n == 0 {
//					continue
//				}
//				_,_e := self.ca.Cl.ReadCa(i*4+int(j)/2).(CacheInterface).FindSample(self.sampleTmp)
//				if _e == nil {
//					continue
//				}
//				if _e.GetCaMapVal(2,I_) != n{
//					continue
//				}
//				if !_e.Check() {
//					continue
//				}
//				self.sampleTmp.SetCheckBak(true)
//				self.ca.SetCShow(5+int(self.sampleTmp.GetTag()&^2) *2,1)
//
//				break G
//			}
//		}
//
//	})
//
//}
