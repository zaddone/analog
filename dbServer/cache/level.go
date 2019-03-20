package cache
import(
	"github.com/zaddone/operate/oanda"
	"github.com/zaddone/analog/config"
	"github.com/zaddone/analog/cluster"
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

type postDB struct {
	ca *Cache
	b int64
	t  byte
}
func NewPostDB(c *Cache,t byte,b int64 ) *postDB {
	po := &postDB {
		ca:c,
		t:t,
		b:b,
	}
	//fmt.Println(c.Ins.Name,s.GetDiff())
	//c.Cshow[5]++
	//c.tmpSample.Store(po.key,s)
	return po
}
func (self *postDB) clear(e int64) byte {

	var eEle,bEle config.Element
	self.ca.read(config.Conf.Local,self.b,e,func(_e config.Element){
		if bEle == nil {
			bEle = _e
		}
		eEle = _e
	})
	//self.ca.FindDB(self.b,e,func(e config.Element){
	//	if bEle == nil {
	//		bEle = e
	//	}
	//	eEle = e
	//	//elist = append(elist,e)
	//})
	if eEle == nil || bEle == nil {
		return 0
	}
	if (eEle.Middle() == bEle.Middle()) {
		return 0
	}

	d := (eEle.Middle() - bEle.Middle())
	//if d > 0 {
	//	return 1
	//}else{
	//	return 2
	//}
	//d := self.ca.GetLastElement().Middle() - self.e.Middle()

	if (d>0) == (self.t==1) {
		return 1
		//self.ca.Cshow[0]++
	}else{
		return 2
		//self.ca.Cshow[1]++
	}

}

type level struct {

	list []config.Element
	dis float64
	par *level
	b config.Element
	child *level
	tag int

	//max float64
	//maxid int
	//update bool
	//next *part
	//tp config.Element
	//sl config.Element

	//lastOrder *order
	ca *Cache
	post []*postDB
	sample *cluster.Sample

}

func NewLevel(tag int,c *Cache,le *level) *level {
	return &level{
		tag:tag,
		ca:c,
		child:le,
		list:make([]config.Element,0,1000),
		//post:make([]*postDB,0,100),
	}
}


func (self *level) LastTime() int64 {
	le := len(self.list)
	if le == 0 {
		return 0
	}
	last :=self.list[le-1]
	return last.DateTime()+last.Duration()

}

func (self *level) duration () int64 {
	last :=self.LastTime()
	if last <= 0 {
		return 0
	}
	return last - self.list[0].DateTime()
}
func (self *level) readf( h func(e config.Element) bool){
	for i :=len(self.list)-1;i>= 0;i-- {
		if !self.list[i].Readf(h){
			return
		}
	}
	if self.par != nil {
		self.par.readf(h)
	}
	//self.par
}
func (self *level) ClearPostAll(){
	self.ClearPost()
	if self.par == nil {
		return
	}
	self.par.ClearPostAll()
}
func (self *level) ClearPost(){
	if len(self.post) == 0 {
		return
	}
	e := self.ca.getLastElement().DateTime()
	for _,p := range self.post{
		self.ca.Cshow[p.clear(e)+1]++
		//if n==0 {
		//	self.ca.Cshow[4]++
		//}else{
		//	self.ca.Cshow[p.t+1]++
		//}
		//}else if n == p.t {
		//}else if n == 2 {
		//	self.ca.Cshow[2]++
		//}else{
		//	self.ca.Cshow[3]++
		//}

		self.ca.Cshow[0]++
	}
	//self.post.clear()
	self.post = nil
}

func (self *level) add(e config.Element,ins *oanda.Instrument) {

	if e.Diff() == 0 {
		return
	}
	//self.update = false
	le := len(self.list)
	if le == 0 {
		self.list =append(self.list,e)
		return
	}

	self.list = append(self.list,e)
	var sumdif,absMax,max,diff,absDiff float64
	var maxid int
	var _e config.Element
	for i:=0 ; i<le ; i++ {
		_e = self.list[i]
		sumdif += math.Abs(_e.Diff())
		diff = e.Middle() - _e.Middle()
		if (diff>0) == (self.dis>0) {
			continue
		}
		absDiff = math.Abs(diff)
		if absDiff > absMax {
			maxid = i
			max = diff
			absMax = absDiff
		}
	}
	sumdif = sumdif/float64(le)
	if (maxid == 0) ||
	(absMax == 0) ||
	(absMax < sumdif) {
		return
	}
	//self.update = true
	self.ClearPost()
	node := NewbNode(self.list[:maxid]...)
	if self.par == nil {
		tag := self.tag+1
		self.par = NewLevel(tag,self.ca,self)
	}else{
		if self.ca != nil {
			self.ca.CheckOrder(self,node,sumdif)
		}
		//self.par.add(node,ins)
	}
	self.par.add(node,ins)
	//self.list = self.list[maxid:]

	li := self.list[maxid:]
	self.list = make([]config.Element,len(li),len(self.list))
	copy(self.list,li)

	if self.ca != nil {
		self.b = self.ca.getLastElement()
	}
	self.dis = max

}
