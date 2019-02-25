package cache
import(
	"github.com/zaddone/operate/oanda"
	"github.com/zaddone/analog/config"
	"github.com/zaddone/analog/cluster"
	"math"
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
	e config.Element
	t  byte
}
func NewPostDB(c *Cache,t byte ) *postDB {
	po := &postDB {
		ca:c,
		t:t,
		e:c.GetLastElement(),
	}
	//fmt.Println(c.Ins.Name,s.GetDiff())

	c.Cshow[2]++
	//c.tmpSample.Store(po.key,s)
	return po
}
func (self *postDB) clear(){

	d := self.ca.GetLastElement().Middle() - self.e.Middle()
	if (d>0) == (self.t==1) {
		self.ca.Cshow[0]++
	}else{
		self.ca.Cshow[1]++
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
	for _,p := range self.post{
		p.clear()
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
	if (maxid == 0) ||
	(absMax == 0) ||
	(absMax < sumdif/float64(le)) {
		return
	}


	//self.update = true
	//self.ClearPost()

	node := NewbNode(self.list[:maxid]...)

	if self.par == nil {
		tag := self.tag+1
		self.par = NewLevel(tag,self.ca,self)
	}else{
		if (self.par.par != nil) && (self.ca.pool != nil){
			ea := cluster.NewSample(append(self.par.list, node))
			self.ca.pool.Add(ea)
			self.ca.Cshow[int(ea.GetTag() &^ 2)]++
			self.ca.Cshow[7]++
			//ea.SetCaMap(self.GetCacheMap())
			pli := self.par.list[len(self.par.list)-1]
			if (self.sample != nil) &&
			(self.sample.GetLastElement() == pli ){
				//self.sample.Long = math.Abs(node.Diff()) > math.Abs(pli.Diff())
				self.sample.Long = ((self.ca.GetLastElement().Middle() - self.b.Middle()) > 0) == (node.Diff()<0)
				if self.sample.Long {
					self.ca.Cshow[3]++
				}else{
					self.ca.Cshow[2]++
				}
				go func (e_ *cluster.Sample){
					if (e_.Count()>1) {
						if e_.Check() {
							self.ca.Cshow[5]++
						}else{
							self.ca.Cshow[4]++
						}
					}
					//}
					//self.sample.SetCaMap(self.ca.GetCacheMap(self.b))
					self.ca.pool.UpdateSample(e_)
				}(self.sample)

				//self.sample.SetCaMap(self.ca.GetCacheMap(self.b))
				//go func(_e *cluster.Sample,b,end config.Element){
				//	.SetCaMap(self.GetCacheMap(b,end))
				//	self.ca.pool.UpdateSample(_e)
				//}(self.sample,self.b,self.ca.GetLastElement())
			}else{
				self.ca.Cshow[6]++
			}

			self.sample = ea
			//self.ca.pool.Add(ea)
			//if self.ca.Cl != nil {
			//	self.ca.Cl.HandMap(
			//		ea.CheckMap(),
			//		func(ca interface{},t byte){
			//			self.post =append(self.post,NewPostDB(ca.(*Cache),t))
			//		},
			//	)
			//}
				//ea := cluster.NewSample(self.par.list, node)
				//ea.SetCaMap(self.GetCacheMap())
				//self.ca.pool.Add(ea)
				//self.ca.Cshow[7]++
			//}else{


				//self.ca.Cshow[7]++
				//self.sample = cluster.NewSample(append(self.par.list, node),nil)
				//if self.ca.pool.Check(sa){
				//	sa.SetDiff(max)
				//	sa.SetEndElement(self.ca.GetLastElement())
				//	self.ca.tmpSample.Store(string(sa.KeyName()),sa)
					//NewPostDB(self.ca,sa)
				//}
				//if self.ca.Cl != nil {
				//	self.ca.Cl.HandMap(
				//		self.ca.pool.CheckSet(sa),
				//		func(ca interface{},t byte){
				//			self.post =append(self.post,NewPostDB(ca.(*Cache),t))
				//		},
				//	)
				//}
			//}
		}
		//self.par.add(node,ins)

	}
	self.par.add(node,ins)

	//self.tp = self.list[0]
	//self.sl = self.list[self.maxid]

	//self.list = self.list[maxid:]

	li := self.list[maxid:]
	self.list = make([]config.Element,len(li),len(self.list))
	copy(self.list,li)

	self.b = self.ca.GetLastElement()
	self.dis = max

}
