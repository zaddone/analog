package cache
import(
	"github.com/zaddone/operate/oanda"
	"github.com/zaddone/analog/config"
	"github.com/zaddone/analog/cluster"
	"math"
	"fmt"
	"time"
	//"encoding/binary"
)
const(
	MaxTag = 6
	//TimeOut = 14400
)

type level struct {

	list []config.Element
	dis float64
	par *level
	child *level
	tag int

	max float64
	maxid int
	update bool
	//next *part
	tp config.Element
	sl config.Element

	//lastOrder *order
	ca *Cache

}

func NewLevel(tag int,c *Cache,ch *level) *level {
	return &level{
		child:ch,
		//list:make([]config.Element,0,100),
		tag:tag,
		ca:c,
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
func (self *level) readDown(hand func(*level)){
	hand(self)
	if self.child != nil {
		self.child.readDown(hand)
	}
}

func (self *level) readUp(hand func(*level)){
	hand(self)
	if self.par != nil {
		self.par.readUp(hand)
	}
}

func (self *level) add(e config.Element,ins *oanda.Instrument) {

	if e.Diff() == 0 {
		return
	}
	self.update = false
	le := len(self.list)
	if le == 0 {
		self.list = []config.Element{e}
		return
	}

	self.list = append(self.list,e)
	var sumdif,absMax,diff,absDiff float64

	mid := e.Middle()
	self.maxid =0
	self.max = 0
	var _e config.Element
	for i:=0 ; i<le ; i++ {
		_e = self.list[i]
		sumdif += math.Abs(_e.Diff())
		diff = mid - _e.Middle()
		if (diff>0) == (self.dis>0) {
			continue
		}
		absDiff = math.Abs(diff)
		if absDiff > absMax {
			self.maxid = i
			self.max = diff
			absMax = absDiff
		}
	}
	if (self.maxid == 0) ||
	(absMax == 0) ||
	(absMax < sumdif/float64(le)) {
		return
	}

	self.update = true
	if self.par == nil {
		tag := self.tag+1
		//fmt.Println(tag)
		//if tag < MaxTag {
			self.par = NewLevel(tag,self.ca,self)
			self.par.add(NewNode(self.list[:self.maxid]...),ins)
		//}
	}else{
		node := NewbNode(self.list[:self.maxid]...)
		if (self.par.par != nil){
			if math.Abs(node.Diff()) > math.Abs(self.par.list[len(self.par.list)-1].Diff()){
				self.ca.pool.Add(cluster.NewSample(self.par.list, node))
				// Clustering self.par.list, node
			}else{
				salist := self.ca.pool.FindSet(cluster.NewSample(append(self.par.list, node),nil))
				if salist != nil {
					se :=&cluster.Set{}
					for _,e := range salist {
						se.SetCount(e)
					}
					fmt.Println(self.ca.Ins.Name,time.Unix(e.DateTime(),0),se.Count)
				}

				//order post  append(self.par.list,node)
			}
		}
		self.par.add(node,ins)
	}

	self.tp = self.list[0]
	self.sl = self.list[self.maxid]
	self.list = self.list[self.maxid:]
	self.dis = self.max
	self.max = 0
	self.maxid = 0

}
