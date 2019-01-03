package cache
import(
	"github.com/zaddone/operate/oanda"
	"github.com/zaddone/analog/config"
	"github.com/zaddone/analog/snap"
	"math"
	"fmt"
	"encoding/binary"
)
const(
	MaxTag = 5
	TimeOut = 14400
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
		if tag < MaxTag{
			self.par = NewLevel(tag,self.ca,self)
			self.par.add(NewNode(self.list[:self.maxid]...),ins)
		}
	}else{
		if (self.par.par != nil) && (self.tag > 0 ) {
			k := make([]byte,8)
			binary.BigEndian.PutUint64(k,uint64(e.DateTime()))
			//l := make([]byte,8)
			//binary.BigEndian.PutUint64(l,uint64(self.duration()))
			k = append(k,byte(self.tag))
			//n :=  self.ca.samples[string(k)]
			sa := snap.NewSample(
				self.par.list,
				self.list,
				absMax,
				k,
				func() (b byte) {
					if self.par.dis>0 {
						b = byte(1)<<uint(1)
					}
					if self.dis >0 {
						b ++
					}
					return
				}(),
			)
			self.ca.samples[string(k)] = sa
			set,_ := self.ca.setPool.Find(sa)
			//set := snap.FindSetPool(self.ca.Ins.Name,sa)
			if set != nil {
				fmt.Println(set.Count,snap.SetLen)
			}
		}

		self.par.add(NewNode(self.list[:self.maxid]...),ins)
	}

	self.tp = self.list[0]
	self.sl = self.list[self.maxid]
	self.list = self.list[self.maxid:]
	self.dis = self.max
	self.max = 0
	self.maxid = 0

}
