package cache
import(
	"github.com/zaddone/operate/oanda"
	"github.com/zaddone/analog/config"
	"github.com/zaddone/analog/cluster"
	"math"
	"sync"
	//"bytes"
	"fmt"
	"time"
	//"encoding/binary"
)
const(
	MaxTag = 6
	//TimeOut = 14400
)

type postDB struct {
	ca *Cache
	key string
}
func NewPostDB(c *Cache,s *cluster.Sample ) *postDB {
	po := &postDB {
		ca:c,
		key:string(s.KeyName()),
	}
	//fmt.Println(c.Ins.Name,s.GetDiff())

	//c.Cshow[0]++
	c.tmpSample.Store(po.key,s)
	return po
}
func (self *postDB) clear(){

	s,ok := self.ca.tmpSample.Load(self.key)
	if !ok{
		return
	}
	sa := s.(*cluster.Sample)
	e := self.ca.GetLastElement()
	d := e.Middle() - sa.GetEndElement().Middle()
	if (d>0) == (sa.GetDiff()>0) {
		self.ca.Cshow[0]++
	}else{
		self.ca.Cshow[1]++
	}
	self.ca.tmpSample.Delete(self.key)
	fmt.Println(self.ca.Ins.Name,time.Unix(e.DateTime(),0),self.ca.Cshow[:3])

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

}

func NewLevel(tag int,c *Cache,le *level) *level {
	return &level{
		tag:tag,
		ca:c,
		child:le,
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
	for i := len(self.list) - 1;i>= 0;i-- {
		if !self.list[i].Readf(h){
			return
		}
	}
	if self.par != nil {
		self.par.readf(h)
	}
	//self.par
}
func (self *level) GetCacheMap() (caMap []byte) {
	if self.ca.Cl == nil {
		return nil
	}
	lastCa := self.ca.GetLastElement()
	dif := lastCa.Middle() - self.b.Middle()
	dur := self.b.DateTime()
	absDif := math.Abs(dif)
	le := self.ca.Cl.Len()
	sumlen := le/8
	if le%8 >0 {
		sumlen++
	}
	caMap = make([]byte,sumlen)
	type tmpdb struct{
		t byte
		i int
	}
	chanTmp := make(chan *tmpdb,le)

	var w,w_ sync.WaitGroup
	w_.Add(1)
	go func(_w_ *sync.WaitGroup){
		for d :=range chanTmp {
			caMap[d.i] |= d.t
		}
		_w_.Done()
	}(&w_)
	w.Add(le)
	self.ca.Cl.Read(func(i int,_c interface{}){
		go func(I int,c *Cache,_w *sync.WaitGroup){
			chanTmp <- &tmpdb{
			t:func()byte{
				if c == self.ca {
					return 0
				}
				d := c.FindDur(dur)
				if math.Abs(d) < absDif {
					return 0
				}
				if d>0{
					return 1
				}else{
					return 2
				}
			}() << uint(I%8),
			i:I/8,
			}
			_w.Done()
		}(i*2,_c.(*Cache),&w)

	})
	w.Wait()
	close(chanTmp)
	w_.Wait()
	return caMap

}


func (self *level) add(e config.Element,ins *oanda.Instrument) {

	if e.Diff() == 0 {
		return
	}
	//self.update = false
	le := len(self.list)
	if le == 0 {
		self.list = []config.Element{e}
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
	if len(self.post) >0 {
		for _,p := range self.post{
			p.clear()
		}
		//self.post.clear()
		self.post = nil
	}

	node := NewbNode(self.list[:maxid]...)

	if self.par == nil {
		tag := self.tag+1
		self.par = NewLevel(tag,self.ca,self)
	}else{
		if (self.par.par != nil) && (self.ca.pool != nil){

			if math.Abs(node.Diff()) > math.Abs(self.par.list[len(self.par.list)-1].Diff()){
				ea := cluster.NewSample(self.par.list, node)
				ea.SetCaMap(self.GetCacheMap())
				self.ca.pool.Add(ea)
				self.ca.Cshow[5]++
				//if config.Conf.Debug {
				//func(e *cluster.Sample){

				//	if set := self.ca.pool.FindSet(e);
				//	set != nil {
				//		if set.FindSameKey(e.KeyName()){
				//			self.ca.Cshow[4]++
				//		}
				//	}
				//}(ea)
				//}

			}else{

				if config.Conf.Debug {
					//sa := cluster.NewSample(append(self.par.list, node),nil)
					//if self.ca.pool.Check(sa){
					//	sa.SetDiff(-node.Diff())
					//	sa.SetEndElement(self.ca.GetLastElement())
					//	NewPostDB(self.ca,sa)

					//}


					//if self.ca.Cl != nil {
					//dur := sa.XMax - sa.XMin
					//self.ca.Cl.HandMap(
					//	self.ca.pool.FindCheck(sa),
					//	func(ca interface{}){
					//		c := ca.(*Cache)
					//		l := c.FindLevelWithSame(dur)
					//		list := l.list
					//		if l.child != nil {
					//			list = append(list,NewbNode(l.child.list...))
					//		}
					//		_sa := cluster.NewSample(list,nil)
					//		if _sa.SetDiff(c.pool){
					//			_sa.SetEndElement(c.GetLastElement())
					//			self.post =append(self.post,NewPostDB(c,_sa))
					//		}

					//	},
					//)
					//}
				}
			}
		}
		//self.par.add(node,ins)

	}
	self.par.add(node,ins)

	//self.tp = self.list[0]
	//self.sl = self.list[self.maxid]

	self.list = self.list[maxid:]
	self.b = self.ca.GetLastElement()
	self.dis = max

}
