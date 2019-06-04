package pool
import(
	"github.com/zaddone/analog/config"
	"sync"
	//"os"
	//"fmt"
	"math"
	//"time"
)

type CacheInter interface {
	SetCShow(int,float64)
	InsName()string
	GetCacheLen() int
	GetLastElement() config.Element

}
type Pools struct{
	//p [2]*Pool
	ca  CacheInter
	t *Tree
	tmp chan *Sample

}
func NewPools(ca CacheInter) (p *Pools) {

	p = &Pools{
		ca:ca,
		//p:[2]*Pool{NewPool(),NewPool()},
		t:&Tree{},
		tmp:make(chan *Sample,1),
	}
	go p.syncRun()
	return p

}
func (self *Pools) syncRun(){
	for{
		e := <-self.tmp
		//continue
		e.SetSnap()
		self.t.update(NewNode(e))
	}
}

func (self *Pools) Check(e *Sample) {

	return
	ms := self.t.Find(NewNode(e))
	if ms == nil {
		return
	}
	var k1,k2 float64
	//var k3 [2]float64
	//var t byte
	t_ := e.tag>>1
	dn := (e.tag>>1) ^ (e.tag&^2)
	var c int
	ms.Range(func(_n,v interface{})bool{
		c++
		n := _n.(*Node)

		if n.sa.tag>>1 == t_ {
			if (n.sa.val>0) == (dn==1) {
				k1++
			}else{
				k2++
			}
		}else{
			if (n.sa.val>0) != (dn==1) {
				k1++
			}else{
				k2++
			}
		}
		//if !n.sa.check {
		//	return true
		//}
		////if n.sa.tag>>1 != t_ {
		//if n.sa.tag != e.tag {
		//	return true
		//}
		//if (n.sa.val>0) != (dn==1) {
		//	e.check_1 = false
		//	return false
		//}
		//t = n.sa.tag>>1
		//k3[int(t)]++
		//if  n.sa.Check() && (n.sa.tag == e.tag)	{

		//	if (n.sa.val>0) == (dn==1) {
		//		k1++
		//	}else{
		//		k2++
		//	}
		//}
		return true
	})
	//fmt.Println(c,len(self.t.list))
	//if ((k3[0]+k3[1])/float64(len(self.t.list))) > 0.1{
	//	e.check_1 = (k3[int(dn)]>k3[int(dn^1)]) && (k1>k2)
	//}else{
	e.check_1 = (k1>k2)
	//}
	return

}
func (self *Pools)ShowPoolNum()[]float64 {
	return []float64{self.ca.GetLastElement().Diff()}
}

func (self *Pools) Add(e *Sample){

	//for _,e := range es {
	if e.check_1 {

		n1 := int(e.tag&^2)*2
		e.Relval = self.ca.GetLastElement().Middle() - e.begin.Middle()
		if e.DisU() == (e.Relval>0) {

			e.Relval = math.Abs(e.Relval)
		}else{
			e.Relval = -math.Abs(e.Relval)
		}
		//e.Relval -= (math.Abs(e.begin.Diff()) + math.Abs(self.ca.GetLastElement().Diff()))/2

		self.ca.SetCShow(4+n1,e.Relval)
		self.ca.SetCShow(4+n1+1,1)
		if e.DisU() == (e.val>0) {
			self.ca.SetCShow(n1,1)
		}else{
			self.ca.SetCShow(n1+1,1)
		}
		//fmt.Println(e.Relval,e.begin.Diff(),e.diff)
	}

	self.tmp <- e


}

type Pool struct {
	list []*Sample
	m sync.RWMutex
	tmp chan *Sample
}
func NewPool() (p *Pool) {

	p = &Pool{tmp:make(chan *Sample,1)}
	go p.syncRun()
	return p

}
func (self *Pool) syncRun(){
	for{
		self.add(<-self.tmp)
	}
}
func (self *Pool) add(e *Sample){
	e.SetSnap()
	self.m.Lock()
	self.list = append(self.list,e)
	for i,_e := range self.list {
		if (e.xMax() - _e.xMax())/config.Conf.DateUnixV < config.Conf.DateOut{
			self.list = self.list[i:]
			break
		}
	}
	//if (e.xMax() - self.list[0].xMax())/config.Conf.DateUnixV > config.Conf.DateOut{
	//	self.list = self.list[1:]
	//}
	self.m.Unlock()
}
func (self *Pool) readList(h func(int,*Sample )bool){
	self.m.RLock()
	for i,_e := range self.list {
		if !h(i,_e){
			break
		}
	}
	self.m.RUnlock()
}

func (self *Pool) FindSameF(e *Sample) (_e_ *Sample) {
	var Min,d float64
	self.m.RLock()
	for _,_e := range self.list{
		d = e.getDis(_e)
		if (Min>d) || (Min==0) {
			Min = d
			_e_ = _e
		}

	}
	self.m.RUnlock()
	return
}
func (self *Pool) FindSame(e *Sample) *Sample {
	var Min,Minf,d float64
	var MinE,MinEf *Sample
	self.m.RLock()
	for _,_e := range self.list{
		d = e.getDis(_e)
		if (Min>d) || (Min==0) {
			Min = d
			MinE = _e
		}
		d = _e.getDis(e)
		if (Minf>d) || (Minf==0) {
			Minf = d
			MinEf = _e
		}
	}
	self.m.RUnlock()
	if MinE == MinEf {
		return MinE
	}
	return nil
}
