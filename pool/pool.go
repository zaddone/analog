package pool
import(
	"github.com/zaddone/analog/config"
	"sync"
	//"os"
	//"fmt"
	//"time"
)

type CacheInter interface {
	SetCShow(int,float64)
	InsName()string
	GetCacheLen() int
}
type Pools struct{
	p [2]*Pool
	ca  CacheInter
}
func NewPools(ca CacheInter) *Pools {

	return &Pools{
		ca:ca,
		p:[2]*Pool{NewPool(),NewPool()},
	}

}

func (self *Pools) Check(e *Sample) {


	if !e.check_1{
		return
	}
	n := int(e.tag>>1)
	//var _k1,_k2,_k3,_k4 float64
	var _k3,_k4 float64
	dis:=e.DisU()
	self.p[n].readList(func(i int,_e *Sample)bool{
		if (_e.CheckChild() >0) == dis {
			_k3++
		}else{
			_k4++
		}

		//if _e.Check() {
		//	if (_e.tag == e.tag) {
		//		if f {
		//			_k1++
		//		}else{
		//			_k2++
		//		}
		//	}

		//}
		return true
	})
	e.check_1 = (_k3>_k4)
		//e.check_1 = ((_k1/_k2)> 1.3)
	return

	//__e := self.p[n^1].FindSame(e)
	//if __e == nil {
	//	return
	//}
	//__f,_ :=  __e.CheckChild()
	//if (__f>0)  == (_f>0){
	//	return
	//}
	////if (e.tag>>1) == (_e.tag>>1){
	//e.same = _e
	//}
	//self.ca.SetCShow(0,1)
	//s := &set{}
	//s.update([]*Sample{e,_e})
	//s.SaveImg()



	//f ,ok := _e.CheckChild()
	//if !ok{
	//	e.SetCheck(false)
	////	self.ca.SetCShow(8,1)
	//	return
	//}
	//if f>0 != e.DisU() {
	//	e.SetCheck(false)
	////	self.ca.SetCShow(9,1)
	//	return
	//}
	//self.ca.SetCShow(5,1)

}
func (self *Pools)ShowPoolNum()[]float64 {
	//return nil
	for _,p := range self.p {
		//I :=i*4+2
		p.m.RLock()
		for _,e := range p.list {
			if !e.check_1 {
				continue
			}
			f := e.CheckChild()
			n1 := int(e.tag&^2)*2
			if ((f>0) == e.DisU()) {
				self.ca.SetCShow(n1,1)
			}else{
				self.ca.SetCShow(n1+1,1)
			}

			//f,ok := e.CheckChild()
			//if !ok{
			//	self.ca.SetCShow(1,1)
			//}else{
			//	if (f>0) == e.DisU(){
			//		self.ca.SetCShow(I+int(e.tag&^2)*2+1,1)
			//	}else{
			//		self.ca.SetCShow(I+int(e.tag&^2)*2,1)
			//	}
			//}
			//}
		}

		//self.ca.SetCShow(0,float64(len(p.list)))
		p.m.RUnlock()
	}
	return nil
}
func (self *Pools) Add(es []*Sample){
	for _,e := range es {
		self.p[int(e.tag>>1)].tmp <- e
		//self.p[0].tmp <- e
	}
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
