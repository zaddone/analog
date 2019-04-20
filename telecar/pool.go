package telecar
import(
	"github.com/zaddone/analog/config"
	"sync"
	//"fmt"
	//"time"

)
//const(
//	TimeOut int64 = 302400
//)
type tmpSet struct {
	s *set
	k int
	dis float64
	//sync.Mutex
}

type CacheInter interface {
	SetCShow(int,int)
	HandMap([]byte,func(interface{},byte))
	HandMapBlack([]byte,func(interface{},byte)bool)
	FindSample(sa *Sample) *Sample
	InsName()string
}

type OrderMsg struct {
	sa *Sample
	ca CacheInter
}

type Pool struct {
	ins string
	//sets [2][]*set
	//chanSam [2]chan *Sample
	sets [2][]*set
	chanSam [2]chan *Sample
	//mu sync.RWMutex
	mu [2]sync.RWMutex
	count [4]int
	_ca  CacheInter
	//weekDay int
}
func NewPool(ins string,c CacheInter) (po *Pool){
	po = &Pool{
		ins:ins,
		sets:[2][]*set{
			make([]*set,0,1000),
			make([]*set,0,1000),
		},
		chanSam:[2]chan *Sample{
			make(chan *Sample,1),
			make(chan *Sample,1),
		},
		_ca:c,
	}
	for i,sam := range po.chanSam {
		go po.syncAdd(sam,i)
	}
	return

}

func (self *Pool) syncAdd(chanSa chan *Sample,i int){
	for{
		e:=<-chanSa
		//fmt.Println(time.Unix(e.XMax(),0),i)
		//self.count[i]++
		//self.mu[i].Lock()
		self.add(e,i)
		//self.mu[i].Unlock()
		//self.addAndCheck(e,i)
		//fmt.Println(time.Unix(e.XMax(),0),len(e.X),i)
		e.stop<-true
	}
}

func (sp *Pool) Add(e *Sample) {
	sp.chanSam[int(e.GetTag()>>1)]<- e
	//sp.chanSam <- e
}

func (self *Pool) FindMinSet(e *Sample,n int) (t *tmpSet) {

	le := len(self.sets[n])
	if le == 0 {
		return nil
	}
	t = &tmpSet{}
	var diff float64
	for _,s := range self.sets[n] {
		//fmt.Println(s.sn)
		s.tmpSamp = nil
		diff = s.distance(e)
		if (diff < t.dis) || (t.dis == 0) {
			t.dis = diff
			t.s = s
			//t.k = k
		}
		//go func(_s *set){
		//	for _,e := range _s.samp{
		//		e.InitSetMap(_s)
		//	}
		//	w.Done()
		//}(s)
	}
	return

}

func (self *Pool) CheckSample(e *Sample) bool{
	n := int(e.GetTag()>>1)
	self.mu[n].RLock()
	defer self.mu[n].RUnlock()
	t := self.FindMinSet(e,n)
	if t == nil {
		return false
	}
	if !t.s.checkSample(e){
		return false
	}
	t.s.SetTMap(e)
	return true
	//self._ca.HandMap(e.caMap[1],func(_c interface{},t byte){
	//	c := _c.(CacheInter)
	//})
	//for _,cm := range e.caMap{
	//	self._ca.
	//}



}


func (self *Pool) add(e *Sample,n int) {

	self.mu[n].RLock()
	t := self.FindMinSet(e,n)
	self.mu[n].RUnlock()


	if t == nil {
		//self.SaveSet(NewSet(e))
		e.SetTestMap(e.caMap[2])
		self.mu[n].Lock()
		self.sets[n] = append(self.sets[n],NewSet(e))
		self.mu[n].Unlock()
		return
	}

	if t.s.checkSample(e){
		t.s.SetTMap(e)
	}else{
		e.SetTestMap(e.caMap[2])
	}

	self.mu[n].Lock()
	if t.s.check(t.dis) {
		t.s.update(append(t.s.samp,e))
		t.s.active = e.XMax()
	}else{
		t.s = NewSet(e)
		self.sets[n] = append(self.sets[n],t.s)
	}
	self.Dressing_only(true,map[*set]bool{t.s:true},n,e.XMax())
	self.mu[n].Unlock()

}

func (self *Pool)Dressing_only(init bool,tmp map[*set]bool,n int,d int64){

	for _s,_ := range tmp {
		if _s.tmpSamp != nil {
			_s.tmpSamp = nil
		}
	}
	_tmp:= make(map[*set]bool)
	for _,s := range self.sets[n] {
		if s.tmpSamp != nil {
			s.tmpSamp = nil
		}
		for _,e := range s.samp{
			if init {
				e.InitSetMap(s)
			}
			if e.dis == 0 {
				e.dis = s.distance(e)
			}
			_tmpSet := &tmpSet{s:s,dis:e.dis}
			var diff float64
			for _s,_ := range tmp {
				if s == _s {
					continue
				}
				if e.CheckSetMap(_s){
					continue
				}
				diff = _s.distance(e)
				if _tmpSet.dis > diff {
					_tmpSet.dis = diff
					_tmpSet.s = _s
				}
			}
			if _tmpSet.s != s {
				_tmp[s] = true
				_tmp[_tmpSet.s] = true
			}
			_tmpSet.s.tmpSamp = append(_tmpSet.s.tmpSamp,e)
		}
	}
	le := len(_tmp)
	if le == 0 {
		self.clearSet(d,n)
		return
	}
	for _s,_ := range _tmp {
		if len(_s.tmpSamp) == 0{
			_s.clear()
			delete(_tmp,_s)
			continue
		}
		_s.active = d
		_s.update(SortSamples(_s.tmpSamp))
	}
	self.Dressing_only(false,_tmp,n,d)

}

func (self *Pool)Dressing(init bool,mu *sync.Mutex,w *sync.WaitGroup,tmp map[*set]bool,n int,d int64){

	_tmp:= make(map[*set]bool)
	//var NewS []*set
	for _,s_ := range self.sets[n] {
		//le := len(s_.samp)
		if s_.tmpSamp != nil {
			//s_.Lock()
			s_.tmpSamp = nil
			//s_.Unlock()
		}
		w.Add(len(s_.samp))
		for _,e_ := range s_.samp{
			if init {
				e_.InitSetMap(s_)
			}
			go func(s *set,e *Sample){
				if e.dis == 0 {
					e.dis = s.distance(e)
				}
				_tmpSet := &tmpSet{s:s,dis:e.dis}
				var diff float64
				for _s,_ := range tmp {
					if s == _s {
						continue
					}
					if e.CheckSetMap(_s){
						continue
					}
					diff = _s.distance(e)
					if _tmpSet.dis > diff {
						_tmpSet.dis = diff
						_tmpSet.s = _s
					}
				}
				if _tmpSet.s != s {
					_tmp[s] = true
					_tmp[_tmpSet.s] = true
				}
				_tmpSet.s.Lock()
				_tmpSet.s.tmpSamp = append(_tmpSet.s.tmpSamp,e)
				_tmpSet.s.Unlock()
				w.Done()
			}(s_,e_)
		}
	}
	w.Wait()

	le := len(_tmp)
	if le == 0 {
		self.clearSet(d,n)
		return
	}
	for _s,_ := range _tmp {
		if len(_s.tmpSamp) == 0{
			_s.clear()
			delete(_tmp,_s)
			continue
		}
		_s.active = d
		w.Add(1)
		go func(s *set){
			s.update(SortSamples(s.tmpSamp))
			w.Done()
		}(_s)
	}
	w.Wait()
	self.Dressing(false,mu,w,_tmp,n,d)

}

func (self *Pool) clearSet(d int64,n int){

	var sort func([]*set,int)
	sort = func(_s []*set,i int){
		if i == 0 {
			return
		}
		I := i -1
		if (_s[I].active <= _s[i].active) {
			return
		}
		_s[I],_s[i] = _s[i],_s[I]
		sort(_s,I)
	}
	le := len(self.sets)
	sets := make([]*set,0,le)
	var i int
	for _,s := range self.sets[n] {
		if (len(s.samp) == 0){
			continue
		}
		sets = append(sets,s)
		sort(sets,i)
		i++
	}

	if ((d - sets[0].active) > config.Conf.DateOut){
		sets = sets[1:]
	}
	self.sets[n] = sets

}

func (self *Pool) ShowPoolNum() (count [2]int) {

	count[0] = len(self.sets[0])
	count[1] = len(self.sets[1])
	return

}

//func (self *Pool) GetSetMap(e *Sample) (m []byte) {
//
//	//if e.tag &^ 2 == 1 {
//	//	return nil
//	//}
//
//	_n := int(e.tag >> 1)
//	self.mu[_n].RLock()
//	defer self.mu[_n].RUnlock()
//	ts := self.FindMinSet(e,_n)
//	if ts == nil {
//		return nil
//	}
//	if len(ts.s.samp) < config.Conf.MinSam {
//		return nil
//	}
//	for _,_e := range ts.s.samp {
//		//if _e.caMap == nil {
//		//	continue
//		//	//return nil
//		//}
//		if _e.tag == e.tag {
//			if !_e.Long {
//				return nil
//			}
//			if m == nil {
//				m = _e.caMap
//			}else{
//				for i,n := range _e.caMap {
//					m[i] |= n
//				}
//			}
//		}else{
//			if _e.Long {
//				return nil
//			}
//			if m == nil {
//				m = _e.caMap
//			}else{
//				for i,n := range _e.caMap {
//					m[i] |= ^n
//				}
//			}
//		}
//	}
//	e.check = true
//	return m
//
//}
