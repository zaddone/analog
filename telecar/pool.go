package telecar
import(
	"github.com/zaddone/analog/config"
	"sync"
	//"fmt"
	"time"

)
type tmpSet struct {
	s *set
	k int
	dis float64
	//sync.Mutex
}
type Pool struct {
	ins string
	sets [2][]*set
	chanSam [2]chan *Sample
	count [4]int
	weekDay int
}
func NewPool(ins string) (po *Pool){

	po = &Pool{
		ins:ins,
		sets:[2][]*set{
			make([]*set,0,1000),
			make([]*set,0,1000),
		},
		chanSam:[2]chan *Sample{
			make(chan *Sample,5),
			make(chan *Sample,5)},
	}

	for i,c := range po.chanSam{
		go po.syncAdd(c,i)
	}

	return

}

func (self *Pool) syncAdd(chanSa chan *Sample,i int){
	for{
		select{
		case e:=<-chanSa:
			//fmt.Println(time.Unix(e.XMax(),0),i)
			//self.count[i]++
			//self.add(e,i)
			self.addAndCheck(e,i)
			e.stop<-true
			//fmt.Println(time.Unix(e.XMax(),0),len(e.X),i)
		}
	}
}

func (sp *Pool) Add(e *Sample) {
	n := int(e.tag >> 1)
	//fmt.Println(n)
	w := int(time.Unix(e.XMax(),0).Weekday())
	if sp.weekDay > w {
		sp.setsUpdate()
	}
	sp.weekDay = w
	sp.chanSam[n]<- e
}

func (self *Pool) FindMinSet(e *Sample,n int) (t *tmpSet) {

	le := len(self.sets[n])

	if le == 0 {
		return nil
	}
	t = &tmpSet{}
	var diff float64
	var w sync.WaitGroup
	w.Add(le)
	for _,s := range self.sets[n] {
		//fmt.Println(s.sn)
		diff = s.distance(e)
		if (diff < t.dis) || (t.dis == 0) {
			t.dis = diff
			t.s = s
			//t.k = k
		}

		go func(_s *set){
			for _,e := range _s.samp{
				e.InitSetMap(_s)
			}
			w.Done()
		}(s)
	}
	w.Wait()
	return
}
func (self *Pool)SetSampleCheck(t *tmpSet,e *Sample) {
	if len(t.s.samp) < config.Conf.MinSam{
		return
	}
	for _,_e := range t.s.samp{
		if _e.tag == e.tag {
			if !_e.Long{
				return
			}
		}else{
			if _e.Long {
				return
			}
		}
	}
	e.check = true
}

func (self *Pool) addAndCheck(e *Sample,n int) {

	t := self.FindMinSet(e,n)
	if t == nil {
		self.sets[n] = append(self.sets[n],NewSet(e))
		return
	}
	self.SetSampleCheck(t,e)

	if t.s.check(t.dis) {
		t.s.update(append(t.s.samp,e))
	}else{
		t.s = NewSet(e)
		self.sets[n] = append(self.sets[n],t.s)
	}
	self.Dressing(map[*set]bool{t.s:true},n)

}
func (self *Pool) add(e *Sample,n int) {

	//n := int(e.tag >> 1)
	t := self.FindMinSet(e,n)
	if t == nil {
		//self.SaveSet(NewSet(e))
		self.sets[n] = append(self.sets[n],NewSet(e))
		return
	}
	//tmpSet := make([]*set,0,1000)
	if t.s.check(t.dis) {
		t.s.update(append(t.s.samp,e))
	}else{
		t.s = NewSet(e)
		self.sets[n] = append(self.sets[n],t.s)
	}
	self.Dressing(map[*set]bool{t.s:true},n)

}

func (self *Pool)Dressing(tmp map[*set]bool,n int){

	_tmp := make(map[*set]bool)
	mu := new(sync.Mutex)
	//NewS := make([]*set,0,10)
	var w sync.WaitGroup
	for _,s_ := range self.sets[n] {
		if tmp[s_] {
			continue
		}
		le := len(s_.samp)
		s_.tmpSamp = make([]*Sample,0,le)
		w.Add(le)
		for _,e_ := range s_.samp{
			go func(s *set,e *Sample){
				if e.dis == 0 {
					e.dis = s.distance(e)
				}
				_tmpSet := &tmpSet{s:s,dis:e.dis}
				var diff float64
				for _s,_ := range tmp {
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
					mu.Lock()
					_tmp[s] = true
					//if e.CheckSetMap(_tmpSet.s){
					//	_ns := NewSet(e)
					//	NewS = append(NewS,_ns)
					//	_tmp[_ns] = true
					//	mu.Unlock()
					//	w.Done()
					//	return
					//}else{
						_tmp[_tmpSet.s] = true
					//}
					mu.Unlock()
				}
				_tmpSet.s.Lock()
				_tmpSet.s.tmpSamp = append(_tmpSet.s.tmpSamp,e)
				_tmpSet.s.Unlock()
				w.Done()
			}(s_,e_)
		}
	}
	w.Wait()
	//if len(NewS)>0 {
	//	self.sets[n] = append(self.sets[n],NewS...)
	//}
	le := len(_tmp)
	if le == 0 {
		self.clearSet(n)
		return
	}
	w.Add(le)
	tmpDel := make(chan *set,le)
	for _s,_ := range _tmp {
		go func(s *set){
			if len(s.tmpSamp) == 0{
				s.clear()
				tmpDel <- s
				w.Done()
				return
			}
			s.update(s.tmpSamp)
			w.Done()
		}(_s)
	}
	w.Wait()
	close(tmpDel)
	for _s := range tmpDel {
		delete(_tmp,_s)
	}

	self.Dressing(_tmp,n)

}
func (self *Pool) setsUpdate(){
	for i,sets := range self.sets{
		_sets := make([]*set,0,len(sets))
		for _,s := range sets {
			if s.active >0 {
				_sets = append(_sets,s)
				s.active = 0
			}
		}
		self.sets[i] = _sets
	}
}

func (self *Pool) clearSet(n int){

	sets := make([]*set,0,len(self.sets[n]))
	var w sync.WaitGroup
	for _,s := range self.sets[n] {
		if len(s.samp) == 0 {
			continue
		}
		w.Add(1)
		go func (_s *set){
			_s.Sort()
			//_s.update(_s.samp)
			w.Done()
		}(s)
		sets = append(sets,s)
	}
	self.sets[n] = sets
	w.Wait()

}
func (self *Pool) ShowPoolNum() (count [2]int) {

	count[0] = len(self.sets[0])
	count[1] = len(self.sets[1])
	return

}
