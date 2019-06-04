package pool
import(
	//"fmt"
	"github.com/zaddone/analog/config"
	"sync"
)
type Node struct {
	top *Node
	min float64
	link *sync.Map
	sa *Sample
}
func NewNode(e *Sample) *Node {
	return &Node{
		sa:e,
		link:&sync.Map{},
	}
}


func (self *Node)read(m *sync.Map, h func(n *Node)){
	self.link.Range(func(n interface{}, v interface{})bool{
		_n := n.(*Node)
		if _,ok := m.Load(_n);!ok{
			if h != nil {
				h(_n)
			}
			m.Store(_n,true)
			_n.read(m,h)
		}

		return true
	})

	if self.top == nil{
		return
	}
	if _,ok := m.Load(self.top);ok{
	//m[self.top] {
		return
	}
	m.Store(self.top,true)
	self.top.read(m,h)
}

type Tree struct {
	//top *Node
	list []*Node
	sync.RWMutex
}

func (self *Tree) Find(n *Node) (ms *sync.Map) {
	var d float64
	self.RLock()
	for _,_n := range self.list{
		d = _n.sa.GetDis(n.sa)
		if d < n.min || n.min == 0  {
			n.min =d
			n.top = _n
		}
	}
	self.RUnlock()
	if d == 0 {
		return nil
	}
	ms = &sync.Map{}
	ms.Store(n.top,true)
	n.top.read(ms,nil)
	//fmt.Println(len(ms),len(self.list))
	return ms
}
//func (self *Tree) read(h func(*Node)){
//	self.top.read(map[*Node]bool{self.top:true},h)
//}

func (self *Tree) update(n *Node){

	self.Lock()
	for _i,_n := range self.list {
		if (n.sa.xMax() - _n.sa.xMax())/config.Conf.DateUnixV < config.Conf.DateOut{
			self.list = self.list[_i:]
			break
		//}else{
		//	if _n.top != nil {
		//		delete(_n.top.link,_n)
		//		_n.top = nil
		//		_n.min = 0
		//	}
		}
	}
	self.Unlock()
	self.Add(n)
	return

}
func (self *Tree) Add(n *Node){

	var d float64
	self.RLock()
	var minN *Node
	var minD float64
	for _,_n := range self.list {
		d = n.sa.GetDis(_n.sa)
		if (n.min > d) || (n.min ==0) {
			n.min = d
			n.top = _n
		}
		d = _n.sa.GetDis(n.sa)
		if (minD>d) || (minD == 0) {
			minD = d
			minN = _n
		}
		if (_n.min > d) || (_n.min==0) {
			//if !_n.top.link[_n] {
			//	panic(0)
			//}
			if _n.top != nil {
				_n.top.link.Delete(_n)
				//delete(_n.top.link,_n)
			}
			_n.min = d
			_n.top = n

			n.link.Store(_n,true)

		}
	}
	self.RUnlock()
	self.Lock()
	self.list = append(self.list,n)
	self.Unlock()
	if n.top != nil {
		//n.top.link.Range(func(k,v interface{})bool {
		//	if k.(*Node).top != n.top{
		//		n.top.link.Delete(k)
		//	}
		//	return true
		//})
		n.top.link.Store(n,true)
	}
	if minN != nil {
		minN.min = minD
		n.link.Store(minN,true)
	}

}
