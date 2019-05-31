package pool
import(
	//"fmt"
	"github.com/zaddone/analog/config"
)
type Node struct {
	top *Node
	min float64
	link map[*Node]bool
	sa *Sample
}
func NewNode(e *Sample) *Node {
	return &Node{
		sa:e,
		link:map[*Node]bool{},
	}
}

func (self *Node)read(m map[*Node]bool, h func(n *Node)){
	for _n,_ := range self.link {
		if _n.top != self {
			panic(0)
		}
		if !m[_n] {
			if h != nil {
				h(_n)
			}
			m[_n] = true
			_n.read(m,h)
		}
	}
	if self.top == nil ||
	m[self.top] {
		return
	}
	m[self.top] = true
	self.top.read(m,h)
}

type Tree struct {
	//top *Node
	list []*Node
}


func (self *Tree) Find(n *Node) (ms map[*Node]bool) {
	var d float64
	for _,_n := range self.list{
		d = _n.sa.GetDis(n.sa)
		if d < n.min || n.min == 0  {
			n.min =d
			n.top = _n
		}
	}
	if d == 0 {
		return nil
	}
	ms = map[*Node]bool{n.top:true}
	n.top.read(ms,nil)
	//fmt.Println(len(ms),len(self.list))
	return ms
}
//func (self *Tree) read(h func(*Node)){
//	self.top.read(map[*Node]bool{self.top:true},h)
//}

func (self *Tree) update(n *Node){

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
	self.Add(n)
	return

}
func (self *Tree) Add(n *Node){

	var d float64
	for _,_n := range self.list {
		d = n.sa.GetDis(_n.sa)
		if n.min > d || (n.min ==0) {
			n.min = d
			n.top = _n
		}
		d = _n.sa.GetDis(n.sa)
		if _n.min > d {
			//if !_n.top.link[_n] {
			//	panic(0)
			//}
			if _n.top != nil {
				delete(_n.top.link,_n)
			}
			_n.min = d
			_n.top = n
			n.link[_n] = true
		}
	}
	self.list = append(self.list,n)
	//self.read(func(_n *Node){
	//	d = n.sa.GetDis(_n.sa)
	//	if n.mim > d {
	//		n.min = d
	//		n.top = _n
	//	}
	//	d = _n.sa.GetDis(n.sa)
	//	if _n.min > d {
	//		//if !_n.top.link[_n] {
	//		//	panic(0)
	//		//}
	//		delete(_n.top.link,_n)
	//		_n.min = d
	//		_n.top = n
	//		n.link[_n] = true
	//	}
	//})
	if n.top != nil {
		n.top.link[n] = true
	}
	//self.top = n

}
