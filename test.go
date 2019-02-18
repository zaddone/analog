package main
import(
	"fmt"
	"math/rand"
	"time"
)
type no interface{
	GetVal() int64
}

type tree struct {
	node no
	big *tree
	small *tree
	top *tree
}
func NewTree(n no) *tree {
	return &tree{
		node:n,
	}
}
func (self *tree) Copy(t *tree) {
	self.node = t.node
	self.big = t.big
	self.small = t.small
	if self.big != nil{
		self.big.top = self
	}
	if self.small != nil {
		self.small.top = self
	}
}
func (self *tree) Add (n no) {
	if self.node.GetVal() >= n.GetVal() {
		if self.small == nil {
			self.small = NewTree(n)
			self.small.top = self
		}else{
			self.small.Add(n)
		}
	}else{
		if self.big == nil {
			self.big = NewTree(n)
			self.big.top = self
		}else{
			self.big.Add(n)
		}
	}
}
func (self *tree) PopSmall() (n no) {

	if self.small != nil {
		return self.small.PopSmall()
	}
	n = self.node
	if self.top != nil {
		if self.big != nil {
			self.big.top = self.top
		}
		self.top.small = self.big
	}else{

		self.Copy(self.big)
	}
	return

}
type test struct{
	val int64
}
func (self *test) GetVal()int64 {
	return self.val
}
func main(){
	rand.Seed(time.Now().UnixNano())
	n:=rand.Int63n(100)
	t := NewTree(&test{0})
	fmt.Println(n)
	for i:=0;i<10;i++{
		n:=rand.Int63n(100)
		t.Add(&test{n})
		fmt.Println(n)
	}
	fmt.Println("out")
	for i:=0;i<10;i++{
		n := t.PopSmall()
		fmt.Println(n.GetVal())
	}
}
