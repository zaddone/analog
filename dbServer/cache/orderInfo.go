package cache
import(
	"github.com/zaddone/analog/config"
	cluster "github.com/zaddone/analog/telecar"
	"math"
)
type Order struct {
	c CacheInterface
	e config.Element
	sa *cluster.Sample
	End bool
}
func NewOrder(c CacheInterface) *Order {

	//c.SetCShow(9,1)
	return &Order{
		c:c,
	}

}
func (self *Order) Start(ea *cluster.Sample){
	self.e = self.c.GetLastElement()
	self.sa = ea
	self.c.SetCShow(9,1)
}

func (self *Order) Check(){
	//e := self.c.GetLastElement()
	//e.Middle() - self.e.Middle()
}

func (self *Order) GetDiff() (df float64) {
	d := self.c.GetLastElement().Middle() - self.e.Middle()
	if (self.sa.GetDiff()>0) == (d>0) {
		return math.Abs(d)
	}else{
		return -math.Abs(d)
	}
}

func (self *Order) CloseCs(){

	d := self.GetDiff()
	if d>0 {
		self.c.SetCShow(10,d)
		self.c.SetCShow(8,d)
		self.Clear()
	}
}
func (self *Order) Close(){
	d := self.GetDiff()
	self.c.SetCShow(10,d)
	if d>0 {
		self.c.SetCShow(8,d)
	}
	self.Clear()
}

func (self *Order) Clear(){
	self.e = nil
	self.sa = nil
	self.End = false
}
//func (self *Order) Merge(f float64) bool {
//	if (self.f>0) != (f>0){
//		return false
//	}
//	nf := self.c.GetLastElement().Middle()+f - self.e.Middle()
//	if (nf>0) != (self.f>0){
//		panic(0)
//	}
//	if math.Abs(nf) > math.Abs(self.f){
//		self.f = nf
//	}
//	return true
//}