package cache
import(
	//"fmt"
	"github.com/zaddone/analog/config"
)


type bNode struct {
	li []config.Element
	middle float64
	diff float64
	duration int64
}
func NewbNode(li ...config.Element) (n *bNode) {
	le := len(li)
	if le == 0 {
		return nil
	}
	endLi := li[le-1]
	beginLi := li[0]
	n = &bNode{
		li:li,
		diff:endLi.Middle() - beginLi.Middle(),
		duration:endLi.DateTime() + endLi.Duration() - beginLi.DateTime(),
	}
	for _,e := range li {
		n.middle+=e.Middle()
	}
	n.middle /= float64(le)
	return
}
func (self *bNode) Duration() int64 {
	return self.duration
}
func (self *bNode) Readf(hand func(config.Element) bool ) bool {

	for i:= len(self.li)-1;i>=0;i--{
	//for _,e := range self.li {
		if !self.li[i].Readf(hand) {
			return false
		}
	}
	return true
}
func (self *bNode) Read(hand func(config.Element) bool ) bool{
	for _,e := range self.li {
		if !e.Read(hand){
			return false
		}
	}
	return true
}
func (self *bNode) DateTime() int64{
	le := len(self.li)
	if le == 0 {
		return 0
	}
	return self.li[0].DateTime()
}
func (self *bNode) Middle() float64{
	return self.middle
}
func (self *bNode) Diff() float64{
	return self.diff
}

type eNode struct {
	//li []element
	middle float64
	diff float64
	dateTime int64
	duration int64
}
func NewNode(li ...config.Element) (n *eNode) {
	le:=len(li)
	endLi:= li[le-1]
	beginLi:= li[0]
	n = &eNode{
		//li:li,
		dateTime:beginLi.DateTime(),
		duration:endLi.DateTime()+endLi.Duration() - beginLi.DateTime(),
		diff:endLi.Middle() - beginLi.Middle(),
	}
	for _,e := range li {
		n.middle+=e.Middle()
	}
	n.middle /= float64(le)
	//fmt.Printf("%.5f %.5f %d \r\n",n.Diff(),n.Middle(),n.DateTime())
	return n
}
func (self *eNode) Duration() int64 {
	return self.duration
}
func (self *eNode) Readf(hand func(config.Element) bool) bool {
	return hand(self)
}
func (self *eNode) Read(hand func(config.Element) bool ) bool{
	return hand(self)
	//for _,e := range self.li {
	//	e.Read(hand)
	//}
}

func (self *eNode) DateTime() int64{
	return self.dateTime
	//le := len(self.li)
	//if le == 0 {
	//	return 0
	//}
	//return self.li[0].DateTime()
}
func (self *eNode) Middle() float64{
	return self.middle
}
func (self *eNode) Diff() float64{
	return self.diff
}
