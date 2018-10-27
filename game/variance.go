package game
import(
	"github.com/zaddone/analog/config"
	"math"
)
type Variance struct {

	sum float64
	vals []float64
	num float64
	//val float64

}
func NewVariance () *Variance {
	return &Variance{}
	//	vals:make([]float64,0,config.Conf.JointMax*2)}
	//}
}

func (self *Variance) in(v float64) {
	self.vals = append(self.vals,v)
	self.sum += v
	self.num++
}
func (self *Variance) out() (v float64) {

	v = self.vals[0]
	self.vals = self.vals[1:]
	self.sum -= v
	self.num--
	return v

}
func (self *Variance) ave() float64 {

	return self.sum/self.num

}
func (self *Variance) val() (val float64) {

	ave := self.ave()
	for _,v := range self.vals {
		val += math.Pow((v-ave),2)
	}
	return val/(self.num-1)
	//self.val = val

}
func (self *Variance) balance () {
	for len(self.vals) > config.Conf.JointMax {
		self.out()
	}
}
//func (self *Variance) rooting(val) float64 {
//
//	return math.Sqrt(self.val)
//
//}
