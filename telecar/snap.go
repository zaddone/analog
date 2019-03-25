package telecar
import(
	"github.com/zaddone/analog/fitting"
	"github.com/zaddone/analog/config"
)

type snap struct {
	LengthX float64
	LengthY float64
	Wei []float64
}
func (self *snap) GetWeiY(x float64) (y float64) {
	var _x float64 = x
	//if len(self.Wei) == 0 {
	//	panic(0)
	//}
	y = self.Wei[0] + self.Wei[1]*_x
	for _,w := range self.Wei[2:] {
		_x *= x
		y += _x*w
	}
	return
}
func CurveFitting(X,Y []float64) (w []float64) {
	w = make([]float64,config.Conf.WeiMin)
	if !fitting.GetCurveFittingWeight(X,Y,w) {
		return nil
	}
	return
}
