package snap
import(
	"fmt"
	"time"
	//"math"
	"github.com/zaddone/analog/fitting"
	"github.com/zaddone/analog/config"
)

type Snap struct {

	BeginX float64
	LengthX float64
	LengthY float64
	//Wei []float64
	Matrix []float64

}

func NewSnap(es []config.Element) (sn *Snap) {

	sn = &Snap{}
	var X,Y []float64
	var x,y,yMin,yMax float64
	for _,e := range es {
		e.Read(func(e_ config.Element){
			x =float64(e_.DateTime())
			y = e_.Middle()
			if (yMin == 0) || (y < yMin){
				yMin = y
			}else if (yMax < y) {
				yMax = y
			}
			Y = append(Y,y)
			X = append(X,x)
		})
	}

	sn.LengthY = yMax - yMin
	le := len(X) -1
	begin:=X[0]
	sn.BeginX = X[le]
	sn.LengthX = sn.BeginX - begin
	for i,x := range X{
		Y[i]  =  (Y[i]  - yMin)/sn.LengthY
		X[i]  =  (x - begin)/sn.LengthX
	}
	Wei := CurveFittingMax(X,Y,nil,0)
	if  len(Wei) == 0 {
		return nil
	}
	sn.CreateMatrix(Wei)

	return sn

}
func (self *Snap) CreateMatrix(w []float64) {

	var y,x,t,_x float64
	self.Matrix = make([]float64,int(self.LengthX)+1)
	for t=0; t <= self.LengthX; t++{
		x  = t/self.LengthX
		y = w[0] +w[1]*x
		_x = x
		for _i :=2 ;_i < len(w);_i++ {
			_x *= x
			y += w[_i]*_x
		}
		y *=self.LengthY
		self.Matrix[int(t)] = y
	}


}

func CurveFittingMax(X,Y,W []float64,vs float64) []float64 {
	wlen := len(W)
	var w []float64
	if wlen <2 {
		w = make([]float64,2)
	}else{
		w = make([]float64,len(W)+1)
	}
	if !fitting.GetCurveFittingWeight(X,Y,w) {
		return W
	}
	v := fitting.CheckCurveFitting(X,Y,w)
	if vs == 0 || v < vs {
		return CurveFittingMax(X,Y,w,v)
	}
	return W
}

func (self *Snap) SaveTemplate(InsName string) {
	fmt.Println(time.Unix(int64(self.BeginX),0),InsName,self.LengthX,self.LengthY)
	return
	//path := filepath.Join(
	//	config.Conf.LogPath,
	//	InsName,
	//	time.Unix(int64(self.BeginX),0).Format("200601"),
	//)
	//_,err := os.Stat(path)
	//if err != nil {
	//	err = os.MkdirAll(path,0700)
	//	if err != nil {
	//		panic(err)
	//	}
	//}
	//f,err := os.OpenFile(
	//	filepath.Join(
	//		path,
	//		fmt.Sprintf("%d_%d",
	//			int(self.LengthX),
	//			int(self.BeginX),
	//		),
	//	),
	//	os.O_APPEND|os.O_CREATE|os.O_RDWR,
	//	0700,
	//)
	//if err != nil {
	//	panic(err)
	//}
	//self.readMatrix(func(x,y int){
	//	f.WriteString(fmt.Sprintf("%d 0 %d\n",x,y))
	//})

	//fmt.Println(f.Name())
	//f.Close()

}
//func (self *Snap) readMatrix(hand func(x,y int)){
//
//	for _i,ma := range self.Matrix {
//		for _j,m := range ma {
//			var i uint = 0
//			for ;i<64;i++{
//				if m == (m|(1<<i)) {
//					hand(_i,_j*64+int(i))
//				}
//			}
//		}
//	}
//
//}
