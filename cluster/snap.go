package cluster
import(
	"fmt"
	"time"
	"github.com/zaddone/analog/fitting"
	"github.com/zaddone/analog/config"

)
type Snap struct {

	BeginX float64
	LengthX float64
	LengthY float64
	Wei []float64

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
	sn.Wei = CurveFittingMax(X,Y,nil,0)
	//fmt.Println("Wei",len(sn.Wei))
	if  len(sn.Wei) == 0 {
		return nil
	}

	//sn.CreateMatrix(Wei)

	return sn

}

func (self *Snap) GetWeiY(x float64) (y float64) {
	var _x float64 = x
	y = self.Wei[0] + self.Wei[1]*_x
	for _,w := range self.Wei[2:] {
		//_x = x/self.LengthX
		_x *= x
		y += _x*w
		//y += math.Pow(x,float64(i)) * w
	}
	return
}
func CurveFittingMax(X,Y,W []float64,vs float64) []float64 {
	wlen := len(W)
	var w []float64
	if wlen <2 {
		w = make([]float64,2)
	}else{
		if wlen>9 {
			return W
		}
		w = make([]float64,wlen+1)
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
