package cache
import(
//	"github.com/zaddone/analog/fitting"
//	"github.com/zaddone/analog/config"
//	"fmt"
//	"os"
//	"time"
//	"path/filepath"
//	"math"
)
//type Snapshot struct {
//
//	BeginX float64
//	LengthX float64
//
//	//BeginY float64
//	LengthY float64
//
//	le *level
//
//	X []float64
//	Y []float64
//	//Z []float64
//
//	Zl []float64
//	Zh []float64
//
//	WeiLen int
//	Matrix [][]int64
//
//}
//func NewSnapshot(le *level) (sn *Snapshot) {
//	sn =  &Snapshot{le:le}
//	//sn.BeginY = yMin
//	var yMin,yMax,v,z,zl,zh float64
//	var _e element
//	in := le.ca.Ins.Integer()
//	le.readDown(func(l *level){
//		for _,e := range l.list {
//			e.Read(func(f interface{}){
//				if f == nil {
//					panic(0)
//				}
//				_e = f.(element)
//				v  = in*_e.Middle()
//				z  = in*math.Abs(_e.Diff())/2
//				zl = v - z
//				zh = v + z
//				//fmt.Println(v,z)
//				if (yMin == 0) || (zl < yMin){
//					yMin = zl
//				}else if (yMax < zh) {
//					yMax = zh
//				}
//				sn.X = append(sn.X,float64(_e.DateTime()))
//				sn.Y = append(sn.Y,v)
//				//sn.Z = append(sn.Z,z)
//
//				sn.Zl = append (sn.Zl,zl)
//				sn.Zh = append (sn.Zh,zh)
//			})
//		}
//	})
//
//	l := len(sn.X)-1
//	sn.LengthY = yMax - yMin
//
//	begin:=sn.X[0]
//	//begin:=sn.X[l]
//	sn.BeginX = sn.X[l]
//	sn.LengthX = sn.BeginX - sn.X[0]
//
//	for i:=0;i<=l;i++{
//		sn.Y[i]  =  (sn.Y[i] - yMin)/sn.LengthY
//		sn.Zl[i] =  (sn.Zl[i] - yMin)/sn.LengthY
//		sn.Zh[i] =  (sn.Zh[i] - yMin)/sn.LengthY
//		//sn.X[i]  =  (begin - sn.X[i])/sn.LengthX
//		sn.X[i]  =  (sn.X[i] - begin)/sn.LengthX
//	}
//	for i:=2;i<9;i++{
//		sn.loadTemplate(i)
//		if sn.TemplateCheck() {
//			sn.WeiLen = i
//			fmt.Println(sn.le.ca.Ins.Name,"weilen",i)
//			return sn
//		}
//	}
//	fmt.Println(sn.le.ca.Ins.Name,"weilen not")
//	return nil
//
//}
//func (self *Snapshot) loadTemplate( weiLen int) error {
//
//	wh := make([]float64,weiLen)
//	wl := make([]float64,weiLen)
//	if !fitting.GetCurveFittingWeight(self.X,self.Zh,wh) {
//		return fmt.Errorf("height fitting err")
//	}
//	if !fitting.GetCurveFittingWeight(self.X,self.Zl,wl) {
//		return fmt.Errorf("low fitting err")
//	}
//
//
//	var _yl,_yh int
//	var yl,yh float64
//	var _x,x,t float64
//	self.Matrix = make([][]int64,int(self.LengthX)+1)
//	var i int = 0
//	for t=0;t<=self.LengthX;t++{
//		x = t/self.LengthX
//		yl = wl[0] +wl[1]*x
//		yh = wh[0] +wh[1]*x
//		_x = x
//		for _i :=2 ;_i < len(wl);_i++ {
//			_x *=x
//			yl += wl[_i]*_x
//			yh += wh[_i]*_x
//		}
//		yl *=self.LengthY
//		yh *=self.LengthY
//
//		_yl = int(fitting.Rounding(math.Min(yl,yh)))
//		_yh = int(fitting.Rounding(math.Max(yl,yh)))
//		leh:=_yh/64 +1
//		if (leh < 0) || leh>50 {
//			return fmt.Errorf("leh = %d",leh)
//		}
//		row := make([]int64,leh)
//		for _j :=_yl;_j<= _yh;_j++{
//			if _j <0 {
//				continue
//			}
//			row[_j/64] |= (int64(1) << uint(_j%64))
//		}
//		//fmt.Println(row,_yl,_yh,yl,yh,_x)
//		self.Matrix[i] = row
//		i++
//	}
//	//self.SaveTemplate()
//
//	//self.ShowTemplate()
//	return nil
//}
//
//func (self *Snapshot) SaveTemplate() {
//	path := filepath.Join(
//		config.Conf.LogPath,
//		self.le.ca.Ins.Name,
//		time.Unix(int64(self.BeginX),0).Format("200601"),
//	)
//	_,err := os.Stat(path)
//	if err != nil {
//		err = os.MkdirAll(path,0700)
//		if err != nil {
//			panic(err)
//		}
//	}
//	f,err := os.OpenFile(
//		filepath.Join(
//			path,
//			fmt.Sprintf("%d_%d_%t",
//				self.le.tag,
//				int(self.BeginX),
//				self.le.dis>0,
//			),
//		),
//		os.O_APPEND|os.O_CREATE|os.O_RDWR|os.O_SYNC,
//		0700,
//	)
//	if err != nil {
//		panic(err)
//	}
//	self.readMatrix(func(x,y int){
//		f.WriteString(fmt.Sprintf("%d 0 %d\n",x,y))
//	})
//
//	fmt.Println(f.Name())
//	f.Close()
//
//}
//func (self *Snapshot) readMatrix(hand func(x,y int)){
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
//func (self *Snapshot) ShowTemplate() {
//	for _,ma := range self.Matrix {
//		str :=""
//		for _,m := range ma {
//			var i uint = 0
//			for ;i<64;i++{
//				if m == (m|(1<<i)) {
//					str+="1"
//				}else{
//					str+="0"
//				}
//			}
//		}
//		fmt.Println(str)
//	}
//}
//
//func (self *Snapshot) TemplateCheck() bool {
//	//w := make([]float64,weiLen)
//	//if !fitting.GetCurveFittingWeight(self.X,self.Y,len(self.X),w) {
//	//	return fmt.Errorf("fitting err")
//	//}
//
//	var _y,j,_x int
//	var t int64
//	var row []int64
//	le := len(self.Matrix)
//	for i,x := range self.X{
//		_x = int(fitting.Rounding( x*self.LengthX ))
//		_y = int(fitting.Rounding( self.Y[i]*self.LengthY ))
//		//fmt.Println(_x,_y)
//		j = int(_y/64)
//		if _x >= le || _x < 0 {
//			fmt.Println(x,_x,le)
//			//panic(0)
//			return false
//		}
//		row = self.Matrix[_x]
//		if j >= len(row) {
//			return false
//		}
//		t = row[j]
//		if t != (t | (int64(1) << uint(_y%64))) {
//			return false
//		}
//	}
//	return true
//
//
//}
