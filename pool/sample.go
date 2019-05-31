package pool
import(
	"sync"
	"math"
	"github.com/zaddone/analog/config"
	"github.com/zaddone/analog/fitting"
	"os"
	"image"
	"image/color"
	"image/png"
	"math/rand"
	"time"
	"fmt"
)

type snap struct {
	lengthX float64
	lengthY float64
	wei []float64
}
func (self *snap) getWeiY(x float64) (y float64) {
	var _x float64 = x
	y = self.wei[0] + self.wei[1]*_x
	for _,w := range self.wei[2:] {
		_x *= x
		y += _x*w
	}
	return
}
func curveFitting(X,Y []float64) (w []float64) {
	w = make([]float64,config.Conf.WeiMin)
	if !fitting.GetCurveFittingWeight(X,Y,w) {
		return nil
	}
	return
}
type Sample struct {
	yMin int64
	yMax int64
	x []int64
	y []int64

	caMap [4][]byte
	m sync.RWMutex

	eleLast config.Element
	begin config.Element

	tag byte
	long bool
	check bool
	check_1 bool
	diff float64
	//last *Sample
	//next *Sample
	par *Sample
	child *Sample

	same *Sample

	sn snap
	tmp float64
	val float64
	Relval float64
}

func NewSample(eles []config.Element,le int) (sa *Sample) {
	sa = &Sample{
		eleLast:eles[len(eles)-1],
	}
	if le !=0 {
		sa.caMap = [4][]byte{
			make([]byte,le),
			make([]byte,le),
			make([]byte,le),
			make([]byte,le),
		}
	}
	var y int64
	var minE,maxE config.Element
	for _,ele := range eles {
		sa.diff += math.Abs(ele.Diff())
		ele.Read(func(e config.Element) bool {
			y = int64(e.Middle())
			if (sa.yMin==0) || (y < sa.yMin) {
				sa.yMin = y
				minE = e
				//sa.YMinEle = e
			}else if (sa.yMax < y) {
				sa.yMax = y
				maxE = e
				//sa.YMaxEle = e
			}
			sa.y = append(sa.y,y)
			sa.x = append(sa.x,e.DateTime())
			return true
		})
	}
	sa.diff /= float64(len(eles))
	sa.tag = func() (t byte) {
		f := minE.DateTime() < maxE.DateTime()
		//f := sa.Y[0] < sa.Y[len(sa.Y)-1]
		if f {
			t = 2
		}
		if (sa.eleLast.Diff() >0) == f {
			t++
		}
		return t
	}()
	return

}

func (self *Sample) SetBegin(e config.Element){
	self.begin = e
}

func (self *Sample) CheckVal(_e config.Element)bool{
	self.val = _e.Middle() - self.eleLast.Middle()
	return math.Abs(self.val) > self.diff
	//ch := self.child
	//d := self.eleLast.Diff()
	//for {
	//	if ch == nil{
	//		break
	//	}
	//	d += ch.eleLast.Diff()
	//	ch = ch.child
	//}

	//f:= math.Abs(self.val) > self.diff
	//if f {
	//	fmt.Println(d,self.val)
	//}
	//return f
}
func (self *Sample) SetSnap(){
	self.sn.lengthX = float64(self.duration())
	self.sn.lengthY = float64(self.yMax- self.yMin)

	le := len(self.x)
	X := make([]float64,0,le)
	Y := make([]float64,0,le)
	var i int
	var x int64
	for i,x = range self.x {
		X = append(X,float64(x-self.xMin())/self.sn.lengthX)
		if self.tag>>1 == 0 {
			Y = append(Y,float64(self.yMax - self.y[i])/self.sn.lengthY)
		}else{
			Y = append(Y,float64(self.y[i]-self.yMin)/self.sn.lengthY)
		}
	}
	self.sn.wei = curveFitting(X,Y)
	if len(self.sn.wei) == 0 {
		panic("w1")
	}
}

func (self *Sample) xMin() int64 {
	return self.x[0]
}

func (self *Sample) xMax() int64 {
	return self.x[len(self.x)-1]
}
func (self *Sample) GetTag() byte {
	return self.tag
}
func (self *Sample) Duration() int64 {
	return self.duration()
}
func (self *Sample) duration() int64 {
	return self.xMax() - self.xMin()
}


func (self *Sample) getDBf(dur int64,f func(x ,y float64)) (durdiff int64) {
	durdiff = self.duration() - dur
	xMin := self.xMin() + durdiff
	var x,x_ int64
	var i,j int
	if durdiff <=0 {
		for i,x = range self.x {
			f(float64(x - xMin),float64(self.yMax - self.y[i]))
		}
		return -durdiff
	}
	for i,x_ = range self.x {
		if x_ < xMin {
			continue
		}
		for j,x = range self.x[i:] {
			f(float64(x - xMin),float64(self.yMax -self.y[i+j]))
		}
		break
	}
	return
}

func (self *Sample) getDB(dur int64,f func(x ,y float64)) (durdiff int64) {

	durdiff = self.Duration() - dur
	xMin := self.xMin() + durdiff
	var x,x_ int64
	var i,j int
	if durdiff <=0 {
		for i,x = range self.x {
			f(float64(x - xMin),float64(self.y[i] -self.yMin))
		}
		return -durdiff
	}
	for i,x_ = range self.x {
		if x_ < xMin {
			continue
		}
		for j,x = range self.x[i:] {
			f(float64(x - xMin),float64(self.y[i+j] -self.yMin))
		}
		break
	}
	return

}

func (self *Sample) GetDB(dur int64,f func(float64,float64)) (int64) {

	if (self.tag>>1) == 0{
		return self.getDBf(dur,f)
	}else{
		return self.getDB(dur,f)
	}

}
func (self *Sample) GetDis(e *Sample) float64 {
	return self.getDis(e)
}
func (self *Sample) getDis(e *Sample) float64 {
	var longDis,l float64
	e.GetDB(int64(self.sn.lengthX),func(x,y float64){
		longDis += math.Pow(self.sn.getWeiY(x/self.sn.lengthX)-y/self.sn.lengthY,2)
		l++
	})
	return longDis/l
}

func (self *Sample) GetCaMap(i int,h func([]byte)){
	self.m.RLock()
	h(self.caMap[i])
	self.m.RUnlock()
}
func (self *Sample) SetCaMapF(i int,m []byte){

	if m == nil {
		m = make([]byte,len(self.caMap[i]))
	}
	self.m.Lock()
	for j,n := range m{
		self.caMap[i][j] |= (^n)
	}
	self.m.Unlock()
}
func (self *Sample) SetCaMap(i int,m []byte){
	if m == nil {
		return
	}
	self.m.Lock()
	for j,n := range m{
		self.caMap[i][j] |= n
	}
	self.m.Unlock()
}

func (self *Sample) SetCaMapV(i,j int,m byte){
	self.m.Lock()
	self.caMap[i][j/8] |= (m << uint(j%8))
	self.m.Unlock()
}

func (self *Sample) GetCaMapVal(I,i int) (v byte) {
	self.GetCaMap(I,func(b []byte){
		v = (b[i/8]>>uint(i%8)) &^ (^byte(3))
	})
	return
}
func (self *Sample) GetLastElement() config.Element {
	return self.eleLast
}
func (self *Sample) SetChild(p *Sample){
	self.child = p
}

func (self *Sample) GetLong() bool {
	return self.long
}
func (self *Sample) SetLong(l bool){
	self.long = l
}
func (self *Sample) SetPar(p *Sample){
	self.par = p
}

func (self *Sample) GetPar() *Sample {
	return self.par
}

func (self *Sample) SetCheckBak(c bool) {
	self.check_1 = c
}
func (self *Sample) GetCheckBak() bool {
	return self.check_1
}
func (self *Sample) SetCheck(c bool) {
	self.check = c
}
func (self *Sample) Check() bool {
	return self.check
}
func (self *Sample) GetDiff() float64 {
	return self.diff
}
//func (self *Sample) CheckChild() (val float64) {
//
//	ch := self.child
//	if ch == nil {
//		return 0
//	}
//	diff_ := float64(self.diff)
//	m := self.eleLast.Middle()
//	for{
//		val = ch.eleLast.Middle() - m
//		if math.Abs(val) > diff_ {
//			//self.child = nil
//			break
//		}
//		if ch.child == nil{
//			break
//		}
//		ch = ch.child
//	}
//	return val
//
//}
func (self *Sample) DisU() (bool) {

	t := (self.tag>>1) ^ (self.tag &^ 2)
	if t ==1 {
		return true
	}else{
		return false
	}

}

func (self *Sample) SaveImg(){

	dx:=800
	dy:=600
	imgfile,err := os.Create(fmt.Sprintf("%d_%d.png",int(self.sn.lengthX),int(self.sn.lengthY)))
	if err != nil {
		panic(err)
	}
	defer imgfile.Close()

	rand.Seed(time.Now().UnixNano())
	img := image.NewNRGBA(image.Rect(0, 0, dx, dy))
	x__ := self.sn.lengthX/float64(dx)
	y_ := float64(dy)/self.sn.lengthY
	x_ := float64(dx)/self.sn.lengthX
	col := int(rand.Intn(256))
	for i:=0;i<dx;i++{
		y := int(self.sn.getWeiY(x__*float64(i)/self.sn.lengthX)*self.sn.lengthY*y_)
		img.Set(i, y, color.RGBA{byte(col), byte(256-col), 0, 255})
	}
	var lx,ly,lx_,ly_ int
	self.GetDB(int64(self.sn.lengthX),func(x,y float64){
		lx_ = int(x*x_)
		ly_ = int(y*y_)
		if lx==0{
			img.Set(lx_, ly_, color.RGBA{byte(256-col), byte(col), 0, 255})
		}else{
			drawline(lx,ly,lx_,ly_,func(_x,_y int){
				img.Set(_x, _y, color.RGBA{byte(256-col), byte(col), 0, 255})
			})
		}
		lx = lx_
		ly = ly_
	})

	err = png.Encode(imgfile, img)
	if err != nil {
		panic(err)
	}


}
func abs(x int) int {
	if x >= 0 {
		return x
	}
	return -x
}
func drawline(x0, y0, x1, y1 int,h func(x,y int)) {
	dx := abs(x1 - x0)
	dy := abs(y1 - y0)
	sx, sy := 1, 1
	if x0 >= x1 {
		//h(x0,y0)
		return
		//sx = -1
	}
	if y0 >= y1 {
		sy = -1
	}
	err := dx - dy
	for {
		h(x0, y0)
		if x0 == x1 && y0 == y1 {
			return
		}
		e2 := err * 2
		if e2 > -dy {
	    		err -= dy
	    		x0 += sx
		}
		if e2 < dx {
			err += dx
			y0 += sy
		}
	}
}
