package pool
import(
	"github.com/zaddone/analog/config"
	"fmt"
	"image"
	"image/color"
	"image/png"
	//"time"
	"math"
	//"sync"
	"os"
)

type set struct {
	samp []*Sample
	//tmpSamp []*Sample
	sn snap
	//tag byte
	//dar *dar
	//sync.Mutex
	active int64
	//maps [][4]int
	//count int
}


func (self *set) SaveImg(){

	//dx :=int(self.sn.LengthX)
	//dy :=int(self.sn.LengthY)
	dx := 1200
	dy := 600
	imgfile, err := os.Create(fmt.Sprintf("%d_%d_%d.png",int(self.sn.lengthX),int(self.sn.lengthY),len(self.samp)))
	if err != nil {
		panic(err)
	}
	defer imgfile.Close()
	img := image.NewNRGBA(image.Rect(0, 0, dx, dy))
	x_ := self.sn.lengthX/float64(dx)
	y_ := float64(dy)/self.sn.lengthY
	x__ := float64(dx)/self.sn.lengthX
	for i:=0;i<dx;i++{
		y := int(self.sn.getWeiY(x_*float64(i)/self.sn.lengthX)*self.sn.lengthY*y_)
		img.Set(i, y, color.RGBA{0, 0, 0, 255})
	}
	var lx,ly,lx_,ly_ int
	for _,e := range self.samp {
		//col := uint8(256/len(self.samp)*(i+1))
		e.GetDB(int64(self.sn.lengthX),func(x,y float64){
			lx_ = int(x*x__)
			ly_ = int(y*y_)
			drawline(lx,ly,lx_,ly_,func(_x,_y int){
				img.Set(_x, _y, color.RGBA{0, 0, 0, 255})
			})
			lx = lx_
			ly = ly_
		})
	}
	err = png.Encode(imgfile, img)
	if err != nil {
		panic(err)
	}
}
func (self *set) ReadAllSample(h func(*Sample)bool){

	for _,e_ := range self.samp{
		if !h(e_){
			return
		}
		p := e_.par
		for{
			if p == nil {
				break
			}
			if !h(p){
				return
			}
			p=p.par
		}
		n := e_.child
		for{
			if n == nil {
				break
			}
			if !h(n){
				return
			}
			n=n.child
		}
	}

}
func NewSet(sa *Sample) (S *set) {
	S = &set{
		//tag:sa.tag>>1,
		samp:[]*Sample{sa},
		sn:snap{
			lengthX:float64(sa.xMax()-sa.xMin()),
			lengthY:float64(sa.yMax - sa.yMin),
		},
		active:sa.xMax(),
		//maps:[][]byte{sa.caMap}
	}
	X := make([]float64,0,len(sa.x))
	Y := make([]float64,0,len(sa.x))
	var i int
	var x int64
	for i,x = range sa.x {
		X = append(X,float64(x-sa.xMin())/S.sn.lengthX)
		if sa.tag>>1 == 0 {
			Y = append(Y,float64(sa.yMax - sa.y[i])/S.sn.lengthY)
		}else{
			Y = append(Y,float64(sa.y[i]-sa.yMin)/S.sn.lengthY)
		}
	}
	S.sn.wei = curveFitting(X,Y)
	if len(S.sn.wei) == 0 {
		panic("w1")
	}
	//sa.setMap.Store(S,true)
	//sa.dis = 0
	//sa.s = S
	//sa.NewC++
	//S.SaveImg()
	return

}

//func (self *set)loadMap(m []byte){
//	l := len(m)
//	if self.maps == nil {
//		self.maps = make([][4]int,l*4)
//	}
//	var j,J uint
//	for i,n := range m {
//		for j=0;j<4;j++ {
//			J = j*2
//			self.maps[i*4+int(j)][int((n&^(^(3<<J)))>>J)]++
//		}
//	}
//	self.count++
//}

func (S *set) update(sa []*Sample) {
	S.clear()
	//S.samp = SortSamples(sa)
	S.samp = sa
	var sum int64
	var df float64
	var count float64 = 0
	for _, _s := range S.samp {
		//_s.setMap.Store(S,true)
		//_s.dis = 0
		//_s.s = S
		//if i>= config.Conf.MinSam{
		//	continue
		//}
		sum += _s.duration()
		df += float64(_s.yMax - _s.yMin)
		count++
		//if df > S.sn.LengthY  {
		//	S.sn.LengthY = df
		//}
	}
	//le := len(S.samp)

	var X,Y []float64
	//X := make([]float64,0,int(sum/5))
	//Y := make([]float64,0,int(sum/5))
	S.sn.lengthY = df/count
	sum /= int64(count)
	S.sn.lengthX = float64(sum)
	for _,e := range S.samp {
		//if i>= config.Conf.MinSam{
		//	break
		//}
		//s.setMap[S] = true

		e.GetDB(sum,func(x,y float64){
			X = append(X,x/S.sn.lengthX)
			Y = append(Y,y/S.sn.lengthY)
		})
	}
	S.sn.wei = curveFitting(X,Y)
	if len(S.sn.wei) == 0 {
		fmt.Println(X,Y)
		panic("w")
	}
	//S.active++
	//S.SaveImg()

}

func (S *set) clear(){
	//S.Lock()
	S.sn.wei = nil
	//S.dar = nil
	S.samp = nil
	//S.tmpSamp = nil
	//S.Unlock()

}

func (self *set) distance(e *Sample) float64 {

	//fmt.Println(len(self.samp))
	var longDis,l float64

	e.GetDB(int64(self.sn.lengthX),func(x,y float64){
		longDis += math.Pow(self.sn.getWeiY(x/self.sn.lengthX)-y/self.sn.lengthY,2)
		l++
	})
	return longDis/l
	//return longDis

}

func ClearSamples(src []*Sample) ([]*Sample){

	//return src
	//le := len(src)
	//if le <= config.Conf.MinSam {
	//	return src
	//}
	m:=make(map[int64]*Sample)
	for _,e := range src {
		k:=e.xMin()
		_e := m[k]
		if _e == nil {
			m[k] = e
		}else{
			if _e.xMax() > e.xMax() {
				m[k] = e
			}
		}
	}
	es := make([]*Sample,0,len(m))
	for _,e := range m {
		es = append(es,e)
	}
	return es

}
func ClearSortSamples(src []*Sample) []*Sample{
	var sort func(int)
	sort = func(i int){
		if i == 0 {
			return
		}
		I := i-1
		if src[I].xMax() <= src[i].xMax() {
			return
		}
		src[I],src[i] = src[i],src[I]
		sort(I)
	}
	for i,_ := range src {
		sort(i)
	}
	return src[1:]
	//self.samp = self.samp[1:]
	//self.update(self.samp)
}

func SortSamples(src []*Sample) []*Sample {

	//return src
	le := len(src)
	if le <= config.Conf.MinSam {
		return src
	}
	var sort func(int)
	sort = func(i int){
		if i == 0 {
			return
		}
		I := i-1
		if src[I].xMax() >= src[i].xMax() {
			return
		}
		src[I],src[i] = src[i],src[I]
		sort(I)
	}
	for i,_ := range src {
		sort(i)
	}
	//return src
	//n := le - config.Conf.MinSam
	return src[:config.Conf.MinSam]
	//self.samp = self.samp[1:]
	//self.update(self.samp)
}

//func (self *set) Sort() (out []*Sample) {
//	var sort func(i int)
//	sort = func(i int){
//		if i == 0 {
//			return
//		}
//		I := i-1
//		if self.samp[I].dis <= self.samp[i].dis{
//			return
//		}
//		self.samp[I],self.samp[i] = self.samp[i],self.samp[I]
//		sort(I)
//	}
//	for _i,e := range self.samp{
//		if e.dis == 0 {
//			e.dis = self.distance(e)
//		}
//		sort(_i)
//	}
//	out = self.samp[config.Conf.MinSam:]
//	//self.samp = self.samp[:config.Conf.MinSam]
//	self.update(self.samp[:config.Conf.MinSam])
//	return
//
//	//self.samp = SortSamples(self.samp)
//}
func (self *set) GetLastTime() int64 {
	return self.samp[len(self.samp)-1].xMax()
}

//func (self *set) SetDar() {
//
//	self.dar = &dar{}
//	for _,e := range self.samp {
//		if e.dis == 0 {
//			e.dis  = self.distance(e)
//		}
//		self.dar.update(e.dis)
//	}
//}

//func (self *set) checkSample (e *Sample) bool {
//	if len(self.samp) < config.Conf.MinSam {
//		return false
//	}
//	//f := (e.GetTag()>>1) == 1
//	d := self.samp[0].CheckChild()
//	for _,e_ := range self.samp[1:] {
//		if (d>0) != (e_.CheckChild()>0){
//			return false
//		}
//	}
//	return true
//}

//func (self *set) SetTMap(e *Sample) {
//	for _,e_ := range self.samp {
//		e.SetTestMap(e_.caMap[1])
//	}
//}
//func (self *set) check (d float64) bool {
//
//	if len(self.samp) == 1 {
//		return true
//	}
//	self.SetDar()
//	val := self.dar.getVal()
//	if val == 0 {
//		fmt.Println(self.samp)
//		for i,e := range self.samp {
//			fmt.Println(i,e.XMin(),e.XMax(),e.dis)
//		}
//		return true
//		//panic(0)
//	}
//	sum := self.dar.sum + d
//	psum := self.dar.psum + (d * d)
//	n := self.dar.n+1
//	v1 := (psum/n) - ((sum*sum)/(n*n))
//	//fmt.Println(val,v1,val > v1)
//	return val > v1
//
//}

