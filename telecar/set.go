package telecar
import(
	"github.com/zaddone/analog/config"
	"fmt"
	//"time"
	"math"
	"sync"
)

type set struct {
	samp []*Sample
	tmpSamp []*Sample
	sn snap
	//tag byte
	dar *dar
	sync.Mutex
	active int64
	maps [][4]int
	count int
}

func NewSet(sa *Sample) (S *set) {
	S = &set{
		//tag:sa.tag>>1,
		samp:[]*Sample{sa},
		sn:snap{
			LengthX:float64(sa.XMax()-sa.XMin()),
			LengthY:sa.YMax - sa.YMin,
		},
		active:sa.XMax(),
		//maps:[][]byte{sa.caMap}
	}
	X := make([]float64,0,len(sa.X))
	Y := make([]float64,0,len(sa.X))
	var i int
	var x int64
	for i,x = range sa.X {
		X = append(X,float64(x-sa.XMin())/S.sn.LengthX)
		if sa.tag>>1 == 0 {
			Y = append(Y,(sa.YMax - sa.Y[i])/S.sn.LengthY)
		}else{
			Y = append(Y,(sa.Y[i]-sa.YMin)/S.sn.LengthY)
		}
	}
	S.sn.Wei = CurveFitting(X,Y)
	if len(S.sn.Wei) == 0 {
		panic("w1")
	}
	sa.setMap.Store(S,true)
	sa.dis = 0
	return

}

func (self *set)loadMap(m []byte){
	l := len(m)
	if self.maps == nil {
		self.maps = make([][4]int,l*4)
	}
	var j,J uint
	for i,n := range m {
		for j=0;j<4;j++ {
			J = j*2
			self.maps[i*4+int(j)][int((n&^(^(3<<J)))>>J)]++
		}
	}
	self.count++
}

func (S *set) update(sa []*Sample) {
	S.clear()
	S.samp = sa
	var sum int64
	var df float64
	for _, _s := range S.samp {
		_s.setMap.Store(S,true)
		sum += _s.Duration()
		df += _s.YMax - _s.YMin
		_s.dis = 0
		//if df > S.sn.LengthY  {
		//	S.sn.LengthY = df
		//}
	}
	X := make([]float64,0,int(sum/5))
	Y := make([]float64,0,int(sum/5))
	le := len(S.samp)
	S.sn.LengthY = df/float64(le)
	sum /= int64(le)
	S.sn.LengthX = float64(sum)
	for _,e := range S.samp {
		//s.setMap[S] = true
		if e.tag>>1 == 0 {
			e.GetDBf(sum,func(x,y float64){
				X = append(X,x/S.sn.LengthX)
				Y = append(Y,y/S.sn.LengthY)
			})
		}else{
			e.GetDB(sum,func(x,y float64){
				X = append(X,x/S.sn.LengthX)
				Y = append(Y,y/S.sn.LengthY)
			})
		}
	}
	S.sn.Wei = CurveFitting(X,Y)
	if len(S.sn.Wei) == 0 {
		fmt.Println(X,Y)
		panic("w")
	}
	S.active++

}

func (S *set) clear(){
	S.Lock()
	S.sn.Wei = nil
	S.dar = nil
	S.samp = nil
	S.tmpSamp = nil
	S.Unlock()

}

func (self *set) distance(e *Sample) float64 {

	//fmt.Println(len(self.samp))
	var longDis,l float64
	if (e.tag>>1) == 0 {
		e.GetDBf(int64(self.sn.LengthX),func(x,y float64){
			longDis += math.Pow(self.sn.GetWeiY(x/self.sn.LengthX)-y/self.sn.LengthY,2)
			l++
		})
	}else{
		e.GetDB(int64(self.sn.LengthX),func(x,y float64){
			longDis += math.Pow(self.sn.GetWeiY(x/self.sn.LengthX)-y/self.sn.LengthY,2)
			l++
		})
	}
	return longDis/l

}

func SortSamples(src []*Sample) []*Sample{

	//return scr
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
		if src[I].XMax() <= src[i].XMax() {
			return
		}
		src[I],src[i] = src[i],src[I]
		sort(I)
	}
	for i,_ := range src {
		sort(i)
	}
	return src[(le - config.Conf.MinSam):]
	//self.samp = self.samp[1:]
	//self.update(self.samp)
}
func (self *set) Sort(){
	self.samp = SortSamples(self.samp)
}
func (self *set) GetLastTime() int64 {
	return self.samp[len(self.samp)-1].XMax()
}

func (self *set) SetDar() {

	self.dar = &dar{}
	for _,e := range self.samp {
		if e.dis == 0 {
			e.dis  = self.distance(e)
		}
		self.dar.update(e.dis)
	}
}

func (self *set) checkSample (e *Sample) bool {
	if len(self.samp) < config.Conf.MinSam {
		return false
	}
	for _,_e := range self.samp {
		if _e.caMap[0] == nil {
			return false
		}
	}
	if e.caMap[0] == nil {
		le := len(self.samp[0].caMap[0])
		e.caMap=[2][]byte{
			make([]byte,le),
			make([]byte,le)}
	}

	for _,_e := range self.samp {
		for i,m := range  _e.caMap[0] {
			e.caMap[0][i] |= ^m
			e.caMap[1][i] |= ^(_e.caMap[1][i])
		}
	}
	return true
	//var t,_t,f,j byte
	//f = 252
	//for i,n := range e.caMap[0]{
	//	if n == 255 {
	//		continue
	//	}
	//	_n := e.caMap[1][i]
	//	if _n == 255 {
	//		continue
	//	}
	//	for j=0;j<8;j+=2{
	//		t  = (n>>j)&^(f)
	//		_t = (_n>>j)&^(f)
	//		if (t != 3) && (_t !=3) {
	//			panic(0)
	//		}
	//		if t != 3 {
	//			fmt.Println(0,i*4+int(j/2))
	//		}
	//		if _t != 3 {
	//			fmt.Println(1,i*4+int(j/2))
	//		}
	//	}
	//}
}
func (self *set) check (d float64) bool {

	if len(self.samp) == 1 {
		return true
	}
	self.SetDar()
	val := self.dar.getVal()
	if val == 0 {
		fmt.Println(self.samp)
		for i,e := range self.samp {
			fmt.Println(i,e.GetFlag(),e.XMin(),e.XMax(),e.dis)
		}
		return true
		//panic(0)
	}
	sum := self.dar.sum + d
	psum := self.dar.psum + (d * d)
	n := self.dar.n+1
	v1 := (psum/n) - ((sum*sum)/(n*n))
	return val > v1

}

