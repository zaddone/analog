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
	var X,Y []float64
	//X := make([]float64,0,int(sum/5))
	//Y := make([]float64,0,int(sum/5))
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

func ClearSamples(src []*Sample) ([]*Sample){

	//return src
	//le := len(src)
	//if le <= config.Conf.MinSam {
	//	return src
	//}
	m:=make(map[int64]*Sample)
	for _,e := range src {
		k:=e.XMin()
		_e := m[k]
		if _e == nil {
			m[k] = e
		}else{
			if _e.XMax() > e.XMax() {
				m[k] = e
			}
		}
	}
	es := make([]*Sample,0,len(m))
	for _,e := range m {
		es = append(es,e)
	}
	return SortSamples(es)

}
func ClearSortSamples(src []*Sample) []*Sample{
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
	return src[1:]
	//self.samp = self.samp[1:]
	//self.update(self.samp)
}

func SortSamples(src []*Sample) []*Sample{

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
func (self *set) check (d float64) bool {

	if len(self.samp) == 1 {
		return true
	}
	self.SetDar()
	val := self.dar.getVal()
	if val == 0 {
		fmt.Println(self.samp)
		for i,e := range self.samp {
			fmt.Println(i,e.XMin(),e.XMax(),e.dis)
		}
		return true
		//panic(0)
	}
	sum := self.dar.sum + d
	psum := self.dar.psum + (d * d)
	n := self.dar.n+1
	v1 := (psum/n) - ((sum*sum)/(n*n))
	//fmt.Println(val,v1,val > v1)
	return val > v1

}

