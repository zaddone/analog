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
	tag byte
	dar *dar
	sync.Mutex
}

func NewSet(sa *Sample) (S *set) {
	S = &set{
		tag:sa.tag>>1,
		samp:[]*Sample{sa},
		sn:snap{
			LengthX:float64(sa.XMax()-sa.XMin()),
			LengthY:sa.YMax - sa.YMin,
		},
	}
	X := make([]float64,0,len(sa.X))
	Y := make([]float64,0,len(sa.X))
	var i int
	var x int64
	for i,x = range sa.X {
		X = append(X,float64(x-sa.XMin())/S.sn.LengthX)
		if S.tag == 0 {
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
	return

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
		//if df > S.sn.LengthY  {
		//	S.sn.LengthY = df
		//}
	}
	X := make([]float64,0,int(sum/5))
	Y := make([]float64,0,int(sum/5))
	le := len(sa)
	S.sn.LengthY = df/float64(le)
	sum /= int64(le)
	S.sn.LengthX = float64(sum)
	for _,e := range S.samp {
		//s.setMap[S] = true
		if S.tag == 0 {
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

}

func (S *set) clear(){
	S.sn.Wei = nil
	S.dar = nil
	S.samp = nil
	S.tmpSamp = nil

}


func (self *set) distance(e *Sample) float64 {

	//fmt.Println(len(self.samp))
	var longDis,l float64
	if self.tag == 0 {
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
func (self *set) Sort(){

	le := len(self.samp)
	if le <= config.Conf.MinSam {
		return
	}
	var sort func(int)
	sort = func(i int){
		if i == 0 {
			return
		}
		I := i-1
		if self.samp[I].XMin() <= self.samp[i].XMin() {
			return
		}
		self.samp[I],self.samp[i] = self.samp[i],self.samp[I]
		sort(I)
	}
	for i,_ := range self.samp{
		sort(i)
	}
	self.samp = self.samp[(le - config.Conf.MinSam):]
	self.update(self.samp)
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
func (self *set) check (d float64) bool {

	if len(self.samp) == 1 {
		return true
	}

	self.SetDar()
	val := self.dar.getVal()
	if val == 0 {
		fmt.Println(self.samp)
		//for _,e := range self.samp {
		//	time.Unix(e.XMax(),0)
		//}
		panic(0)
	}
	sum := self.dar.sum + d
	psum := self.dar.psum + (d * d)
	n := self.dar.n+1
	v1 := (psum/n) - ((sum*sum)/(n*n))
	return val > v1

}

