package telecar
import(
	"github.com/zaddone/analog/config"
	"sync"
)
type Sample struct {
	//xMin int64
	//xMax int64
	YMin float64
	YMax float64
	X []int64
	Y []float64

	stop chan bool
	eleLast config.Element
	tag byte
	dis float64

	setMap *sync.Map
	Long bool
	check bool
}

func NewSample(eles []config.Element) (sa *Sample) {

	sa = &Sample{
		eleLast:eles[len(eles)-1],
		Y:make([]float64,0,2000),
		X:make([]int64,0,2000),
		stop:make(chan bool,1),
		setMap:new(sync.Map),
	}
	var y float64
	for _,ele := range eles {
		ele.Read(func(e config.Element) bool {
			y = e.Middle()
			if (sa.YMin==0) || (y < sa.YMin) {
				sa.YMin = y
			}else if (sa.YMax < y) {
				sa.YMax = y
			}
			sa.Y = append(sa.Y,y)
			sa.X = append(sa.X,e.DateTime())
			return true
		})
	}
	sa.tag = func() (t byte) {
		f := sa.Y[0] < sa.Y[len(sa.Y)-1]
		if f {
			t = 2
		}
		if (eles[len(eles)-1].Diff() >0) == f {
			t++
		}
		return t
	}()
	return

}
func (self *Sample) GetLastElement() config.Element {
	return self.eleLast
}
func (self *Sample) Wait(){
	<-self.stop
}
func (self *Sample) GetCheck() bool {
	return self.check
}
func (self *Sample) GetTag() byte {
	return self.tag
}

func (self *Sample) XMax () int64 {
	return self.X[len(self.X)-1]
}
func (self *Sample) XMin () int64 {
	return self.X[0]
}
func (self *Sample) Duration() int64 {
	return self.XMax() - self.XMin()
}

func (self *Sample) GetDBf(dur int64,f func(x ,y float64)) (durdiff int64) {
	durdiff = self.Duration() - dur
	xMin := self.XMin() + durdiff
	var x,x_ int64
	var i,j int
	if durdiff <=0 {
		for i,x = range self.X {
			f(float64(x - xMin),self.YMax - self.Y[i])
		}
		return -durdiff
	}
	for i,x_ = range self.X {
		if x_ < xMin {
			continue
		}
		for j,x = range self.X[i:] {
			f(float64(x - xMin),self.YMax -self.Y[i+j])
		}
		break
	}
	return
}

func (self *Sample) GetDB(dur int64,f func(x ,y float64)) (durdiff int64) {

	durdiff = self.Duration() - dur
	xMin := self.XMin() + durdiff
	var x,x_ int64
	var i,j int
	if durdiff <=0 {
		for i,x = range self.X {
			f(float64(x - xMin),self.Y[i] -self.YMin)
		}
		return -durdiff
	}
	for i,x_ = range self.X {
		if x_ < xMin {
			continue
		}
		for j,x = range self.X[i:] {
			f(float64(x - xMin),self.Y[i+j] -self.YMin)
		}
		break
	}
	return

}
func (self *Sample)CheckSetMap(s *set) bool {
	_,ok := self.setMap.Load(s)
	return ok
}
func (self *Sample)InitSetMap(s *set){
	self.setMap = new(sync.Map)
	self.setMap.Store(s,true)
}


