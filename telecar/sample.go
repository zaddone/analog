package telecar
import(
	"github.com/zaddone/analog/config"
	"sync"
	//"fmt"
)

//type LevelInterface interface{
//	GetCache() CacheInterface
//}

type Sample struct {
	//xMin int64
	//xMax int64
	YMin float64
	YMax float64
	//YMinEle config.Element
	//YMaxEle config.Element
	X []int64
	Y []float64

	//stop chan bool
	eleLast config.Element
	tag byte
	dis float64

	setMap *sync.Map
	Long bool
	check bool

	//leMap [2][]LevelInterface
	caMap [3][]byte
	m sync.RWMutex

	//t bool
	//par *Sample
	//flag int
	//toSa map[*Sample]bool
	//fromSa map[*Sample]bool

	//End bool

	//CaMap []*Map
	//node config.Element
	//caMapCheck []byte
	//s *Set
}
func (self *Sample) SetCheck(c bool) {
	self.check = c
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
	self.caMap[i][j/8] |= m<<uint(j%8)
	self.m.Unlock()
}

func (self *Sample) Check() bool {
	return self.check
}
//func (self *Sample) SetPar(s *Sample){
//	self.par = s
//}

func NewSample(eles []config.Element,le int) (sa *Sample) {

	sa = &Sample{
		eleLast:eles[len(eles)-1],
		Y:make([]float64,0,2000),
		X:make([]int64,0,2000),
		//stop:make(chan bool,1),
		setMap:new(sync.Map),
	}
	if le !=0 {
		sa.caMap = [3][]byte{
			make([]byte,le),
			make([]byte,le),
			make([]byte,le),
		}
	}
	var y float64
	for _,ele := range eles {
		ele.Read(func(e config.Element) bool {
			y = e.Middle()
			if (sa.YMin==0) || (y < sa.YMin) {
				sa.YMin = y
				//sa.YMinEle = e
			}else if (sa.YMax < y) {
				sa.YMax = y
				//sa.YMaxEle = e
			}
			sa.Y = append(sa.Y,y)
			sa.X = append(sa.X,e.DateTime())
			return true
		})
	}
	//fmt.Println(sa.Y,sa.X)
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
//func (self *Sample) SetTestMap(n []byte){
//	self.m.Lock()
//	for i,m:= range n {
//		self.caMap[2][i] |= ^m
//	}
//	self.m.Unlock()
//}

func (self *Sample) GetCaMap(i int,h func([]byte)){
	self.m.RLock()
	h(self.caMap[i])
	self.m.RUnlock()
}
//func (self *Sample) GetCaMap() [3][]byte{
//	return self.caMap
//}
//func (self *Sample) SetNode(no config.Element ){
//	self.node = no
//}
//func (self *Sample) GetNode() config.Element {
//	return self.node
//}
//
//func (self *Sample)SetFlag(i int){
//	self.flag = i
//}
//
//func (self *Sample)GetFlag() int {
//	return self.flag
//}
func (self *Sample) DisU() (bool) {

	t := self.tag>>1
	if ((self.tag &^ 2)  == 1) {
		t ^= 1
	}
	if t ==1 {
		return true
	}else{
		return false
	}

}

func (self *Sample) GetLastElement() config.Element {
	return self.eleLast
}
func (self *Sample) Wait(){
	//<-self.stop
}
//func (self *Sample) IsCheck() bool {
//	return self.s != nil
//}
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
//func (self *Sample) MapCheck(c CacheInter){
//	if self.s == nil {
//		return
//	}
//	if self.CaMap == nil {
//		return
//	}
//}
func (self *Sample)CheckSetMap(s *set) bool {
	_,ok := self.setMap.Load(s)
	return ok
}
func (self *Sample)InitSetMap(s *set){
	self.setMap = new(sync.Map)
	self.setMap.Store(s,true)
}


