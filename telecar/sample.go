package telecar
import(
	"github.com/zaddone/analog/config"
	"sync"
	"math"
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
	diff float64

	//setMap *sync.Map
	Long bool
	check bool
	checkBak bool

	//leMap [2][]LevelInterface
	caMap [4][]byte
	m sync.RWMutex
	//countMap []int

	//t bool
	//par *Sample
	//flag int
	//toSa map[*Sample]bool
	//fromSa map[*Sample]bool

	//End bool

	//CaMap []*Map
	//node config.Element
	//caMapCheck []byte
	//s *set
	par *Sample
	child *Sample
	//NewC int
}


//func (self *Sample) GetParDiff() ( d float64) {
//	d = math.Abs(self.eleLast.Diff())
//	p := self.par
//	var c float64 = 1
//	for {
//		if p == nil {
//			break
//		}
//		d += math.Abs(p.eleLast.Diff())
//		c++
//	}
//	d /= c
//	if !self.DisU(){
//		d = -d
//	}
//	return
//}

func (self *Sample) GetDiff() float64 {
	return self.diff
}
func (self *Sample) CheckChild() (float64,bool) {

	ch   := self.child
	if ch == nil {
		return 0,false
	}
	diff := self.eleLast.Diff()
	//absDiff := math.Abs(diff)
	isOut:= false
	//var count float64 = 1
	for{
		//absDiff += math.Abs(f)
		diff += ch.eleLast.Diff()
		//count++
		if math.Abs(diff) > self.diff {
			isOut = true
			break
		}
		if ch.child == nil{
			break
		}
		ch = ch.child
	}
	return diff,isOut

}

func (self *Sample) SetChild(p *Sample){
	self.child = p
}

func (self *Sample) SetPar(p *Sample){
	self.par = p
	//if p != nil {
	//	p.chile = self
	//}
}

func (self *Sample) GetPar() *Sample {
	return self.par
}

func (self *Sample) SetCheckBak(c bool) {
	self.checkBak = c
}
func (self *Sample) GetCheckBak() bool {
	return self.checkBak
}
func (self *Sample) SetCheck(c bool) {
	self.check = c
}
//func (self *Sample) CheckSet() bool {
//	if self.s == nil {
//		return false
//	}
//	return self.s.checkSample(self)
//}

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

func (self *Sample) SetCaMapClear(i,j int){
	self.m.Lock()
	self.caMap[i][j/8] &^= byte(3 << uint(j%8))
	self.m.Unlock()
}
func (self *Sample) SetCaMapV(i,j int,m byte){
	self.m.Lock()
	self.caMap[i][j/8] |= (m << uint(j%8))
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
		//Y:make([]float64,0,2000),
		//X:make([]int64,0,2000),
		//stop:make(chan bool,1),
		//setMap:new(sync.Map),
	}
	if le !=0 {
		sa.caMap = [4][]byte{
			make([]byte,le),
			make([]byte,le),
			make([]byte,le),
			make([]byte,le),
		}
		//sa.countMap = make(int,le/2)
	}
	var y float64
	var minE,maxE config.Element
	for _,ele := range eles {
		sa.diff += math.Abs(ele.Diff())
		ele.Read(func(e config.Element) bool {
			y = e.Middle()
			if (sa.YMin==0) || (y < sa.YMin) {
				sa.YMin = y
				minE = e
				//sa.YMinEle = e
			}else if (sa.YMax < y) {
				sa.YMax = y
				maxE = e
				//sa.YMaxEle = e
			}
			sa.Y = append(sa.Y,y)
			sa.X = append(sa.X,e.DateTime())
			return true
		})
	}
	sa.diff/=float64(len(eles))
	//fmt.Println(sa.Y,sa.X)
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

func (self *Sample) GetCaMapVal(I,i int) (v byte) {
	self.GetCaMap(I,func(b []byte){
		v = (b[i/8]>>uint(i%8)) &^ (^byte(3))
	})
	return
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
//func (self *Sample)CheckSetMap(s *set) (ok bool) {
//	_,ok = self.setMap.Load(s)
//	return ok
//}
//func (self *Sample)InitSetMap(s *set){
//	self.setMap = new(sync.Map)
//	self.setMap.Store(s,true)
//}


