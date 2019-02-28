package cluster
import(
	"fmt"
	//"math"
	"github.com/zaddone/analog/config"
	"encoding/binary"
	"bytes"
	"encoding/gob"
	"sync"
)

type Sample struct {

	xMin int64
	xMax int64

	YMin float64
	YMax float64

	//Dur int64
	X []int64
	Y []float64
	CaMap []byte
	Long bool

	dis float64
	//durDis float64

	diff float64

	key []byte
	//Same []byte
	endEle config.Element
	//eleList []config.Element
	eleLast config.Element
	tag byte

	s *Set
	setMap *sync.Map

	stop chan bool
	check bool

}
func NewSampleDB(db []byte,k *saEasy) (sa *Sample){
	sa = &Sample{}
	sa.load(db,k)
	sa.setMap = new(sync.Map)// make(map[*Set]bool)
	return
}
func NewSample(eles []config.Element) (sa *Sample) {
	//if e != nil {
	//	sa = &Sample{
	//		dis:e.Diff(),
	//		//durDis:e.Duration(),
	//		//diff : eles[len(eles)-1].Diff(),
	//	}
	//}else{
	sa = &Sample{
		//eleList:eles,
		eleLast:eles[len(eles)-1],
		Y:make([]float64,0,2000),
		X:make([]int64,0,2000),
		stop:make(chan bool,1),
		//setMap:make(map[*Set]bool),
		setMap:new(sync.Map),
		//diff : eles[len(eles)-1].Diff(),
	}
	//}

	//sa = &Sample{)
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
	sa.xMin = sa.X[0]
	le := len(sa.X)-1
	sa.xMax = sa.X[le]
	sa.tag = func() (t byte) {
		f := sa.Y[0] < sa.Y[le]
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
func (self *Sample) GetTag() byte {
	return self.tag
}
func (self *Sample) GetSet() *Set {
	return self.s
}

func (self *Sample) Count() (int) {

	<-self.stop
	return len(self.s.samp)

}

func (self *Sample) Check() (bool) {
	//<-self.stop
	//var l,s int
	for _,sa := range self.s.samp {
		if (sa == self){
			//panic(0)
			continue
		}
		self.s.count[int(sa.tag >> 1)]++
		//if (sa.tag != self.tag) {
		//	s++
		//}else{
		//	l++
		//}
		//if (sa.tag != self.tag) || !sa.Long {
		//	s++
		//	continue
		//}
		//if sa.Long {
		//	l++
		//}

	}
	n := int(self.tag >> 1)
	return (self.s.count[n] > self.s.count[n^1])
}
func (self *Sample) CheckMap() (m []byte) {
	if self.s == nil || len(self.s.samp) < 3 {
		return nil
	}
	var l,s int
	for _,sa := range self.s.samp {
		if (sa == self) ||
		(sa.CaMap == nil) ||
		(sa.tag != self.tag) {
			continue
		}

		if sa.Long {
			l++
		}else{
			s++
		}
		if m == nil {
			m = sa.CaMap
		}else{
			for i,n := range sa.CaMap{
				m[i] |= n
			}
		}
	}
	//if s >= l {
	//	return nil
	//}
	return m
}
func (self *Sample) GetLastElement() config.Element{
	return self.eleLast
	//return self.eleList[len(self.eleList)-1]
}
func (self *Sample) SetDiff(diff float64) {
	self.diff = diff
}

func (self *Sample) GetDiff() float64 {
	return self.diff
}
func (self *Sample) GetEndElement() config.Element {
	return self.endEle
}

func (self *Sample) SetEndElement(e config.Element) {
	self.endEle  = e
}

func (self *Sample) toByte() []byte {
	var b bytes.Buffer
	err := gob.NewEncoder(&b).Encode(self)
	if err != nil {
		panic(err)
	}
	return b.Bytes()
}
func (self *Sample) Duration() int64 {
	return self.xMax - self.xMin
}
func (self *Sample) SetCaMap( m []byte){
	self.CaMap = m
}
func (self *Sample) KeyName() []byte {
	if self.key == nil {
		self.key = make([]byte,9)
		binary.BigEndian.PutUint64(self.key,uint64(self.xMax))
		self.key[8] = self.tag
		//if self.caMap != nil{
		//	self.key = append(self.key,self.caMap...)
		//}
	}
	if self.key == nil {
		fmt.Println(self.xMin,self.tag)
		panic(0)
	}
	return self.key
}

func (self *Sample) load(db []byte,k *saEasy) {

	err := gob.NewDecoder(bytes.NewBuffer(db)).Decode(self)
	if err != nil {
		panic(err)
	}

	//self.key = make([]byte,len(k))
	//copy(self.key,k)
	self.key = k.Key
	//self.CaMap = k.CaMap
	//self.dis = k.Dis

	self.xMax = self.X[len(self.X)-1]
	self.xMin = self.X[0]
	self.tag = self.key[8]
	//self.durDis = k.DurDis

}

func (self *Sample) GetDB_(dur int64,f func(x ,y float64)) (durdiff int64) {

	durdiff = self.Duration() - dur
	xMin := self.xMin + durdiff
	if durdiff <=0 {
		//xMin:= self.XMax - dur
		for i,x := range self.X {
			f(float64(x - xMin),self.Y[i] - self.YMin)
		}
		return -durdiff
	}
	var yMin float64
	Le := len(self.X)
	var X []int64 = make([]int64,0,Le)
	var Y []float64 = make([]float64,0,Le)
	var x int64
	for i,y := range self.Y{
		x = self.X[i]
		if x >= xMin {
			if (y < yMin) || (yMin == 0) {
				yMin = y
			}
			X = append(X,x)
			Y = append(Y,y)
		}
	}
	for i,y := range Y {
		f(float64(X[i]-xMin),y - yMin)
	}
	return
}
func (self *Sample) GetDB(dur int64,f func(x ,y float64)) (durdiff int64) {
	durdiff = self.Duration() - dur
	xMin := self.xMin + durdiff
	var x,x_ int64
	var i,j int
	if durdiff <=0 {
		//xMin:= self.XMax - dur
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
	//var yMin float64 = self.Y[0]
	//Le := len(self.X)
	//var X []int64 = make([]int64,0,Le)
	//var Y []float64 = make([]float64,0,Le)
	//var x int64
	//for i,y := range self.Y{
	//	x = self.X[i]
	//	if x >= xMin {
	//		if (y < yMin){
	//			yMin = y
	//		}
	//		X = append(X,x)
	//		Y = append(Y,y)
	//	}
	//}
	//for i,y := range Y {
	//	f(float64(X[i]-xMin),y-self.YMin)
	//}
	//return

}
