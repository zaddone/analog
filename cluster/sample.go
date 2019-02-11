package cluster
import(
	"fmt"
	//"math"
	"github.com/zaddone/analog/config"
	"encoding/binary"
	"bytes"
	"encoding/gob"
)

type Sample struct {

	XMin int64
	XMax int64
	YMin float64
	YMax float64

	//Dur int64
	X []int64
	Y []float64

	dis float64
	//durDis float64

	tag byte
	diff float64

	key []byte
	//Same []byte
	endEle config.Element
	caMap []byte


}
func NewSample(eles []config.Element,e config.Element) (sa *Sample) {
	if e != nil {
		sa = &Sample{
			dis:e.Diff(),
			//durDis:e.Duration(),
			//diff : eles[len(eles)-1].Diff(),
		}
	}else{
		sa = &Sample{
			//diff : eles[len(eles)-1].Diff(),
		}
	}

	//sa = &Sample{)
	var y float64
	for _,ele := range eles {
		ele.Read(func(e config.Element) bool {
			y = e.Middle()
			if (sa.YMin==0) || (y < sa.YMin) {
				sa.YMin = y
			}
			if (sa.YMax < y) {
				sa.YMax = y
			}
			sa.Y = append(sa.Y,y)
			sa.X = append(sa.X,e.DateTime())
			return true
		})
	}
	sa.XMin = sa.X[0]
	le := len(sa.X)-1
	sa.XMax = sa.X[le]
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
	return self.XMax - self.XMin
}
func (self *Sample) SetCaMap( m []byte){
	self.caMap = m
}
func (self *Sample) KeyName() []byte {
	if self.key == nil {
		self.key = make([]byte,8)
		binary.BigEndian.PutUint64(self.key,uint64(self.XMin))
		self.key = append(self.key,self.tag)
		//if self.caMap != nil{
		//	self.key = append(self.key,self.caMap...)
		//}
	}
	if self.key == nil {
		fmt.Println(self.XMin,self.tag)
		panic(0)
	}
	return self.key
}

func (self *Sample) load(db []byte,k *saEasy) {

	err := gob.NewDecoder(bytes.NewBuffer(db)).Decode(self)
	if err != nil {
		panic(err)
	}
	self.key = k.Key
	self.caMap = k.CaMap
	self.dis = k.Dis
	//self.durDis = k.DurDis

}

func (self *Sample) GetDBF(dur int64,f func(x ,y float64)) (durdiff int64) {

	durdiff = self.Duration() - dur
	xMin := self.XMin + durdiff
	if durdiff <=0 {
		//xMin:= self.XMax - dur
		for i,x := range self.X {
			f(float64(x - xMin),self.YMax - self.Y[i])
		}
		return -durdiff
	}
	var yMax float64
	Le := len(self.X)
	var X []int64 = make([]int64,0,Le)
	var Y []float64 = make([]float64,0,Le)
	var x int64
	for i,y := range self.Y{
		x = self.X[i]
		if x >= xMin {
			if (y > yMax) {
				yMax = y
			}
			X = append(X,x)
			Y = append(Y,y)
		}
	}
	for i,y := range Y {
		f(float64(X[i]-xMin),yMax - y)
	}
	return
}
func (self *Sample) GetDB(dur int64,f func(x ,y float64)) (durdiff int64) {
	durdiff = self.Duration() - dur
	xMin := self.XMin + durdiff
	if durdiff <=0 {
		//xMin:= self.XMax - dur
		for i,x := range self.X {
			f(float64(x - xMin),self.Y[i] -self.YMin)
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
		f(float64(X[i]-xMin),y-yMin)
	}
	return

}
