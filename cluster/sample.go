package cluster
import(
	//"fmt"
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

	//Dis float64
	//DurDis int64

	Tag byte
	diff float64

	Key []byte
	//Same []byte
	endEle config.Element
	//CaMap map[int]float64

}
func NewSample(eles []config.Element,e config.Element) (sa *Sample) {
	if e != nil {
		sa = &Sample{
			//Dis:e.Diff(),
			//DurDis:e.Duration(),
			diff : eles[len(eles)-1].Diff(),
		}
	}else{
		sa = &Sample{
			diff : eles[len(eles)-1].Diff(),
		}
	}
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
	sa.Tag = func() (t byte) {
		f := sa.Y[0] < sa.Y[le]
		if f {
			t = 1
		}
		t = t<<1
		if (eles[len(eles)-1].Diff() >0) == f {
			t++
		}
		return t
	}()
	sa.Key = make([]byte,8)
	binary.BigEndian.PutUint64(sa.Key,uint64(sa.XMax))
	sa.Key = append(sa.Key,sa.Tag)
	//sa.Dis  = sa.xMax - sa.xMin
	return

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
func (self *Sample) KeyName() []byte {
	return self.Key
}

func (self *Sample) load(db []byte) {
	err := gob.NewDecoder(bytes.NewBuffer(db)).Decode(self)
	if err != nil {
		panic(err)
	}

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
