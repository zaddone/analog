package snap
import(

	"github.com/zaddone/analog/config"
	//"encoding/json"
	"encoding/gob"
	//"fmt"
	"bytes"

)

type Sample struct {

	//longEs []config.Element
	LongDur int64
	XLongMin int64
	XLongMax int64
	YLongMin float64
	YLongMax float64
	LongX []int64
	LongY []float64

	//sortEs []config.Element
	SortDur int64
	XSortMin int64
	XSortMax int64
	YSortMin float64
	YSortMax float64
	SortX []int64
	SortY []float64


	Dis float64
	Tag byte
	Diff float64
	endEle config.Element
	Key []byte

}

func NewSample(le []config.Element,se []config.Element,diff float64,key []byte,tag byte) (sa *Sample) {

	sa = &Sample{
		Diff:diff,
		Tag:tag,
		Key:key,
		//longDur:le,
		//sortDur:se,
	}
	var y float64

	Len := len(le)
	end :=le[Len-1]
	sa.XLongMax = end.DateTime()+end.Duration()
	sa.XLongMin = le[0].DateTime()
	sa.LongDur = sa.XLongMax - sa.XLongMin
	sa.LongX = make([]int64,Len)
	sa.LongY = make([]float64,Len)
	for i,e := range le {
		//x = e_.DateTime()
		e.Read(func(e_ config.Element){
			y = e_.Middle()
			if (sa.YLongMin == 0) || (y < sa.YLongMin){
				sa.YLongMin = y
			}
			if (sa.YLongMax < y) {
				sa.YLongMax = y
			}
			sa.LongY[i] = y
			sa.LongX[i] = e_.DateTime()
		})
	}

	//ends :=se[len(se)-1]
	//sa.sortDur = ends.DateTime()+ends.Duration() - se[0].DateTime()
	Len = len(se)
	end = se[Len-1]
	sa.endEle = end
	sa.XSortMax = end.DateTime()+end.Duration()
	sa.XSortMin = se[0].DateTime()
	sa.SortDur = sa.XSortMax - sa.XSortMin
	sa.SortX = make([]int64,Len)
	sa.SortY = make([]float64,Len)
	for i,e := range se {
		e.Read(func(e_ config.Element){
			y = e_.Middle()
			if (sa.YSortMin == 0) || (y < sa.YSortMin){
				sa.YSortMin = y
			}
			if (sa.YSortMax < y) {
				sa.YSortMax = y
			}
			sa.SortY[i] = y
			sa.SortX[i] = e_.DateTime()
		})
	}

	return

}
func (self *Sample) GetEndEle() (config.Element) {
	return self.endEle
}

func (self *Sample) load(db []byte) {
	err := gob.NewDecoder(bytes.NewBuffer(db)).Decode(self)
	if err != nil {
		panic(err)
	}
}
func (self *Sample) String() []byte {
	var b bytes.Buffer
	err := gob.NewEncoder(&b).Encode(self)
	if err != nil {
		panic(err)
	}
	return b.Bytes()
}

func (self *Sample) KeyName() (k []byte) {
	return self.Key
	//return fmt.Sprintf("%d%d",len(self.SortX),self.XSortMax)
}

//func (self *Sample) Save() (name string) {
//	config.UpdateKvDBWithName(config.Conf.SampleDbPath,
//}

//func (self *Sample) Key() []byte {
//	k := []byte(fmt.Sprintf("%d",self.LongDur + self.SortDur))
//	le := l-len(k)
//	if le <= 0 {
//		return k[-le:]
//	}
//	k_ := make([]byte,le)
//	return append(k_,k...)
//
//}
func (self *Sample) LongDuration() int64 {
	return self.LongDur
}
func (self *Sample) SortDuration() int64 {
	return self.SortDur
}
func findk(k int64,li []int64) (le int) {
	le = len(li)/2
	v := le
	var x int64
	var f bool
	for{
		if le == 0 {
			break
		}
		x =li[le]
		if ( x == k) {
			break
		}else{
			if v == 1 {
				if (x > k) == f {
					if f {
						le --
					}else{
						le ++
					}
				}else{

					if !f {
						le --
					}
					break
				}
			}else{
				v = v/2
				f  = x > k
				if f {
					le -= v
				}else{
					le += v
				}
			}

		}
	}
	return
}
func find(k int64,li []int64) (le int) {
	Len :=len(li)
	le = Len/2
	v := le
	Len--
	var x int64
	var f bool
	for{
		if le > Len{
			le = Len
			break
		}
		x =li[le]
		if ( x == k) {
			break
		}else{
			if v == 1 {
				if (x > k) == f {
					if f {
						le --
					}else{
						le ++
					}
				}else{

					if f {
						le ++
					}
					break
				}
			}else{
				v = v/2
				f  = x > k
				if f {
					le -= v
				}else{
					le += v
				}
			}

		}
	}
	return
}
func (self *Sample) GetSortDB(dur int64,f func(x,y float64)) (durdiff int64) {
	durdiff = self.SortDur - dur
	if durdiff <= 0 {
		for i,x := range self.SortX {
			f(float64(x - self.XSortMin),self.SortY[i]-self.YSortMin)
		}
		return -durdiff
	}
	var l int
	//l := findk(self.XSortMax - dur,self.SortX)
	//if !_f {
	//	l--
	//}
	var yMin  float64
	kill := self.XSortMin + dur
	for i,y := range self.SortY {
		if self.SortX[i] >= kill {
			l = i
			break
		}
		if (y < yMin) || (yMin == 0) {
			yMin = y
		}
	}
	for i,x := range self.SortX[:l] {
		//fmt.Println(i,l)
		f(float64(x-self.XSortMin),self.SortY[i]-yMin)
	}

	return
}
func (self *Sample) GetLongDB(dur int64,f func(x ,y float64)) (durdiff int64) {

	durdiff = self.LongDur - dur
	if durdiff <= 0 {
		for i,x := range self.LongX {
			f(float64(x - self.XLongMin),self.LongY[i] -self.YLongMin)
		}
		return -durdiff
	}
	//var l int
	kill := self.XLongMin + durdiff
	//l := find(self.XLongMax - dur,self.LongX)
	//if !_f {
	//	l++
	//}
	var yMin float64
	//xMin := self.LongX[l]
	//fmt.Println(self.XLongMin,kill,self.XLongMax)
	Le := len(self.LongX)
	var X []int64 = make([]int64,0,Le)
	var Y []float64 = make([]float64,0,Le)
	var x int64
	for i,y := range self.LongY{
		x = self.LongX[i]
		if x >= kill {
			if (y < yMin) || (yMin == 0) {
				yMin = y
			}
			X = append(X,x)
			Y = append(Y,y)
		}
	}
	for i,y := range Y {
		f(float64(X[i]-kill),y-yMin)
	}
	//for i:=l+1;i<len(self.LongX);i++{
	//	//fmt.Println(self.LongX[i:])
	//	f(float64(self.LongX[i]-xMin),self.LongY[i]-yMin)
	//}
	return

}
