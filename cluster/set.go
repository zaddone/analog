package cluster
import(
	"fmt"
	"math"
	"encoding/binary"
	"encoding/gob"
	"bytes"
	"github.com/boltdb/bolt"
)
type saEasy struct {
	Key []byte
	//CaMap []byte
	//Dis float64
	//DurDis float64
}
type Set struct {
	Sn *Snap
	List  []*saEasy

	tag byte

	count [3]int
	key []byte

	samp []*Sample
}
func NewSetLoad(k,v []byte) (S *Set) {
	S = &Set{}
	S.load(k,v)
	return
}
func NewSet(sa *Sample) (S *Set) {
	S = &Set{
		tag:sa.tag>>1,
		samp:[]*Sample{sa},
		List:[]*saEasy{
			&saEasy{
				Key:sa.KeyName(),
				//CaMap:sa.caMap,
				//Dis:sa.dis,
				//DurDis:sa.durDis,
			}},
		Sn:&Snap{
			LengthX:float64(sa.xMax-sa.xMin),
			LengthY:sa.YMax - sa.YMin,
		},
	}
	X := make([]float64,0,len(sa.X))
	Y := make([]float64,0,len(sa.X))
	var i int
	var x int64
	for i,x = range sa.X {
		X = append(X,float64(x-sa.xMin)/S.Sn.LengthX)
		Y = append(Y,(sa.Y[i]-sa.YMin)/S.Sn.LengthY)
	}
	S.Sn.Wei = CurveFitting(X,Y)

	if len(S.Sn.Wei) == 0 {
		panic("w1")
	}
	return

}

//func (self *Set)CheckCountMax(n int) bool {
//	return self.count[n] > self.count[n^1]
//}

func (self *Set) FindSameKey(k []byte) bool {
	for _,_k := range self.List {
		if bytes.Equal(_k.Key,k){
			return true
		}
	}
	return false
}

func (self *Set)saveDB(sp *Pool){

	sp.updatePoolDB([]byte{self.tag},func(db *bolt.Bucket)error{
		return db.Put(self.Key(),self.toByte())
	})
	//sp.PoolCount++

}

func (self *Set) deleteDB(sp *Pool) {

	sp.updatePoolDB([]byte{self.tag},func(db *bolt.Bucket)error{
		return db.Delete(self.Key())
	})
	//sp.PoolCount--

}
func (S *Set) Key() ([]byte){

	if S.key == nil {
		S.key = make([]byte,8)
		binary.BigEndian.PutUint64(S.key,uint64(S.Sn.LengthX))
		S.key =  append(S.key,S.List[0].Key...)
	}
	return S.key

}
func (self *Set) toByte() []byte {

	var b bytes.Buffer
	err := gob.NewEncoder(&b).Encode(self)
	if err != nil {
		panic(err)
	}
	return b.Bytes()

}

func (S *Set) clear(){
	S.Sn = &Snap{}
	S.count = [3]int{0,0,0}
	S.key = nil
	S.List = nil
	S.samp = nil
}

func (self *Set) load(k,v []byte) {
	//self.samp = nil
	err := gob.NewDecoder(bytes.NewBuffer(v)).Decode(self)
	if err != nil {
		panic(err)
	}
	self.key = make([]byte,len(k))
	copy(self.key,k)
	//self.key = k
	self.tag = self.key[16]>>1
	//for _,l := range self.List{
	//	self.count[int(l.Key[8]) &^ 2]++
	//}
}

func (self *Set) loadSamp(sp *Pool) bool {

	self.samp = make([]*Sample,0,len(self.List))
	err := sp.SampDB.View(func(t *bolt.Tx)error{
		b := t.Bucket([]byte{9})
		if b == nil {
			return nil
		}
		for _,k := range self.List {
			v := b.Get(k.Key)
			if len(v) == 0 {
				continue
			}
			self.samp=append(self.samp,NewSampleDB(v,k))
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	//sp.viewPoolDB([]byte{9},func(db *bolt.Bucket) error {
	//	for _,k := range self.List {
	//		v := db.Get(k.Key)
	//		if len(v) == 0 {
	//			continue
	//		}
	//		sa = &Sample{}
	//		//sampTag[j] = k
	//		sa.load(v,k)
	//		self.samp=append(self.samp,sa)
	//	}
	//	return nil
	//})
	if len(self.samp) == 0 {
		//go self.deleteDB(sp)
		return false
	}
	//self.Samplist = sampTag[:j]
	return true

}

func (self *Set) sort(){
	le := len(self.samp)
	for i:=0;i<le;i++{
		for j:=i+1;j<le;j++{
			if self.samp[i].diff < self.samp[j].diff {
				self.samp[i],self.samp[j] = self.samp[j],self.samp[i]
			}
		}
	}

}
func (self *Set) checkDar(d float64) bool {

	var val,sum,psum,n float64
	for _,s := range self.samp {
		if s.diff == 0 {
			s.diff  = self.distance(s)
		}
		psum += s.diff*s.diff
		sum  += s.diff
	}
	n = float64(len(self.samp))
	val = psum/(n*n)-(sum*sum)/n

	sum += d
	psum += d * d

	n++
	return val > psum/(n*n)-(sum*sum)/n

}

func (S *Set) findLong() (sa *Sample,Max float64) {

	if len(S.samp) == 0 {
		return
	}
	//var id int
	for _,s := range S.samp {
		s.diff  = S.distance(s)
		if s.diff > Max {
			Max = s.diff
			sa = s
			//id = i
		}
	}
	//S.samp = append(S.samp[:id],S.samp[id+1:]...)
	return

}

func (S *Set) update(sa []*Sample) {

	S.clear()
	S.samp = sa
	S.List = make([]*saEasy,len(S.samp))
	var sum int64
	var df float64
	for _i,_s := range S.samp {
		S.List[_i] =&saEasy{
			Key:_s.KeyName(),
			//CaMap:_s.caMap,
			//Dis:_s.dis,
			//DurDis:s.durDis,
		}
		sum +=_s.Duration()
		df = _s.YMax - _s.YMin
		if S.Sn.LengthY < df {
			S.Sn.LengthY = df
		}
	}
	X := make([]float64,0,int(sum/5))
	Y := make([]float64,0,int(sum/5))

	sum /= int64(len(sa))
	//S.Sn.LengthX = float64(sum/ float64(len(sa))
	S.Sn.LengthX = float64(sum)
	for _,s := range S.samp {
		s.GetDB(sum,func(x,y float64){
			X = append(X,x/S.Sn.LengthX)
			Y = append(Y,y/S.Sn.LengthY)
		})
	}
	S.Sn.Wei = CurveFitting(X,Y)
	if len(S.Sn.Wei) == 0 {
		fmt.Println(X,Y)
		panic("w")
	}

}

func (self *Set) distanceF(e *Sample) float64 {
	var longDis,l float64
	//ld := float64(e.GetDBF(int64(self.Sn.LengthX),func(x,y float64){
	//	longDis += math.Pow(self.Sn.GetWeiY(x/self.Sn.LengthX)-y/self.Sn.LengthY,2)
	//	l++
	//}))
	//ld /=5
	//return (longDis+ld)/(l+ld)
	e.GetDBF(int64(self.Sn.LengthX),func(x,y float64){
		longDis += math.Pow(self.Sn.GetWeiY(x/self.Sn.LengthX)-y/self.Sn.LengthY,2)
		l++
	})
	return longDis/l
}
func (self *Set) distance(e *Sample) float64 {

	var longDis,l float64
	//ld := float64(e.GetDB(int64(self.Sn.LengthX),func(x,y float64){
	//	longDis += math.Pow(self.Sn.GetWeiY(x/self.Sn.LengthX)-y/self.Sn.LengthY,2)
	//	l++
	//}))
	//ld /=5
	//return (longDis+ld)/(l+ld)
	e.GetDB(int64(self.Sn.LengthX),func(x,y float64){
		longDis += math.Pow(self.Sn.GetWeiY(x/self.Sn.LengthX)-y/self.Sn.LengthY,2)
		l++
	})
	return longDis/l

}

