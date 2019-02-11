package cluster
import(
	//"fmt"
	"math"
	"encoding/binary"
	"encoding/gob"
	"bytes"
	"github.com/boltdb/bolt"
)
type saEasy struct {
	Key []byte
	CaMap []byte
	Dis float64
	//DurDis float64
}
type Set struct {
	Sn *Snap
	samp []*Sample
	//Samplist [][]byte
	List  []*saEasy
	Count [2]int
	KeyName []byte
	Tag byte
}
func NewSet(sa *Sample) (S *Set) {
	S = &Set{
		Tag:sa.KeyName()[8]>>1,
		samp:[]*Sample{sa},
		List:[]*saEasy{
			&saEasy{
				Key:sa.KeyName(),
				CaMap:sa.caMap,
				Dis:sa.dis,
				//DurDis:sa.durDis,
			}},
		//Samplist:[][]byte{sa.KeyName()},
		Sn:&Snap{
			LengthX:float64(sa.XMax-sa.XMin),
			LengthY:sa.YMax - sa.YMin,
		},
	}
	var X,Y []float64
	sa.GetDB(sa.Duration(),func(x,y float64){
		X = append(X,x/S.Sn.LengthX)
		Y = append(Y,y/S.Sn.LengthY)
	})
	S.Sn.Wei = CurveFitting(X,Y,nil)
	//S.List[0].DurDis = S.distance(sa)
	S.SetCount(sa)
	return

}
func (self *Set)CheckCountMax(n int) bool {
	return self.Count[n] > self.Count[n^1]
}

func (self *Set) FindSameKey(k []byte) bool {
	for _,_k := range self.List {
		if bytes.Equal(_k.Key,k){
			return true
		}
	}
	return false
}
func (self *Set) FindSame(e *Sample,sp *Pool) (e_ *Sample) {
	if self.samp == nil && !self.loadSamp(sp) {
		return nil
	}
	S := NewSet(e)
	var min,d float64
	for _,_e := range self.samp {
		d = S.distance(_e)
		if min == 0  || min >d {
			min = d
			e_ = _e
		}
	}
	return
}
func (S *Set) SetCount(e *Sample) {
	S.Count[int(e.KeyName()[8] &^ 2)]++
}


func (self *Set)saveDB(sp *Pool){

	func(){
		err := sp.PoolDB.Update(func(tx *bolt.Tx)error{
			db, err := tx.CreateBucketIfNotExists([]byte{self.Tag})
			if err != nil {
				return err
			}
			return db.Put(self.Key(),self.toByte())
		})
		if err != nil {
			panic(err)
		}
	}()
	sp.PoolCount++

}

func (self *Set) deleteDB(sp *Pool) {

	func(){
		err := sp.PoolDB.Update(func(tx *bolt.Tx) error{
			db,err := tx.CreateBucketIfNotExists([]byte{self.Tag})
			if err != nil {
				return err
			}
			return db.Delete(self.Key())
		})
		if err != nil {
			panic(err)
		}
	}()
	sp.PoolCount--

}
func (S *Set) Key() ([]byte){

	if S.KeyName == nil {
		k := make([]byte,8)
		binary.BigEndian.PutUint64(k,uint64(S.Sn.LengthX))
		S.KeyName =  append(k,S.List[0].Key[:8]...)
	}
	return S.KeyName

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
	S.Count = [2]int{0,0}
	S.KeyName = nil
	S.List = nil
	S.samp = nil
}

func (self *Set) load(db []byte) {
	//self.samp = nil
	err := gob.NewDecoder(bytes.NewBuffer(db)).Decode(self)
	if err != nil {
		panic(err)
	}
}

func (self *Set) loadSamp(sp *Pool) bool {

	self.samp = make([]*Sample,len(self.List))
	var sa *Sample
	var j int
	err := sp.PoolDB.View(func(tx *bolt.Tx)error{
		db := tx.Bucket([]byte{9})
		if db == nil {
			return nil
		}
		for _,k := range self.List {
			sa = &Sample{}
			v := db.Get(k.Key)
			if len(v) == 0 {
				continue
			}
			//sampTag[j] = k
			sa.load(v,k)
			self.samp[j] = sa
			j++
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	if j == 0 {
		self.deleteDB(sp)
		return false
	}
	//self.Samplist = sampTag[:j]
	self.samp = self.samp[:j]
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
		s.diff  = self.distance(s)
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

func (S *Set) dar() (val,sum,psum float64) {
	for _,s := range S.samp {
		s.diff  = S.distance(s)
		psum += s.diff*s.diff
		sum  += s.diff
	}
	n := float64(len(S.samp))
	val = psum/(n*n)-(sum*sum)/n
	return

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

func (self *Set) SectionDiff() uint64 {

	min := binary.BigEndian.Uint64(self.List[0].Key[:8])
	max := min
	for _,ks_ := range self.List[1:] {
		k := binary.BigEndian.Uint64(ks_.Key[:8])
		if min>k {
			min = k
		}else if max<k {
			max = k
		}
	}
	return max-min
}

func (self *Set) SectionCheck(e *Sample) bool {

	k_ := binary.BigEndian.Uint64(e.KeyName()[:8])
	var min,max uint64 = k_,k_
	for _,ks_ := range self.List {
		k := binary.BigEndian.Uint64(ks_.Key[:8])
		if min>k {
			min = k
		}else if max<k {
			max = k
		}
	}
	if (min == k_) || (max == k_) {
		return false
	}
	return true

}

func (S *Set) update(sa []*Sample) {

	S.clear()
	S.samp = sa
	S.List = make([]*saEasy,len(S.samp))
	for _i,s := range S.samp {
		S.List[_i] =&saEasy{
			Key:s.KeyName(),
			CaMap:s.caMap,
			Dis:s.dis,
			//DurDis:s.durDis,
		}
		S.Sn.LengthX += float64(s.Duration())
		S.SetCount(s)
	}
	le := float64(len(sa))
	S.Sn.LengthX /= le

	var X,Y []float64
	for _,s := range S.samp {
		s.GetDB(int64(S.Sn.LengthX),func(x,y float64){
			X = append(X,x)
			Y = append(Y,y)
			if y>S.Sn.LengthY {
				S.Sn.LengthY = y
			}
		})
	}

	for i,x := range X {
		X[i] = x / S.Sn.LengthX
		Y[i] = Y[i] / S.Sn.LengthY
	}
	S.Sn.Wei = CurveFitting(X,Y,nil)

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

