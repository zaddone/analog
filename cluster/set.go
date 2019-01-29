package cluster
import(
	//"fmt"
	"math"
	"encoding/binary"
	"encoding/gob"
	"bytes"
	"github.com/boltdb/bolt"
)

type Set struct {
	Sn *Snap
	samp []*Sample
	Samplist [][]byte
	Count [4]int
	KeyName []byte
}
func NewSet(sa *Sample) (S *Set) {
	S = &Set{
		samp:[]*Sample{sa},
		Samplist:[][]byte{sa.Key},
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
	S.SetCount(sa)
	return

}
func (self *Set)CheckCountMax(n int) bool {
	nv := self.Count[n]
	for _i,v := range self.Count {
		if _i == n {
			continue
		}
		if nv < v {
			return false
		}
	}
	return true
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
	S.Count[int(e.Tag)]++
}
func (self *Set)saveDB(sp *Pool){
	err := sp.PoolDB.Update(func(tx *bolt.Tx)error{
		db, err := tx.CreateBucketIfNotExists([]byte{sp.tag})
		if err != nil {
			return err
		}
		return db.Put(self.Key(),self.toByte())
	})
	if err != nil {
		panic(err)
	}
	return
}

func (self *Set) deleteDB(sp *Pool) {

	err := sp.PoolDB.Update(func(tx *bolt.Tx) error{
		db,err := tx.CreateBucketIfNotExists([]byte{sp.tag})
		if err != nil {
			return err
		}
		return db.Delete(self.Key())
	})
	if err != nil {
		panic(err)
	}

}
func (S *Set) Key() ([]byte){

	if S.KeyName == nil {
		k := make([]byte,8)
		binary.BigEndian.PutUint64(k,uint64(S.Sn.LengthX))
		S.KeyName =  append(k,S.Samplist[0][:8]...)
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
	S.Count = [4]int{0,0,0,0}
	S.KeyName = nil
	//S.samp = nil
}

func (self *Set) load(db []byte) {
	//self.samp = nil
	err := gob.NewDecoder(bytes.NewBuffer(db)).Decode(self)
	if err != nil {
		panic(err)
	}
}

func (self *Set) loadSamp(sp *Pool) bool {

	self.samp = make([]*Sample,len(self.Samplist))
	//sampTag := make([][]byte,Le)
	var sa *Sample
	var j int
	err := sp.SampDB.View(func(tx *bolt.Tx)error{
		db := tx.Bucket([]byte{1})
		if db == nil {
			return nil
		}
		for _,k := range self.Samplist {
			sa = &Sample{}
			v := db.Get(k)
			if len(v) == 0 {
				continue
			}
			//sampTag[j] = k
			sa.load(v)
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

func (S *Set) findLong() (sa *Sample,Max float64) {

	if len(S.samp) == 0 {
		return
	}
	var d float64
	var id int
	for i,s := range S.samp {
		d  = S.distance(s)
		if d > Max {
			Max = d
			sa = s
			id = i
		}
	}
	S.samp = append(S.samp[:id],S.samp[id+1:]...)
	return

}

func (self *Set) SectionCheck(e *Sample) bool {

	k_ := binary.BigEndian.Uint64(e.KeyName()[:8])
	var min,max uint64 = k_,k_
	for _,ks_ := range self.Samplist {
		k := binary.BigEndian.Uint64(ks_[:8])
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
	S.Samplist = make([][]byte,len(S.samp))
	for _i,s := range S.samp {
		S.Samplist[_i] = s.KeyName()
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

func (self *Set) distance(e *Sample) float64 {

	var longDis,l float64
	ld := float64(e.GetDB(int64(self.Sn.LengthX),func(x,y float64){
		longDis += math.Pow(self.Sn.GetWeiY(x/self.Sn.LengthX)-y/self.Sn.LengthY,2)
		l++
	}))
	ld /=5
	return (longDis+ld)/(l+ld)
	//return (longDis+ld)/(l+ld)

	//if longDis == 0 {
	//	longDis = 99
	//}else{
	//	longDis /= l
	//}
	//return math.Sqrt(math.Pow(longDis/l,2) + math.Pow(ld/self.Sn.LengthX,2))
	//return math.Sqrt(math.Pow(math.Sqrt(math.Pow(ld,2)+math.Pow(sd,2)),2)+math.Pow(math.Sqrt(math.Pow(longDis/l,2)+math.Pow(sortDis/s,2)),2))

}

