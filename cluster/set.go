package cluster
import(
	"fmt"
	"math"
	"sync"
	"encoding/binary"
	"encoding/gob"
	"bytes"
	"github.com/boltdb/bolt"
	"github.com/zaddone/analog/config"
)
type saEasy struct {
	Key []byte
	t uint64
	//CaMap []byte
	//Dis float64
	//DurDis float64
}

type Dar struct {
	sum,psum,n,val float64
}

func (self *Dar) update (diff float64) {
	self.sum += diff
	self.n ++
	self.psum += diff*diff
}
func (self *Dar)getVal() float64 {
	//if self.val == 0 {
	self.val = (self.psum/self.n) - ((self.sum*self.sum) / (self.n*self.n))
	//}
	return self.val
}
type Set struct {

	Sn *Snap
	List  []*saEasy

	tag byte

	count [3]int
	_key []byte

	samp []*Sample
	up bool
	tmp float64
	dar *Dar
	//num int
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
	sa.setMap.Store(S,true)
	sa.init = true
	//sa.setMap[S] = true
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

func (self *Set) saveDB (sp *Pool){

	sp.updatePoolDB([]byte{self.tag},func(db *bolt.Bucket)error{
		return db.Put(self.Key(),self.toByte())
	})
	//sp.PoolCount++

}
//
//func (self *Set) deleteDB(sp *Pool) {
//
//	sp.updatePoolDB([]byte{self.tag},func(db *bolt.Bucket)error{
//		return db.Delete(self.Key())
//	})
//	//sp.PoolCount--
//
//}
func (S *Set) Key() ([]byte){

	if S._key == nil {
		S._key = make([]byte,8,20)
		binary.BigEndian.PutUint64(S._key,uint64(S.Sn.LengthX))
		S._key =  append(S._key,S.List[0].Key...)
	}
	return S._key

}
func (self *Set) toByte() []byte {

	var b bytes.Buffer
	err := gob.NewEncoder(&b).Encode(self)
	if err != nil {
		panic(err)
	}
	return b.Bytes()

}

func (self *Set) SortDB(sp *Pool){

	le := len(self.List)
	Sli := make([]*saEasy,0,le)
	var sort func(int)
	sort = func(i int){
		if i == 0 {
			return
		}
		I:= i-1
		if Sli[I].t < Sli[i].t {
			return
		}
		Sli[I],Sli[i] = Sli[i],Sli[I]
		sort(I)
	}
	for _i,l := range self.List {
		l.t = binary.BigEndian.Uint64(l.Key[:8])
		Sli = append(Sli,l)
		sort(_i)
	}
	self.List = Sli
	n :=le - config.Conf.MinSam
	if n <= 0 {
		return
	}
	go func(k []*saEasy){
		if err := sp.SampDB.Batch(func(tx *bolt.Tx)error{
			db, err := tx.CreateBucketIfNotExists([]byte{9})
			if err != nil {
				return err
			}
			for _,k_ := range k {
				err = db.Delete(k_.Key)
				if err != nil {
					return err
				}
			}
			return nil
			//return db.Delete(k)
		});err != nil {
			panic(err)
		}
	}(self.List[:n])
	self.List = self.List[n:]

}

func (S *Set) clear(){
	S.Sn = &Snap{}
	S.count = [3]int{0,0,0}
	S._key = nil
	S.List = nil
	S.samp = nil
	S.up = false
	S.dar = nil
}

func (self *Set) load(k,v []byte) {
	//self.samp = nil
	err := gob.NewDecoder(bytes.NewBuffer(v)).Decode(self)
	if err != nil {
		panic(err)
	}
	self._key = make([]byte,len(k))
	copy(self._key,k)
	//self.key = k
	self.tag = self._key[16]>>1
	for _,l := range self.List{
		self.count[int(l.Key[8]>>1)]++
	}
}

func (self *Set) loadSamp(sp *Pool) bool {

	self.samp = make([]*Sample,0,len(self.List))
	//samp := sp.openSampDB()
	err := sp.SampDB.View(func(t *bolt.Tx)error{
		b := t.Bucket([]byte{9})
		if b == nil {
			return nil
		}
		for _,k := range self.List {
			v := b.Get(k.Key)
			if len(v) == 0 {
				//panic(9)
				continue
			}
			e := NewSampleDB(v,k)
			//e.setMap[self] = true
			e.setMap.Store(self,true)
			self.samp=append(self.samp,e)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	//samp.Close()
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

func (self *Set) SetDisAll(){
	var w sync.WaitGroup
	w.Add(len(self.samp))
	for _,_e := range self.samp {
		go func(e *Sample){
			e.diff  = self.distance(e)
			w.Done()
		}(_e)
	}
	w.Wait()
}

func (self *Set) GetDar() float64 {

	//if self.dar == nil {
	self.dar = &Dar{}
	for _,e := range self.samp {
		if e.diff == 0 {
			e.diff  = self.distance(e)
		}
		//fmt.Println("diff", s.diff)
		self.dar.update(e.diff)
		//self.dar.psum += s.diff*s.diff
		//self.dar.sum  += s.diff
	}
		//self.dar.n = float64(len(self.samp))
		//self.dar.getVal()
		//self.dar.val = self.dar.psum/(self.dar.n*self.dar.n)-(self.dar.sum*self.dar.sum)/self.dar.n
	//}
	return self.dar.getVal()
}

func (self *Set) checkDar(d float64) bool {
	val := self.GetDar()
	if val == 0 {
		return true
	}
	sum := self.dar.sum + d
	psum := self.dar.psum + (d * d)

	n := self.dar.n+1

	v1 := (psum/n) - ((sum*sum)/(n*n))
	//fmt.Println(val,v1)
	return val > v1

}

//func (S *Set) findLong() (sa *Sample,Max float64) {
//
//	if len(S.samp) == 0 {
//		return
//	}
//	//var id int
//	for _,s := range S.samp {
//		s.diff  = S.distance(s)
//		if s.diff > Max {
//			Max = s.diff
//			sa = s
//			//id = i
//		}
//	}
//	//S.samp = append(S.samp[:id],S.samp[id+1:]...)
//	return
//
//}

func (S *Set) update(sa []*Sample) {

	S.clear()
	S.samp = sa
	S.List = make([]*saEasy,len(S.samp))
	//fmt.Println("update",len(S.samp))
	var sum int64
	var df float64
	for _i,_s := range S.samp {
		S.List[_i] =&saEasy{
			Key:_s.KeyName(),
			//CaMap:_s.caMap,
			//Dis:_s.dis,
			//DurDis:s.durDis,
		}
		_s.setMap.Store(S,true)
		_s.diff = 0
		sum +=_s.Duration()
		df += _s.YMax - _s.YMin
		//if S.Sn.LengthY < df {
		//	S.Sn.LengthY = df
		//}
	}
	X := make([]float64,0,int(sum/5))
	Y := make([]float64,0,int(sum/5))
	le := len(sa)
	S.Sn.LengthY = df/float64(le)
	sum /= int64(le)
	//S.Sn.LengthX = float64(sum/ float64(len(sa))
	S.Sn.LengthX = float64(sum)
	for _,s := range S.samp {
		//s.setMap[S] = true
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
	//S.num++

}

//func (self *Set) distanceF(e *Sample) float64 {
//	var longDis,l float64
//	//ld := float64(e.GetDBF(int64(self.Sn.LengthX),func(x,y float64){
//	//	longDis += math.Pow(self.Sn.GetWeiY(x/self.Sn.LengthX)-y/self.Sn.LengthY,2)
//	//	l++
//	//}))
//	//ld /=5
//	//return (longDis+ld)/(l+ld)
//	e.GetDBF(int64(self.Sn.LengthX),func(x,y float64){
//		longDis += math.Pow(self.Sn.GetWeiY(x/self.Sn.LengthX)-y/self.Sn.LengthY,2)
//		l++
//	})
//	return longDis/l
//}

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

