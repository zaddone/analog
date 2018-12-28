package snap
import(
	"github.com/zaddone/analog/config"
	"github.com/boltdb/bolt"
	"fmt"
	"math"
	//"encoding/json"
	"encoding/binary"
	"encoding/gob"
	"bytes"
	"path/filepath"
	"os"
	"time"
)
var (
	SetLen int = 0
	MaxTime int64
	MaxApp int64
)

type Set struct {

	LongSn *Snap
	SortSn *Snap

	samp []*Sample
	Samplist [][]byte
	Count [2]int

}
func NewSet(sa *Sample) (S *Set) {
	S = &Set{
		samp:[]*Sample{sa},
		Samplist:[][]byte{sa.Key},
		LongSn:&Snap{
			LengthX:float64(sa.LongDuration()),
			LengthY:sa.YLongMax - sa.YLongMin,
		},
		SortSn:&Snap{
			LengthX:float64(sa.SortDuration()),
			LengthY:sa.YSortMax - sa.YSortMin,
		},
	}

	var Xl,Yl,Xs,Ys []float64

	sa.GetLongDB(sa.LongDuration(),func(x,y float64){
		Xl = append(Xl,x/S.LongSn.LengthX)
		Yl = append(Yl,y/S.LongSn.LengthY)
	})
	S.LongSn.CreateMatrix(CurveFittingMax(Xl,Yl,nil,0))

	sa.GetSortDB(sa.SortDuration(),func(x,y float64){
		Xs = append(Xs,x/S.SortSn.LengthX)
		Ys = append(Ys,y/S.SortSn.LengthY)
	})
	S.SortSn.CreateMatrix(CurveFittingMax(Xs,Ys,nil,0))
	SetLen++
	fmt.Println(time.Unix(int64(binary.BigEndian.Uint64(sa.KeyName()[:8])),0),SetLen)
	return

}
func (self *Set) LoadSamp(sp *SetPool) {

	self.samp = make([]*Sample,len(self.Samplist))
	//sampTag := make([][]byte,Le)
	var sa *Sample
	var j int
	err := config.ViewKvDBWithName(
		sp.SampDB,
		sp.u,
		func(db *bolt.Bucket)error{
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
		},
	)
	if err != nil {
		panic(err)
	}
	//self.Samplist = sampTag[:j]
	self.samp = self.samp[:j]

}
//func (self *Set) DeleteDB(sp *SetPool,OldKey []byte){
//	err := config.UpdateKvDBWithName(
//		sp.PoolDB,
//		sp.u,
//		func(db *bolt.Bucket)error{
//			//fmt.Println(OldKey)
//			return db.Delete(OldKey)
//			//if err != nil {
//			//	//fmt.Println(err)
//			//	panic(err)
//			//}
//		},
//	)
//	if err != nil {
//		panic(err)
//	}
//	SetLen--
//}
func (self *Set) UpdateDB(sp *SetPool,OldKey []byte){
	err := config.UpdateKvDBWithName(
		sp.PoolDB,
		sp.u,
		func(db *bolt.Bucket)error{
			//fmt.Println(OldKey)
			if OldKey != nil {
				err := db.Delete(OldKey)
				if err != nil {
					//fmt.Println(err)
					panic(err)
				}
			}
			return db.Put(self.Key(),self.String())
		},
	)
	if err != nil {
		panic(err)
	}
	fmt.Println("update",self.Count,SetLen,time.Unix(int64(binary.BigEndian.Uint64(self.Samplist[0][:8])),0))
	return
}
func (self *Set) SaveDB(sp *SetPool){
	err := config.UpdateKvDBWithName(
		sp.PoolDB,
		sp.u,
		func(db *bolt.Bucket)error{
			return db.Put(self.Key(),self.String())
		},
	)
	if err != nil {
		panic(err)
	}
	return
}
func (self *Set) Load(db []byte) {
	//self.samp = nil
	err := gob.NewDecoder(bytes.NewBuffer(db)).Decode(self)
	if err != nil {
		panic(err)
	}
}

func (self *Set) String() []byte {

	var b bytes.Buffer
	err := gob.NewEncoder(&b).Encode(self)
	if err != nil {
		panic(err)
	}
	return b.Bytes()


}
func (S *Set) Key() (k []byte){

	k = make([]byte,8)
	binary.BigEndian.PutUint64(k,uint64(S.LongSn.LengthX + S.SortSn.LengthX))
	return append(k,S.Samplist[0][:8]...)

}

func (S *Set) clear(){
	S.LongSn = &Snap{}
	S.SortSn = &Snap{}
	S.Count = [2]int{0,0}
	//S.samp = nil
}
func (S *Set) update(sa []*Sample) {
	S.clear()
	S.samp = sa
	S.Samplist = make([][]byte,len(S.samp))
	for _i,s := range S.samp {
		S.Samplist[_i] = s.KeyName()
		S.LongSn.LengthX += float64(s.LongDuration())
		S.SortSn.LengthX += float64(s.SortDuration())

		if s.Dis>0 {
			S.Count[0]++
		}else{
			S.Count[1]++
		}
	}
	le := float64(len(sa))
	S.LongSn.LengthX /= le
	S.SortSn.LengthX /= le

	var Xl,Yl,Xs,Ys []float64

	for _,s := range S.samp {
		s.GetLongDB(int64(S.LongSn.LengthX),func(x,y float64){
			Xl = append(Xl,x)
			Yl = append(Yl,y)
			if y>S.LongSn.LengthY {
				S.LongSn.LengthY = y
			}
		})
		s.GetSortDB(int64(S.SortSn.LengthX),func(x,y float64){
			Xs = append(Xs,x)
			Ys = append(Ys,y)
			if y>S.SortSn.LengthY {
				S.SortSn.LengthY = y
			}
		})
	}

	for i,x := range Xl {
		Xl[i] = x / S.LongSn.LengthX
		Yl[i] = Yl[i] / S.LongSn.LengthY
	}
	S.LongSn.CreateMatrix(CurveFittingMax(Xl,Yl,nil,0))

	for i,x := range Xs {
		Xs[i] = x / S.SortSn.LengthX
		Ys[i] = Ys[i] / S.SortSn.LengthY
	}
	S.SortSn.CreateMatrix(CurveFittingMax(Xs,Ys,nil,0))

	//var d float64
	//var id  int
	//for i,s := range S.Samp {
	//	d  = S.distance(s)
	//	if d > S.MaxVal {
	//		S.MaxVal = d
	//		S.MaxLong = s
	//		id = i
	//	}
	//	if s.Dis {
	//		S.Count[0]++
	//	}else{
	//		S.Count[1]++
	//	}
	//}
	//S.Samp = append(S.Samp[:id],S.Samp[id+1:]...)

}
func (S *Set) FindLong() (sa *Sample,id int) {

	var d,Max float64
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

func (self *Set) distance(e *Sample) (errDis float64) {

	fh := func(n float64) float64 {
		if n>1{
			return 1
		}
		return n
	}
	errDis += float64(e.GetLongDB(int64(self.LongSn.LengthX),func(x,y float64){
		//fmt.Println(self.LongSn.LengthX,len(self.LongSn.Matrix),x)
		errDis += fh(math.Abs(self.LongSn.Matrix[int(x)] - y)/self.LongSn.LengthY)
	}))

	errDis += float64(e.GetSortDB(int64(self.SortSn.LengthX),func(x,y float64){
		//fmt.Println(len(self.SortSn.Matrix),x)
		errDis += fh(math.Abs(self.SortSn.Matrix[int(x)] - y)/self.SortSn.LengthY)
	}))
	return

}

type SetPool struct {
	//pool []*Set
	SampDB string
	PoolDB string
	u []byte
	LastKey []byte
	//e *Sample
	CountApp int
}
func NewSetPool(ins string,tag byte) (sp *SetPool) {
	p:=filepath.Join("db",ins)
	_,err := os.Stat(p)
	if err != nil {
		err = os.MkdirAll(p,0700)
		if err != nil {
			panic(err)
		}
	}
	sp = &SetPool {
		u:[]byte{tag},
		//e:e,
		SampDB:filepath.Join(p,config.Conf.SampleDbPath),
		PoolDB:filepath.Join(p,config.Conf.PoolDbPath),
	}
	return sp
}

func FindSetPool(ins string, e *Sample) (s *Set) {
	s,_ = NewSetPool(ins,e.Tag).Find(e)
	return
}

func LoadSetPool(ins string, e *Sample){
	sp := NewSetPool(ins,e.Tag)
	keyE :=e.KeyName()
	DateKey := time.Unix( int64(binary.BigEndian.Uint64(keyE[:8])),0)
	ke :=uint64(DateKey.AddDate(-1,0,0).Unix())
	config.UpdateKvDBWithName(
		sp.SampDB,
		sp.u,
		func(db *bolt.Bucket)error{
			c := db.Cursor()
			for{
				k,_ := c.First()
				if k == nil {
					break
				}
				if binary.BigEndian.Uint64(k[:8])<ke {
					fmt.Println("delete",k,keyE)
					db.Delete(k)
				}else{
					break
				}
			}
			return db.Put(keyE,e.String())
		},
	)
	timeB := time.Now().Unix()
	sp.Add(e)
	dif := time.Now().Unix() - timeB
	if dif > MaxTime {
		MaxTime = dif
		fmt.Println("times",MaxTime,sp.CountApp)
	}
}
func (self *SetPool) Find(e *Sample) (MinSet *Set,MinKey []byte) {

	dur := uint64(e.LongDur + e.SortDur)
	key := make([]byte,16)
	binary.BigEndian.PutUint64(key,dur)
	var diff,diffErr float64
	S :=  &Set{}
	MinSet = &Set{}
	MinKey = make([]byte,16)
	var k,v []byte
	err := config.ViewKvDBWithName(
		self.PoolDB,
		self.u,
		func(db *bolt.Bucket)error{
			c := db.Cursor()
			//k,v = c.Seek(key)
			//fmt.Println("next",key)
			for k,v = c.Seek(key);k!= nil;k,v = c.Next() {
				//fmt.Println(k,diff)
				if (diffErr!=0) &&
				(float64(binary.BigEndian.Uint64(k[:8]) - dur) > diffErr) {
					break
				}
				S.Load(v)
				diff = S.distance(e)
				if (diffErr == 0) || (diff < diffErr) {
					MinSet.Load(v)
					diffErr = diff
					copy(MinKey , k)
				}
			}
			c.Seek(key)
			//fmt.Println("prev",key)
			for k,v = c.Prev(); k!= nil;k,v = c.Prev() {
				//fmt.Println(k,diff)
				if (diffErr!=0) &&
				(float64(dur - binary.BigEndian.Uint64(k[:8])) > diffErr) {
					break
				}
				S.Load(v)
				diff = S.distance(e)
				if (diffErr == 0) || (diff < diffErr) {
					MinSet.Load(v)
					diffErr = diff
					copy(MinKey , k)
				}
			}
			return nil
		},
	)
	if err != nil {
		panic(err)
	}
	if diffErr == 0 {
		//MinSet = NewSet(e)
		//MinSet.SaveDB(self)
		//fmt.Println("nil")
		return nil,nil
	}
	return

}
func (self *SetPool) Add(e *Sample) {

	self.CountApp++

	MinSet,MinKey := self.Find(e)
	if MinSet == nil {
		NewSet(e).SaveDB(self)
		return
	}
	if bytes.Equal(MinKey,self.LastKey) {
		NewSet(e).SaveDB(self)
		return
	}
	//self.LastKey = nil
	MinSet.LoadSamp(self)
	if len(MinSet.samp)   == 0 {
		err := config.UpdateKvDBWithName(
			self.PoolDB,
			self.u,
			func(db *bolt.Bucket)error{
				return db.Delete(MinKey)
			},
		)
		if err != nil {
			panic(err)
		}
		SetLen--
		self.Add(e)
		return
	}
	MinSet.update(append(MinSet.samp,e))
	//MinSet.UpdateDB(self,MinKey)

	_e,_ := MinSet.FindLong()

	if func()bool{
		if _e.Dis>0{
			return MinSet.Count[0] > MinSet.Count[1]
		}
		return MinSet.Count[0] < MinSet.Count[1]
	}(){
		//fmt.Println(MinKey)
		MinSet.UpdateDB(self,MinKey)
		return
	}


	MinSet.update(MinSet.samp)
	MinSet.UpdateDB(self,MinKey)

	self.LastKey = MinSet.Key()
	fmt.Println("add",MinSet.Count,SetLen)
	self.Add(_e)

}

