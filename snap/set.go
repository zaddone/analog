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
	Count [6]int
	KeyName []byte

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
	S.LongSn.Wei = CurveFittingMax(Xl,Yl,nil,0)

	sa.GetSortDB(sa.SortDuration(),func(x,y float64){
		Xs = append(Xs,x/S.SortSn.LengthX)
		Ys = append(Ys,y/S.SortSn.LengthY)
	})
	S.SortSn.Wei = CurveFittingMax(Xs,Ys,nil,0)
	S.SetCount(sa)

	fmt.Println("New",time.Unix(int64(binary.BigEndian.Uint64(sa.KeyName()[:8])),0),SetLen)
	return

}
func (self *Set) DeleteDB(sp *SetPool) {
	err := config.UpdateKvDBWithName(
		sp.PoolDB,
		sp.u,
		func(db *bolt.Bucket)error{
			return db.Delete(self.Key())
		},
	)
	if err != nil {
		panic(err)
	}
	SetLen--
	fmt.Println("delete",self.Key(),SetLen)
}
func (self *Set) LoadSamp(sp *SetPool) bool {

	self.samp = make([]*Sample,len(self.Samplist))
	//sampTag := make([][]byte,Le)
	var sa *Sample
	var j int
	err := config.ViewKvDBWithName(
		sp.SampDB,
		//sp.u,
		[]byte{'1'},
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
	if j == 0 {
		self.DeleteDB(sp)
		return false
	}
	//self.Samplist = sampTag[:j]
	self.samp = self.samp[:j]
	return true

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
	SetLen++
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
	//fmt.Println("setlen",b.Len())
	return b.Bytes()


}
func (S *Set) Key() (k []byte){

	if S.KeyName == nil {
		k = make([]byte,8)
		binary.BigEndian.PutUint64(k,uint64(S.LongSn.LengthX + S.SortSn.LengthX))
		S.KeyName =  append(k,S.Samplist[0][:8]...)
	}
	return S.KeyName

}

func (S *Set) clear(){
	S.LongSn = &Snap{}
	S.SortSn = &Snap{}
	S.Count = [6]int{0,0,0,0,0,0}
	S.KeyName = nil
	//S.samp = nil
}

func (S *Set) SetCount(e *Sample) {
	if e.Dis>0 {
		S.Count[0]++
	}else{
		S.Count[1]++
	}
	S.Count[int(e.Tag)+2]++
}
func (S *Set) update(sa []*Sample) {
	S.clear()
	S.samp = sa
	S.Samplist = make([][]byte,len(S.samp))
	for _i,s := range S.samp {
		S.Samplist[_i] = s.KeyName()
		S.LongSn.LengthX += float64(s.LongDuration())
		S.SortSn.LengthX += float64(s.SortDuration())
		S.SetCount(s)
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
	S.LongSn.Wei = CurveFittingMax(Xl,Yl,nil,0)
	//fmt.Println("Weilong",len(S.LongSn.Wei))
	//S.LongSn.CreateMatrix(CurveFittingMax(Xl,Yl,nil,0))

	for i,x := range Xs {
		Xs[i] = x / S.SortSn.LengthX
		Ys[i] = Ys[i] / S.SortSn.LengthY
	}
	S.SortSn.Wei = CurveFittingMax(Xs,Ys,nil,0)
	//fmt.Println("Weisort",len(S.SortSn.Wei))
	//S.SortSn.CreateMatrix(CurveFittingMax(Xs,Ys,nil,0))


}
func (S *Set) FindLong() (sa *Sample,Max float64) {

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

func (self *Set) distance(e *Sample) float64 {

	var longDis,sortDis,l,s float64
	ld := e.GetLongDB(int64(self.LongSn.LengthX),func(x,y float64){
		longDis += math.Abs(self.LongSn.GetWeiY(x/self.LongSn.LengthX)-y/self.LongSn.LengthY)
		l++
	})
	if longDis == 0 {
		longDis = 9999
	}
	//fmt.Println("ld",longDis,l)
	sd := e.GetSortDB(int64(self.SortSn.LengthX),func(x,y float64){
		sortDis += math.Abs(self.SortSn.GetWeiY(x/self.SortSn.LengthX)-y/self.SortSn.LengthY)
		s++
	})
	if sortDis == 0 {
		sortDis = 9999
	}

	//fmt.Println("sd",sortDis,s)
	//k1 := float64(ld+sd)/(self.LongSn.LengthX+self.SortSn.LengthX)
	//k2 := (longDis+sortDis)/(l+s)
	//k3 :=math.Sqrt2(math.Pow(k1,2)+math.Pow(k2,2))
	return math.Sqrt(math.Pow(float64(ld+sd)/(self.LongSn.LengthX+self.SortSn.LengthX),2)+math.Pow((longDis+sortDis)/(l+s),2))

}

type SetPool struct {
	//pool []*Set
	SampDB string
	PoolDB string
	u []byte
	LastKey [][]byte
	//e *Sample
	CountApp int
	Diff float64
}
func NewSetPool(ins string) (sp *SetPool) {
	p:=filepath.Join("db",ins)
	_,err := os.Stat(p)
	if err != nil {
		err = os.MkdirAll(p,0700)
		if err != nil {
			panic(err)
		}
	}
	sp = &SetPool {
		u:[]byte("pool"),
		//e:e,
		SampDB:filepath.Join(p,config.Conf.SampleDbPath),
		PoolDB:filepath.Join(p,config.Conf.PoolDbPath),
	}
	return sp
}

func FindSetPool(ins string, e *Sample) (s *Set) {
	s,_ = NewSetPool(ins).Find(e)
	return
}

func LoadSetPool(ins string, e *Sample){
	sp := NewSetPool(ins)
	keyE :=e.KeyName()
	DateKey := time.Unix( int64(binary.BigEndian.Uint64(keyE[:8])),0)
	ke :=uint64(DateKey.AddDate(-4,0,0).Unix())
	config.UpdateKvDBWithName(
		sp.SampDB,
		[]byte{'1'},
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
func (self *SetPool) Find(e *Sample) (MinSet *Set,diffErr float64) {

	dur := uint64(e.LongDur + e.SortDur)
	key := make([]byte,16)
	binary.BigEndian.PutUint64(key,dur)
	var diff float64
	S :=  &Set{}
	MinSet = &Set{}
	MinKey := make([]byte,16)
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
		return nil,diffErr
	}

	//err = config.UpdateKvDBWithName(
	//	self.PoolDB,
	//	self.u,
	//	func(db *bolt.Bucket)error{
	//		return db.Delete(MinKey)
	//	},
	//)
	//if err != nil {
	//	panic(err)
	//}
	//SetLen--

	return

}
func (self *SetPool) SameKey(k []byte) bool {
	for _,k_ := range self.LastKey {
		if bytes.Equal(k,k_){
			return true
		}
	}
	return false
}
func (self *SetPool) add(e *Sample) bool {

	MinSet,diff := self.Find(e)
	if MinSet == nil {
		NewSet(e).SaveDB(self)
		return true
	}
	if !MinSet.LoadSamp(self) {
		return self.add(e)
	}
	//defer MinSet.UpdateDB(self)
	if (self.Diff!=0) && (diff>self.Diff) {
		return false
	}

	TmpSet := &Set{}
	TmpSet.update(append(MinSet.samp,e))
	_e,maxdiff := TmpSet.FindLong()
	if _e == e {
		NewSet(e).SaveDB(self)
		return true
	}
	MinSet.DeleteDB(self)

	//sp := &self
	for{
		self.Diff = maxdiff
		//self.Diff = maxdiff
		if self.add(_e) {
			le :=len(TmpSet.samp)
			if le == 0 {
				break
			}else if le == 1 {
				_e = TmpSet.samp[0]
				continue
			}
			//MinSet.SaveDB(self)
			TmpSet.update(TmpSet.samp)
			_e,maxdiff = TmpSet.FindLong()
		}else{
			TmpSet.SaveDB(self)
			break
		}
	}
	return true

}
func (self *SetPool) Add(e *Sample) {

	if !self.add(e) {
		panic(0)
	}

}

