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
	"time")
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
			LengthY:sa.yLongMax - sa.yLongMin,
		},
		SortSn:&Snap{
			LengthX:float64(sa.SortDuration()),
			LengthY:sa.ySortMax - sa.ySortMin,
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

	//fmt.Println("New",time.Unix(int64(binary.BigEndian.Uint64(sa.KeyName()[:8])),0),SetLen)
	return

}
func (self *Set) DeleteDB(sp *SetPool) {
	err := sp.PoolDB.Update(func(tx *bolt.Tx) error{
		db,err := tx.CreateBucketIfNotExists([]byte{1})
		if err != nil {
			return err
		}
		return db.Delete(self.Key())
	})
	if err != nil {
		panic(err)
	}
	sp.PoolDB.Sync()
	SetLen--
	//fmt.Println("delete",self.Key(),SetLen)
}
func (self *Set) LoadSamp(sp *SetPool) bool {

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
		self.DeleteDB(sp)
		return false
	}
	//self.Samplist = sampTag[:j]
	self.samp = self.samp[:j]
	return true

}

func (self *Set) SaveDB(sp *SetPool){
	err := sp.PoolDB.Update(func(tx *bolt.Tx)error{
		db, err := tx.CreateBucketIfNotExists([]byte{1})
		if err != nil {
			return err
		}
		return db.Put(self.Key(),self.String())
	})
	if err != nil {
		panic(err)
	}
	sp.PoolDB.Sync()
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
	ld := float64(e.GetLongDB(int64(self.LongSn.LengthX),func(x,y float64){
		longDis += math.Abs(self.LongSn.GetWeiY(x/self.LongSn.LengthX)-y/self.LongSn.LengthY)
		l++
	}))
	ld /= self.LongSn.LengthX
	if longDis == 0 {
		longDis = 99
	}
	sd := float64(e.GetSortDB(int64(self.SortSn.LengthX),func(x,y float64){
		sortDis += math.Abs(self.SortSn.GetWeiY(x/self.SortSn.LengthX)-y/self.SortSn.LengthY)
		s++
	}))
	sd /= self.SortSn.LengthX
	if sortDis == 0 {
		sortDis = 99
	}
	//long := math.Sqrt(math.Pow(ld,2)+math.Pow(sd,2))
	//dis := math.Sqrt(math.Pow(longDis/l,2)+math.Pow(sortDis/s,2))
	return math.Sqrt(math.Pow(math.Sqrt(math.Pow(ld,2)+math.Pow(sd,2)),2)+math.Pow(math.Sqrt(math.Pow(longDis/l,2)+math.Pow(sortDis/s,2)),2))

}

type SetPool struct {
	//pool []*Set

	SampDB *bolt.DB
	PoolDB *bolt.DB

	//u []byte
	//LastKey [][]byte
	//e *Sample
	//CountApp int

	Diff float64
	tmpSample map[string]*Sample
}

func (self *SetPool) FindSame(sa *Sample) (sa_ *Sample) {
	_S,_:= self.Find(sa)
	if _S == nil {
		return nil
	}
	if !_S.LoadSamp(self){
		//return nil
		return self.FindSame(sa)
	}
	S := NewSet(sa)
	var d,mid float64
	for _,s := range _S.samp {
		d = S.distance(s)
		if (mid == 0) || (d<mid) {
			sa_ = s
			mid = d
		}
	}
	if sa.Tag != sa_.Tag{
		return nil
	}
	return


}
func (self *SetPool)Close(){
	self.SampDB.Close()
	self.PoolDB.Close()
}
func (self *SetPool)clear(){
	self.Diff = 0
	self.tmpSample = map[string]*Sample{}
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
		//u:[]byte("pool"),
		tmpSample:map[string]*Sample{},
		//e:e,
		//SampDB:filepath.Join(p,config.Conf.SampleDbPath),
		//PoolDB:filepath.Join(p,config.Conf.PoolDbPath),
	}
	sp.SampDB,err = bolt.Open(filepath.Join(p,config.Conf.SampleDbPath),0600,nil)
	if err != nil {
		panic(err)
	}
	sp.PoolDB,err = bolt.Open(filepath.Join(p,config.Conf.PoolDbPath),0600,nil)
	if err != nil {
		panic(err)
	}
	return sp
}

//func FindSetPool(ins string, e *Sample) (s *Set) {
//	s,_ = NewSetPool(ins).Find(e)
//	return
//}
//func LoadSetPool(ins string, e *Sample){
//	sp := NewSetPool(ins)
//	keyE :=e.KeyName()
//	DateKey := time.Unix( int64(binary.BigEndian.Uint64(keyE[:8])),0)
//	ke :=uint64(DateKey.AddDate(-4,0,0).Unix())
//	err := sp.SampDB.Update(func(tx *bolt.Tx)error{
//		db, err := tx.CreateBucketIfNotExists([]byte{1})
//		if err != nil {
//			return err
//		}
//		c := db.Cursor()
//		for{
//			k,_ := c.First()
//			if k == nil {
//				break
//			}
//			if binary.BigEndian.Uint64(k[:8])<ke {
//				fmt.Println("delete",k,keyE)
//				db.Delete(k)
//			}else{
//				break
//			}
//		}
//		return db.Put(keyE,e.String())
//	})
//	if err != nil {
//		panic(err)
//	}
//	timeB := time.Now().Unix()
//	sp.Add(e)
//	dif := time.Now().Unix() - timeB
//	if dif > MaxTime {
//		MaxTime = dif
//		fmt.Println("times",MaxTime,sp.CountApp)
//	}
//}
func (self *SetPool) Find(e *Sample) (MinSet *Set,diffErr float64) {

	dur := uint64(e.LongDuration() + e.SortDuration())
	key := make([]byte,16)
	binary.BigEndian.PutUint64(key,dur)
	var diff float64
	S :=  &Set{}
	MinSet = &Set{}
	MinKey := make([]byte,16)
	var k,v []byte
	//var i int
	err := self.PoolDB.View(func(tx *bolt.Tx)error{
		db := tx.Bucket([]byte{1})
		if db == nil {
			return nil
		}
		c := db.Cursor()
		for k,v = c.Seek(key);k!= nil;k,v = c.Next() {
			//i++
			S.Load(v)
			diff = S.distance(e)
			//fmt.Println("next",i,k,S.Count[0]+S.Count[1],diff,diffErr)
			if (diffErr == 0) || (diff < diffErr) {
				MinSet.Load(v)
				diffErr = diff
				copy(MinKey , k)
				//i=0
			}else{
				//if i>5{
				if diff/diffErr >2 {
					break
				}
				//i++
			}
		}
		//for ;i>0;i--{
		//	c.Prev()
		//}
		c.Seek(key)
		//i = 0
		for k,v = c.Prev(); k!= nil;k,v = c.Prev() {
			//i++
			S.Load(v)
			diff = S.distance(e)
			//fmt.Println("prev",i,k,S.Count[0]+S.Count[1],diff,diffErr)
			if (diffErr == 0) || (diff < diffErr) {
				MinSet.Load(v)
				diffErr = diff
				copy(MinKey , k)
				//i=0
			}else{
				if diff/diffErr >2 {
				//if i>5{
					break
				}
				//i++
			}
		}
		return nil
	})

	if err != nil {
		panic(err)
	}
	if diffErr == 0 {
		return nil,diffErr
	}
	//fmt.Println(key,MinKey)

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

//func (self *SetPool) SameKey(k []byte) bool {
//	for _,k_ := range self.LastKey {
//		if bytes.Equal(k,k_){
//			return true
//		}
//	}
//	return false
//}

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
	self.Diff = maxdiff
	var k string
	for{
		if !self.add(_e) {
			TmpSet.SaveDB(self)
			break
		}
		le :=len(TmpSet.samp)
		if le == 0 {
			break
		}
		TmpSet.update(TmpSet.samp)
		_e,self.Diff = TmpSet.FindLong()
		k = string(_e.KeyName())
		if self.tmpSample[k] != nil {
			TmpSet.SaveDB(self)
			break
		}
		self.tmpSample[k] = _e
	}
	return true

}
func (sp *SetPool) Add(e *Sample) {
	keyE := e.KeyName()
	DateKey := time.Unix( int64(binary.BigEndian.Uint64(keyE[:8])),0)
	ke :=uint64(DateKey.AddDate(-4,0,0).Unix())
	err := sp.SampDB.Update(func(tx *bolt.Tx)error{
		db, err := tx.CreateBucketIfNotExists([]byte{1})
		if err != nil {
			return err
		}
		c := db.Cursor()
		for{
			k,_ := c.First()
			if k == nil {
				break
			}
			if binary.BigEndian.Uint64(k[:8])<ke {
				//fmt.Println("delete",k,keyE)
				db.Delete(k)
			}else{
				break
			}
		}
		return db.Put(keyE,e.String())
	})
	sp.SampDB.Sync()
	if err != nil {
		panic(err)
	}
	//fmt.Println("save sample",keyE)
	timeB := time.Now().Unix()
	if !sp.add(e){
		NewSet(e).SaveDB(sp)
	}
	sp.clear()

	dif := time.Now().Unix() - timeB
	if dif > MaxTime {
		MaxTime = dif
		fmt.Println("times",MaxTime)
	}
	//self.add(e)

}

