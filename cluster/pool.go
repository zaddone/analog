package cluster
import(
	"github.com/boltdb/bolt"
	"github.com/zaddone/analog/config"
	"time"
	//"fmt"
	"path/filepath"
	"encoding/binary"
	"os"
	//"bytes"
	//"sync"
)

type Pool struct {

	SampDB *bolt.DB
	PoolDB *bolt.DB

	MaxTime int64
	//Diff float64
	//tmpSample *sync.Map
	//SumTime int64
	//CountTime int64
	SetCount int64
	tag byte
	//tmpSample map[string]*Sample
}
func (self *Pool) GetLastTime() (t int64) {
	if err := self.SampDB.View(func(tx *bolt.Tx)error{
		db := tx.Bucket([]byte{1})
		if db == nil {
			return nil
		}
		c := db.Cursor()
		k,_ := c.Last()
		t =int64( binary.BigEndian.Uint64(k[:8]))
		return nil
	}); err != nil {
		panic(err)
	}
	return
}


func NewPool(ins string) (po *Pool) {

	p := filepath.Join(config.Conf.ClusterPath,ins)
	_,err := os.Stat(p)
	if err != nil {
		err = os.MkdirAll(p,0700)
		if err != nil {
			panic(err)
		}
	}
	po = &Pool{
		//tmpSample:new(sync.Map),
	}
	po.SampDB,err = bolt.Open(filepath.Join(p,config.Conf.SampleDbPath),0600,nil)
	if err != nil {
		panic(err)
	}
	po.PoolDB,err = bolt.Open(filepath.Join(p,config.Conf.PoolDbPath),0600,nil)
	if err != nil {
		panic(err)
	}
	return po

}

func (self *Pool) Copy() *Pool {

	return &Pool{
		PoolDB:self.PoolDB,
		SampDB:self.SampDB,
		//Diff:self.Diff,
		//tmpSample:self.tmpSample,
		tag:self.tag,
	}

}

func (self *Pool) Close(){
	self.PoolDB.Close()
	self.SampDB.Close()
}
//func (self *Pool) clear(){
	//self.tmpSample = new(sync.Map)
	//self.tmpSample = map[string]*Sample{}
	//self.Diff = 0

//}


func (self *Pool) FindCheck(e *Sample) []uint8 {

	n := e.KeyName()[8]
	self.tag = n>>1
	set,_ := self.find(e)
	if (set == nil) || !set.CheckCountMax(int(n)){
		return nil
	}
	var _n byte = ^byte(1)
	var kn byte

	n &^= _n
	ke := set.List[0].CaMap
	for _,k := range set.List[1:]{
		kn = k.Key[8] &^ _n
		if kn != n{
			continue
		}
		for i,_k := range k.CaMap {
			ke[i] |=  _k
		}
	}

	setf,_ := self.findF(e)

	if (setf == nil) || !setf.CheckCountMax(int(n)){
		return nil
	}
	for _,k := range setf.List {
		kn = k.Key[8] &^ _n
		if kn != n{
			continue
		}
		for i,_k := range k.CaMap {
			ke[i] |=  ^_k
		}
	}
	return ke

}
func (self *Pool) FindSet(e *Sample) (set *Set) {

	self.tag = e.KeyName()[8]>>1
	set,_ = self.find(e)
	return

}

func (self *Pool) add(e *Sample,_diff float64) bool{

	MinSet,diff := self.find(e)
	if MinSet == nil {
		return false
	}
	if !MinSet.loadSamp(self) {
		return self.add(e,_diff)
	}
	if (_diff!=0) && (diff>_diff) {
		return false
	}
	MinSet.deleteDB(self)

	le := len(MinSet.samp)
	if le < config.Conf.MinSam {
		MinSet.update(append(MinSet.samp,e))
		MinSet.saveDB(self)
		return true
	}
	_,diffMax := MinSet.findLong()
	if diff < diffMax {
		MinSet.update(append(MinSet.samp,e))
		MinSet.saveDB(self)
		return true
	}


	NewSet(e).saveDB(self)

	tmp_e:=make([]*Sample,0,le)
	for _,e_ := range MinSet.samp {
		if !self.add(e_,e_.diff){
			tmp_e = append(tmp_e,e_)
		}
	}
	le_ := len(tmp_e)
	if (le_ == le) && (le == len(MinSet.List)) {
		MinSet.saveDB(self)
		return true
	}
	if le_>0 {
		MinSet.update(tmp_e)
		MinSet.saveDB(self)
	}
	return true

	//chan_tmp := make(chan *Sample,le)
	//var w sync.WaitGroup
	//w.Add(le)
	//for _,e_ := range MinSet.samp {
	//	go func(_e_ *Sample,w_ *sync.WaitGroup){
	//		if !self.add(_e_,_e_.diff){
	//			chan_tmp <- _e_
	//			//tmp_e = append(tmp_e,_e_)
	//		}
	//		w_.Done()
	//	}(e_,&w)
	//}
	//w.Wait()
	//close(chan_tmp)
	//le_ := len(chan_tmp)
	//if le_ == le {
	//	MinSet.saveDB(self)
	//	return true
	//}
	//if le_>0 {
	//	tmp_e:=make([]*Sample,0,le_)
	//	for e_ := range chan_tmp {
	//		tmp_e = append(tmp_e,e_)
	//	}
	//	MinSet.update(tmp_e)
	//	MinSet.saveDB(self)
	//}
	//return true
}

func (self *Pool) findF(e *Sample) (*Set, float64) {

	dur := uint64(e.Duration())
	key := make([]byte,16)
	binary.BigEndian.PutUint64(key,dur)
	var diff,minDiff float64
	var minS,S *Set
	err := self.PoolDB.View(func(tx *bolt.Tx)error{
		db := tx.Bucket([]byte{self.tag^1})
		if db == nil {
			return nil
		}
		c := db.Cursor()
		next := func(du uint64){
			for _k,_v := c.Next();_k != nil;_k,_v = c.Next(){
				if (binary.BigEndian.Uint64(_k[:8]) - dur)>du {
					break
				}
				S = &Set{}
				S.load(_v)
				diff = S.distanceF(e)
				if diff<minDiff {
					minDiff = diff
					minS = S
				}
			}
		}
		prev := func(du uint64){
			for _k,_v := c.Prev();_k != nil;_k,_v = c.Prev(){
				if (dur - binary.BigEndian.Uint64(_k[:8]))>du {
					break
				}
				S = &Set{}
				S.load(_v)
				diff = S.distanceF(e)
				if diff<minDiff {
					minDiff = diff
					minS = S
				}
			}
		}
		k,v := c.Seek(key)
		if k == nil {
			k,v =c.Last()
			if k == nil {
				return nil
			}
			minS = &Set{}
			minS.load(v)
			minDiff = minS.distanceF(e)
			prev(dur - binary.BigEndian.Uint64(k[:8]))
			return nil
		}
		k_,v_ := c.Prev()
		if k_ == nil {
			minS = &Set{}
			minS.load(v)
			minDiff = minS.distanceF(e)
			c.Next()
			next(binary.BigEndian.Uint64(k[:8]) - dur)
			return nil
		}
		d_1 := binary.BigEndian.Uint64(k[:8]) - dur
		d_2 := dur - binary.BigEndian.Uint64(k_[:8])
		if (d_1>d_2) {
			minS = &Set{}
			minS.load(v_)
			minDiff = minS.distanceF(e)
			prev(d_2)
		}else{
			minS = &Set{}
			minS.load(v)
			minDiff = minS.distanceF(e)
			c.Next()
			next(d_1)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	return minS,minDiff
}
func (self *Pool) find(e *Sample) (*Set, float64) {
	dur := uint64(e.Duration())
	key := make([]byte,16)
	binary.BigEndian.PutUint64(key,dur)
	var diff,minDiff float64
	var minS,S *Set
	err := self.PoolDB.View(func(tx *bolt.Tx)error{
		db := tx.Bucket([]byte{self.tag})
		if db == nil {
			return nil
		}
		c := db.Cursor()
		next := func(du uint64){
			for _k,_v := c.Next();_k != nil;_k,_v = c.Next(){
				if (binary.BigEndian.Uint64(_k[:8]) - dur)>du {
					break
				}
				S = &Set{}
				S.load(_v)
				diff = S.distance(e)
				if diff<minDiff {
					minDiff = diff
					minS = S
				}
			}
		}
		prev := func(du uint64){
			for _k,_v := c.Prev();_k != nil;_k,_v = c.Prev(){
				if (dur - binary.BigEndian.Uint64(_k[:8]))>du {
					break
				}
				S = &Set{}
				S.load(_v)
				diff = S.distance(e)
				if diff<minDiff {
					minDiff = diff
					minS = S
				}
			}
		}
		k,v := c.Seek(key)
		if k == nil {
			k,v =c.Last()
			if k == nil {
				return nil
			}
			minS = &Set{}
			minS.load(v)
			minDiff = minS.distance(e)
			prev(dur - binary.BigEndian.Uint64(k[:8]))
			return nil
		}
		k_,v_ := c.Prev()
		if k_ == nil {
			minS = &Set{}
			minS.load(v)
			minDiff = minS.distance(e)
			c.Next()
			next(binary.BigEndian.Uint64(k[:8]) - dur)
			return nil
		}
		d_1 := binary.BigEndian.Uint64(k[:8]) - dur
		d_2 := dur - binary.BigEndian.Uint64(k_[:8])
		if (d_1>d_2) {
			minS = &Set{}
			minS.load(v_)
			minDiff = minS.distance(e)
			prev(d_2)
		}else{
			minS = &Set{}
			minS.load(v)
			minDiff = minS.distance(e)
			c.Next()
			next(d_1)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	return minS,minDiff

}

func (sp *Pool) Add(e *Sample) {

	go func(_e *Sample){
		//sp.SampleCount++
		DateKey := time.Unix( int64(binary.BigEndian.Uint64(_e.KeyName()[:8])),0)
		//ke :=uint64(DateKey.AddDate(-1,0,0).Unix())
		ke := uint64(DateKey.AddDate(-config.Conf.Year,0,0).Unix())
		//err := sp.SampDB.Batch(func(tx *bolt.Tx)error{
		err := sp.SampDB.Update(func(tx *bolt.Tx)error{
			db, err := tx.CreateBucketIfNotExists([]byte{1})
			if err != nil {
				return err
			}
			c := db.Cursor()
			for k,_ := c.First();k!=nil;k,_ = c.Next() {
				if binary.BigEndian.Uint64(k[:8])<ke {
					db.Delete(k)
					//sp.SampleCount--
				}else{
					break
				}
			}
			return db.Put(_e.KeyName(),_e.toByte())
		})
		if err != nil {
			panic(err)
		}
	}(e)

	sp.tag = e.KeyName()[8]>>1

	//timeB := time.Now().Unix()
	//sp.clear()
	if !sp.add(e,0){
		NewSet(e).saveDB(sp)
	}

	//dif := time.Now().Unix() - timeB
	//if dif > sp.MaxTime {
	//	sp.MaxTime = dif
	//	fmt.Println(time.Unix(e.X[0],0),sp.PoolDB.Path(),sp.MaxTime)
	//}
}
