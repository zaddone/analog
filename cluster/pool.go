package cluster
import(
	"github.com/boltdb/bolt"
	"github.com/zaddone/analog/config"
	"time"
	//"fmt"
	"path/filepath"
	"encoding/binary"
	"os"
	"bytes"
	//"sync"
)

type Pool struct {

	//SampDB *bolt.DB
	PoolDB *bolt.DB

	MaxTime int64
	//Diff float64
	//tmpSample *sync.Map
	//SumTime int64
	//CountTime int64
	PoolCount int64
	//tag byte
	//tmpSample map[string]*Sample
}
func (self *Pool) HandPoolDB(bucket []byte,h func(*bolt.Bucket)error){
	err := self.PoolDB.Update(func(tx *bolt.Tx)error{
		db, err := tx.CreateBucketIfNotExists(bucket)
		if err != nil {
			return err
		}
		return h(db)
		//return db.Put(self.Key(),self.toByte())
	})
	if err != nil {
		panic(err)
	}

}
func (self *Pool) GetLastTime() (t int64) {
	if err := self.PoolDB.View(func(tx *bolt.Tx)error{
		db := tx.Bucket([]byte{9})
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
	po.PoolDB,err = bolt.Open(filepath.Join(p,config.Conf.PoolDbPath),0600,nil)
	if err != nil {
		panic(err)
	}
	return po

}

func (self *Pool) Copy() *Pool {

	return &Pool{
		PoolDB:self.PoolDB,
		//SampDB:self.SampDB,
		//Diff:self.Diff,
		//tmpSample:self.tmpSample,
		//tag:self.tag,
	}

}

func (self *Pool) Close(){
	self.PoolDB.Close()
	//self.SampDB.Close()
}
//func (self *Pool) clear(){
	//self.tmpSample = new(sync.Map)
	//self.tmpSample = map[string]*Sample{}
	//self.Diff = 0

//}

func (self *Pool) findSamesHandle(e *Sample,hand func(*saEasy,bool)) {
	n := e.KeyName()[8]
	//self.tag = n>>1
	set,_ := self.find(e)
	var _n byte = ^byte(1)
	n &^= _n
	//if (set == nil) || !set.CheckCountMax(int(n)){
	//var kn byte
	//var sum float64

	if (set != nil){
		for _,k := range set.List{
			//kn = k.Key[8] &^ _n
			if k.Key[8] &^ _n != n{
				continue
			}
			hand(k,true)
		}
	}

	setf,_ := self.findF(e)
	//if (setf == nil) || !setf.CheckCountMax(int(n)){
	if (setf != nil) {
		for _,k := range setf.List {
			//kn = k.Key[8] &^ _n
			if k.Key[8] &^ _n != n{
				continue
			}
			hand(k,false)
		}
	}

}

func (self *Pool) SetDiff(e *Sample) bool {
	var sum,l float64
	self.findSamesHandle(e,func(s *saEasy,f bool){
		l++
		if f {
			sum += s.Dis
		}else{
			sum += -s.Dis
		}
	})
	if l == 0 {
		return false
	}
	e.diff = sum/l
	return true
}

func (self *Pool) FindCheck(e *Sample) (ke []byte) {
	self.findSamesHandle(e,func(s *saEasy,f bool){
		if ke == nil {
			ke = s.CaMap
			return
		}
		if f {
			for i,_k := range s.CaMap{
				ke[i] |=  _k
			}
		}else{
			for i,_k := range s.CaMap{
				ke[i] |=  ^_k
			}
		}
	})
	return
}
func (self *Pool) FindSet(e *Sample) (set *Set) {

	//self.tag = e.KeyName()[8]>>1
	set,_ = self.find(e)
	return

}
func (self *Pool) UpdateSet(oldkey []byte,newSet *Set) {
	self.HandPoolDB([]byte{newSet.Tag},
		func(b *bolt.Bucket)error{
			if !bytes.Equal(oldkey,newSet.Key()){
				if err := b.Delete(oldkey);err != nil {
					return err
				}
			}
			return b.Put(newSet.Key(),newSet.toByte())
		},
	)
}

func (self *Pool) addFirst(e *Sample) bool{

	MinSet,diff := self.find(e)
	if MinSet == nil {
		return false
	}
	if !MinSet.loadSamp(self) {
		return self.addFirst(e)
	}

	le := len(MinSet.samp)
	if le < config.Conf.MinSam || MinSet.checkDar(diff) {
	//if MinSet.checkDar(diff) {
		//TmpSet := &Set{}
		key := MinSet.Key()
		MinSet.update(append(MinSet.samp,e))
		self.UpdateSet(key,MinSet)
		return true
	}
	New := NewSet(e)
	self.HandPoolDB([]byte{MinSet.Tag},
		func(db *bolt.Bucket)error {
			if err := db.Delete(MinSet.Key());err != nil {
				return err
			}
			//self.PoolCount--
			return db.Put(New.Key(),New.toByte())
		},
	)
	//MinSet.deleteDB(self)

	MinSet.sort()
	tmp:= make([]*Sample,0,len(MinSet.samp))
	for _,_e := range MinSet.samp {
		if !self.add_1(_e,_e.diff){
			tmp = append(tmp,_e)
		}
	}
	if len(tmp) == 0 {
		return true
		//return self.add_1(e,diff)
	}
	MinSet.update(tmp)
	MinSet.saveDB(self)
	return true

}
func (self *Pool) add_1(e *Sample,_diff float64) bool{

	MinSet,diff := self.find(e)
	if MinSet == nil {
		return false
	}
	if !MinSet.loadSamp(self) {
		return self.add_1(e,_diff)
	}
	if (diff < _diff) {

		//if (len(MinSet.samp) > config.Conf.MinSam) &&
		//!MinSet.checkDar(diff){
		//	NewSet(e).saveDB(self)
		//}else{
		key := MinSet.Key()
		//TmpSet := &Set{}
		MinSet.update(append(MinSet.samp,e))
		self.UpdateSet(key,MinSet)
		//}
		return true
	}

	//le := len(MinSet.samp)
	//if le > config.Conf.MinSam && MinSet.checkDar(diff) {

	//if MinSet.checkDar(diff) {
	//	MinSet.deleteDB(self)
	//	MinSet.sort()
	//	tmp:= make([]*Sample,0,len(MinSet.samp))
	//	for _,_e := range MinSet.samp {
	//		if !self.add_1(_e,_e.diff){
	//			tmp = append(tmp,_e)
	//		}
	//	}
	//	if len(tmp) > 0 {
	//		MinSet.update(tmp)
	//		MinSet.saveDB(self)
	//	}
	//	//return false
	//}
	return false
	//NewSet(e).saveDB(self)
	//return true
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
	if le_ == len(MinSet.List) {
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

	//if le_ == len(MinSet.List) {
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

//func (self *Pool) findCheck(e *Sample) (*Set, float64) {
//
//	dur := uint64(e.Duration())
//	key := make([]byte,16)
//	binary.BigEndian.PutUint64(key,dur)
//	err := self.PoolDB.View(func(tx *bolt.Tx)error{
//		db := tx.Bucket([]byte{(e.KeyName()[8]>>1)^1})
//		if db == nil {
//			return nil
//		}
//		c := db.Cursor()
//		k,v := c.Seek(key)
//	})
//	if err != nil {
//		panic(err)
//	}
//
//}

func (self *Pool) findF(e *Sample) (*Set, float64) {

	dur := uint64(e.Duration())
	key := make([]byte,16)
	binary.BigEndian.PutUint64(key,dur)
	var diff,minDiff float64
	var minS,S *Set
	err := self.PoolDB.View(func(tx *bolt.Tx)error{
		db := tx.Bucket([]byte{(e.KeyName()[8]>>1)^1})
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
		db := tx.Bucket([]byte{e.KeyName()[8]>>1})
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

	func(_e *Sample){
		//sp.SampleCount++
		DateKey := time.Unix( int64(binary.BigEndian.Uint64(_e.KeyName()[:8])),0)
		//ke :=uint64(DateKey.AddDate(-1,0,0).Unix())
		ke := uint64(DateKey.AddDate(-config.Conf.Year,0,0).Unix())
		//err := sp.PoolDB.Batch(func(tx *bolt.Tx)error{
		err := sp.PoolDB.Update(func(tx *bolt.Tx)error{
			db, err := tx.CreateBucketIfNotExists([]byte{9})
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

	//sp.tag = e.KeyName()[8]>>1

	//timeB := time.Now().Unix()
	//sp.clear()
	if !sp.addFirst(e){
		NewSet(e).saveDB(sp)
	}

	//dif := time.Now().Unix() - timeB
	//if dif > sp.MaxTime {
	//	sp.MaxTime = dif
	//	fmt.Println(time.Unix(e.X[0],0),sp.PoolDB.Path(),sp.MaxTime)
	//}
}
