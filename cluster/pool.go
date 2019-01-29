package cluster
import(
	"github.com/boltdb/bolt"
	"github.com/zaddone/analog/config"
	"time"
	"fmt"
	"path/filepath"
	"encoding/binary"
	"os"
	//"bytes"
	"sync"
)
var (
	MaxTime int64
)

type Pool struct {

	SampDB *bolt.DB
	PoolDB *bolt.DB

	//Diff float64
	tmpSample *sync.Map
	tag byte
	//tmpSample map[string]*Sample
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
		tmpSample:new(sync.Map),
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
		tmpSample:self.tmpSample,
		tag:self.tag,
	}

}

func (self *Pool) Close(){
	self.PoolDB.Close()
	self.SampDB.Close()
}
func (self *Pool) clear(){
	self.tmpSample = new(sync.Map)
	//self.tmpSample = map[string]*Sample{}
	//self.Diff = 0

}

func (self *Pool) FindSet(e *Sample) (set *Set) {

	set = &Set{}
	s_ := self.findSet(e)
	for _,s := range s_ {
		set.samp = append(set.samp,s.samp...)
		for _i,v := range s.Count {
			set.Count[_i]+=v
		}
	}
	return

}

func (self *Pool) add(e *Sample,_diff float64,level int) bool{

	MinSet,diff := self.find(e)
	if MinSet == nil {
		return false
	}
	if !MinSet.loadSamp(self) {
		return self.add(e,_diff,level)
	}
	if (_diff!=0) && (diff>_diff) {
		return false
	}

	TmpSet := &Set{}
	TmpSet.update(append(MinSet.samp,e))
	le := len(TmpSet.samp)
	if le < 4 {
		MinSet.deleteDB(self)
		TmpSet.saveDB(self)
		return true
	}
	_e, diff := TmpSet.findLong()
	if (_e == e) {
		//return false
		//if _diff>0{
		NewSet(_e).saveDB(self)
		//}else{
		//	MinSet.deleteDB(self)
		//	TmpSet.saveDB(self)
		//}
		return true
	}
	MinSet.deleteDB(self)
	if level == config.Conf.FindLevel{
		TmpSet.saveDB(self)
		return true
	}

	self.tmpSample.Store(string(e.KeyName()),e)
	//var _e *Sample
	tmp_e:=make(chan *Sample,le)
	var w sync.WaitGroup
	for{
		if _,ok := self.tmpSample.Load(string(_e.KeyName()));ok{
			//NewSet(_e).saveDB(self)
			tmp_e <- _e
		}else{

			if len(TmpSet.samp) > 0 {
				TmpSet.update(TmpSet.samp)
				diff = TmpSet.distance(_e)
			}

			w.Add(1)
			go func(_w *sync.WaitGroup,__e *Sample,diff_ float64){
				if !self.add(__e,diff_,level+1){
					tmp_e <- __e
				}
				_w.Done()
			}(&w,_e,diff)
		}
		_e, diff = TmpSet.findLong()
		if _e == nil {
			break
		}
	}
	w.Wait()
	close(tmp_e)
	for e_ := range tmp_e{
		TmpSet.samp = append(TmpSet.samp,e_)
	}
	TmpSet.update(TmpSet.samp)
	TmpSet.saveDB(self)
	return true

}

//func (self *Pool) addBak(e *Sample,w *sync.WaitGroup,level int) bool{
//
//	MinSet,diff := self.find(e)
//	if MinSet == nil {
//		return false
//		//NewSet(e).saveDB(self)
//		//return true
//	}
//	if !MinSet.loadSamp(self) {
//		return self.add(e,w,level)
//	}
//	if (self.Diff!=0) && (diff>self.Diff) {
//		return false
//	}
//	TmpSet := &Set{}
//	TmpSet.update(append(MinSet.samp,e))
//	if len(TmpSet.samp) < 4 {
//		MinSet.deleteDB(self)
//		TmpSet.saveDB(self)
//		return true
//	}
//
//	var _e *Sample
//	_e, self.Diff = TmpSet.findLong()
//	if bytes.Equal(_e.KeyName(),e.KeyName()) {
//		NewSet(e).saveDB(self)
//		return true
//	}
//	if level > config.Conf.FindLevel {
//		return false
//	}
//	MinSet.deleteDB(self)
//	w.Add(1)
//	go func(s *Set,p *Pool,__e *Sample,_w *sync.WaitGroup){
//		var k string
//		var TmpSet_ *Set
//		defer _w.Done()
//		//var tmpE []*Sample
//		for{
//			le := len(s.samp)
//			if le > 0 {
//				TmpSet_ = &Set{}
//				TmpSet_.update(s.samp)
//				p.Diff = TmpSet_.distance(__e)
//			}
//			if !p.add(__e,_w,level+1) {
//				s.saveDB(p)
//				break
//				//tmpE = append(tmpE,__e)
//			}
//			if le == 0 {
//				break
//			}
//			s = TmpSet_
//			__e,p.Diff = s.findLong()
//			k = string(__e.KeyName())
//			if _,ok:= p.tmpSample.Load(k);ok {
//				s.saveDB(p)
//				break
//			}
//			p.tmpSample.Store(k,__e)
//		}
//		//if len(tmpE) > 0 {
//		//	TmpSet_ = &Set{}
//		//}
//	}(TmpSet,self.Copy(),_e,w)
//	return true
//
//}

func (self *Pool) findSet(e *Sample) (s_ []*Set) {

	key := make([]byte,16)
	binary.BigEndian.PutUint64(key,uint64(e.Duration()))
	SetChan := make(chan *Set,100)
	var w,w_1 sync.WaitGroup
	w_1.Add(1)
	go func(_w *sync.WaitGroup){
		for s := range SetChan{
			s_ = append(s_,s )
		}
		_w.Done()
	}(&w_1)
	w.Add(2)
	go func(_w *sync.WaitGroup){
		var diff,sum,count float64
		var S *Set
		err := self.PoolDB.View(func(tx *bolt.Tx)error{
			db := tx.Bucket([]byte{self.tag})
			if db == nil {
				return nil
			}
			c := db.Cursor()
			for k,v := c.Seek(key);k!= nil;k,v = c.Next() {
				count++
				S = &Set{}
				S.load(v)
				SetChan <-S

				diff = S.distance(e)
				sum += diff

				if !S.SectionCheck(e) &&
				(sum/count < diff) {
					break
				}
			}
			return nil
		})
		if err != nil {
			panic(err)
		}
		_w.Done()
	}(&w)
	go func(_w *sync.WaitGroup){
		var diff,sum,count float64
		var S *Set
		err := self.PoolDB.View(func(tx *bolt.Tx)error{
			db := tx.Bucket([]byte{self.tag})
			if db == nil {
				return nil
			}
			c := db.Cursor()
			c.Seek(key)
			for k,v := c.Prev(); k != nil;k,v = c.Prev() {
				count++
				S = &Set{}
				S.load(v)
				SetChan <-S

				diff = S.distance(e)
				sum += diff
				if !S.SectionCheck(e) &&
				(sum/count < diff) {
					break
				}
			}
			return nil
		})
		if err != nil {
			panic(err)
		}
		_w.Done()
	}(&w)
	w.Wait()
	close(SetChan)

	w_1.Wait()
	return

}

func (self *Pool) find(e *Sample) (*Set, float64) {

	key := make([]byte,16)
	binary.BigEndian.PutUint64(key,uint64(e.Duration()))

	var diff_1,diff_2 float64
	var S_1,S_2 *Set
	var w sync.WaitGroup
	w.Add(2)
	go func(_w *sync.WaitGroup){
		var diff,sum,count float64
		var S *Set
		err := self.PoolDB.View(func(tx *bolt.Tx)error{
			db := tx.Bucket([]byte{self.tag})
			if db == nil {
				return nil
			}
			c := db.Cursor()
			for k,v := c.Seek(key);k!= nil;k,v = c.Next() {
				count++
				S = &Set{}
				S.load(v)
				diff = S.distance(e)
				sum += diff
				if (diff_1 == 0) || (diff_1 > diff) {
					S_1 = S
					diff_1 = diff
				}else if !S.SectionCheck(e) && (sum/count < diff) {
					break
				}
			}
			return nil
		})
		if err != nil {
			panic(err)
		}
		_w.Done()
	}(&w)
	go func(_w *sync.WaitGroup){
		var diff,sum,count float64
		var S *Set
		err := self.PoolDB.View(func(tx *bolt.Tx)error{
			db := tx.Bucket([]byte{self.tag})
			if db == nil {
				return nil
			}
			c := db.Cursor()
			c.Seek(key)
			for k,v := c.Prev(); k != nil;k,v = c.Prev() {
				count++
				S = &Set{}
				S.load(v)
				diff = S.distance(e)
				sum += diff
				if (diff_2 == 0) || (diff_2 > diff) {
					S_2 = S
					diff_2 = diff
				}else if !S.SectionCheck(e) && (sum/count < diff) {
					break
				}
			}
			return nil
		})
		if err != nil {
			panic(err)
		}
		_w.Done()
	}(&w)
	w.Wait()

	if (diff_1==0) && (diff_2==0) {
		return nil,0
	}
	if diff_1 < diff_2 {
		return S_1,diff_1
	}else{
		return S_2,diff_2
	}

}

//func (self *Pool) findBak(e *Sample) (S_1 *Set,diff_1 float64) {
//
//	key := make([]byte,16)
//	binary.BigEndian.PutUint64(key,uint64(e.Duration()))
//
//	var diff float64
//	err := self.PoolDB.View(func(tx *bolt.Tx)error{
//		db := tx.Bucket([]byte{self.tag})
//		if db == nil {
//			return nil
//		}
//		c := db.Cursor()
//		for k,v := c.Seek(key);k!= nil;k,v = c.Next() {
//			S := &Set{}
//			S.load(v)
//			diff = S.distance(e)
//			if (diff_1 == 0) || (diff_1 > diff) {
//				S_1 = S
//				diff_1 = diff
//			}else if diff_1/diff < config.Conf.DisPool {
//				break
//			}
//		}
//		c.Seek(key)
//		for k,v := c.Prev(); k != nil;k,v = c.Prev() {
//			S := &Set{}
//			S.load(v)
//			diff = S.distance(e)
//			if (diff_1 > diff) {
//				S_1 = S
//				diff_1 = diff
//			}else if diff_1/diff < config.Conf.DisPool {
//				break
//			}
//		}
//		return nil
//	})
//	if err != nil {
//		panic(err)
//	}
//
//	if diff_1==0 {
//		return nil,0
//	}
//	return S_1,diff_1
//
//}

func (sp *Pool) Add(e *Sample) {

	go func(_e *Sample){
		DateKey := time.Unix( int64(binary.BigEndian.Uint64(_e.Key[:8])),0)
		ke :=uint64(DateKey.AddDate(-4,0,0).Unix())
		err := sp.SampDB.Batch(func(tx *bolt.Tx)error{
		//err := sp.SampDB.Update(func(tx *bolt.Tx)error{
			db, err := tx.CreateBucketIfNotExists([]byte{1})
			if err != nil {
				return err
			}
			c := db.Cursor()
			for k,_ := c.First();k!=nil;k,_ = c.Next() {
				if binary.BigEndian.Uint64(k[:8])<ke {
					db.Delete(k)
				}else{
					break
				}
			}
			return db.Put(_e.Key,_e.toByte())
		})
		if err != nil {
			panic(err)
		}
	}(e)

	sp.tag = e.Key[8]>>1

	timeB := time.Now().Unix()
	sp.clear()
	if !sp.add(e,0,0){
		NewSet(e).saveDB(sp)
	}

	dif := time.Now().Unix() - timeB
	if dif > MaxTime {
		MaxTime = dif
		fmt.Println("times",MaxTime)
	}
}
