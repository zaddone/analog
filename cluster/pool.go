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
	PoolDB *bolt.DB
}
func (self *Pool) findMin(e *Sample) (minSet *Set,minDiff float64) {
	var diff float64
	self.findSets(e,func(s *Set){
		diff = s.distance(e)
		if (minDiff == 0) || diff < minDiff {
			minDiff = diff
			minSet = s
		}
	})
	return
}
func (self *Pool) Check(e *Sample) bool {

	var sets []*Set
	self.findSets(e,func(s *Set){
		sets = append(sets,s)
	})
	e.tag ^= 3
	self.findSets(e,func(s *Set){
		sets = append(sets,s)
	})
	return true


}

func (self *Pool) ShowPoolNum() (Count int) {
	self.viewPoolDB([]byte{0},func(db *bolt.Bucket)error{
		return db.ForEach(func(k,v []byte)error{
			Count++
			return nil
		})
	})
	self.viewPoolDB([]byte{1},func(db *bolt.Bucket)error{
		return db.ForEach(func(k,v []byte)error{
			Count++
			return nil
		})
	})
	return
}

func (self *Pool) viewPoolDB(bucket []byte,h func(*bolt.Bucket)error){
	err := self.PoolDB.View(func(tx *bolt.Tx)error{
		db := tx.Bucket(bucket)
		if db == nil {
			return nil
		}
		return h(db)
	})
	if err != nil {
		panic(err)
	}
}
func (self *Pool) updatePoolDB(bucket []byte,h func(*bolt.Bucket)error){
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
	self.viewPoolDB([]byte{9},func(db *bolt.Bucket)error{
		c := db.Cursor()
		k,_ := c.Last()
		if k != nil {
			t =int64( binary.BigEndian.Uint64(k[:8]))
		}
		return nil
	})
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
	}
	po.PoolDB,err = bolt.Open(filepath.Join(p,config.Conf.PoolDbPath),0600,nil)
	if err != nil {
		panic(err)
	}
	return po

}

func (self *Pool) Close(){
	self.PoolDB.Close()
}

func (self *Pool) UpdateSet(oldkey []byte,newSet *Set) {
	self.updatePoolDB([]byte{newSet.tag},
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

func (self *Pool) findSetDouble(e *Sample,h func(*Set)){

	dur := uint64(e.Duration())
	key := make([]byte,8+len(e.KeyName()))
	binary.BigEndian.PutUint64(key,dur)
	var S *Set
	var ke,k,v,_k,_v []byte
	self.viewPoolDB([]byte{e.tag>>1},
	func(db *bolt.Bucket)error{
		c := db.Cursor()
		k,v = c.Seek(key)
		if k == nil {
			return nil
		}
		ke = k[:8]
		S = &Set{}
		S.load(k,v)
		h(S)
		for _k,_v = c.Next();_k != nil;_k,_v = c.Next(){
			if !bytes.Equal(ke,_k[:8]){
				break
			}
			S = &Set{}
			S.load(_k,_v)
			h(S)
		}
		c.Seek(k)
		k,v = c.Prev()
		if k == nil {
			return nil
		}
		ke = k[:8]
		S = &Set{}
		S.load(k,v)
		h(S)
		for _k,_v = c.Prev();_k != nil;_k,_v = c.Prev(){

			if !bytes.Equal(ke,_k[:8]){
				break
			}
			S = &Set{}
			S.load(_k,_v)
			h(S)

		}
		return nil
	})

}
func (self *Pool) findSets(e *Sample,h func(*Set)){

	dur := uint64(e.Duration())
	key := make([]byte,8+len(e.KeyName()))
	binary.BigEndian.PutUint64(key,dur)
	var S *Set
	self.viewPoolDB([]byte{e.tag>>1},
	func(db *bolt.Bucket)error{
		c := db.Cursor()
		next := func(k []byte){
			for _k,_v := c.Next();_k != nil;_k,_v = c.Next(){
				if !bytes.Equal(k,_k[:8]){
					break
				}
				S = &Set{}
				S.load(_k,_v)
				h(S)
			}
		}
		prev := func(k []byte){
			for _k,_v := c.Prev();_k != nil;_k,_v = c.Prev(){
				if !bytes.Equal(k,_k[:8]){
					break
				}
				S = &Set{}
				S.load(_k,_v)
				h(S)
			}
		}
		k,v := c.Seek(key)
		if k == nil {
			k,v =c.Last()
			if k == nil {
				return nil
			}
			S = &Set{}
			S.load(k,v)
			h(S)
			prev(k[:8])
			return nil
		}
		k_,v_ := c.Prev()
		if k_ == nil {
			S = &Set{}
			S.load(k,v)
			h(S)
			c.Next()
			next(k[:8])
			return nil
		}
		d_1 := binary.BigEndian.Uint64(k[:8]) - dur
		d_2 := dur - binary.BigEndian.Uint64(k_[:8])
		if (d_1>d_2) {

			S = &Set{}
			S.load(k_,v_)
			h(S)
			prev(k_[:8])
		}else{
			S = &Set{}
			S.load(k,v)
			h(S)
			c.Next()
			next(k[:8])
		}
		return nil
	})
	return
}

func (self *Pool) add(e *Sample) bool {
	var minDiff,diff float64
	var minSet *Set
	var Sets []*Set
	var keys [][]byte
	self.findSetDouble(e,func(s *Set){
		diff = s.distance(e)
		if (minDiff == 0) || diff < minDiff {
			minDiff = diff
			minSet = s
		}
		if s.loadSamp(self){
			Sets = append(Sets,s)
			keys = append(keys,s.Key())
		}
	})
	if minSet == nil {
		return false
	}
	var ns *Set = nil
	if len(minSet.samp) > 0  {
		if (len(minSet.samp) < config.Conf.MinSam) ||
		minSet.checkDar(diff) {
			minSet.samp = append(minSet.samp,e)
			e.SetDiff(diff)
			ns = minSet
		}
	}
	if ns == nil {
		ns = NewSet(e)
		e.diff = ns.distance(e)
	}
	Sets_ := append(Sets,ns)
	tmps := make([][]*Sample,len(Sets_))
	for i,_ := range tmps {
		tmps[i] = make([]*Sample,0,100)
	}
	var I int
	for i,s := range Sets {
		for _,_e := range s.samp {
			I = i
			_e.diff = s.distance(_e)
			for _i,_s := range Sets_{
				if i == _i {
					continue
				}
				d := _s.distance(_e)
				if _e.diff > d {
					_e.diff = d
					I = _i
				}
			}
			tmps[I] =append(tmps[I],_e)
		}
	}
	le := len(Sets)
	for _,_e := range ns.samp {
		I = le
		if _e ==e {
			tmps[I] = append(tmps[I],e)
			continue
		}
		for _i,_s := range Sets {
			d := _s.distance(_e)
			if _e.diff > d {
				_e.diff = d
				I = _i
			}
		}
		tmps[I] =append(tmps[I],_e)
	}
	type tmpdb struct {
		k []byte
		v []byte
	}
	savedb := make([]*tmpdb,0,len(tmps))
	for i,t := range tmps {
		if len(t) > 0 {
			s := Sets_[i]
			s.update(t)
			savedb = append(savedb,&tmpdb{s.Key(),s.toByte()})
		}
	}
	self.updatePoolDB([]byte{ns.tag},
	func(db *bolt.Bucket)error{
		for _,k:= range keys {
			db.Delete(k)
		}
		for _,s := range savedb {
			db.Put(s.k,s.v)
		}
		return nil
	})
	return true

}
func (self *Pool) add_1(e *Sample,_diff float64) bool{

	minSet,minDiff := self.findMin(e)
	if minSet == nil {
		return false
	}
	if (minDiff > _diff) {
		return false
	}

	if !minSet.loadSamp(self) {
		return false
		//NewSet(e).saveDB(self)
		//return true
	}
	if (len(minSet.samp) > config.Conf.MinSam) &&
	!minSet.checkDar(minDiff){
		//return false
		NewSet(e).saveDB(self)
	}else{
		key := minSet.Key()
		minSet.update(append(minSet.samp,e))
		self.UpdateSet(key,minSet)
	}
	return true

}

func (sp *Pool) Add(e *Sample) {

	func(_e *Sample){
		DateKey := time.Unix( int64(binary.BigEndian.Uint64(_e.KeyName()[:8])),0)
		ke := uint64(DateKey.AddDate(-config.Conf.Year,0,0).Unix())
		err := sp.PoolDB.Update(func(tx *bolt.Tx)error{
			db, err := tx.CreateBucketIfNotExists([]byte{9})
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
			return db.Put(_e.KeyName(),_e.toByte())
		})
		if err != nil {
			panic(err)
		}
	}(e)

	if !sp.add(e){
		NewSet(e).saveDB(sp)
	}

}
