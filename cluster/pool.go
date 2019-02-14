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
	"sync"
)

type tmpdb struct {
	k []byte
	v []byte
}

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
	//var ke []byte
	self.viewPoolDB([]byte{e.tag>>1},
	func(db *bolt.Bucket)error{
		c := db.Cursor()
		k,v := c.Seek(key)
		if k == nil {
			return nil
		}
		h(NewSetLoad(k,v))
		ke := k[:8]
		c_ := db.Cursor()
		c_.Seek(k)
		k_,v_ := c_.Prev()
		if k_ == nil {
			for k,v := c.Next();k != nil;k,v = c.Next() {
				if !bytes.Equal(ke,k[:8]) {
					break
				}
				h(NewSetLoad(k,v))
			}
			return nil
		}
		h(NewSetLoad(k_,v_))
		d  := binary.BigEndian.Uint64(ke) - dur
		d_ := dur - binary.BigEndian.Uint64(k_[:8])
		if d < d_ {
			d = d_
		}
		var w sync.WaitGroup
		w.Add(2)
		go func(n uint64,_w *sync.WaitGroup){
			for k,v := c.Next();k != nil;k,v = c.Next() {
				if (binary.BigEndian.Uint64(k[:8]) - dur)>n {
					break
				}
				h(NewSetLoad(k,v))
			}
			_w.Done()
		}(d,&w)
		go func(n uint64,_w *sync.WaitGroup){
			for k,v := c_.Prev();k != nil;k,v = c_.Prev() {
				if (dur - binary.BigEndian.Uint64(k[:8]))>n {
					break
				}
				h(NewSetLoad(k,v))
			}
			_w.Done()
		}(d,&w)
		w.Wait()
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
	var Sets []*Set
	var keys [][]byte
	SetChan:=make(chan *Set,100)
	var w,w_ sync.WaitGroup
	w_.Add(1)
	go func(_w *sync.WaitGroup){
		for s := range SetChan {
			//diff = s.distance(e)
			//if (minDiff == 0) || diff < minDiff {
			//	minDiff = diff
			//	minSet = s
			//}
			Sets = append(Sets,s)
			keys = append(keys,s.Key())
		}
		_w.Done()
	}(&w_)
	self.findSetDouble(e,func(s *Set){
		w.Add(1)
		go func (_s *Set,_w *sync.WaitGroup) {
			if _s.loadSamp(self){
				SetChan <- _s
			}
			_w.Done()
		}(s,&w)
	})
	w.Wait()
	close(SetChan)
	w_.Wait()

	le := len(Sets)
	if le == 0 {
		return false
	}

	tmps := make([][]*Sample,le)
	for i,_ := range tmps {
		tmps[i] = make([]*Sample,0,100)
	}
	ns := NewSet(e)
	Sets = append(Sets,ns)
	tmps = append(tmps,ns.samp)
	var I int
	for i,s := range Sets[:le] {
		for _,_e := range s.samp {
			I = i
			_e.diff = s.distance(_e)
			for _i,_s := range Sets{
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
	savedb := make(chan *tmpdb,le+1)
	for i,t := range tmps {
		if len(t) == 0 {
			continue
		}
		w_.Add(1)
		go func(i_ int,t_ []*Sample,_w *sync.WaitGroup){
			s := Sets[i_]
			s.update(t_)
			savedb <- &tmpdb{s.Key(),s.toByte()}
			_w.Done()
		}(i,t,&w_)
	}
	w_.Wait()
	close(savedb)
	self.updatePoolDB([]byte{ns.tag},
	func(db *bolt.Bucket)error{
		for _,k:= range keys {
			db.Delete(k)
		}
		for s := range savedb {
			db.Put(s.k,s.v)
		}
		return nil
	})
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
