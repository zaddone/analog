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
	SampDB *bolt.DB
	path string
	//samp string
	TmpSa [2]chan *Sample
}
func (self *Pool) syncAdd(chanSa chan *Sample){
	for{
		e := <-chanSa
		e.s = NewSet(e)
		if !self.add(e){
			e.s.saveDB(self)
		}
		e.stop<-true
	}
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

func (self *Pool) openSampDB() {
	sampDB,err := bolt.Open(filepath.Join(self.path,config.Conf.SampleDbPath),0600,nil)
	if err != nil {
		panic(err)
	}
	self.SampDB = sampDB
	return
}
func (self *Pool) openPoolDB(){
	PoolDB,err := bolt.Open(filepath.Join(self.path,config.Conf.PoolDbPath),0600,nil)
	if err != nil {
		panic(err)
	}
	self.PoolDB = PoolDB
	return
}
func (self *Pool) viewPoolDB(bucket []byte,h func(*bolt.Bucket)error){
	//pooldb := self.openPoolDB()
	err :=self.PoolDB.View(func(tx *bolt.Tx)error{
		db := tx.Bucket(bucket)
		if db == nil {
			return nil
		}
		return h(db)
	})
	if err != nil {
		panic(err)
	}
	//pooldb.Close()
}
func (self *Pool) updatePoolDB(bucket []byte,h func(*bolt.Bucket)error){
	//pooldb := self.openPoolDB()
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
	//pooldb.Close()
}
func (self *Pool) GetLastTime() (t int64) {
	//SampDB := self.openSampDB()
	err := self.SampDB.View(func(_t *bolt.Tx)error{
		db := _t.Bucket([]byte{9})
		if db == nil {
			return nil
		}
		c := db.Cursor()
		k,_ := c.Last()
		if k != nil {
			t = int64(binary.BigEndian.Uint64(k[:8]))
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	//SampDB.Close()
	return
}


func NewPool(ins string) (po *Pool) {

	po = &Pool{
		TmpSa:[2]chan *Sample{make(chan *Sample,5),make(chan *Sample,5)},
		path:filepath.Join(config.Conf.ClusterPath,ins),
	}
	_,err := os.Stat(po.path)
	if err != nil {
		err = os.MkdirAll(po.path,0700)
		if err != nil {
			panic(err)
		}
	}
	po.openSampDB()
	po.openPoolDB()
	//po.SampDB,err = bolt.Open(filepath.Join(p,config.Conf.SampleDbPath),0600,nil)
	//if err != nil {
	//	panic(err)
	//}
	//po.PoolDB,err = bolt.Open(filepath.Join(p,config.Conf.PoolDbPath),0600,nil)
	//if err != nil {
	//	panic(err)
	//}
	go po.syncAdd(po.TmpSa[0])
	go po.syncAdd(po.TmpSa[1])
	return po

}

//func (self *Pool) Close(){
//	//self.PoolDB.Close()
//	//self.SampDB.Close()
//}

func (self *Pool) findSetDouble(e *Sample,tag byte,h func(*Set)){

	dur := uint64(e.Duration())
	key := make([]byte,8+len(e.KeyName()))
	binary.BigEndian.PutUint64(key,dur)
	//var ke []byte
	self.viewPoolDB([]byte{tag},
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

func (self *Pool) add(e *Sample) bool {
	Sets:=make([]*Set,0,100)
	keys:=make([][]byte,0,100)
	self.findSetDouble(e,e.tag>>1,func(s *Set){
		keys = append(keys,s.Key())
		if s.loadSamp(self){
			Sets = append(Sets,s)
		}
	})
	le := len(Sets)
	if le == 0 {
		return false
	}
	tmps := make([][]*Sample,le+1)
	for i:=0;i<le ;i++{
		tmps[i] = make([]*Sample,0,100)
	}
	Sets = append(Sets,e.s)
	tmps[le] = append(tmps[le],e)
	var I int
	var d float64
	for i,s := range Sets[:le] {
		for _,_e := range s.samp {
			I = i
			_e.diff = s.distance(_e)
			for _i,_s := range Sets{
				if i == _i {
					continue
				}
				d = _s.distance(_e)
				if _e.diff > d {
					_e.diff = d
					I = _i
				}
			}
			tmps[I] = append(tmps[I],_e)
		}
	}
	for i,_t := range tmps {
		if len(_t) >0 {
			Sets[i].update(_t)
		}else{
			Sets[i]=nil
		}
	}
	self.updatePoolDB([]byte{e.s.tag},
	func(db *bolt.Bucket)(err error){
		for _,k:= range keys {
			err = db.Delete(k)
			if err != nil {
				return err
			}
		}
		for _, _s := range Sets {
			if _s == nil {
				continue
			}
			err = db.Put(_s.Key(),_s.toByte())
			if err != nil {
				return err
			}
		}
		return nil
	})
	return true

}

func (self *Pool) add_(e *Sample) bool {
	Sets:=make([]*Set,0,100)
	SetsChan := make( chan *Set,100)
	var keys [][]byte
	var w,w_ sync.WaitGroup
	w_.Add(1)
	go func (w__ *sync.WaitGroup) {
		for s := range SetsChan {
			Sets = append(Sets,s)
		}
		w__.Done()
	}(&w_)
	self.findSetDouble(e,e.tag>>1,func(s *Set){
		keys = append(keys,s.Key())
		w.Add(1)
		go func(s_ *Set,_w *sync.WaitGroup){
			if s_.loadSamp(self){
				SetsChan<-s_
			}
			_w.Done()
		}(s,&w)
	})
	w.Wait()
	close(SetsChan)
	w_.Wait()

	le := len(Sets)
	if le == 0 {
		return false
	}
	le++
	tmpchan := make([]chan *Sample,le)
	tmps := make([][]*Sample,le)
	w_.Add(le)
	for i:=0;i<le ;i++{
		tmps[i] = make([]*Sample,0,100)
		tmpchan[i] = make(chan *Sample,100)
		go func(_w *sync.WaitGroup,i_ int,tc chan *Sample){
			for _e := range tc {
				tmps[i_] = append(tmps[i_],_e)
			}
			_w.Done()
		}(&w_,i,tmpchan[i])

	}
	//e.s = NewSet(e)
	Sets_ := append(Sets,e.s)
	tmps[le-1] = append(tmps[le-1],e)
	//tmps = append(tmps,ns.samp)
	for i,s := range Sets {
		w.Add(len(s.samp))
		for _,_e := range s.samp {
			go func(i_ int,__e *Sample,_w *sync.WaitGroup){
				I := i_
				__e.diff = Sets[i_].distance(__e)
				for _i,_s := range Sets_{
				if i_ == _i {
						continue
					}
					d := _s.distance(__e)
					if __e.diff > d {
						__e.diff = d
						I = _i
					}
				}
				tmpchan[I] <- __e
				_w.Done()
			}(i,_e,&w)
		}
	}
	w.Wait()
	for i:=0;i<le ;i++{
		close(tmpchan[i])
	}
	w_.Wait()


	le = len(tmps)
	savedb := make(chan *tmpdb,le)
	w.Add(le)
	for i,t := range tmps {
		go func (i_ int,t_ []*Sample,_w *sync.WaitGroup){
			if len(t_) > 0 {
				_s := Sets_[i_]
				_s.update(t_)
				savedb <- &tmpdb{_s.Key(),_s.toByte()}
			}
			_w.Done()
			//savedb =append(savedb,&tmpdb{s.Key(),s.toByte()})
		}(i,t,&w)
	}
	w.Wait()
	close(savedb)
	self.updatePoolDB([]byte{e.s.tag},
	func(db *bolt.Bucket)(err error){
		for _,k:= range keys {
			err = db.Delete(k)
			if err != nil {
				return err
			}
		}
		for _s := range savedb {
			err = db.Put(_s.k,_s.v)
			if err != nil {
				return err
			}
		}
		return nil
	})
	return true

}

func (self *Pool) UpdateSample(e *Sample) {
	//sampDB := self.openSampDB()
	err := self.SampDB.Batch(func(tx *bolt.Tx)error{
		db, err := tx.CreateBucketIfNotExists([]byte{9})
		if err != nil {
			return err
		}
		return db.Put(e.KeyName(),e.toByte())
	})
	if err != nil {
		panic(err)
	}
	//sampDB.Close()
}
func (sp *Pool) Add(e *Sample) {

	//e.s = NewSet(e)
	//if e.s == nil {
	//	return
	//}
	go func(_e *Sample){
		DateKey := time.Unix( int64(binary.BigEndian.Uint64(_e.KeyName()[:8])),0)
		ke := uint64(DateKey.AddDate(-config.Conf.Year,0,0).Unix())
		//db := sp.openSampDB()
		err := sp.SampDB.Batch(func(tx *bolt.Tx)error{
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
		//db.Close()
	}(e)
	sp.TmpSa[int(e.tag>>1)] <- e
}
