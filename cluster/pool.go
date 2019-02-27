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
			//e.s = NewSet(e)
			//e.s.saveDB(self)
			self.updatePoolDB([]byte{0},func(db *bolt.Bucket)error{
				return db.Put(e.s.Key(),e.s.toByte())
			})
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
	//go po.syncAdd(po.TmpSa[1])
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

func (self *Pool) add_2(e *Sample) bool {

	SetsChan := make( chan *Set,100)
	Sets:=make([]*Set,0,100)
	keys:=make([][]byte,0,100)
	var w,w_ sync.WaitGroup
	w_.Add(1)
	var diff,minDiff float64
	var minSet *Set
	go func () {
		for s := range SetsChan {
			Sets = append(Sets,s)
			diff = s.distance(e)
			if (diff < minDiff) || (minDiff == 0) {
				minSet = s
				minDiff = diff
			}
		}
		w_.Done()
	}()
	//self.findSetDouble(e,e.tag>>1,func(s *Set){
	self.findSetDouble(e,0,func(s *Set){
		keys = append(keys,s.Key())
		w.Add(1)
		go func(s_ *Set){
			if s_.loadSamp(self){
				SetsChan<-s_
			}
			w.Done()
		}(s)
	})
	w.Wait()
	close(SetsChan)
	w_.Wait()
	if minSet == nil {
		return false
	}
	if minSet.checkDar(minDiff){
		e.diff = minDiff
		e.s = minSet
	}else{
		e.s = NewSet(e)
		Sets = append(Sets,e.s)
	}
	le := len(Sets)
	tmps := make([][]*Sample,le)
	for i:=0;i<le ;i++{
		tmps[i] = make([]*Sample,0,100)
	}

	var I int
	var d float64
	for i,s := range Sets {
		for _,_e := range s.samp {

			I = i
			if _e == e {
				tmps[I] = append(tmps[I],_e)
				continue
			}
			if _e.diff == 0 {
				_e.diff = s.distance(_e)
			}
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
	//self.updatePoolDB([]byte{e.s.tag},
	self.updatePoolDB([]byte{0},
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

func (self *Pool) Check(e *Sample) bool {
	return e.check
}

func (self *Pool) add(e *Sample) bool {
	Sets:=make([]*Set,0,100)
	SetsMap := new(sync.Map)
	keys:=make([][]byte,0,100)
	//self.findSetDouble(e,e.tag>>1,func(s *Set){
	self.findSetDouble(e,0,func(s *Set){
		if s.loadSamp(self){
			Sets = append(Sets,s)
			SetsMap.Store(s,true)
		}else{
			keys = append(keys,s.Key())
		}
	})
	le := len(Sets)
	if le == 0 {
		return false
	}
	Sets = append(Sets,e.s)
	var I int
	var d float64
	up := false
	var Sets__ []*Set
	for{
		Sets__ = make([]*Set,0,len(Sets))
		tmps := make([][]*Sample,len(Sets))
		for i ,_ := range tmps {
			tmps[i] = make([]*Sample,0,100)
		}
		for i,s := range Sets {
			for _,_e := range s.samp {
				I = i
				if _e.diff == 0 {
					_e.diff = s.distance(_e)
				}
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
				if I != i {

					if _e.setMap[Sets[I]] {
						tmps[i] = append(tmps[i],_e)
						continue
					}
					up = true
					s.up = true

					Sets[I].up = true
				}
				tmps[I] = append(tmps[I],_e)
			}
		}
		if !up{
			break
		}
		for i,s := range Sets {
			if !s.up{
				Sets__ = append(Sets__,s)
				continue
			}
			if _,ok := SetsMap.Load(s);ok {
				SetsMap.Delete(s)
				keys = append(keys,s.Key())
			}
			if len(tmps[i]) == 0 {
				continue
			}
			s.update(tmps[i])
			Sets__ = append(Sets__,s)
		}
		Sets = Sets__
		up = false
	}

	//self.updatePoolDB([]byte{e.s.tag},
	self.updatePoolDB([]byte{0},
	func(db *bolt.Bucket)(err error){
		for _,k:= range keys {
			err = db.Delete(k)
			if err != nil {
				return err
			}
		}
		for _, _s := range Sets {
			if _,ok := SetsMap.Load(_s);!ok {
				err = db.Put(_s.Key(),_s.toByte())
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
	return true
}
func (self *Pool) add_s(e *Sample) bool {
	Sets:=make([]*Set,0,100)
	SetsMap := new(sync.Map)
	keys:=make([][]byte,0,100)
	var w sync.WaitGroup
	self.findSetDouble(e,0,func(s *Set){
		if s.loadSamp(self){
			Sets = append(Sets,s)
			SetsMap.Store(s,true)
			//SetsMap[s] = true
		}else{
			keys = append(keys,s.Key())
		}
	})
	le := len(Sets)
	if le == 0 {
		return false
	}

	Sets = append(Sets,e.s)
	var I int
	var d float64
	up := false
	var Sets__ []*Set
	for{
		Sets__ = make([]*Set,0,len(Sets))
		tmps := make([][]*Sample,len(Sets))
		for i ,_ := range tmps {
			tmps[i] = make([]*Sample,0,100)
		}
		for i,s := range Sets {
			for _,_e := range s.samp {
				I = i
				if _e.diff == 0 {
					_e.diff = s.distance(_e)
				}
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
				if I != i {
					if _e.setMap[Sets[I]] {
						tmps[i] = append(tmps[i],_e)
						continue
					}
					up = true
					s.up = true
					Sets[I].up = true
				}
				tmps[I] = append(tmps[I],_e)
			}
		}
		if !up{
			break
		}

		le := len(Sets)
		chanSets := make(chan *Set,le)
		chanDel := make(chan []byte,le)
		w.Add(le)
		for i,s := range Sets {
			go func(s_ *Set, tmp []*Sample){
				defer w.Done()
				if !s_.up{
					chanSets <- s_
					return
				}
				if _,ok := SetsMap.Load(s_);ok {
					SetsMap.Delete(s_)
					chanDel<-s_.Key()
				}
				if len(tmp) == 0 {
					return
				}
				s_.update(tmp)
				chanSets <- s_
			}(s,tmps[i])
		}
		w.Wait()
		close(chanSets)
		close(chanDel)
		for del := range chanDel {
			keys = append(keys,del)
		}
		for s := range chanSets {
			Sets__ = append(Sets__,s)
		}

		Sets = Sets__
		//if len(Sets) < 2 {
		//	break
		//}
		up = false

	}

	//self.updatePoolDB([]byte{e.s.tag},
	self.updatePoolDB([]byte{0},
	func(db *bolt.Bucket)(err error){
		for _,k:= range keys {
			err = db.Delete(k)
			if err != nil {
				return err
			}
		}
		for _, _s := range Sets {
			//if len(_s.samp) == 0 {
			//	continue
			//}
			if _,ok := SetsMap.Load(_s);!ok {
				err = db.Put(_s.Key(),_s.toByte())
				if err != nil {
					return err
				}
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
	//sp.TmpSa[int(e.tag>>1)] <- e
	sp.TmpSa[0] <- e
}
