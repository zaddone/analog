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

	//runChan chan bool
	//runChan := make(chan bool,7)
}

func (self *Pool) syncAdd(chanSa chan *Sample){
	for{
		e := <-chanSa
		e.s = NewSet(e)
		if !self.add_s(e){
			e.s = NewSet(e)
			e.s.saveDB(self)
			//self.updatePoolDB([]byte{0},func(db *bolt.Bucket)error{
			//	return db.Put(e.s.Key(),e.s.toByte())
			//})
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
		TmpSa:[2]chan *Sample{make(chan *Sample,3),make(chan *Sample,5)},
		path:filepath.Join(config.Conf.ClusterPath,ins),
		//runChan:make(chan bool,7)
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
func (self *Pool) Check(e *Sample) bool {

	var diff,minDiff float64
	var minSet *Set
	SetsChan := make(chan *Set,100)
	var w sync.WaitGroup
	w.Add(1)
	go func(){
		for s := range SetsChan{
			diff = s.distance(e)
			if minDiff > diff || minDiff == 0 {
				minDiff = diff
				minSet = s
			}
		}
		w.Done()
	}()
	self.findSetDouble(e,e.tag>>1,func(s *Set){
	//self.findSetDouble(e,0,func(s *Set){
		SetsChan <- s
	})
	close(SetsChan)
	w.Wait()
	if minSet == nil {
		return false
	}
	n := e.tag>>1
	return minSet.count[n] > minSet.count[n^1]

	//return e.check
}

func (self *Pool) add(e *Sample) bool {

	Sets:=make([]*Set,0,100)
	KeysMap := new(sync.Map)
	SetsMap := new(sync.Map)

	var w sync.WaitGroup
	self.findSetDouble(e,e.s.tag,func(s *Set){
	//self.findSetDouble(e,0,func(s *Set){
		w.Add(1)
		go func(s_ *Set){
			if s_.loadSamp(self){
				SetsMap.Store(s_,true)
			}else{
				KeysMap.Store(string(s_.Key()),true)
			}
			w.Done()
		}(s)
	})
	w.Wait()
	SetsMap.Range(func(k,v interface{})bool{
		Sets = append(Sets,k.(*Set))
		return true
	})

	//self.findSetDouble(e,e.s.tag,func(s *Set){
	////self.findSetDouble(e,0,func(s *Set){
	//	if s.loadSamp(self){
	//		Sets = append(Sets,s)
	//		SetsMap.Store(s,true)
	//	}else{
	//		KeysMap.Store(string(s.Key()),true)
	//	}
	//})

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

		le = len(Sets)
		Sets__ = make([]*Set,0,le)
		tmps := make([][]*Sample,le)
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
					if _,ok := _e.setMap.Load(_s);ok {
						continue
					}
					d = _s.distance(_e)
					if _e.diff > d {
						_e.diff = d
						I = _i
					}
				}
				if I != i {
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

		chanSets := make(chan *Set,len(Sets))
		for i,s := range Sets {
			if !s.up{
				//chanSets <- s
				Sets__ = append(Sets__,s)
				continue
				//return
			}
			if _,ok := SetsMap.Load(s);ok {
				SetsMap.Delete(s)
				//keys = append(keys,s.Key())
				KeysMap.Store(string(s.Key()),true)
			}
			if len(tmps[i]) == 0 {
				continue
				//return
			}
			w.Add(1)
			go func(s_ *Set, tmp []*Sample){
				s_.update(tmp)
				chanSets <- s_
				w.Done()
				//Sets__ = append(Sets__,s)
			}(s,tmps[i])
		}
		w.Wait()
		close(chanSets)
		for s := range chanSets {
			Sets__ = append(Sets__,s)
		}
		Sets = Sets__
		up = false

	}

	self.updatePoolDB([]byte{e.s.tag},
	//self.updatePoolDB([]byte{0},
	func(db *bolt.Bucket)(err error){
		KeysMap.Range(func(k,v interface{})bool{
			err = db.Delete([]byte(k.(string)))
			if err != nil {
				panic(err)
			}
			return true
		})
		//for _,k:= range keys {
		//	err = db.Delete(k)
		//	if err != nil {
		//		return err
		//	}
		//}
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

	Sets:= make([]*Set,0,100)
	SetsMap := new(sync.Map)
	KeysMap := new(sync.Map)
	var w,w_ sync.WaitGroup
	self.findSetDouble(e,e.s.tag,func(s *Set){
	//self.findSetDouble(e,0,func(s *Set){
		w.Add(1)
		go func(s_ *Set){
			if s_.loadSamp(self){
				SetsMap.Store(s_,true)
			}else{
				KeysMap.Store(string(s_.Key()),true)
			}
			w.Done()
		}(s)
	})
	w.Wait()
	SetsMap.Range(func(k,v interface{})bool{
		Sets = append(Sets,k.(*Set))
		return true
	})
	le := len(Sets)
	if le == 0 {
		return false
	}
	Sets = append(Sets,e.s)
	//up := false
	var Sets__ []*Set
	//for n:=0;n<20;n++{
	type tmpDB struct {
		id int
		sa *Sample
	}
	//for n:=0;n<10;n++ {
	for{
		//fmt.Println(n)
		le = len(Sets)
		//fmt.Println(le)
		tmps := make([][]*Sample,le)
		up := false
		for i ,_ := range tmps {
			tmps[i] = make([]*Sample,0,100)
		}
		tmpsChan := make(chan *tmpDB,le)
		w_.Add(1)
		go func(){
			for db := range tmpsChan{
				tmps[db.id] = append(tmps[db.id],db.sa)
			}
			w_.Done()
		}()
		for i_,s_ := range Sets {
			w.Add(len(s_.samp))
			for _,_e_ := range s_.samp {
				go func(i int,_e *Sample){
					I := i
					if _e.diff == 0 {
						_e.diff = Sets[i].distance(_e)
					}
					for _i,_s := range Sets{
						if _,ok := _e.setMap.Load(_s);ok {
							continue
						}
						d := _s.distance(_e)
						if _e.diff > d {
							_e.diff = d
							I = _i
						}
					}
					if I != i {
						if !up {
							up = true
						}
						if !Sets[i].up {
							Sets[i].up = true
						}
						if !Sets[I].up {
							Sets[I].up = true
						}
					}
					tmpsChan <- &tmpDB{I,_e}
					w.Done()
				}(i_,_e_)
			}
		}
		w.Wait()
		close(tmpsChan)
		w_.Wait()
		if !up{
			break
		}

		Sets__ = make([]*Set,0,le)
		for i,s := range Sets {
			if !s.up{
				Sets__ = append(Sets__,s)
				continue
			}
			if _,ok := SetsMap.Load(s);ok {
				SetsMap.Delete(s)
				KeysMap.Store(string(s.Key()),true)
			}
			if len(tmps[i]) == 0 {
				continue
			}

			Sets__ = append(Sets__,s)
			w.Add(1)
			go func(s_ *Set, tmp []*Sample){
				s_.update(tmp)
				w.Done()
			}(s,tmps[i])
		}
		w.Wait()
		Sets = Sets__

	}

	self.updatePoolDB([]byte{e.s.tag},
	//self.updatePoolDB([]byte{0},
	func(db *bolt.Bucket)(err error){
		KeysMap.Range(func(k,v interface{})bool{
			err = db.Delete([]byte(k.(string)))
			if err != nil {
				panic(err)
			}
			return true
		})
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
	//sp.TmpSa[0] <- e
}
