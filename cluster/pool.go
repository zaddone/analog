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
	TmpSa chan *Sample
}
func (self *Pool) syncAdd(){
	for{
		e := <-self.TmpSa
		if !self.add(e){
			NewSet(e).saveDB(self)
		}
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

	err := self.SampDB.View(func(_t *bolt.Tx)error{
		db := _t.Bucket([]byte{9})
		if db == nil {
			return nil
		}
	//self.viewPoolDB([]byte{9},func(db *bolt.Bucket)error{
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
		TmpSa:make(chan *Sample,5),
	}

	po.SampDB,err = bolt.Open(filepath.Join(p,config.Conf.SampleDbPath),0600,nil)
	if err != nil {
		panic(err)
	}
	po.PoolDB,err = bolt.Open(filepath.Join(p,config.Conf.PoolDbPath),0600,nil)
	if err != nil {
		panic(err)
	}
	go po.syncAdd()
	return po

}

func (self *Pool) Close(){
	self.PoolDB.Close()
	self.SampDB.Close()
}
func (self *Pool) CheckSet(e *Sample) (m []byte) {

	var minS,minSf *Set
	var w sync.WaitGroup
	w.Add(2)
	go func(){
		var diff,minDiff float64
		self.findSetDouble(e,e.tag>>1,func(s *Set){
			diff = s.distance(e)
			if diff < minDiff || minDiff == 0 {
				minDiff = diff
				minS = s
			}
		})
		w.Done()
	}()
	go func(){
		var diff,minDiff float64
		self.findSetDouble(e,(e.tag>>1)^1,func(s *Set){
			diff = s.distanceF(e)
			if diff < minDiff || minDiff == 0 {
				minDiff = diff
				minSf = s
			}
		})
		w.Done()
	}()
	w.Wait()
	if (minS == nil) || (minSf == nil)  {
		return nil
	}


	m = minS.List[0].CaMap
	for _,_m := range minS.List[1:]{
		for i,n := range m{
			m[i] = _m.CaMap[i]|n
		}
	}
	for _,_m := range minSf.List{
		for i,n := range m{
			m[i] = (^_m.CaMap[i])|n
		}
	}
	return

	//if len(minS.List) < config.MinSam {
	//	return nil
	//}

}

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
	s := self.find(e)
	if s == nil {
		return false
	}
	n := int(e.tag &^ 2)
	s.count[n]--
	return s.CheckCountMax(n)
}

func (self *Pool) find(e *Sample) *Set {
	var Sets []*Set
	self.findSetDouble(e,e.tag>>1,func(s *Set){
		if s.loadSamp(self){
			Sets = append(Sets,s)
		}
	})
	le := len(Sets)
	if le == 0 {
		return nil
	}

	ns := NewSet(e)
	Sets = append(Sets,ns)
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
			if I == le{
				ns.samp = append(ns.samp,_e)
				ns.count[int(_e.tag &^ 2)] ++
			}
		}
	}
	return ns
	//return nil

}

func (self *Pool) add(e *Sample) bool {
	Sets:=make([]*Set,0,10)
	SetsChan := make( chan *Set,10)
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
		tmps[i] = make([]*Sample,0,10)
		tmpchan[i] = make(chan *Sample,10)
		go func(_w *sync.WaitGroup,i_ int){
			for _e := range tmpchan[i_] {
				tmps[i_] = append(tmps[i_],_e)
			}
			_w.Done()
		}(&w_,i)

	}
	ns := NewSet(e)
	Sets_ := append(Sets,ns)
	tmps[le-1] = append(tmps[le-1],e)
	//tmps = append(tmps,ns.samp)
	for i,s := range Sets {
		w.Add(len(s.samp))
		for _,_e := range s.samp {
			go func(i_ int,__e *Sample,_w *sync.WaitGroup){
				I := i_
				__e.diff = s.distance(__e)
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
	self.updatePoolDB([]byte{ns.tag},
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

func (sp *Pool) Add(e *Sample) {
	go func(_e *Sample){
		DateKey := time.Unix( int64(binary.BigEndian.Uint64(_e.KeyName()[:8])),0)
		ke := uint64(DateKey.AddDate(-config.Conf.Year,0,0).Unix())
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
	}(e)
	sp.TmpSa <- e
}
