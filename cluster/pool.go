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
	//"math"
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
	//timeInterval int64

	//runChan chan bool
	//runChan := make(chan bool,7)
	//setCountChan chan float64
	//setCount float64
	samCount float64
	open bool
	//tmpSet [][]*Set
}

func (self *Pool) syncAdd(chanSa chan *Sample){
	for{
		select{
		case e:=<-chanSa:
			self.add_s_1(e)
			e.stop<-true
		//case s:=<-self.setCountChan:
		//	self.setCount += s
		}
		//e := <-chanSa
		//e.s = NewSet(e)
		//self.samCount++
		//if e.check {
		//	self.add_check(e)
		//}else{
		//self.add_s_1(e)
		//}
		//if !self.add_s(e){
		//	e.s = NewSet(e)
		//	e.s.saveDB(self)
		//	//self.updatePoolDB([]byte{0},func(db *bolt.Bucket)error{
		//	//	return db.Put(e.s.Key(),e.s.toByte())
		//	//})
		//}
		//e.stop<-true
	}
}

func (self *Pool) ShowPoolNum() (count [3]int) {

	//count[2] = int(self.setCount)
	count[0] = int(self.samCount)
	err := self.SampDB.View(func(t *bolt.Tx)error{
		db := t.Bucket([]byte{9})
		if db == nil {
			return nil
		}
		return db.ForEach(func(k,v []byte)error{
			count[2]++
			return nil
		})
	})
	if err != nil {
		panic(err)
	}
	//self.samCount = float64(count[2])

	self.viewPoolDB([]byte{0},func(db *bolt.Bucket)error{
		return db.ForEach(func(k,v []byte)error{
			//count[0] += len(NewSetLoad(k,v).List)
			count[1] ++
			return nil
		})
	})
	self.viewPoolDB([]byte{1},func(db *bolt.Bucket)error{
		return db.ForEach(func(k,v []byte)error{
			//count[0] += len(NewSetLoad(k,v).List)
			count[1] ++
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
	err := self.PoolDB.Batch(func(tx *bolt.Tx)error{
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
		//setCountChan:make(chan float64,100),
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

func (self *Pool) findAll(tag byte,h func(*Set)){

	self.viewPoolDB([]byte{tag},
	func(db *bolt.Bucket)error{
		return db.ForEach(func(k,v []byte)error {
			h(NewSetLoad(k,v))
			return nil
		})
	})
}

func (self *Pool) findSetDouble(e *Sample,tag byte,h func(*Set)){

	dur := uint64(e.Duration())
	key := make([]byte,8+len(e.KeyName()))
	binary.BigEndian.PutUint64(key,dur)

	self.viewPoolDB([]byte{tag},
	func(db *bolt.Bucket)error{
		c := db.Cursor()
		k,v := c.Seek(key)
		if k == nil {
			k,v := c.Prev()
			if k == nil {
				return nil
			}
			ke := k[:8]
			h(NewSetLoad(k,v))
			for k,v = c.Prev();k != nil;k,v = c.Prev() {
				if !bytes.Equal(ke,k[:8]){
					break
				}
				h(NewSetLoad(k,v))
			}
			return nil
		}
		h(NewSetLoad(k,v))
		ke := k[:8]
		for k,v = c.Next();k != nil;k,v = c.Next() {
			if !bytes.Equal(ke,k[:8]) {
				if bytes.Equal(ke,key[:8]){
					ke = k[:8]
				}else{
					break
				}
			}
			h(NewSetLoad(k,v))
		}

		c.Seek(key)
		k,v = c.Prev()
		if k == nil {
			return nil
		}
		ke = k[:8]
		for k,v := c.Prev();k != nil;k,v = c.Prev() {
			if !bytes.Equal(ke,k[:8]) {
				break
			}
			h(NewSetLoad(k,v))
		}
		return nil

	})

}
func (self *Pool) findSetDoubleBak(e *Sample,tag byte,h func(*Set)){

	dur := uint64(e.Duration())
	key := make([]byte,8+len(e.KeyName()))
	binary.BigEndian.PutUint64(key,dur)
	//var ke []byte
	self.viewPoolDB([]byte{tag},
	func(db *bolt.Bucket)error{
		c := db.Cursor()
		k,v := c.Seek(key)
		if k == nil {
			k,v := c.Prev()
			if k == nil {
				return nil
			}
			ke := k[:8]
			h(NewSetLoad(k,v))
			for k,v = c.Prev();k != nil;k,v = c.Prev() {
				if !bytes.Equal(ke,k[:8]){
					break
				}
				h(NewSetLoad(k,v))
			}
			return nil
		}
		h(NewSetLoad(k,v))
		ke := k[:8]
		c_ := db.Cursor()
		c_.Seek(k)
		k_,v_ := c_.Prev()
		if k_ == nil {
			for k,v = c.Next();k != nil;k,v = c.Next() {
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

func (self *Pool) GetSetMap(e *Sample) []byte {

	//_n := int(e.tag &^ 2)
	//if _n == 1 {
	//	return nil
	//}
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
		SetsChan <- s
	})
	close(SetsChan)
	w.Wait()
	if minSet == nil {
		return nil
	}
	//fmt.Println(len(minSet.List))
	if len(minSet.List) < config.Conf.MinSam{
		return nil
	}
	//if minSet.count[_n] < minSet.count[_n^1] {
	//	return nil
	//}

	var m []byte
	//m := make([]byte,len(minSet.List[0].CaMap))
	for _,l := range minSet.List{
		if (l.Key[8] == e.tag) && (l.CaMap != nil) {
		//if (l.Key[8] == e.tag) {
			m = l.CaMap
			break
		}
	}
	if m == nil {
		return nil
	}

	for _,l := range minSet.List{
		if l.Key[8] == e.tag{
			if l.CaMap == nil {
				return nil
			}
			for i,n := range l.CaMap {
				m[i] |= n
			}
		}else{
			if l.CaMap == nil {
				continue
			}
			for i,n := range l.CaMap {
				m[i] |= ^n
			}
		}
	}
	//fmt.Println(m)
	return m


}

func (self *Pool) Check(e *Sample) int {

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
		return 0
		//return false
	}
	if len(minSet.List) < config.Conf.MinSam {
		return 0
	}
	//minSet
	n := int(e.tag &^ 2)
	n1:= minSet.count[n]
	n2:= minSet.count[n^1]
	if (n1 == n2) {
		return 1
	}else{
		if n == 0 {
			if (n1>n2){
				return 2
			}else{
				return 3
			}
		}else{
			if (n1>n2){
				return 4
			}else{
				return 5
			}
		}
	}

	//return e.check
}


func Dressing(Sets []*Set) []*Set {

	var Sets__ []*Set
	var up bool
	var le int
	var tmps [][]*Sample
	for{
		up = false
		le = len(Sets)
		tmps = make([][]*Sample,le)
		Sets__ = make([]*Set,0,le)
		for i:=0; i<le; i++ {
			tmps[i] = make([]*Sample,0,len(Sets[i].samp)*2)
		}
		for i,s := range Sets {
			for _,_e := range s.samp {
				I := i
				if _e.init {
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
					//if _,ok := _e.setMap.Load(_s);ok {
					//	continue G
					//}
					d := _s.distance(_e)
					if _e.diff > d {
						_e.diff = d
						I = _i
					}
				}
				if I != i {
					up = true
					s.up = true
					if _,ok := _e.setMap.Load(Sets[I]); ok {
						Sets__ = append(Sets__,NewSet(_e))
						continue
					}
					Sets[I].up = true
				}
				tmps[I] = append(tmps[I],_e)
			}
		}
		if !up {
			break
		}
		for i,s := range Sets {
			if len(tmps[i]) == 0 {
				continue
			}
			Sets__ = append(Sets__,s)
			if s.up {
				s.update(tmps[i])
			}
			//Sets__ = append(Sets__,s)
		}
		Sets = Sets__
	}
	return Sets
}

func Dressing_s(Sets []*Set) []*Set {

	type tmpDB struct {
		id int
		sa *Sample
	}
	var Sets__ []*Set
	var up bool
	var le int
	var tmps [][]*Sample
	var tmpsChan chan *tmpDB
	var w,w_ sync.WaitGroup
	//fmt.Println(len(Sets))
	for{
		up = false
		le = len(Sets)
		tmps = make([][]*Sample,le)
		for i:=0; i<le; i++ {
			tmps[i] = make([]*Sample,0,len(Sets[i].samp)*2)
		}
		tmpsChan = make(chan *tmpDB,le)
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
						up = true
						Sets[i].up = true
						Sets[I].up = true
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
			if len(tmps[i]) == 0 {
				continue
			}

			Sets__ = append(Sets__,s)
			if s.up{
				w.Add(1)
				go func(s_ *Set, tmp []*Sample){
					s_.update(tmp)
					w.Done()
				}(s,tmps[i])
			}
		}
		w.Wait()
		Sets = Sets__
	}
	return Sets

}
//
//func (self *Pool) findMinSet(e *Sample) (minSet *Set) {
//
//	var diff,minDiff float64
//	SetsChan := make(chan *Set,100)
//	var w sync.WaitGroup
//	w.Add(1)
//	go func(){
//		for s := range SetsChan {
//			diff = s.distance(e)
//			if (minDiff > diff) || (minDiff == 0) {
//				minDiff = diff
//				minSet = s
//			}
//		}
//		w.Done()
//	}()
//	self.findSetDouble(e,e.tag>>1,func(s *Set){
//		SetsChan <- s
//	})
//	close(SetsChan)
//	w.Wait()
//	return
//
//}
//
//func (self *Pool) add(e *Sample) bool {
//
//	minSet := self.findMinSet(e)
//	if minSet == nil {
//		NewSet(e).saveDB(self)
//		return true
//	}
//	if !minSet.loadSamp(self) {
//		return self.add(e)
//	}
//
//	if minSet.checkDar(minDiff) {
//		minSet.update(append(minSet.samp,e))
//	}else{
//		Sets = append(Sets,NewSet(e))
//	}
//
//	return
//
//}

func (self *Pool) add_s_1(e *Sample) {

	Sets:= make([]*Set,0,100)
	chanSets:= make(chan *Set,100)
	KeysMap := new(sync.Map)

	var minDiff float64
	var minSet *Set
	//var darVal Dar
	var w,w_ sync.WaitGroup
	w_.Add(1)
	go func(){
		for s := range chanSets {
			if (s.tmp < minDiff) || (minDiff ==0) {
				minDiff = s.tmp
				minSet = s
			}
			Sets = append(Sets,s)
		}
		w_.Done()
	}()
	self.findSetDouble(e,e.tag>>1,func(s *Set){
		w.Add(1)
		go func(s_ *Set){
			if s_.loadSamp(self){
				s_.tmp = s_.distance(e)
				chanSets <- s_
			}
			KeysMap.Store(string(s_.Key()),true)
			w.Done()
		}(s)
	})
	w.Wait()
	close(chanSets)
	w_.Wait()
	le := len(Sets)
	if le == 0 {
		NewSet(e).saveDB(self)
		return
	}
	if minSet.checkDar(minDiff) {
		minSet.update(append(minSet.samp,e))
	}else{
		Sets = append(Sets,NewSet(e))
	}
	Sets = Dressing_s(Sets)

	//var co int
	//KeysMap.Range(func(k,v interface{}) bool {
	//	co++
	//	return true
	//})
	//self.setCountChan <- float64(len(Sets) - co)

	self.updatePoolDB([]byte{e.tag>>1},
	func(db *bolt.Bucket)(err error){
		for _, _s := range Sets {
			_s.SortDB(self)
			err = db.Put(_s.Key(),_s.toByte())
			if err != nil {
				return err
			}
			KeysMap.Delete(string(_s.Key()))
		}
		KeysMap.Range(func(k,v interface{})bool{
			err = db.Delete([]byte(k.(string)))
			if err != nil {
				panic(err)
			}
			return true
		})
		return nil
	})
	return

}

func (self *Pool) UpdateSample(e *Sample) {
	//sampDB := self.openSampDB()
	err := self.SampDB.Batch(func(tx *bolt.Tx)error{
		db, err := tx.CreateBucketIfNotExists([]byte{9})
		if err != nil {
			return err
		}

		//if (self.setCount>10000) && int(self.samCount/self.setCount) > config.Conf.MinSam {
		//	c := db.Cursor()
		//	k,_ := c.First()
		//	db.Delete(k)
		//}
		//self.samCount++
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
	func(_e *Sample){
		DateKey := time.Unix( int64(binary.BigEndian.Uint64(_e.KeyName()[:8])),0)
		ke := uint64(DateKey.AddDate(-config.Conf.Year,0,0).Unix())
		//db := sp.openSampDB()
		err := sp.SampDB.Batch(func(tx *bolt.Tx)error{
		//err := sp.SampDB.Update(func(tx *bolt.Tx)error{
			db, err := tx.CreateBucketIfNotExists([]byte{9})
			if err != nil {
				return err
			}
			//k,_ := db.Cursor().Last()
			//if (k != nil) &&
			//(int(time.Unix(int64(binary.BigEndian.Uint64(k[:8])),0).Year()) != int(time.Unix(int64(binary.BigEndian.Uint64(_e.KeyName()[:8])),0).Year())) {
			//	_e.check = true
			//}
			c := db.Cursor()
			k,_ := c.First()
			if k != nil && binary.BigEndian.Uint64(k[:8])<ke {
				sp.samCount--
				db.Delete(k)
			}

			//for k,_ := c.First();k!=nil;k,_ = c.Next() {
			//	if binary.BigEndian.Uint64(k[:8])<ke {
			//		sp.samCount--
			//		db.Delete(k)
			//	}else{
			//		break
			//	}
			//}
			//if (sp.setCount>10000) && int(sp.samCount/sp.setCount) > config.Conf.MinSam {
			//	c := db.Cursor()
			//	k,_ := c.First()
			//	db.Delete(k)
			//	//sp.samCount--
			////}else{
			//	//sp.samCount++
			//}

			sp.samCount++

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
