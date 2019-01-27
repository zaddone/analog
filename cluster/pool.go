package cluster
import(
	"github.com/boltdb/bolt"
	"github.com/zaddone/analog/config"
	"time"
	"fmt"
	"path/filepath"
	"encoding/binary"
	"os"
	"bytes"
	"sync"
)
var (
	MaxTime int64
)

type Pool struct {

	SampDB *bolt.DB
	PoolDB *bolt.DB

	Diff float64
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
		Diff:self.Diff,
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
	self.Diff = 0

}
func (self *Pool) FindSet(e *Sample) (set *Set) {
	set,_ = self.find(e)
	return

}

func (self *Pool) add(e *Sample,w *sync.WaitGroup) bool{

	MinSet,diff := self.find(e)
	if MinSet == nil {
		return false
		//NewSet(e).saveDB(self)
		//return true
	}
	if !MinSet.loadSamp(self) {
		return self.add(e,w)
	}
	if (self.Diff!=0) && (diff>self.Diff) {
		return false
	}
	TmpSet := &Set{}
	TmpSet.update(append(MinSet.samp,e))
	if len(TmpSet.samp) < 4 {
		MinSet.deleteDB(self)
		TmpSet.saveDB(self)
		return true
	}

	var _e *Sample
	_e, self.Diff = TmpSet.findLong()
	if bytes.Equal(_e.KeyName(),e.KeyName()) {
		NewSet(e).saveDB(self)
		return true
	}
	MinSet.deleteDB(self)

	w.Add(1)
	go func(s *Set,p *Pool,__e *Sample,_w *sync.WaitGroup){
		var k string
		var TmpSet_ *Set
		defer _w.Done()
		for{
			le := len(s.samp)
			if le > 0 {
				TmpSet_ = &Set{}
				TmpSet_.update(s.samp)
				p.Diff = TmpSet_.distance(__e)
			}
			if !p.add(__e,_w) {
				s.saveDB(p)
				break
			}
			if le == 0 {
				break
			}
			s = TmpSet_
			__e,p.Diff = s.findLong()
			k = string(__e.KeyName())
			if _,ok:= p.tmpSample.Load(k);ok {
				s.saveDB(p)
				break
			}
			p.tmpSample.Store(k,__e)
		}
	}(TmpSet,self.Copy(),_e,w)
	return true

}

func (self *Pool) find(e *Sample) (*Set,float64) {
	key := make([]byte,16)
	binary.BigEndian.PutUint64(key,uint64(e.Duration()))
	var S_1,S_2 *Set
	var diff_1,diff_2 float64
	var w sync.WaitGroup
	w.Add(2)
	go func(_w *sync.WaitGroup){
		var diff float64
		err := self.PoolDB.View(func(tx *bolt.Tx)error{
			db := tx.Bucket([]byte{self.tag})
			if db == nil {
				return nil
			}
			c := db.Cursor()
			for k,v := c.Seek(key);k!= nil;k,v = c.Next() {
				//t++
				S := &Set{}
				S.load(v)
				diff = S.distance(e)
				if (diff_1 == 0) || (diff_1 > diff) {
					S_1 = S
					diff_1 = diff
				}else{
					if diff_1/diff < config.Conf.DisPool {
						break
					}
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
		var diff float64
		err := self.PoolDB.View(func(tx *bolt.Tx)error{
			db := tx.Bucket([]byte{self.tag})
			if db == nil {
				return nil
			}
			c := db.Cursor()
			c.Seek(key)
			for k,v := c.Prev(); k != nil;k,v = c.Prev() {
				S := &Set{}
				S.load(v)
				diff = S.distance(e)
				if (diff_2 == 0) || (diff_2 > diff) {
					S_2 = S
					diff_2 = diff
				}else{
					if diff_2/diff < config.Conf.DisPool {
						break
					}
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
	if diff_1==0 && diff_2 ==0 {
		return nil,0
	}
	if diff_1 > diff_2 {
		return S_2,diff_2
	}else{
		return S_1,diff_1
	}

}

func (sp *Pool) Add(e *Sample) {
	sp.tag = e.Key[8]>>1
	DateKey := time.Unix( int64(binary.BigEndian.Uint64(e.Key[:8])),0)
	ke :=uint64(DateKey.AddDate(-4,0,0).Unix())
	err := sp.SampDB.Update(func(tx *bolt.Tx)error{
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
		return db.Put(e.Key,e.toByte())
	})
	if err != nil {
		panic(err)
	}
	timeB := time.Now().Unix()
	sp.clear()
	var w sync.WaitGroup
	if !sp.add(e,&w){
		NewSet(e).saveDB(sp)
	}
	w.Wait()

	dif := time.Now().Unix() - timeB
	if dif > MaxTime {
		MaxTime = dif
		fmt.Println("times",MaxTime)
	}
}
