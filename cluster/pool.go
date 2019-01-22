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
)
var (
	MaxTime int64
)

type Pool struct {

	SampDB *bolt.DB
	PoolDB *bolt.DB

	Diff float64
	tmpSample map[string]*Sample
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
		tmpSample:map[string]*Sample{},
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
func (self *Pool) Close(){
	self.PoolDB.Close()
	self.SampDB.Close()
}
func (self *Pool) clear(){
	self.tmpSample = map[string]*Sample{}
	self.Diff = 0

}
func (self *Pool) FindSet(e *Sample) (set *Set) {
	set,_ = self.find(e)
	return

}

func (self *Pool) add(e *Sample) bool{

	MinSet,diff := self.find(e)
	if MinSet == nil {
		return false
		//NewSet(e).saveDB(self)
		//return true
	}
	if !MinSet.loadSamp(self) {
		return self.add(e)
	}
	if (self.Diff!=0) && (diff>self.Diff) {
		return false
	}
	TmpSet := &Set{}
	TmpSet.update(append(MinSet.samp,e))
	var _e *Sample
	_e, self.Diff = TmpSet.findLong()
	if bytes.Equal(_e.KeyName(),e.KeyName()) {
		NewSet(e).saveDB(self)
		return true
	}
	MinSet.deleteDB(self)
	var k string
	var TmpSet_ *Set
	for{
		le := len(TmpSet.samp)
		if le > 0 {
			TmpSet_ = &Set{}
			TmpSet_.update(TmpSet.samp)
			self.Diff = TmpSet_.distance(_e)
		}
		if !self.add(_e) {
			TmpSet.saveDB(self)
			break
		}
		if le == 0 {
			break
		}
		TmpSet = TmpSet_
		//TmpSet.update(TmpSet.samp)
		_e,self.Diff = TmpSet.findLong()
		k = string(_e.KeyName())
		if self.tmpSample[k] != nil {
			TmpSet.saveDB(self)
			break
		}
		self.tmpSample[k] = _e

	}

	return true

}

func (self *Pool) find(e *Sample) (MinSet *Set,diffErr float64) {
	key := make([]byte,16)
	binary.BigEndian.PutUint64(key,uint64(e.Duration()))
	var diff float64
	//var MinSet *Set
	var k,v []byte
	//var t int
	err := self.PoolDB.View(func(tx *bolt.Tx)error{
		db := tx.Bucket([]byte{1})
		if db == nil {
			return nil
		}
		c := db.Cursor()
		for k,v = c.Seek(key);k!= nil;k,v = c.Next() {
			//t++
			S := &Set{}
			S.load(v)
			diff = S.distance(e)
			if (diffErr == 0) || (diff < diffErr) {
				MinSet = S
				diffErr = diff
			}else{
				if diff > config.Conf.DisPool {
					break
				}
			}
		}
		c.Seek(key)
		for k,v = c.Prev(); k!= nil;k,v = c.Prev() {
			//t++
			S := &Set{}
			S.load(v)
			diff = S.distance(e)
			if (diffErr == 0) || (diff < diffErr) {
				MinSet = S
				diffErr = diff
			}else{
				if diff > config.Conf.DisPool {
					break
				}
			}
		}
		return nil

	})
	if err != nil {
		panic(err)
	}
	//fmt.Println(t)
	if diffErr == 0 {
		return nil,diffErr
	}
	return

}

func (sp *Pool) Add(e *Sample) {
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
	if !sp.add(e){
		NewSet(e).saveDB(sp)
	}
	sp.clear()

	dif := time.Now().Unix() - timeB
	if dif > MaxTime {
		MaxTime = dif
		fmt.Println("times",MaxTime)
	}
}
