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
	set,_ = self.find(e)

	//self.tag = e.Key[8]>>1
	//set = &Set{}
	//s_ := self.findSet(e)
	//if len(s_) == 0 {
	//	return nil
	//}
	//for _,s := range s_ {
	//	set.Samplist = append(set.Samplist,s.Samplist...)
	//	for _i,v := range s.Count {
	//		set.Count[_i]+=v
	//	}
	//}
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
	if (le < config.Conf.MinSam){
		MinSet.deleteDB(self)
		TmpSet.saveDB(self)
		return true
	}
	_e, diff := TmpSet.findLong()
	if (_diff == 0) && (_e == e) {
		NewSet(_e).saveDB(self)
		return true
	}
	MinSet.deleteDB(self)
	if level == config.Conf.FindLevel{
	//if (_diff !=0){
		TmpSet.saveDB(self)
		return true
	}

	//self.tmpSample.Store(string(e.KeyName()),e)
	//var _e *Sample
	//tmp_e:=make(chan *Sample,le)
	tmp_e:=make([]*Sample,0,le)
	//var w sync.WaitGroup
	for{
		//if _,ok := self.tmpSample.Load(string(_e.KeyName()));ok{
		//	//NewSet(_e).saveDB(self)
		//	tmp_e <- _e
		//}else{

			//if len(TmpSet.samp) > 0 {
			//	TmpSet.update(TmpSet.samp)
			//	diff = TmpSet.distance(_e)
			//}

			//w.Add(1)
			//go func(_w *sync.WaitGroup,__e *Sample,diff_ float64){
			//	if !self.add(__e,diff_,level+1){
			//		tmp_e <- __e
			//	}
			//	_w.Done()
			//}(&w,_e,diff)
		//}
		if !self.add(_e,diff,level+1){
			tmp_e =append(tmp_e,_e)
		}
		_e, diff = TmpSet.findLong()
		if _e == nil {
			break
		}
	}
	//w.Wait()
	if len(tmp_e)==0 {
		return true
	}
	TmpSet.update(tmp_e)
	TmpSet.saveDB(self)
	return true

}

func (self *Pool) find(e *Sample) (*Set, float64) {
	dur := uint64(e.Duration())
	key := make([]byte,16)
	binary.BigEndian.PutUint64(key,dur)
	var diff,minDiff float64
	var minS,S *Set
	err := self.PoolDB.View(func(tx *bolt.Tx)error{
		db := tx.Bucket([]byte{self.tag})
		if db == nil {
			return nil
		}
		c := db.Cursor()
		next := func(du uint64){
			for _k,_v := c.Next();_k != nil;_k,_v = c.Next(){
				if (binary.BigEndian.Uint64(_k[:8]) - dur)>du {
					break
				}
				S = &Set{}
				S.load(_v)
				diff = S.distance(e)
				if diff<minDiff {
					minDiff = diff
					minS = S
				}
			}
		}
		prev := func(du uint64){
			for _k,_v := c.Prev();_k != nil;_k,_v = c.Prev(){
				if (binary.BigEndian.Uint64(_k[:8]) - dur)>du {
					break
				}
				S = &Set{}
				S.load(_v)
				diff = S.distance(e)
				if diff<minDiff {
					minDiff = diff
					minS = S
				}
			}
		}
		k,v := c.Seek(key)
		if k == nil {
			k,v =c.Last()
			if k == nil {
				return nil
			}
			minS = &Set{}
			minS.load(v)
			minDiff = minS.distance(e)
			prev(dur - binary.BigEndian.Uint64(k[:8]))
			return nil
		}
		k_,v_ := c.Prev()
		if k_ == nil {
			minS = &Set{}
			minS.load(v)
			minDiff = minS.distance(e)
			c.Next()
			next(binary.BigEndian.Uint64(k[:8]) - dur)
			return nil
		}
		d_1 := binary.BigEndian.Uint64(k[:8]) - dur
		d_2 := dur - binary.BigEndian.Uint64(k_[:8])
		if (d_1>d_2) {
			minS = &Set{}
			minS.load(v_)
			minDiff = minS.distance(e)
			prev(d_2)
		}else{
			minS = &Set{}
			minS.load(v)
			minDiff = minS.distance(e)
			c.Next()
			next(d_1)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	return minS,minDiff

}

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
