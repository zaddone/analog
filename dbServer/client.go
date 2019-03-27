package main
import(
	"github.com/zaddone/analog/dbServer/cache"
	"github.com/zaddone/analog/config"
	"github.com/zaddone/analog/request"
	"github.com/zaddone/operate/oanda"
	"github.com/boltdb/bolt"
	"encoding/json"
	//"fmt"
	//"time"
)
var (
	CL *cacheList = NewCacheList()
)
type cacheList struct {
	cas []*cache.Cache
}
func NewCacheList() *cacheList {
	return &cacheList{
	}
}
func (self *cacheList) Show() (n int) {
	return 0
}
func (self *cacheList) Len() int {
	return len(self.cas)
}
func (self *cacheList) Read(h func(int,interface{})){
	for i,c := range self.cas {
		h(i,c)
	}
}
func (self *cacheList) HandMap(m []byte,hand func(interface{},byte)){

	if m == nil {
		return
	}
	var t byte
	var j,J uint
	for i,n := range m {
		if n == 255 || n == 0 {
			continue
		}
		for j=0;j<4;j++{
			J = j*2
			t = (n&^(^(3<<J)))>>J
			if t == 3 || t == 0 {
				continue
			}
			hand(self.cas[i*4+int(j)],t)
		}
	}

}

func loadCache(){
	err := config.HandDB(func(db *bolt.DB)error{
		return db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket(request.Ins_key)
			if b == nil {
				return nil
			}
			return b.ForEach(func(k,v []byte)error{
				_ins := &oanda.Instrument{}
				err := json.Unmarshal(v,_ins)
				if err != nil {
					panic(err)
				}
				ca := cache.NewCache(_ins)
				CL.cas = append(CL.cas,ca)
				if _ins.Name == config.Conf.InsName {
					go ca.SyncRun(CL)

				}
				return nil
			})
		})
	})
	if err != nil {
		panic(err)
	}
	if CL.Len() == 0 {
		err = request.DownAccountProperties()
		if err != nil {
			panic(err)
		}
		loadCache()
	}
}
func main(){
	loadCache()
	//for CL.cas
	select{}
}
