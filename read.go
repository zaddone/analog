package main
import(

	"github.com/zaddone/analog/config"
	"github.com/zaddone/analog/request"
	"github.com/zaddone/analog/cache"
	"github.com/zaddone/operate/oanda"
	"github.com/boltdb/bolt"
	"encoding/json"
	"fmt"
	"time"
	//"strings"

)
var (
	calist []*oanda.Instrument
	FirstCache *cache.Cache
)
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
				//ca := cache.NewCache(_ins)
				if _ins.Name == config.Conf.InsName {
					FirstCache  = cache.NewCache(_ins)
				}
				calist = append(calist,_ins)
				//NewCache(_ins,InsList)
				return nil
			})
		})
	})
	if err != nil {
		panic(err)
	}
	if len(calist) == 0 {
		err = request.DownAccountProperties()
		if err != nil {
			panic(err)
		}
		loadCache()
	}
}
func main(){
	loadCache()
	FirstCache.SetPool()
	var begin int64
	//fmt.Println(FirstCache.Ins.Name)
	FirstCache.ReadAll(func (t int64){
		fmt.Println(time.Unix(t,0))
		if t - begin > 604800 {
			FirstCache.SaveTestLog(t)
			begin = t
		}
	})
	fmt.Println("read cache over")

}
