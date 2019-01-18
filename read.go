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

)
func main(){
	var FirstCache *cache.Cache
	err := config.HandDB(func(db *bolt.DB)error{
		return db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket(request.Ins_key)
			if b == nil {
				return nil
			}
			fmt.Println(config.Conf.InsName)
			v := b.Get([]byte(config.Conf.InsName))
			if v == nil {
				panic("v == nil")
			}
			_ins := &oanda.Instrument{}
			err := json.Unmarshal(v,_ins)
			if err != nil {
				panic(err)
			}
			FirstCache = cache.NewCache(_ins)
			FirstCache.SetPool()
			return nil

		})
	})
	if err != nil {
		panic(err)
	}
	if FirstCache == nil {
		panic("cache == nil")
	}
	fmt.Println("read cache")
	var begin int64
	FirstCache.Read(func (t int64){
		if begin == 0 {
			begin = t
		}
		if t - begin >= 604800 {
			begin = t
			fmt.Printf("%s %.2f %.2f,%.0f\r\n",time.Unix(t,0),FirstCache.Cshow[4]/FirstCache.Cshow[3],FirstCache.Cshow[1]/FirstCache.Cshow[0],FirstCache.Cshow)
			FirstCache.Cshow = [5]float64{0,0,0,0,0}
		}
	})
	fmt.Println("read cache over")

}
