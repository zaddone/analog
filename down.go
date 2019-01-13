package main
import(
	//"fmt"
	//"os"
	//"os/signal"
	"time"
	"github.com/zaddone/analog/cache"
	"github.com/zaddone/analog/request"
	"github.com/boltdb/bolt"
	"github.com/zaddone/analog/config"
	"github.com/zaddone/operate/oanda"
	"encoding/json"
)
func main(){
	loadCacheDown()
	for{
		<- time.After(time.Second *3600)
	}
	//c:=make(chan os.Signal)
	//signal.Notify(c)
	//fmt.Println("get signal:",<-c)
}
func loadCacheDown(){
	var b *bolt.Bucket
	err := config.HandDB(func(db *bolt.DB)error{
		return db.View(func(tx *bolt.Tx) error {
			b = tx.Bucket(request.Ins_key)
			if b == nil {
				return nil
			}
			return b.ForEach(func(k,v []byte)error{
				_ins := &oanda.Instrument{}
				err := json.Unmarshal(v,_ins)
				if err != nil {
					panic(err)
				}
				go cache.NewCache(_ins).RunDown()
				return nil
			})
		})
	})
	if err != nil {
		panic(err)
	}
	if b == nil {
		err = request.DownAccountProperties()
		if err != nil {
			panic(err)
		}
		loadCacheDown()
	}
}
