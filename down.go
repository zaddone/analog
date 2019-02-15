package main
import(
	//"fmt"
	//"os"
	//"os/signal"
	//"syscall"

	"time"
	"github.com/zaddone/analog/cache"
	"github.com/zaddone/analog/request"
	"github.com/boltdb/bolt"
	"github.com/zaddone/analog/config"
	"github.com/zaddone/operate/oanda"
	"encoding/json"

)
//var (
//	cachelist []*cache.Cache
//)
func main(){
	loadCacheDown()
	for{
		time.Sleep(time.Second*3600)
	}
	//c := make(chan os.Signal)
	//signal.Notify(c,syscall.SIGBUS, syscall.SIGINT, syscall.SIGTERM)
	//fmt.Println("receive signal:", <-c)
	//for _,ca := range cachelist {
	//	ca.Close()
	//}

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
				ca :=cache.NewCache(_ins)
				//fmt.Println("run",_ins.Name)
				go ca.RunDown()
				//cachelist = append(cachelist,ca)
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
