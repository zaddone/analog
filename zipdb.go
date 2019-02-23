package main
import(
	"github.com/zaddone/analog/config"
	"github.com/zaddone/analog/request"
	"github.com/zaddone/operate/oanda"
	"github.com/zaddone/analog/cache"
	"encoding/json"
	"github.com/boltdb/bolt"
	"fmt"
	//"time"
)
var (
	calist []*cache.Cache
)

func main(){
	loadCache()
	run := make(chan bool,4)
	for _,ca := range calist{
		go func(){
			run<-true
			fmt.Println(ca.Ins.Name)
			ca.SaveMinDB()
			<-run
		}()
	}

	//t := time.Tick(time.Second * 3600)
	//for e := range t {
	//	log.Println(e)
	//}

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
				calist = append(calist,cache.NewCache(_ins))
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
