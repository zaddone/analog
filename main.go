package main
import (
	//"fmt"
	"github.com/zaddone/analog/request"
	"github.com/zaddone/analog/cache"
	"github.com/zaddone/analog/config"
	//"github.com/zaddone/analog/printit"
	"strings"
	"github.com/zaddone/operate/oanda"
	"encoding/json"
	"sync"
	//"github.com/zaddone/analog/server"
	"github.com/boltdb/bolt"
)
var (
	InsSet sync.Map = sync.Map{}
)
func main() {

	syncPrice()
	run()
	//server.Router.Run(config.Conf.Port)
	//var cmd string
	//for {
	//	fmt.Scanf("%s\r", &cmd)
	//	//if cmd == "print" {
	//	//	printit.ReadLog()
	//	//}
	//	fmt.Println(cmd)
	//	cmd = ""
	//}

}
func run(){
	if insCache,ok := InsSet.Load(config.Conf.InsName);ok {
		//var w sync.WaitGroup
		insCache.(*cache.Cache).Run(func(t int64){
			//InsSet.Range(func(k interface{},v interface{})bool {
			//	if k.(string) != config.Conf.InsName {
			//		w.Add(1)
			//		go v.(*cache.Cache).Follow(t,&w)
			//	}
			//	return true
			//})
			//w.Wait()
		})
	}else{
		panic("run err"+config.Conf.InsName)
	}
}

func syncPrice() {
	var b *bolt.Bucket
	//fmt.Println("sync price start")
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
				kins := strings.Split(config.Conf.InsName,"_")
				if strings.Contains(_ins.Name,kins[0]) ||
				strings.Contains(_ins.Name,kins[1]){
				//if _ins.Type == "CURRENCY" {
					InsSet.Store(string(k),cache.NewCache(_ins,InsSet))
				}
				//Ins = append(Ins,string(k))
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
		syncPrice()
	}

}
