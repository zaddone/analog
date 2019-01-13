package main
import (
	"fmt"
	"github.com/zaddone/analog/request"
	"github.com/zaddone/analog/cache"
	"github.com/zaddone/analog/config"
	//"github.com/zaddone/analog/printit"
	"strings"
	"github.com/zaddone/operate/oanda"
	"encoding/json"
	//"sync"

	//"os"
	//"os/signal"

	//"github.com/zaddone/analog/server"
	"github.com/boltdb/bolt"
	//"time"
)
var (
	//InsSet sync.Map = sync.Map{}
	InsList []*cache.Cache
	FirstCache *cache.Cache
)
func main() {

	//syncPrice()

	run()
	//c:=make(chan os.Signal)
	//signal.Notify(c)
	//fmt.Println("get signal:",<-c)
	//Close()
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
//func Close(){
//	for _,ca := range InsList {
//		ca.Close()
//		fmt.Println(ca.Ins.Name,"close")
//	}
//	fmt.Println("close")
//}
func run(){
	//Test()
	//syncPrice()
	loadCache()
	//for _,ca := range InsList {
	//	ca.CacheAll = InsList
	//	go ca.Run(func(int64){})
	//}

	FirstCache.Run(func(t int64){
		//var w sync.WaitGroup
		//w.Add(len(InsList)-1)
		//for _,ca := range InsList {
		//	ca.CacheAll = InsList
		//	if ca == FirstCache {
		//		continue
		//	}
		//	go ca.Follow(t,&w)
		//}
		//w.Wait()
		//fmt.Println(time.Unix(t,0))
	})
	fmt.Println("run Success")
	//if insCache,ok := InsSet.Load(config.Conf.InsName);ok {
	//	var w sync.WaitGroup
	//	insCache.(*cache.Cache).Run(func(t int64){
	//		InsSet.Range(func(k interface{},v interface{})bool {
	//			if k.(string) != config.Conf.InsName {
	//				w.Add(1)
	//				go v.(*cache.Cache).Follow(t,&w)
	//			}
	//			return true
	//		})
	//		w.Wait()
	//	})
	//}else{
	//	panic("run err"+config.Conf.InsName)
	//}
}
func loadCache(){
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
				if (FirstCache == nil) && strings.EqualFold(string(k),config.Conf.InsName) {
					FirstCache = cache.NewCache(_ins)
					InsList = append(InsList,FirstCache)
				}else{
					InsList = append(InsList,cache.NewCache(_ins))
				}
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
		loadCache()
	}
}

//func syncPrice() {
//	var b *bolt.Bucket
//	//fmt.Println("sync price start")
//	err := config.HandDB(func(db *bolt.DB)error{
//		return db.View(func(tx *bolt.Tx) error {
//			b = tx.Bucket(request.Ins_key)
//			if b == nil {
//				return nil
//			}
//			return b.ForEach(func(k,v []byte)error{
//				_ins := &oanda.Instrument{}
//				err := json.Unmarshal(v,_ins)
//				if err != nil {
//					panic(err)
//				}
//				//kins := strings.Split(config.Conf.InsName,"_")
//				//if strings.Contains(_ins.Name,kins[0]) ||
//				//strings.Contains(_ins.Name,kins[1]){
//				////if _ins.Type == "CURRENCY" {
//				//	InsSet.Store(string(k),cache.NewCache(_ins))
//				//}
//				//Ins = append(Ins,string(k))
//				InsSet.Store(string(k),cache.NewCache(_ins))
//				return nil
//			})
//		})
//	})
//	if err != nil {
//		panic(err)
//	}
//	if b == nil {
//		err = request.DownAccountProperties()
//		if err != nil {
//			panic(err)
//		}
//		syncPrice()
//	}
//
//}
//func Test(){
//	var b *bolt.Bucket
//	err := config.HandDB(func(db *bolt.DB)error{
//		return db.View(func(tx *bolt.Tx) error {
//			b = tx.Bucket(request.Ins_key)
//			if b == nil {
//				return nil
//			}
//			_ins := &oanda.Instrument{}
//			err := json.Unmarshal(b.Get([]byte(config.Conf.InsName)),_ins)
//			if err != nil {
//				panic(err)
//			}
//			InsSet.Store(config.Conf.InsName,cache.NewCache(_ins))
//			return nil
//		})
//	})
//	if err != nil {
//		panic(err)
//	}
//	if b == nil {
//		err = request.DownAccountProperties()
//		if err != nil {
//			panic(err)
//		}
//		Test()
//	}
//}
//func Close(){
//	fmt.Println("close")
//	InsSet.Range(func(k,v interface{})bool{
//		v.(*cache.Cache).Close()
//		return true
//	})
//
//}
