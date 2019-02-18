package main
import(
	"log"
	"time"
	"github.com/zaddone/analog/cache"
	"github.com/zaddone/analog/request"
	"github.com/boltdb/bolt"
	"github.com/zaddone/analog/config"
	"github.com/zaddone/operate/oanda"
	"encoding/json"
	"net"
)
var (
	CacheMap map[string]*cache.Cache =  map[string]*cache.Cache{}
	Buffer [8192]
)

func main(){
	loadCacheDown()
	for{
		time.Sleep(time.Second*3600)
	}
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
				CacheMap[_ins.Name] = ca
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
func server(){
	ln, err := net.Listen("tcp", config.Conf.Port)
	if err != nil {
		panic(err)
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		go handleConnection(conn)
	}
	ln.Close()
}
func handleConnection(conn net.Conn) {
	//n,err := conn.Read(Buffer[:])
	//if err != nil {
	//	log.Println(err)
		conn.Close()
	//	return
	//}
	//if n == 0 {
	//	conn.Close()
	//	return
	//}
	//Buffer[:n]
}
