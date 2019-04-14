package main
import(
	"fmt"
	//"log"
	"github.com/zaddone/analog/config"
	//"github.com/zaddone/analog/cache"
	"github.com/zaddone/analog/request"
	"github.com/zaddone/operate/oanda"
	"github.com/boltdb/bolt"

	"encoding/json"
	//"encoding/binary"
	//"encoding/gob"
	"net"
	//"io"
	//"bytes"
	"os"

	"github.com/zaddone/analog/dbServer/data"
	"github.com/zaddone/analog/dbServer/proto"
)
var (
	CL  *dataList
)

type dataList struct {
	cas map[string]*data.Data
}
func NewDataList() (cl *dataList) {
	cl =  &dataList{cas:make(map[string]*data.Data)}
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
				cl.cas[_ins.Name] = data.NewData(_ins)

				go UnixServer(fmt.Sprintf("%s_%s",config.Conf.Local,_ins.Name))
				return nil
			})
		})
	})
	if err != nil {
		panic(err)
	}
	if cl.Len() == 0 {
		err = request.DownAccountProperties()
		if err != nil {
			panic(err)
		}
		return NewDataList()
	}
	return cl
}

func (self *dataList)Len() int {
	return len(self.cas)
}

func (self *dataList) FindCa(name string) *data.Data {

	return self.cas[name]
}
func init(){
	CL = NewDataList()
	fmt.Println(CL.Len())
}

func main(){
	//go UnixServer(fmt.Sprintf("%s_main",config.Conf.Local))
	go UnixServer(config.Conf.Local)
	select{}
}

func UnixServer(local string){
	err := os.Remove(local)
	if err != nil {
		fmt.Println(err)
	}
	unixAddr, err := net.ResolveUnixAddr("unixgram",local)
	if err != nil {
		fmt.Println(err)
		return
	}
	ln, err := net.ListenUnixgram("unixgram", unixAddr )
	if err!= nil {
		fmt.Println(err)
		return
	}
	//ln.SetWriteBuffer(1048576)
	var buf [1024]byte
	for{
		n,raddr,err := ln.ReadFromUnix(buf[:])
		if err != nil {
			panic(err)
		}
		p := proto.NewProto(buf[:n])
		ca := CL.FindCa(p.Ins)
		if ca == nil {
			panic(p)
		}
		go ca.StreamDB(p,ln,raddr)
	}
	ln.Close()
}
