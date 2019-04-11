package main
import(
	"fmt"
	"github.com/zaddone/analog/dbServer/proto"
	"github.com/zaddone/analog/dbServer/cache"
	"github.com/zaddone/analog/request"
	"github.com/zaddone/operate/oanda"
	"github.com/zaddone/analog/config"
	"github.com/boltdb/bolt"
	"encoding/json"
	"encoding/binary"
	"path/filepath"
	"net/url"
	"io/ioutil"
	"bufio"
	"io"
	"strings"
	"time"
	"os"
	"net"
	"log"
	//"sync"
)

type EasyPrice struct {
	dateTime int64
	//dateTimeMin int64
	bid int32
	ask int32
}

func NewEasyPrice(p *oanda.Price,integer float64)*EasyPrice{
	return &EasyPrice{
		//dateTimeMin:p.DateTimeMin(),
		dateTime:time.Unix(p.DateTime(),p.DateTimeMin()).UnixNano(),
		bid:int32(p.Bid()*integer),
		ask:int32(p.Ask()*integer),
	}
}
func (self *EasyPrice) ToByte()(k []byte,v []byte){
	k = make([]byte,8)
	binary.BigEndian.PutUint64(k,uint64(self.dateTime))
	v = make([]byte,8)
	binary.BigEndian.PutUint32(v,uint32(self.bid))
	binary.BigEndian.PutUint32(v[4:],uint32(self.ask))
	return
}
func (self *EasyPrice) Load(k []byte,v []byte){
	self.dateTime = int64(binary.BigEndian.Uint64(k))
	self.bid = int32(binary.BigEndian.Uint32(v[:4]))
	self.ask = int32(binary.BigEndian.Uint32(v[4:]))
}

func (self *EasyPrice) Readf(hand func(config.Element)bool)bool{
	return hand(self)
}
func (self *EasyPrice) Read(hand func(config.Element)bool)bool{
	return hand(self)
}
func (self *EasyPrice) DateTime() int64 {
	return self.dateTime
}
func (self *EasyPrice) Diff() float64 {
	return float64(self.ask - self.bid)
}
func (self *EasyPrice) Middle() float64 {
	return float64(self.ask+self.bid)/2
}
func (self *EasyPrice) Duration() int64 {
	return 0
}
var (
	CL *cacheList = NewCacheList()
)

type cacheList struct {
	cas []*cache.Cache
	casMap map[string]*cache.Cache
	LogDB *bolt.DB
	//sync.RWMutex
}

func (self *cacheList) Show() (n int) {
	return 0
}
func (self *cacheList) Len() int {
	return len(self.cas)
}
func (self *cacheList) GetCache(ins string) *cache.Cache {
	return self.casMap[ins]
}
func (self *cacheList) Handle(ins string,d *oanda.Price){

	c := self.GetCache(ins)
	if c == nil {
		panic(0)
	}
	ep := NewEasyPrice(d,c.Ins().Integer())
	if config.Conf.Server {
		go func(){
			err := self.LogDB.Batch(func(t *bolt.Tx)error{
				b,e := t.CreateBucketIfNotExists([]byte(ins))
				if e != nil {
					return e
				}
				k,v := ep.ToByte()
				return b.Put(k,v)
			})
			if err != nil {
				panic(err)
			}
		}()
	}

	c.Add(ep)
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
func NewCacheList() (cl *cacheList) {
	cl = &cacheList{
		casMap:make(map[string]*cache.Cache),
	}

	if config.Conf.Server {
		pa := config.Conf.LogPath
		_,err := os.Stat(pa)
		if err != nil {
			err = os.MkdirAll(pa,0700)
			if err != nil {
				panic(err)
			}
		}
		cl.LogDB,err = bolt.Open(filepath.Join(pa,"cache.db"),0600,nil)
		if err != nil {
			panic(err)
		}
		go cl.UnixServer(config.Conf.Local)
	}
	return
}

func (self *cacheList) add(ins *oanda.Instrument){
	c :=cache.NewCache(ins)
	self.cas = append(self.cas,c)
	self.casMap[c.InsName()] = c
	c.SyncInit(self)
}
func (self *cacheList) PriceVarUrl() string {
	var Ins []string
	for n,_ := range self.casMap {
		Ins = append(Ins,n)
	}
	return config.Conf.GetStreamAccPath()+"/pricing/stream?"+(&url.Values{"instruments":[]string{strings.Join(Ins,",")}}).Encode()
}
func (self *cacheList) syncGetPriceVar(){
	var err error
	var lr,r []byte
	var p bool
	for{
		err = request.ClientHttp(0,
		"GET",
		self.PriceVarUrl(),
		nil,
		func(statusCode int,data io.Reader) error {
			if statusCode != 200 {
				msg,_ := ioutil.ReadAll(data)
				return fmt.Errorf("%s",string(msg))
			}
			buf := bufio.NewReader(data)
			for{
				r,p,err = buf.ReadLine()
				//fmt.Println(string(r),p)
				if p {
					//fmt.Println(string(r))
					lr = r
				}else if len(r)>0 {
					if lr != nil {
						r = append(lr,r...)
						lr = nil
					}

					var d oanda.Price
					er := json.Unmarshal(r,&d)
					if er != nil {
						log.Println(er,string(r))
						continue
					}
					name := string(d.Instrument)
					if name != "" {
						self.Handle(name,&d)
					}

				}
				if err != nil {
					//if err != io.EOF {
					//	//panic(err)
					//	//log.Println(err)
					//}
					return err
				}
			}
			return nil
		})
		if err != nil {
			//panic(err)
			log.Println(err)
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
				CL.add(_ins)
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
func (self *cacheList) UnixServer(local string){
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
		//p := proto.NewProto(buf[:n])
		//ca := CL.FindCa(p.Ins)
		//if ca == nil {
		//	panic(p)
		//}
		go self.StreamDB(proto.NewProto(buf[:n]),ln,raddr)
	}
	ln.Close()
}
func (self *cacheList) StreamDB(R *proto.Proto,c *net.UnixConn,addr *net.UnixAddr){
	ca := self.GetCache(R.Ins)
	if ca == nil {
		log.Println(R)
		return
	}
	var err error
	var n int
	self.read(R.Ins,R.B,R.E,func(k,v []byte)bool{
		n,err = c.WriteToUnix(append(k,v...),addr)
		if err != nil {
			log.Println(err)
			return false
		}
		if n  != 12 {
			log.Println("n =",n)
			return false
		}
		return true
	})
	//c.CloseWrite()
	//fmt.Println(R)
	if err == nil {
		_,err = c.WriteToUnix(nil,addr)
		if err != nil {
			log.Println(err)
		}
	}

}
func (self *cacheList) read(ins string,b,e int64,h func([]byte,[]byte)bool){
	begin := make([]byte,8)
	binary.BigEndian.PutUint64(begin,uint64(b))
	end := uint64(e)
	err := self.LogDB.View(func(t *bolt.Tx) error{
		bu := t.Bucket([]byte(ins))
		if bu == nil {
			return nil
		}
		c := bu.Cursor()
		for k,v := c.Seek(begin);k!= nil;k,v = c.Next() {
			if binary.BigEndian.Uint64(k) > end {
				return nil
			}
			if !h(k,v) {
				return nil
			}
		}
		return nil
	})
	if err != nil {
		log.Println(err)
	}
}

func main(){
	fmt.Print("run")
	loadCache()
	go CL.syncGetPriceVar()
	select{}

}
