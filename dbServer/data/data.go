package data
import(

	"github.com/boltdb/bolt"
	"github.com/zaddone/operate/oanda"
	"github.com/zaddone/analog/request"
	"github.com/zaddone/analog/config"
	"github.com/zaddone/analog/dbServer/proto"
	"fmt"
	"log"
	"os"
	"io/ioutil"
	"time"
	"encoding/json"
	"path/filepath"
	"encoding/binary"
	"net/url"
	"io"
	"net"
)
const (
	//Scale int64 = 5
	Count int = 500
)
var (
	Bucket  = []byte{0}
)

type Data struct {
	Ins *oanda.Instrument
	db *bolt.DB
	candlesChan chan config.Element
}
func NewData(ins *oanda.Instrument) (c *Data) {

	c = &Data{
		Ins:ins,
		candlesChan:make(chan config.Element,Count),
	}
	var err error
	path := config.Conf.DbPath
	_,err = os.Stat(path)
	if err != nil {
		err = os.MkdirAll(path,0700)
		if err != nil {
			panic(err)
		}
	}
	c.db,err = bolt.Open(filepath.Join(path,c.Ins.Name),0600,nil);
	if err != nil {
		panic(err)
	}
	go c.downCan()
	return c

}
func (self *Data) read(b,e int64, h func([]byte,[]byte)bool) {

	begin := make([]byte,4)
	binary.BigEndian.PutUint32(begin,uint32(b))
	end := uint32(e)
	err := self.db.View(func(t *bolt.Tx) error{
		bu := t.Bucket(Bucket)
		if bu == nil {
			return nil
		}
		c := bu.Cursor()
		for k,v := c.Seek(begin);k!= nil;k,v = c.Next() {
			if binary.BigEndian.Uint32(k) > end {
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

func (self *Data) StreamDB(R *proto.Proto,c *net.UnixConn,addr *net.UnixAddr){
	var err error
	//var n int
	self.read(R.B,R.E,func(k,v []byte)bool{
		_,err = c.WriteToUnix(append(k,v...),addr)
		if err != nil {
			log.Println(err)
			return false
		}
		//if n  != 12 {
		//	log.Println("n =",n)
		//	return false
		//}
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
	//fmt.Println(addr.String())
}

func (self *Data) findLastTime() (lt int64) {
	err := self.db.View(func(t *bolt.Tx) error{
		b := t.Bucket(Bucket)
		if b == nil {
			return nil
		}
		k,_ := b.Cursor().Last()
		if k != nil {
			lt = int64(binary.BigEndian.Uint32(k))+config.Scale
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("e",time.Unix(lt,0),self.Ins.Name)
	return
}

func (self *Data) downCan(){
	from := self.findLastTime()
	if from == 0 {
		from = config.GetFromTime()
	}
	var err error
	begin := from
	xin := self.Ins.Integer()
	n := 1
	for{
		u :=url.Values{
			"granularity": []string{"S5"},
			"price": []string{"AB"},
			"count": []string{fmt.Sprintf("%d", Count)},
			"from": []string{fmt.Sprintf("%d", from)},
			//"dailyAlignment":[]string{"3"},
			}.Encode()
		err = request.ClientHttp(
		0,
		"GET",
		fmt.Sprintf(
			"%s/instruments/%s/candles?%s",
			config.Host,
			self.Ins.Name,
			u,
		),
		nil,
		func(statusCode int,body io.Reader)(er error){
			if statusCode != 200 {
				if statusCode == 429 {
					time.Sleep(time.Second*time.Duration(n))
					n++
					return nil
				}
				db,err := ioutil.ReadAll(body)
				if err != nil {
					panic(err)
				}
				return fmt.Errorf("%d %s",statusCode,string(db))
			}
			n = 1
			var da interface{}
			er = json.NewDecoder(body).Decode(&da)
			if er != nil {
				return er
			}
			for _,c := range da.(map[string]interface{})["candles"].([]interface{}) {
				can := proto.NewCandles(c.(map[string]interface{})).ToMin(xin)
				begin = can.DateTime() + config.Scale
				select{
				case self.candlesChan <- can:
					continue
				default:
					go self.saveToDB(can)
				}
				//if (h!=nil) && !h(can) {
				//	return io.EOF
				//}
			}
			return nil
		})

		go self.saveToDB(nil)
		if (err != nil) {
			if (err == io.EOF) {
				return
			}else{
				log.Println(err)
			}
		}
		//f := time.Unix(from,0)
		//b := time.Unix(begin,0)
		//if f.Month() != b.Month() {
		//	fmt.Println(self.Ins.Name,b)
		//}
		from = begin
		if from > time.Now().Unix() {
			time.Sleep(time.Minute*5)
		}
	}
}
func (self *Data) saveToDB(can config.Element){

	if len(self.candlesChan)== 0 {
		return
	}
	var c config.Element
	err := self.db.Batch(func(t *bolt.Tx)error{
		b,er := t.CreateBucketIfNotExists(Bucket)
		if er != nil {
			return er
		}
		if can != nil {
			k,v := config.Zip(can)
			er = b.Put(k,v)
			if er != nil {
				return er
			}
		}
		for{
			select{
			case c = <-self.candlesChan:
				k,v := config.Zip(c)
				er = b.Put(k,v)
				if er != nil {
					return er
				}

			default:
				return nil
			}
		}
	})
	if err != nil {
		panic(err)
	}

}
