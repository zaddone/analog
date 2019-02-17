package cache
import(
	"strconv"
	//"encoding/json"
	//"github.com/zaddone/analog/request"
	"github.com/zaddone/analog/config"
	//"github.com/boltdb/bolt"
	//"io"
	//"os"
	//"path/filepath"
	"encoding/gob"
	"bytes"
	//"io/ioutil"
	//"bufio"
	"time"
	"fmt"
	//"log"
	//"net/url"
	"encoding/binary"

)
const (
	//Count = 5000
	Scale int64 = 5
)
var Loc *time.Location
func init(){
	var err error
	Loc,err = time.LoadLocation("Etc/GMT-3")
	if err != nil {
		panic(err)
	}
}
type CandlesMin struct{
	Val float64
	Dif float64
	time int64
}

func NewCandlesMin(k,db []byte) (c *CandlesMin) {
	c = &CandlesMin{}
	err := gob.NewDecoder(bytes.NewBuffer(db)).Decode(c)
	if err != nil {
		fmt.Println(string(db))
		//return nil
		panic(err)
	}
	c.time = int64(binary.BigEndian.Uint64(k))
	//fmt.Println(c)
	return c
}
func ObjToByte(db interface{}) []byte {
	var b bytes.Buffer
	err := gob.NewEncoder(&b).Encode(db)
	if err != nil {
		panic(err)
	}
	return b.Bytes()
}
func (self *CandlesMin) toByte() ([]byte){
	var b bytes.Buffer
	err := gob.NewEncoder(&b).Encode(self)
	if err != nil {
		panic(err)
	}
	return b.Bytes()

}
func (self *CandlesMin) Key() (k []byte) {
	k = make([]byte,8)
	binary.BigEndian.PutUint64(k,uint64(self.time))
	return
}
func (self *CandlesMin) Readf(h func(config.Element)bool) bool {
	return h(self)
}
func (self *CandlesMin) Read(h func(config.Element)bool) bool {
	return h(self)
}
func (self *CandlesMin) Diff() float64 {
	return self.Dif
}
func (self *CandlesMin) Middle() float64 {
	return self.Val
}

func (self *CandlesMin) Duration() int64 {
	return Scale
}

func (self *CandlesMin) DateTime() int64 {
	return self.time
}

type Candles struct {
	//Mid    [4]float64
	Ask    [4]float64
	Bid    [4]float64
	time   int64
	//Volume float64
	val    float64
	diff   float64
	scale  int64
}
func (self *Candles) SetTime(t int64) {
	self.time = t
}

func (self *Candles) Key() (k []byte) {
	k = make([]byte,8)
	binary.BigEndian.PutUint64(k,uint64(self.time))
	return
}

func (self *Candles) Duration() int64 {
	return self.scale
}
func (self *Candles) DateTime() int64 {
	return self.time
}
func (self *Candles) Middle() float64 {
	if self.val == 0 {
		for i, m := range self.Ask {
			self.val += m
			self.val += self.Bid[i]
		}
		self.val /= 8
	}
	//fmt.Println("val",self.val)
	return self.val
}
func (self *Candles) Diff() float64 {
	if self.diff == 0 {
		self.diff = self.Ask[2] - self.Bid[3]
		if self.Ask[1] > self.Ask[0] {
			self.diff = -self.diff
		}
		//self.diff = ((self.Ask[0] - self.Bid[0]) + (self.Ask[1] - self.Bid[1]))/2
	}
	//fmt.Println("diff",self.diff)
	return self.diff
}

func (self *Candles) Readf(h func(config.Element) bool ) bool {
	return h(self)
}
func (self *Candles) Read(h func(config.Element) bool ) bool {
	return h(self)
}
func NewCandlesWithDB(db []byte) (c *Candles) {

	c = &Candles{}
	err := gob.NewDecoder(bytes.NewBuffer(db)).Decode(c)
	if err != nil {
		fmt.Println(string(db))
		//return nil
		panic(err)
	}
	//fmt.Println(c)
	return c
}
func (self *Candles) toMin(xin float64) *CandlesMin {
	return &CandlesMin{
		time:self.DateTime(),
		Dif:self.Diff()*xin,
		Val:self.Middle()*xin,
	}
}

func (self *Candles) toByte() (db []byte) {

	var b bytes.Buffer
	err := gob.NewEncoder(&b).Encode(self)
	if err != nil {
		panic(err)
	}
	return b.Bytes()
}
func NewCandles(tmp map[string]interface{}) (c *Candles) {
	c = &Candles{}
	var err error
	//Mid := tmp["mid"].(map[string]interface{})
	//if Mid != nil {
	//	c.Mid[0], err = strconv.ParseFloat(Mid["o"].(string), 64)
	//	if err != nil {
	//		panic(err)
	//	}
	//	c.Mid[1], err = strconv.ParseFloat(Mid["c"].(string), 64)
	//	if err != nil {
	//		panic(err)
	//	}
	//	c.Mid[2], err = strconv.ParseFloat(Mid["h"].(string), 64)
	//	if err != nil {
	//		panic(err)
	//	}
	//	c.Mid[3], err = strconv.ParseFloat(Mid["l"].(string), 64)
	//	if err != nil {
	//		panic(err)
	//	}
	//}
	ask := tmp["ask"].(map[string]interface{})
	if ask != nil {
		c.Ask[0], err = strconv.ParseFloat(ask["o"].(string), 64)
		if err != nil {
			panic(err)
		}
		c.Ask[1], err = strconv.ParseFloat(ask["c"].(string), 64)
		if err != nil {
			panic(err)
		}
		c.Ask[2], err = strconv.ParseFloat(ask["h"].(string), 64)
		if err != nil {
			panic(err)
		}
		c.Ask[3], err = strconv.ParseFloat(ask["l"].(string), 64)
		if err != nil {
			panic(err)
		}
	}
	bid := tmp["bid"].(map[string]interface{})
	if bid != nil {
		c.Bid[0], err = strconv.ParseFloat(bid["o"].(string), 64)
		if err != nil {
			panic(err)
		}
		c.Bid[1], err = strconv.ParseFloat(bid["c"].(string), 64)
		if err != nil {
			panic(err)
		}
		c.Bid[2], err = strconv.ParseFloat(bid["h"].(string), 64)
		if err != nil {
			panic(err)
		}
		c.Bid[3], err = strconv.ParseFloat(bid["l"].(string), 64)
		if err != nil {
			panic(err)
		}
	}

	//c.Volume = tmp["volume"].(float64)
	ti, err := strconv.ParseFloat(tmp["time"].(string), 64)
	if err != nil {
		panic(err)
	}
	c.time = int64(ti)
	return c
}
