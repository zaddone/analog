package cache
import(
	"strconv"
	"encoding/json"
	//"github.com/zaddone/analog/request"
	"github.com/zaddone/analog/config"
	//"github.com/boltdb/bolt"
	"io"
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
	//"encoding/binary"
)
const (
	//Count = 5000
)
var Loc *time.Location
func init(){

	var err error
	Loc,err = time.LoadLocation("Etc/GMT-3")
	if err != nil {
		panic(err)
	}
}

type Candles struct {
	//Mid    [4]float64
	Ask    [4]float64
	Bid    [4]float64
	Time   int64
	//Volume float64
	val    float64
	diff   float64
	scale  int64
}

func (self *Candles) Duration() int64 {
	return self.scale
}
func (self *Candles) DateTime() int64 {
	return self.Time
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

func (self *Candles) GetByte() (db []byte) {
	var err error
	db,err = json.Marshal(self)
	if err != nil {
		panic(err)
	}
	return db
}
func (self *Candles) load(db []byte) error {
	err := json.Unmarshal(db,self)
	//err := gob.NewDecoder(bytes.NewBuffer(db)).Decode(self)
	//fmt.Println(self,len(db),string(db))
	if err != nil && err != io.EOF {
		//fmt.Println(string(db))
		return err
		//panic(err)
	}
	return nil
}
//func (self *Candles) Save(insName string) (path string,f *os.File) {
//	da := time.Unix(self.DateTime(),0)
//	path = filepath.Join(config.Conf.LogPath,insName,da.Format("200601"))
//	//da := time.Unix(self.DateTime(),0).Format("20060102")
//	_,err := os.Stat(path)
//	if err != nil {
//		err = os.MkdirAll(path,0700)
//		if err != nil {
//			panic(err)
//		}
//	}
//	f,err = os.OpenFile(filepath.Join(path,fmt.Sprintf("%d",da.Day())),os.O_APPEND|os.O_CREATE|os.O_RDWR|os.O_SYNC,0600)
//	if err != nil {
//		panic(err)
//	}
//
//	err = json.NewEncoder(f).Encode(self)
//	if err != nil {
//		panic(err)
//	}
//	//f.Write([]byte{'\n'})
//	return
//	//f.Close()
//
//}
//func ReadCandles(insName string,scale int64,h func(* Candles)bool) {
//
//	var from int64
//	err := filepath.Walk(filepath.Join(config.Conf.LogPath,insName),func(p string,info os.FileInfo,err error)error{
//		if info == nil || info.IsDir() {
//			return nil
//		}
//		f,err := os.Open(p)
//		if err != nil {
//			return err
//		}
//		fmt.Println(p)
//		buf := bufio.NewReader(f)
//		for{
//			li,err := buf.ReadSlice('\n')
//			if len(li) >1 {
//				c := &Candles{}
//				c.load(li)
//				c.scale = scale
//				//fmt.Println(c)
//				if from >= c.DateTime(){
//					//fmt.Println(insName,from)
//					continue
//					//panic(from)
//				}
//				from = c.DateTime()
//				if !h(c) {
//					return io.EOF
//				}
//			}
//			if err != nil {
//				if err != io.EOF {
//					fmt.Println(err)
//				}
//				break
//			}
//		}
//		f.Close()
//		return nil
//
//	})
//	if err != nil {
//		if err == io.EOF {
//			return
//		}
//		panic(err)
//	}
//	if from == 0 {
//		from = config.GetFromTime()
//	}else{
//		from += scale
//	}
//	//fmt.Println("down",insName,from)
//	DownCandles(insName,from,scale,h)
//
//}


//func DownCandles(insName string,from int64,scale int64,hand func(* Candles)bool) {
//
//	var err error
//	var begin int64 = from
//	var fi *os.File = nil
//	defer func(){
//		if fi != nil {
//			fi.Close()
//		}
//	}()
//	var dir,file string
//	var date time.Time
//	save := func(can *Candles){
//		date = time.Unix(can.Time,0).In(Loc)
//		file = date.Format("20060102")
//		if (fi != nil) && (fi.Name()  == file){
//			err = json.NewEncoder(fi).Encode(can)
//			if err != nil {
//				panic(err)
//			}
//		}else{
//			fi.Close()
//			dir = filepath.Join(config.Conf.LogPath,insName,date.Format("200601"))
//			_,err = os.Stat(dir)
//			if err != nil {
//				err = os.MkdirAll(dir,0700)
//				if err != nil {
//					panic(err)
//				}
//			}
//			fi,err = os.OpenFile(filepath.Join(dir,file),os.O_APPEND|os.O_CREATE|os.O_RDWR,0600)
//			if err != nil {
//				panic(err)
//			}
//			err = json.NewEncoder(fi).Encode(can)
//			if err != nil {
//				panic(err)
//			}
//		}
//	}
//
//
//	for{
//		err = request.ClientHttp(
//		0,
//		"GET",
//		fmt.Sprintf(
//			"%s/instruments/%s/candles?%s",
//			config.Host,
//			insName,
//			url.Values{
//				"granularity": []string{"S5"},
//				"price": []string{"AB"},
//				"count": []string{fmt.Sprintf("%d", Count)},
//				"from": []string{fmt.Sprintf("%d", from)},
//				//"dailyAlignment":[]string{"3"},
//			}.Encode(),
//		),
//		nil,
//		func(statusCode int,body io.Reader)(er error){
//			if statusCode != 200 {
//				return fmt.Errorf("%d",statusCode)
//			}
//			var da interface{}
//			er = json.NewDecoder(body).Decode(&da)
//			if er != nil {
//				return er
//			}
//			for _,c := range da.(map[string]interface{})["candles"].([]interface{}) {
//				can := NewCandles(c.(map[string]interface{}))
//				can.scale = scale
//				begin = can.Time
//				save(can)
//				//can.Save(insName)
//				if !hand(can) {
//					return io.EOF
//				}
//			}
//			return nil
//		})
//		if (err == nil){
//			if begin != from {
//				from = begin+scale
//			}
//		}else if err == io.EOF {
//			return
//		}else{
//			log.Println(err)
//		}
//	}
//}

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
	c.Time = int64(ti)
	return c
}
