package server
import(
	"fmt"
	"strings"
	"strconv"
	"time"
	"log"
	"net/url"
	"io"
	"encoding/json"
	"os"
	"github.com/zaddone/analog/config"
	"path/filepath"
)

type Candles struct {
	Mid    [4]float64
	Time   int64
	Volume float64
	Val    float64
	Scale  int64
}
func (self *Candles) GetScale() int64 {
	return self.Scale
}
func (self *Candles) SetScale(sc int64){
	self.Scale = sc
}

func (self *Candles) GetMidLong() float64 {
	return self.Mid[2] - self.Mid[3]
}

func (self *Candles) GetMidAverage() float64 {
	if self.Val == 0 {
		var sum float64 = 0
		for _, m := range self.Mid {
			sum += m
		}
		self.Val = sum / 4
	}
	return self.Val
}

func (self *Candles) GetInOut() bool {
	return self.Mid[0] < self.Mid[1]
}

func (self *Candles) GetTime() int64 {
	return self.Time
}
func (self *Candles) GetTimer() time.Time {
	return time.Unix(self.Time, 0).UTC()

}

func (self *Candles) Show() {
	fmt.Printf("%.6f %.6f %.6f %.6f %s %.6f\r\n", self.Mid[0], self.Mid[1], self.Mid[2], self.Mid[3], time.Unix(self.Time, 0).String(), self.Volume)
}

func (self *Candles) Load(str string) (err error) {
	strl := strings.Split(str, " ")
	self.Mid[0], err = strconv.ParseFloat(strl[0], 64)
	if err != nil {
		return err
	}
	self.Mid[1], err = strconv.ParseFloat(strl[1], 64)
	if err != nil {
		return err
	}
	self.Mid[2], err = strconv.ParseFloat(strl[2], 64)
	if err != nil {
		return err
	}
	self.Mid[3], err = strconv.ParseFloat(strl[3], 64)
	if err != nil {
		return err
	}
	Ti, err := strconv.Atoi(strl[4])
	if err != nil {
		return err
	}
	self.Time = int64(Ti)
	self.Volume, err = strconv.ParseFloat(strl[5], 64)
	if err != nil {
		return err
	}
	return nil
}

func (self *Candles) String() string {
	return fmt.Sprintf("%.5f %.5f %.5f %.5f %d %.5f\r\n", self.Mid[0], self.Mid[1], self.Mid[2], self.Mid[3], self.Time, self.Volume)
}
func (self *Candles) CheckSection( val float64 ) bool {
	//fmt.Println(self.Mid[2] , val , self.Mid[3])
	if val > self.Mid[2] || val < self.Mid[3] {
		return false
	}
	return true
}
func (self *Candles) Init(tmp map[string]interface{}) (err error) {
	Mid := tmp["mid"].(map[string]interface{})
	if Mid != nil {
		self.Mid[0], err = strconv.ParseFloat(Mid["o"].(string), 64)
		if err != nil {
			return err
		}
		self.Mid[1], err = strconv.ParseFloat(Mid["c"].(string), 64)
		if err != nil {
			return err
		}
		self.Mid[2], err = strconv.ParseFloat(Mid["h"].(string), 64)
		if err != nil {
			return err
		}
		self.Mid[3], err = strconv.ParseFloat(Mid["l"].(string), 64)
		if err != nil {
			return err
		}
	}
	self.Volume = tmp["volume"].(float64)
	ti, err := strconv.ParseFloat(tmp["time"].(string), 64)
	if err != nil {
		return err
	}
	self.Time = int64(ti)
	return nil
}
func DownCandles(name string, from, to int64, gr int64, gran string, Handle func(*Candles)error) {

	var file *os.File = nil
	var LogFile string = ""
	var err error
	var Begin time.Time
	var can *Candles
	for {
		err = GetCandlesHandle(name, gran, from, 500, func(c interface{}) error {
			can = new(Candles)
			can.Init(c.(map[string]interface{}))
			err := Handle(can)
			if err != nil {
				log.Println(err)
				return io.EOF
			}
			if config.Conf.DbPath == "" {
				return nil
			}
			Begin = can.GetTimer()
			path := filepath.Join(config.Conf.DbPath,name, gran, fmt.Sprintf("%d", Begin.Year()))
			_, err = os.Stat(path)
			if err != nil {
				os.MkdirAll(path, 0777)
			}
			path = filepath.Join(path, Begin.Format("20060102"))
			if file == nil {
				LogFile = path
				file, err = os.OpenFile(LogFile, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0777)
				if err != nil {
					panic(err)
				}
			} else if LogFile != path {
				//	fmt.Println(path)
				file.Close()
				LogFile = path
				file, err = os.OpenFile(LogFile, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0777)
				if err != nil {
					panic(err)
				}
			}
			file.WriteString(can.String())
			return nil
		})
		if err != nil {
			if err == io.EOF {
				return
			}
			//log.Println(err)
			<-time.After(time.Second * 1)
		} else {
			from = can.GetTime() + gr
			if to > 0 && from >= to {
				break
			}
			aft := from - time.Now().Unix()
			if aft > 0 {
				<-time.After(time.Second *time.Duration(aft/2))
			}
		}
		//<-time.After(time.Second * 1)
	}

}
func GetCandlesHandle(Ins_name, granularity string, from, count int64, f func(c interface{}) error) (err error) {

	path := fmt.Sprintf("%s/instruments/%s/candles?", Host, Ins_name)
	uv := url.Values{}
	uv.Add("granularity", granularity)
	uv.Add("price", "M")
	uv.Add("from", fmt.Sprintf("%d", from))
	uv.Add("count", fmt.Sprintf("%d", count))
	path += uv.Encode()
	//	fmt.Println(path)

	da := make(map[string]interface{})
	err =  ClientDo(path,func(body io.Reader)(er error){
		er =  json.NewDecoder(body).Decode(&da)
		if er != nil {
			panic(er)
			return er
		}
		return
	})
	if err != nil {
		return err
	}
	ca := da["candles"].([]interface{})
	lc := len(ca)
	if lc == 0 {
		return fmt.Errorf("candles len = 0")
	}
	for _, c := range ca {
		err = f(c)
		if err != nil {
			fmt.Println(err)
			break
		}
	}
	return nil

}
