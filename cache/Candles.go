package cache
import(
	"strconv"
	"encoding/json"
	"github.com/zaddone/analog/request"
	"github.com/zaddone/analog/config"
	"io"
	//"io/ioutil"
	"fmt"
	"log"
	"net/url"
)
const (
	Count = 5000
)

type Candles struct {
	Mid    [4]float64
	Time   int64
	Volume float64
	Val    float64
	Scale  int64
}
func (self *Candles) Duration() int64 {
	return self.Scale
}
func (self *Candles) DateTime() int64 {
	return self.Time
}
func (self *Candles) Middle() float64 {
	if self.Val == 0 {
		var sum float64 = 0
		for _, m := range self.Mid {
			sum += m
		}
		self.Val = sum / 4
	}
	return self.Val
}
func (self *Candles) Diff() float64 {
	return (self.Mid[2] - self.Mid[3])
}
func (self *Candles) Read(h func(config.Element)) {
	h(self)
}

func DownCandles(insName string,from int64,hand func(* Candles)) {

	var err error
	var begin int64 = from
	for{
		err = request.ClientHttp(
		0,
		"GET",
		fmt.Sprintf(
			"%s/instruments/%s/candles?%s",
			config.Host,
			insName,
			url.Values{
				"granularity": []string{"S5"},
				"price": []string{"M"},
				"count": []string{fmt.Sprintf("%d", Count)},
				"from": []string{fmt.Sprintf("%d", from)},
				//"dailyAlignment":[]string{"3"},
			}.Encode(),
		),
		nil,
		func(statusCode int,body io.Reader)(er error){
			if statusCode != 200 {
				return fmt.Errorf("%d",statusCode)
			}
			var da interface{}
			er = json.NewDecoder(body).Decode(&da)
			if er != nil {
				return er
			}
			for _,c := range da.(map[string]interface{})["candles"].([]interface{}) {
				can := NewCandles(c.(map[string]interface{}))
				can.Scale = 5
				begin = can.Time
				hand(can)
			}
			return nil
		})
		if err != nil {
			log.Println(err)
		}
		if begin != from {
			from = begin+5
		}
	}
}

func NewCandles(tmp map[string]interface{}) (c *Candles) {
	c = &Candles{}
	var err error
	Mid := tmp["mid"].(map[string]interface{})
	if Mid != nil {
		c.Mid[0], err = strconv.ParseFloat(Mid["o"].(string), 64)
		if err != nil {
			panic(err)
		}
		c.Mid[1], err = strconv.ParseFloat(Mid["c"].(string), 64)
		if err != nil {
			panic(err)
		}
		c.Mid[2], err = strconv.ParseFloat(Mid["h"].(string), 64)
		if err != nil {
			panic(err)
		}
		c.Mid[3], err = strconv.ParseFloat(Mid["l"].(string), 64)
		if err != nil {
			panic(err)
		}
	}
	c.Volume = tmp["volume"].(float64)
	ti, err := strconv.ParseFloat(tmp["time"].(string), 64)
	if err != nil {
		panic(err)
	}
	c.Time = int64(ti)
	return c
}
