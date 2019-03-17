package proto
import(
	"github.com/zaddone/analog/config"
	"strconv"
)


type CandlesMin struct{
	Val float64
	Dif float64
	time int64
}
func NewCandlesMin(k,db []byte) (c *CandlesMin) {
	c = &CandlesMin{}
	c.time,c.Val,c.Dif = config.UnZip(k,db)
	return c
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
	return config.Scale
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
	return self.val
}
func (self *Candles) Diff() float64 {
	if self.diff == 0 {
		self.diff = self.Ask[2] - self.Bid[3]
		if self.Ask[1] > self.Ask[0] {
			self.diff = -self.diff
		}
	}
	return self.diff
}

func (self *Candles) Readf(h func(config.Element) bool ) bool {
	return h(self)
}
func (self *Candles) Read(h func(config.Element) bool ) bool {
	return h(self)
}
func (self *Candles) ToMin(xin float64) *CandlesMin {
	return &CandlesMin{
		time:self.DateTime(),
		Dif:float64(int(self.Diff()*xin)),
		Val:float64(int(self.Middle()*xin)),
	}
}
func NewCandles(tmp map[string]interface{}) (c *Candles) {
	c = &Candles{}
	var err error
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
