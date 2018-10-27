package config
import(
	"flag"
	"github.com/BurntSushi/toml"
	"os"
	"log"
)
const (
	//FileName string = "request.log"
	TimeFormat = "2006-01-02T15:04:05"
)
var (
	FileName   = flag.String("c", "conf.log", "config log")
	InsName   = flag.String("n", "", "Sample EUR_JPY|EUR_USD")
	Conf *Config
)

type Config struct {

	Account_ID string
	Authorization string
	Proxy string
	LogFile string
	Host string
	StreamHost string
	BEGINTIME string
	Port string
	InsName string
	Server bool
	Price bool
	Units int
	JointMax int
	Granularity map[string]int
	Rate float64
	RateMax float64
	RateMin float64
	TrLogPath string
	DbPath string

}
func (self *Config) LoadGranularity(){

	self.Granularity = map[string]int{
				"S5"  : 5,
				"S10" : 10,
				"S15" : 15,
				"S30" : 30,
				"M1"  : 60,
				"M2"  : 60*2,
				"M4"  : 60*4,
				"M5"  : 60*5,
				"M10" : 60*10,
				"M15" : 60*15,
				"M30" : 60*30,
				"H1"  : 3600,
				"H2"  : 3600 * 2,
				"H3"  : 3600 * 3,
				"H4"  : 3600 * 4,
				"H6"  : 3600 * 6,
				"H8"  : 3600 * 8,
				"H12" : 3600 * 12,
				"D"   : 3600 * 24}

}
func (self *Config) Save() {

	fi,err := os.OpenFile(*FileName,os.O_CREATE|os.O_WRONLY,0777)
	//fi,err := os.Open(FileName)
	if err != nil {
		log.Fatal(err)
	}
	defer fi.Close()
	e := toml.NewEncoder(fi)
	err = e.Encode(self)
	if err != nil {
		log.Fatal(err)
	}

}
func NewConfig()  *Config {

	var c Config
	_,err := os.Stat(*FileName)
	if err != nil {
		c.Account_ID = "101-011-2471429-001"
		c.Authorization = ""
		c.Proxy = ""
		c.LogFile = "LogInfo.log"
		c.Host = "https://api-fxpractice.oanda.com/v3"
		c.StreamHost = "https://Stream-fxpractice.oanda.com/v3"
		c.BEGINTIME = "2018-07-21T00:00:00"
		c.Port=":50051"
		//c.InsName = "EUR_USD|EUR_JPY"
		c.InsName = "EUR_JPY"
		c.Units = 100
		c.Server = false
		c.Price = false
		c.JointMax = 150
		c.RateMax = 3
		c.Rate = 2
		c.RateMin = 1.5
		c.TrLogPath = "TrLog"
		c.LoadGranularity()
		c.DbPath = "db"
		c.Save()
	}else{
		if _,err := toml.DecodeFile(*FileName,&c);err != nil {
			log.Fatal(err)
		}
	}
	if *InsName != "" {
		c.InsName = *InsName
	}
	return &c

}
func init(){
	flag.Parse()
	Conf = NewConfig()
}
