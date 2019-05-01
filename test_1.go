package main
import(
	"github.com/zaddone/analog/request"
	"github.com/zaddone/analog/config"
	//"github.com/zaddone/operate/oanda"
	//"bufio"
	//"log"
	"time"
	"encoding/json"
	"github.com/zaddone/analog/dbServer/proto"
	"net/url"
	"fmt"
	"io"
	"io/ioutil"
)
func main() {
	from := 1556591749
	Count := 500
	n:=0

	var err error
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
		config.Conf.InsName,
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
			can := proto.NewCandles(c.(map[string]interface{}))
			fmt.Println(can)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}


}
