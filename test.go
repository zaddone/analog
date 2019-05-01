package main
import(
	"github.com/zaddone/analog/request"
	"github.com/zaddone/analog/config"
	"github.com/zaddone/operate/oanda"
	"bufio"
	"log"
	//"time"
	"encoding/json"
	//"github.com/zaddone/analog/dbServer/proto"
	"net/url"
	"fmt"
	"io"
	"io/ioutil"
)

func PriceVarUrl() string {

	return config.Conf.GetStreamAccPath()+"/pricing/stream?"+(&url.Values{"instruments":[]string{"EUR_USD"}}).Encode()
}
func main(){
	var err error
	var lr,r []byte
	var p bool
	for{
		err = request.ClientHttp(0,
		"GET",
		PriceVarUrl(),
		nil,
		func(statusCode int,data io.Reader) error {
			if statusCode != 200 {
				msg,_ := ioutil.ReadAll(data)
				return fmt.Errorf("%s",string(msg))
			}
			buf := bufio.NewReader(data)
			for{
				r,p,err = buf.ReadLine()
				if p {
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
						fmt.Println(d)
						//Handle(name,&d)
					}

				}
				if err != nil {
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
