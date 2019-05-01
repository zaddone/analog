package request
import(
	"net/http"
	"github.com/boltdb/bolt"
	"github.com/zaddone/analog/config"
	"github.com/zaddone/operate/oanda"
	"io"
	"time"
	"compress/gzip"
	"fmt"
	"encoding/json"
	"net/url"
	"strings"
	"bytes"
	//"strconv"
)
var (
	Header  http.Header
	Ins_key []byte = []byte{1}
	AccountSummary map[string]interface{}
)
func init(){

	Header = http.Header{}
	Header.Set("Authorization", "Bearer "+ config.Authorization)
	Header.Set("Connection", "Keep-Alive")
	Header.Set("Accept-Datetime-Format", "UNIX")
	Header.Set("Content-type", "application/json")

}

func ClientHttp(num int ,methob string,path string,body interface{}, hand func(statusCode int,data io.Reader )error) error {
	return clientHttp(num,methob,path,body,hand)
}

func clientHttp(num int ,methob string,path string,body interface{}, hand func(statusCode int,data io.Reader )error) error {
//func clientHttp(num int ,methob string,path string,body *url.Values, hand func(statusCode int,data io.Reader )error) error {
	if num >5 {
		return fmt.Errorf("num >5")
	}

	var r io.Reader
	if body != nil {
		switch body.(type){
		case url.Values:
			r = strings.NewReader(body.(url.Values).Encode())
		case []byte:
			r = bytes.NewReader(body.([]byte))
		case string:
			r = strings.NewReader(body.(string))
		default:
			fmt.Printf("%v\r\n",body)
			panic(0)

		}
		//fmt.Println(body.Encode())
	}
	Req, err := http.NewRequest(methob, path, r)
	//fmt.Println(Req.Form)
	if err != nil {
		fmt.Println("req",err)
		return err
	}
	Req.Header = Header
	cli := http.Client{}
	res, err := cli.Do(Req)
	if err != nil {
		fmt.Println(err)
		time.Sleep(time.Second*5)
		//res.Body.Close()
		return clientHttp(num+1,methob,path,body,hand)
	}

	var reader io.ReadCloser
	switch res.Header.Get("Content-Encoding") {
	case "gzip":
		reader, _ = gzip.NewReader(res.Body)
		//defer reader.Close()
	default:
		reader = res.Body
	}
	err = hand(res.StatusCode,reader)
	reader.Close()
	//fmt.Println(path)
	return err

}

func DownAccountProperties() error {
	return clientHttp(0,"GET",config.Conf.GetAccPath()+"/instruments",nil,func(statusCode int,data io.Reader)(err error){
		jsondb := json.NewDecoder(data)
		if statusCode != 200 {
			var da interface{}
			err = jsondb.Decode(&da)
			return fmt.Errorf("%d %v %v",statusCode,da,err)
		}
		type ResInstrument struct {
			Instruments []map[string]interface{} `json:"instruments"`
		}
		da := &ResInstrument{}
		err = jsondb.Decode(da)
		if err != nil {
			return err
		}
		return config.UpdateKvDB(Ins_key,func(b *bolt.Bucket)error{
			for _,_ins := range da.Instruments {
				var ins oanda.Instrument
				err = ins.Load(_ins)
				if err != nil {
					return err
				}
				db,err :=  json.Marshal(ins)
				if err != nil {
					return err
				}
				err = b.Put([]byte(ins.Name),db)
				if err != nil {
					return err
				}
			}
			return nil
		})

	})
}
func candlesHandle(path string, f func(c interface{}) error) (err error) {
	da := make(map[string]interface{})
	err = clientHttp(0,"GET",path,nil,func(statusCode int,data io.Reader)(err error){
		jsondb := json.NewDecoder(data)
		if statusCode != 200 {
			var errda interface{}
			err = jsondb.Decode(&errda)
			return fmt.Errorf("%d %v %v",statusCode,errda,err)
		}
		return jsondb.Decode(&da)
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

