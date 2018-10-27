package server
import(
	//"http/template"
	"encoding/json"
	"net/http"
	"compress/gzip"
	"github.com/gin-gonic/gin"
	//"net/http"
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"os"
	"io"
	"fmt"
	"strings"
	"reflect"
	//"github.com/zaddone/analog/config"
	//"sync"
	"strconv"
	"net/url"
	"time"
	"log"
	"bytes"
)
const(
	ConfigDB string = "./Config.db"
	//InsDB string = "./dbs/Ins.db"
	DBType string = "sqlite3"
	Authorization string = "3c3a8046ec7145d0b2e953cdd680c12a-bf7e7b6697ea52ea4d0f90696be4de95"
	Host string = "https://api-fxpractice.oanda.com/v3"
	StreamHost string = "https://Stream-fxpractice.oanda.com/v3"
)
var (
	Router *gin.Engine
	Header   http.Header
	Accounts []*Account
	AccountsMap map[int]*Account
)
func init(){

	AccountsMap = make(map[int]*Account)
	Header = make(http.Header)
	Header.Add("Authorization", "Bearer "+ Authorization)
	Header.Add("Connection", "Keep-Alive")
	Header.Add("Accept-Datetime-Format", "UNIX")
	Header.Add("Content-type", "application/json")

	Router = gin.Default()
	Router.LoadHTMLGlob("./templates/view/*")
	_,err :=  os.Stat(ConfigDB)
	if err != nil {
		createAccountDB()
	}else{
		loadAccountDB()
	}
	var acclist []map[string]interface{}
	for _i,ac := range Accounts {
		acclist = append(acclist,map[string]interface{}{
			"id":ac.Id,
			"uid":_i,
			"ins":ac.GetTemplateDataIns()})
	}
	Router.GET("/",func(c *gin.Context){
		c.HTML(http.StatusOK,"index.tmpl",gin.H{"accounts":acclist})
	})
	Router.GET("/getproxy/:accid",func(c *gin.Context){
		id,err := strconv.Atoi(c.Param("accid"))
		if err != nil {
			c.JSON(http.StatusNotFound,err.Error())
			//c.HTML(http.StatusNotFound,"404.tmpl",nil)
			return
		}
		ac := AccountsMap[id]
		if ac == nil {
			c.JSON(http.StatusNotFound,"ac == nil")
			//c.HTML(http.StatusNotFound,"404.tmpl",nil)
			return
		}
		path := ac.GetAccountPath()+c.Query("url")
		//fmt.Println(path)
		var res interface{}
		err =  ClientDo(path,func(body io.Reader) error{
			return json.NewDecoder(body).Decode(&res)
		})
		if err != nil {
			c.JSON(http.StatusNotFound,err.Error())
			return
		}
		c.JSON(http.StatusOK,res)

	})
	Router.GET("/view/:id/:insid",func(c *gin.Context){
		id,err := strconv.Atoi(c.Param("id"))
		if err != nil {
			c.HTML(http.StatusNotFound,"404.tmpl",nil)
			return
		}
		insid,err := strconv.Atoi(c.Param("insid"))
		if err != nil {
			c.HTML(http.StatusNotFound,"404.tmpl",nil)
			return
		}
		ac := AccountsMap[id]
		if ac == nil {
			c.HTML(http.StatusNotFound,"404.tmpl",nil)
			return
		}

		ins :=ac.InstrumentsId[insid]
		if ins == nil {
			c.HTML(http.StatusNotFound,"404.tmpl",nil)
			return
		}
		c.HTML(http.StatusOK,"instrument.tmpl",gin.H{
			"account":ac.GetTemplateData(id),
			"ins" : ins.GetTemplateData()})
	})
	Router.GET("/posttest/:accid/:insid",func(c *gin.Context){
		accid,err := strconv.Atoi(c.Param("accid"))
		if err != nil {
			fmt.Println(err)
			c.JSON(http.StatusNotFound,err)
			return
		}
		ac := AccountsMap[accid]
		if ac == nil {
			c.JSON(http.StatusNotFound,"ac == nil")
			return
		}
		insid,err := strconv.Atoi(c.Param("insid"))
		if err != nil {
			fmt.Println(err)
			c.JSON(http.StatusNotFound,err)
			return
		}
		ins :=ac.InstrumentsId[insid]
		if ins == nil {
			c.JSON(http.StatusNotFound,"ins == nil")
			return
		}
		Path :=ac.GetAccountPath() +"/pricing?"+url.Values{"instruments":[]string{ins.Name}}.Encode()
		var res PricesResponses
		err = ClientDo(Path,func(body io.Reader) error{
			return json.NewDecoder(body).Decode(&res)
		})
		if err != nil {
			c.JSON(http.StatusNotFound,err)
			return
		}
		c.JSON(http.StatusOK,res)

	})
	Router.GET("/trades/:accid/:orderid",func(c *gin.Context){
		accid,err := strconv.Atoi(c.Param("accid"))
		if err != nil {
			fmt.Println(err)
			c.JSON(http.StatusNotFound,err)
			return
		}
		ac := AccountsMap[accid]
		if ac == nil {
			c.JSON(http.StatusNotFound,"ac == nil")
			return
		}
		orderid,err := strconv.Atoi(c.Param("orderid"))
		if err != nil {
			fmt.Println(err)
			c.JSON(http.StatusNotFound,err)
			return
		}
		path := fmt.Sprintf("%s/trades/%d", ac.GetAccountPath(),orderid)
		var res TradeRes
		//var res interface{}
		err =  ClientDo(path,func(body io.Reader) error{
			return json.NewDecoder(body).Decode(&res)
		})
		if err != nil {
			fmt.Println(err)
			c.JSON(http.StatusNotFound,err)
			return
		}
		c.JSON(http.StatusOK,res)
	})
	Router.GET("/transaction/:accid/:insid/:orderid",func(c *gin.Context){
		accid,err := strconv.Atoi(c.Param("accid"))
		if err != nil {
			fmt.Println(err)
			c.JSON(http.StatusNotFound,err)
			return
		}
		ac := AccountsMap[accid]
		if ac == nil {
			c.JSON(http.StatusNotFound,"ac == nil")
			return
		}
		insid,err := strconv.Atoi(c.Param("insid"))
		if err != nil {
			fmt.Println(err)
			c.JSON(http.StatusNotFound,err)
			return
		}
		ins :=ac.InstrumentsId[insid]
		if ins == nil {
			c.JSON(http.StatusNotFound,"ins == nil")
			return
		}
		orderid,err := strconv.Atoi(c.Param("orderid"))
		if err != nil {
			fmt.Println(err)
			c.JSON(http.StatusNotFound,err)
			return
		}
		path := fmt.Sprintf("%s/transactions/sinceid?%s", ac.GetAccountPath(),url.Values{"id":[]string{fmt.Sprintf("%d",orderid)}}.Encode())
		//path := fmt.Sprintf("%s/transactions/%d", ac.GetAccountPath(),orderid)

		//var da interface{}
		var da TransactionSinceidRes
		var trc []*OrderFillTransaction
		err =  ClientDo(path,func(body io.Reader) error{
			err :=  json.NewDecoder(body).Decode(&da)
			if err != nil {
				return err
			}
			for _,t := range da.Transactions {
				if (string(t.Instrument) != ins.Name) || (string(t.Type) != "ORDER_FILL" ) {
					continue
				}
				fmt.Println(t.Type)
				trc = append(trc,t)
				//fmt.Println(t.TradesClosed)
			}
			return nil
		})
		if err != nil {
			fmt.Println(err)
			c.JSON(http.StatusNotFound,err)
			return
		}
		c.JSON(http.StatusOK,trc)
	})

}

func GetNowAccount() *Account {
	return Accounts[0]
}
func FindNowAccount(id string) *Account {
	for _,ac := range Accounts {
		if ac.Id == id {
			return ac
		}
	}
	return  nil
}

func StructUpdateForDB(db *sql.DB,TableName string,st interface{},keyname string ,key interface{}) sql.Result {
	re := reflect.TypeOf(st).Elem()
	va := reflect.ValueOf(st).Elem()
	var fi []string
	var val []interface{}
	for i:=0;i<re.NumField();i++ {
		str :=re.Field(i).Tag.Get("json")
		if str != "" {
			v:= va.Field(i).Interface()
			if v  != nil{
				fi = append(fi,str+" = ?")
				val = append(val,v)
			}
		}
	}
	val = append(val,key)
	sql_ := fmt.Sprintf("UPDATE %s SET %s WHERE %s = ?",TableName,strings.Join(fi,","),keyname)
	if db == nil {
		fmt.Println(sql_)

		panic("db == nil")
	}
	res,err := db.Exec(sql_,val...)
	if err != nil {
		panic(err)
	}
	return res

}
func StructSaveForDB(db *sql.DB,TableName string,st interface{}) sql.Result {
	re := reflect.TypeOf(st).Elem()
	va := reflect.ValueOf(st).Elem()
	var fi []string
	var x []string
	var val []interface{}
	for i:=0;i<re.NumField();i++ {
		str :=re.Field(i).Tag.Get("json")
		if str != "" {
			v:= va.Field(i).Interface()
			if v  != nil{
				x = append(x,"?")
				fi = append(fi,str)
				val = append(val,v)
			}
		}
	}
	sql_ := fmt.Sprintf("INSERT INTO %s (%s) values (%s)",TableName,strings.Join(fi,","),strings.Join(x,","))
	//fmt.Println(sql_)
	res,err := db.Exec(sql_,val...)
	if err != nil {
		panic(err)
	}
	return res
	//__id,__err :=m.RowsAffected()

	//fmt.Println(_id,_err,__id,__err)
}
func loadAccountDB(){

	//AccountsMap = make(map[int]*Account)
	HandDB(ConfigDB,func(db *sql.DB){
		row,err := db.Query("SELECT Id,mt4AccountID,uid FROM account")
		if err != nil {
			panic(err)
		}
		for row.Next() {
			ac := Account{}
			err = row.Scan(&ac.Id,&ac.Mt4AccountID,&ac._id)
			if err != nil {
				panic(err)
			}
			ac.loadInstruments(db)
			Accounts = append(Accounts,&ac)
			AccountsMap[int(ac._id)] = &ac
		}
		row.Close()
	})

}
//func ClientIn(path string,in io.Reader,ty string,statusCode int,hand func(io.Reader)error) error{
func ClientIn(path string,in []byte,ty string,statusCode int,hand func(io.Reader)error) error{
	Req,err := http.NewRequest(ty,path,bytes.NewReader(in))
	if err != nil {
		return err
	}
	Req.Header = Header
	//Client := new(http.Client)
	var Client http.Client
	res, err := Client.Do(Req)
	if err != nil {
		log.Println(err)
		time.Sleep(time.Second*5)
		return ClientIn(path,in,ty,statusCode,hand)
	}
	if res.StatusCode != statusCode {
		if res.StatusCode == 429 {
			res.Body.Close()
			log.Println(res.StatusCode)
			time.Sleep(time.Second*5)
			return ClientIn(path,in,ty,statusCode,hand)
		}
		var da [1024]byte
		n,err := res.Body.Read(da[0:])
		//if err != nil {
		//	panic(err)
		//}
		res.Body.Close()
		return fmt.Errorf("status code %d %s %s", res.StatusCode, path,string(da[:n]),err.Error())
		//res.Body.Close()
		//if res.StatusCode == 429 {
		//	<-time.After(time.Second)
		//	//res.Header
		//	//log.Println(res.Header)
		//	return ClientIn(path,in,ty,statusCode,hand)

		//}else{
		//	return fmt.Errorf("status code %d %s", res.StatusCode, path)
		//}
	}
	//err = hand(res.Body)
	var reader io.ReadCloser
	switch res.Header.Get("Content-Encoding") {
	case "gzip":
		reader, err = gzip.NewReader(res.Body)
		if err != nil {
			panic(err)
		}
		//defer reader.Close()
	default:
		reader = res.Body
	}
	if hand!= nil {
		err = hand(reader)
	}
	reader.Close()
	return err

}
func ClientDo(path string, hand func(io.Reader)error) error {

	Req, err := http.NewRequest("GET", path, nil)
	if err != nil {
		return err
	}
	Req.Header = Header
	//Client := new(http.Client)
	var Client http.Client
	res, err := Client.Do(Req)
	if err != nil {
		log.Println(err)
		time.Sleep(time.Second*5)
		return ClientDo(path,hand)
	}
	if res.StatusCode != 200 {
		if res.StatusCode == 429 {
			res.Body.Close()
			time.Sleep(time.Second*5)
			return ClientDo(path,hand)
		}
		//da := make(map[string]interface{})
		var da [1024]byte
		n,err := res.Body.Read(da[0:])
		res.Body.Close()
		return fmt.Errorf("status code %d %s %s", res.StatusCode, path,string(da[:n]),err)
	}
	var reader io.ReadCloser
	switch res.Header.Get("Content-Encoding") {
	case "gzip":
		reader, _ = gzip.NewReader(res.Body)
		//defer reader.Close()
	default:
		reader = res.Body
	}
	if hand != nil {
		err = hand(reader)
	}
	reader.Close()
	return err

}
func HandDB(dbfile string,handle func(*sql.DB)){

	DB,err := sql.Open(DBType,dbfile)
	if err != nil {
		panic(err)
	}
	handle(DB)
	DB.Close()

}
func createAccountDB(){
	sql_ := `
	CREATE TABLE instruments (
		id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
		uid INTEGER NOT NULL,
		displayName TEXT,
		displayPrecision REAL,
		marginRate REAL,
		maximumOrderUnits INTEGER,
		maximumPositionSize INTEGER,
		maximumTrailingStopDistance REAL,
		minimumTradeSize INTEGER,
		minimumTrailingStopDistance REAL,
		name TEXT,
		pipLocation REAL,
		tradeUnitsPrecision REAL,
		type TEXT
	);
	CREATE TABLE account (
		uid INTEGER NOT NULL PRIMARY KEY UNIQUE,
		id TEXT NOT NULL,
		mt4AccountID INTEGER,
		tags TEXT
	);
	`

	HandDB(ConfigDB,func(db *sql.DB){
		_,err := db.Exec(sql_)
		if err != nil {
			panic(err)
		}
		downAccountProperties(db)
	})
}
func downAccountProperties(db *sql.DB){
	err := ClientDo(Host+"/accounts",func(body io.Reader)(err error){
		//da := make(map[string]interface{})
		da := make(map[string][]*Account)
		err =  json.NewDecoder(body).Decode(&da)
		if err != nil {
			return err
		}
		Accounts = da["accounts"]
		//	L := len(self.Accounts)
		if len(Accounts) == 0 {
			return fmt.Errorf("accounts == nil")
		}
		for _,ac := range Accounts{
			res := StructSaveForDB(db,"account",ac)
			ac._id,err = res.LastInsertId()
			if err != nil {
				panic(err)
			}
			ac.downInstruments(db)
			AccountsMap[int(ac._id)] = ac
		}

		return
	})
	if err != nil {
		fmt.Println(err)
	}
}
