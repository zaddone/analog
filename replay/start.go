package replay

import (
	"log"
	"strings"
	"github.com/zaddone/analog/config"
	"github.com/zaddone/analog/server"
	"github.com/gin-gonic/gin"
	"net/http"
	"strconv"
	"fmt"
	"time"
)

var (
	InsCaches []*InstrumentCache
	//SignalGroup []Signal = nil
	SignalM Signal = nil
	SignalChan chan Signal
	//SignalBox [6]float64
	//JointLibs []JointLib = nil
)
func LoadSignal(s Signal){
	//SignalChan <- s
	SignalM = s
	//log.Println("write chan")
}

type Signal interface{
	//New()
	Update(config.Cache)
	Check(config.Instrument)
	Show(string)
	Report(config.Instrument,func(config.SignalForm)error) error
	GetSiForm() config.SignalForm
}

func RunSignal( f func(Signal)){
	runSignal(f)
}
func runSignal( f func(Signal)){
	for SignalM == nil {
		time.Sleep(100)
		//SignalM = <-SignalChan
		//log.Println("read chan")
	}
	f(SignalM)

}
//type TableReport struct {
//	//cycle
//	PlSum float64
//	GamePlSum float64
//	List []interface{}
//}
//func (self *TableReport) Append(rf config.Form) {
//	self.PlSum += rf.GetPl()
//	self.GamePlSum += rf.GetGamePl()
//	self.List = append(self.List,rf.ToMap())
//}
//func ReportWithWeek(rf []config.Form) []interface{} {
//	
//
//}

func init() {
	SignalChan = make(chan Signal,1)
	nas := strings.Split(config.Conf.InsName, "|")
	log.Println("start replay",nas)
	acc := server.FindNowAccount(config.Conf.Account_ID)
	if acc == nil {
		panic(config.Conf.Account_ID)
	}

	InsCaches = make([]*InstrumentCache, len(nas))
	//var InsC *InstrumentCache
	j:=0
	for _, na := range nas {
		log.Println(na)
		//InsC = new(InstrumentCache)
		ins  := acc.Instruments[na]
		if ins == nil {
			log.Println(na,"Not fount")
			continue
		}
		InsC := NewInstrumentCache(ins)
		log.Println(na,InsC.GetInsStopDis())
		InsCaches[j] = InsC
		j++
		//InsC.Init(na)
		InsC.run()
	}
	InsCaches = InsCaches[:j]
	server.Router.GET("/order/:id/:buy",func(c *gin.Context){
		id,err := strconv.Atoi(c.Param("id"))
		if err != nil {
			c.JSON(http.StatusNotFound,err)
			return
		}
		InsC := InsCaches[id]
		if InsC == nil {
			c.JSON(http.StatusNotFound,"insC == nil")
			return
		}
		unit:=config.Conf.Units
		if c.Param("buy") != "buy" {
			unit = -unit
		}
		InsC.TestOrder(unit,func(res interface{}){
			c.JSON(http.StatusOK,res)
		})
		return
	})
	server.Router.GET("/price/:id",func(c *gin.Context){
		id,err := strconv.Atoi(c.Param("id"))
		if err != nil {
			c.JSON(http.StatusNotFound,err)
			return
		}
		InsC := InsCaches[id]
		if InsC == nil {
			c.JSON(http.StatusNotFound,"insC == nil")
			return
		}
		c.JSON(http.StatusOK,InsC.Price.ShowMsg())
		return
	})
	server.Router.GET("/run",func(c *gin.Context){
		db:=make([]map[string]interface{},len(InsCaches))
		for i,_ := range db {
			db[i] = InsCaches[i].Ins.GetTemplateData()
		}
		switch c.DefaultQuery("content_type", "") {
		case "json":
			c.JSON(http.StatusOK,map[string]interface{}{
				"rundbs":db})
		default:
			c.HTML(http.StatusOK,"run.tmpl",map[string]interface{}{
				"rundbs":db})
		}
	})
	server.Router.GET("/runall",func(c *gin.Context){

		var db []interface{}
		if SignalM == nil {
			c.JSON(http.StatusNotFound,"SignalM == nil")
			return
		}
		SignalM.GetSiForm().ReadInsList(func(count,ins int){
			for _,ac := range server.Accounts {
				in := ac.InstrumentsId[ins]
				if in != nil {
					db = append(db,in.GetTemplateData())
					return
				}
			}
			log.Println(count,ins)
			panic("fount not ins")
		})
		c.JSON(http.StatusOK,map[string]interface{}{
			"rundbs":db})

	})
	server.Router.GET("/run/:id/:ins",func(c *gin.Context){
		id,err := strconv.Atoi(c.Param("id"))
		if err != nil {
			c.JSON(http.StatusNotFound,err)
			return
		}
		ac := server.AccountsMap[id]
		if ac == nil {
			c.JSON(http.StatusNotFound,"ac==nil")
			return
		}
		_ins,err := strconv.Atoi(c.Param("ins"))
		if err != nil {
			c.JSON(http.StatusNotFound,err)
			return
		}
		ins := ac.InstrumentsId[_ins]
		if nil == ins {
			c.JSON(http.StatusNotFound,"ins = nil")
			return
		}
		insC := Start(ins)
		if insC == nil {
			c.JSON(http.StatusNotFound,"insc == nil")
			return
		}
		c.JSON(http.StatusOK,map[string]interface{}{"rundb":insC.Ins.GetTemplateData()})

	})

	server.Router.GET("/report/:id/:ins",func(c *gin.Context){

		orID,err :=strconv.Atoi(c.DefaultQuery("orderid","0"))
		if err != nil {
			c.JSON(http.StatusNotFound,err)
			return
		}
		id,err := strconv.Atoi(c.Param("id"))
		if err != nil {
			c.JSON(http.StatusNotFound,err)
			return
		}
		_ins :=server.AccountsMap[id]
		if _ins == nil {
			c.JSON(http.StatusNotFound,"_ins == nil")
			return
		}
		insid,err := strconv.Atoi(c.Param("ins"))
		if err != nil {
			c.JSON(http.StatusNotFound,err)
			return
		}
		ins := _ins.InstrumentsId[insid]
		if ins == nil {
			c.JSON(http.StatusNotFound,"ins == nil")
			return
		}
		var db []interface{}
		SignalM.GetSiForm().ReadDB(ins.GetId(),orID,100,func(f config.Form) bool {
			db = append(db,f.ToMap())
			return true
		})
		switch c.DefaultQuery("content_type","html"){
		case "html":
			c.HTML(http.StatusOK,"report.tmpl",gin.H{"db":db})
		case "json":
			c.JSON(http.StatusOK,gin.H{"db":db})
		default:
			c.HTML(http.StatusNotFound,"404.tmpl",nil)
		}

	})
	server.Router.GET("/close/:id",func(c *gin.Context){
		id,err := strconv.Atoi(c.Param("id"))
		if err != nil {
			c.JSON(http.StatusNotFound,err)
			return
		}
		err = Close(id)
		if err != nil {
			c.JSON(http.StatusNotFound,err)
			return
		}
		c.JSON(http.StatusOK,gin.H{"status":"close"})
	})

}
func Close(index int) error {
	le :=len(InsCaches)
	if index <0 || index >= le {
		return fmt.Errorf("index = %d",index)
	}
	err:= InsCaches[index].Close()
	if err != nil {
		return err
	}
	fmt.Println(InsCaches[index].GetInsName(),"close")

	if le-1 == index {
		InsCaches = InsCaches[:index]
	}else{
		InsCaches = append(InsCaches[:index],InsCaches[index+1:]...)
	}
	return nil
}

func Start(Ins *server.Instrument) (InsC *InstrumentCache) {
	for _, insc := range InsCaches {
		if insc.Ins.GetId() == Ins.GetId() {
			return nil
		}
	}
	InsC = NewInstrumentCache(Ins)
	if InsC == nil {
		log.Println(Ins,"Not fount")
		return nil
	}
	//InsC.Init(Ins)
	InsCaches = append(InsCaches, InsC)
	InsC.run()
	log.Println("start", Ins)
	return

}
func Show() {
	for _,ins := range InsCaches {
		fmt.Println(ins.GetInsName())
		ins.GetCacheList(func(i int,c config.Cache){
			fmt.Println(c.GetName(),time.Unix(c.GetEndTime(),0),c.Show())
		})
	}
}
