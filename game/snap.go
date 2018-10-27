package game
import(
	"github.com/zaddone/analog/config"
	"github.com/zaddone/analog/server"
	//"github.com/zaddone/RoutineWork/request"
	"math"
	"sync"
	"fmt"
	"database/sql"
	"strconv"
	"time"
	"os"
	"io"
	"encoding/json"
	"log"
	//"bytes"
)
const (
	SnapDB  string = "./SnapDB.db"
	ReportTable string ="reportform"
)
type snap struct {

	jo   *Joint
	tp   float64
	sl   float64

	tpP  float64
	slP  float64
	val  float64
	ca   config.Candles
	_ca   config.Candles
	cache config.Cache
	par *Signal

	Ediff float64
	start bool
	forecast bool
	Post bool
	//key uint64
	lastSn *snap

	mr *server.OrderResponse
	Reduce config.Reduce

	gamePl float64
	//End bool

	//res string
}
//func (self *snap) IsEnd() bool {
//
//	if self.mr == nil {
//		return self.End
//	}
//	return self.Reduce != nil
//
//}
func (self *snap) GetResId() string {
	if self.mr == nil {
		return ""
	}
	return string(self.mr.OrderFillTransaction.Id)
}
func (self *snap) NewReportForm() *ReportForm {
	id,err :=strconv.Atoi( self.GetResId())
	if err != nil {
		log.Println(err)
		return nil
	}
	return &ReportForm{
		Id:id,
		Ins:self.cache.GetInsCache().GetInsId(),
		Acc:self.par.InsCache.GetAccountID(),
		BeginTime:time.Now().Unix(),
		EndTime:time.Now().Unix()}

}
func (self *snap) CheckOrder() bool {
	if self.mr != nil {
		return true
	}
	if self.lastSn  == nil {
		return false
	}
	return self.lastSn.CheckOrder()
}

func (self *snap) updateDiff(val float64){

	self.Ediff = val - self.val
	self.forecast = ((self.Ediff>0) == (self.tp>0))
	self.Ediff = math.Abs(self.Ediff)
	self.gamePl = func() float64 {
		if !self.forecast {
			return -self.Ediff
		}
		return self.Ediff
	}()

}


func (self *snap) ReadAll(f func(sn *snap)) {
	f(self)
	if self.lastSn != nil {
		self.lastSn.ReadAll(f)
	}
}

//func (self *snap) checkAll(val float64,f func(sn *snap)) {
//	self.updateDiff(val)
//	f(self)
//	if self.lastSn != nil {
//		self.lastSn.checkAll(val,f)
//	}
//}

//func (self *snap) UpdateInfo_(_sn *snap) {
//
//	if (_sn.tp>0) {
//		
//	}
//
//}
func (self *snap) UpdateInfo(_sn *snap) {
	//f:=(_sn.tp>0)

	if (_sn.tp>0) {
		math.Max(self.tpP,_sn.tpP)
		math.Min(self.slP,_sn.slP)
	}else{
		math.Min(self.tpP,_sn.tpP)
		math.Max(self.slP,_sn.slP)
	}
	if !self.start {
		sl := _sn.val - self.slP
		tp := _sn.val - self.tpP
		if math.Abs(sl)>math.Abs(tp) {
			self.start = true
			self.val = _sn.val
			self.ca = _sn.ca
			self._ca =_sn._ca
			self.cache = _sn.cache
		}
	}

	//	x := math.Max(math.Abs(sl),math.Abs(tp))
	//	if (x > self.par.minDis) {
	//		self.start = true
	//		self.val = _sn.val
	//		self.ca = _sn.ca
	//		self._ca =_sn._ca
	//		self.cache = _sn.cache
	//		//if (_sn.tp>0) {
	//		//	self.tpP = _sn.val+x
	//		//	self.slP = _sn.val-x
	//		//}else{
	//		//	self.tpP = _sn.val-x
	//		//	self.slP = _sn.val+x
	//		//}
	//		//self.slP = 
	//	}
	//}

	//if !self.start
	//if tp > sl {
		//fmt.Println(math.Abs(tp-sl),self.par.minDis)
	//	self.start = true
	//}
	//if !self.start {
	//	if (sl>self.par.minDis) && (tp > self.par.minDis){
	//		self.start = true
	//	}
	//}
}
func (self *snap) UpdateInfo_(_sn *snap) {

	sl := _sn.slP - self.val
	tp := _sn.tpP - self.val
	f := tp>0
	if ( sl > 0 ) == f {
		//if (self.tp >0) == f {
		self.Post = true
		//}
	}
	//self.tp = tp
	self.slP = _sn.slP
	self.tpP = _sn.tpP
	if self.lastSn != nil {
		self.lastSn.UpdateInfo(_sn)
	}

}
//func (self *snap) Close(db *MapSnaps) error {
//	orderid,err :=strconv.Atoi(self.GetResId())
//	if err != nil {
//		return err
//	}
//	db.Close(self.par.InsCache.GetAccountID(),
//	path :=fmt.Sprintf("%s/trades/%d", server.AccountsMap[accid].GetAccountPath(),orderid)
//	var res TradeRes
//	//var res interface{}
//	err =  ClientDo(path,func(body io.Reader) error{
//		return json.NewDecoder(body).Decode(&res)
//	})
//	if err != nil {
//		fmt.Println(err)
//		c.JSON(http.StatusNotFound,err)
//		return
//	}
//
//}

func (self *snap) check () bool {

	if self.forecast {
		if self.Post && self.Ediff > math.Abs(self.val - self.slP) {
			return true
		}
		if self.Ediff > math.Abs(self.val-self.tpP) {
			return true
		}
	}else{
		if self.Post && self.Ediff > math.Abs(self.val - self.tpP) {
			return true
		}
		if self.Ediff > math.Abs(self.val - self.slP) {
			return true
		}
	}
	return false
}
type ReportForm struct{
	Id int `json:"id"`
	GamePl float64 `json:"gamepl"`
	Pl float64 `json:"pl"`
	Ins int `json:"ins"`
	Acc int `json:"acc"`
	BeginTime int64 `json:"begintime"`
	EndTime int64 `json:"endtime"`
}
func (self *ReportForm) GetBegin() int64 {
	return self.BeginTime
}
func (self *ReportForm) GetEnd() int64 {
	return self.EndTime
}
func (self *ReportForm) GetPl() float64 {
	return self.Pl
}
func (self *ReportForm) GetGamePl() float64 {
	return self.GamePl
}

func (self *ReportForm)ToMap() map[string]interface{} {

	return map[string]interface{}{
		"id":self.Id,
		"gamePl":self.GamePl,
		"pl":self.Pl,
		"ins":self.Ins,
		"acc":self.Acc,
		"beginTime":self.BeginTime,
		"endTime":self.EndTime}
		//"beginTime":time.Unix(self.BeginTime,0).Format(config.TimeFormat),
		//"endTime":time.Unix(self.EndTime,0).Format(config.TimeFormat)}

}
type MapSnaps struct{
	sync.RWMutex
	//snaps map[string]*snap
	//LastSnap *snap
}
func NewMapSnaps() *MapSnaps{
	//return &MapSnaps{snaps:make(map[string]*snap)}

	_,err :=  os.Stat(SnapDB)
	if err != nil {
		sql_ := fmt.Sprintf(`
		CREATE TABLE %s (
			id	INTEGER NOT NULL UNIQUE,
			pl	REAL,
			gamepl	REAL,
			ins	INTEGER NOT NULL,
			acc	INTEGER NOT NULL,
			begintime INTEGER NOT NULL,
			endtime	INTEGER NOT NULL,
			PRIMARY KEY(id,ins)
		);	
		`,ReportTable)
		server.HandDB(SnapDB,func(db *sql.DB){
			_,err := db.Exec(sql_)
			if err != nil {
				panic(err)
			}
		})
	}
	return &MapSnaps{}
}
func (self *MapSnaps) add(r *ReportForm){

	//self.LastSnap = sn
	//id,err :=strconv.Atoi( sn.GetResId())
	//if err != nil {
	//	panic(err)
	//}
	//r:= ReportForm{
	//	Id:id,
	//	Ins:sn.cache.GetInsCache().GetInsId(),
	//	Acc:sn.par.InsCache.GetAccountID(),
	//	BeginTime:time.Now().Unix(),
	//	EndTime:time.Now().Unix()}
	self.Lock()
	server.HandDB(SnapDB,func(db *sql.DB){
		server.StructSaveForDB(db,ReportTable,r)
	})
	//self.snaps[sn.GetResId()] = sn
	self.Unlock()

}

func (self *MapSnaps) Read(id int)(rf *ReportForm){
	//id,err :=strconv.Atoi( id_)
	//if err != nil {
	//	panic(err)
	//}
	sql_ := fmt.Sprintf("SELECT id,gamepl,pl,ins,acc,begintime,endtime FROM %s WHERE id = ?",ReportTable)
	self.RLock()
	server.HandDB(SnapDB,func(db *sql.DB){
		row := db.QueryRow(sql_,id)
		rf = new(ReportForm)
		err := row.Scan(&rf.Id,&rf.GamePl,&rf.Pl,&rf.Ins,&rf.Acc,&rf.BeginTime,&rf.EndTime)
		if err != nil {
			panic(err)
		}
	})
	self.RUnlock()
	return
}
func (self *MapSnaps) Close (rf *ReportForm) {
	defer self.Update(rf)
	path :=fmt.Sprintf("%s/trades/%d", server.AccountsMap[rf.Acc].GetAccountPath(),rf.Id)
	var res server.TradeRes
	da, err := json.Marshal(map[string]string{"units":"ALL"})
	if err != nil {
		panic(err)
	}

	for {
		err := server.ClientDo(path,func(body io.Reader)error {
			return json.NewDecoder(body).Decode(&res)
		})
		if err != nil {
			log.Println("trades",err)
			time.Sleep(time.Second)
			break
		}
		log.Println(res.Trade.State)
		pl := res.Trade.RealizedPL.GetFloat()
		if (res.Trade.State == "OPEN") {
			err := server.ClientIn(path+"/close",da,"PUT",200,nil)
			if err != nil {
				log.Println("trades close",err)
				time.Sleep(time.Second)
			}
			continue
		}
		rf.Pl = pl
		return
	}
}
func (self *MapSnaps) ReadInsList(f func(int,int)) {
	sql_ := fmt.Sprintf("SELECT COUNT(*) as c,ins FROM %s GROUP BY ins",ReportTable)
	self.RLock()
	var count,ins int
	server.HandDB(SnapDB,func(db *sql.DB){
		rows,err := db.Query(sql_)
		if err != nil {
			panic(err)
		}
		for rows.Next() {
			err = rows.Scan(&count,&ins)
			if err != nil {
				panic(err)
			}
			f(count,ins)
		}
		rows.Close()
	})
	self.RUnlock()

}
func (self *MapSnaps) ReadDB(InsId,lastOrderId,limit int,f func(config.Form) bool){
	sql_ := fmt.Sprintf( "SELECT id,gamepl,pl,ins,begintime,endtime FROM %s WHERE ins = ? and id > ? limit ? ",ReportTable)
	//fmt.Println(sql_)
	var rf *ReportForm
	self.RLock()
	server.HandDB(SnapDB,func(db *sql.DB){
		rows,err := db.Query(sql_,InsId,lastOrderId,limit)
		if err != nil {
			panic(err)
		}
		for rows.Next() {
			rf = new(ReportForm)
			err = rows.Scan(&rf.Id,&rf.GamePl,&rf.Pl,&rf.Ins,&rf.BeginTime,&rf.EndTime)
			if err != nil {
				panic(err)
			}
			if !f(rf){
				break
			}
		}
		rows.Close()
	})
	self.RUnlock()

}
//func (self *MapSnaps) ReadAll(orderid int,f func(config.Form) bool){
//
//	if self.LastSnap == nil {
//		return
//	}
//	self.ReadDB(self.LastSnap.cache.GetInsCache().GetInsId(),orderid,100,f)
//}


func (self *MapSnaps) GetReportForms() (rf []*ReportForm) {

	//var Sum,GameSum float64
	//var count int
	//self.ReadAll(func(id int,r *ReportForm)bool{
	//	rf = append(rf,r)
	//	Sum += r.Pl
	//	GameSum += r.GamePl
	//	count++
	//	return true
	//})
	//fmt.Println("sum",Sum)
	//fmt.Println("GameSum",GameSum)
	//fmt.Println("count",count)
	return

}
func (self *MapSnaps) Update(rf *ReportForm){
	rf.EndTime = time.Now().Unix()
	self.Lock()
	server.HandDB(SnapDB,func(db *sql.DB){
		server.StructUpdateForDB(db,ReportTable,rf,"id",rf.Id)
	})
	self.Unlock()

}
