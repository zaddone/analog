package game
import(
	//"github.com/zaddone/RoutineWork/config"
	//"github.com/zaddone/RoutineWork/request"
	"github.com/zaddone/analog/server"
	"github.com/zaddone/analog/config"
	"time"
	"math"
	"fmt"
	"log"
	"strconv"
	"io"
	"encoding/json"
	"os"
	"path/filepath"
	//"bytes"
)
//type Joints struct {
//
//	msg []float64
//	jos map[]*Joint
//
//}

type Signal struct {

	InsCache config.Instrument
	Joint *JointMap
	MsgBox [8]float64
	CacheLen int
	//snap  int64
	minDis   float64
	maxDis   float64
	TmpSnap  *MapSnaps
	Snap     *snap
	Snaps     []*snap
	sample   int64
	LastTime time.Time
	TestMap  map[int64][]int

	Vars  [2]*Variance
	Vars1 [2]*Variance
	//Vars2 [2]*Variance
	//mr *request.OrderResponse
	TmpJo []*Joint

}

func NewSignal(Ins config.Instrument) (s *Signal) {

	dis := Ins.GetInsStopDis()
	s = &Signal{
		minDis:dis*config.Conf.RateMin,
		maxDis:dis*config.Conf.RateMax,
		Joint:NewJointMap(),
		TmpSnap:SnapMap,
		//TestMap:make(map[int64][]int),
		Vars:[2]*Variance{NewVariance(),NewVariance()},
		Vars1:[2]*Variance{NewVariance(),NewVariance()},
		InsCache:Ins}

	if Ins != nil {
		s.CacheLen=Ins.GetCacheLen()
		//go s.SyncUpdateTmpSnap()
	}
	return s
}
func (self *Signal) ShowTmpJo() (float64,float64) {

	var j1,j2 float64
	var j1_,j2_ float64
	for _,jo := range self.TmpJo {
		if jo.Diff >0 {
			if jo.Cans == nil {
				j2 ++
			}else if jo.Next != nil {
				j1 ++
			}
		}else{
			if jo.Cans == nil {
				j2_ ++
			}else if jo.Next != nil {
				j1_ ++
			}
		}
	}
	//fmt.Println(j2,j1,j2_,j1_)
	return j2/(j2+j1),j2_/(j2_+j1_)

}
func (self *Signal) AddTmpJo(jo *Joint) {
	self.TmpJo = append(self.TmpJo,jo)
	for len(self.TmpJo) > config.Conf.JointMax {
		self.TmpJo = self.TmpJo[1:]
	}
}
//func (self *Signal)SyncUpdateTmpSnap(){
//	//if self.InsCache == nil {
//	//	return
//	//}
//	for{
//		res := self.InsCache.GetResponse()
//		var sn *snap
//		res.GetTradesClosed(func(re config.Reduce)bool {
//			sn = self.TmpSnap.Read(re.GetId())
//			if sn != nil {
//				sn.Reduce = re
//			}
//			return true
//		})
//	}
//}

func (self *Signal) GetSample() (sample int64) {

	self.ReadJoint(func(jo *JointCache,i int){
		sample = sample << 1
		if jo == nil {
			return
		}
		if jo.IsSplit {
			sample++
		}
	})
	return sample

}

func (self *Signal) ReadJoint(f func(*JointCache,int)) {
	self.InsCache.GetCacheList(func(i int,c config.Cache) {
		f(self.Joint.Get(c.GetScale()),i)
	})
}

func (self *Signal) CheckSample() (ids []int) {

	sample := self.GetSample()
	if self.sample == sample {
		return
	}
	xor := self.sample^sample
	self.sample = sample
	//fmt.Printf("%b %b %b\r\n",snap,self.snap,xor)
	for i:=self.CacheLen-1; i>=0; i-- {
		if (xor|1 == xor) && (sample|1 == sample) {
			ids = append(ids,i)
		}
		xor = xor>>1
		sample= sample>>1
	}
	return

}

func (self *Signal) NewSnap (id int,lastCan config.Candles) *snap {

	canVal := lastCan.GetMidAverage()
	cache  := self.InsCache.GetCache(id)
	jc := self.Joint.Get(cache.GetScale())
	if jc.EndJoint.Last == nil {
		return nil
	}
	_tp :=jc.EndJoint.Last.Cans[0].GetMidAverage()-canVal
	f  := (jc.EndJoint.Diff>0)
	if f != (_tp >0) {
		//panic(100)
		return nil
	}
	_sl :=-jc.EndJoint.Diff
	tp := math.Abs(_tp)
	sl := math.Abs(_sl)
	//if tp < (sl * config.Conf.Rate) {
	if tp < sl {
		return nil
	}

	var vars,vars1 *Variance
	if _tp >0 {
		vars   = self.Vars[0]
		vars1  = self.Vars1[0]
	}else{
		vars   = self.Vars[1]
		vars1  = self.Vars1[1]
	}
	v1 := vars.val()
	v1_ := vars1.val()
	vars.in(tp)
	vars1.in(sl)
	v2 := vars.val()
	v2_ := vars1.val()
	//fmt.Println(v1,v2)
	vars.balance()
	vars1.balance()

	//self.AddTmpJo(jc.EndJoint)
	//k1,k2 := self.ShowTmpJo()
	//if (jc.EndJoint.Diff >0){
	//	if k1 > k2 {
	//		return nil
	//	}
	//}else{
	//	if k1 < k2 {
	//		return nil
	//	}
	//}


	if v2 > v1 {
		return nil
	}
	if v2_ > v1_ {
		return nil
	}



	////if tp < self.minDis {
	//if (tp < self.minDis) || (tp > self.maxDis) {
	//if (tp < self.minDis) {
	//	return nil
	//}

	/**
	le := self.InsCache.GetCacheLen()
	//var slca config.Candles
	ide := id+1
	if ide >= le {
		return nil
	}
	jc_ := self.Joint.Get(self.InsCache.GetCache(ide).GetScale())
	if jc_ == nil {
		return nil
	}
	//var sl float64
	if f != (jc_.EndJoint.Diff >0) {
		if jc_.EndJoint.MaxDiff == 0 {
			return nil
		}else{
		//	sl = math.Abs(jc_.EndJoint.MaxCan.GetMidAverage() - canVal)
		}
	}else{
		//return nil
		if jc_.EndJoint.MaxDiff != 0 {
			return nil
		}else{
		//	sl = math.Abs(jc_.EndJoint.Cans[0].GetMidAverage() - canVal)
		}
	}
	//if sl == 0 {
	//	return nil
	//}
	//if sl > tp {
	//	return nil
	//}

	//tpP := canVal + _tp * 1.1
	//slP := canVal - _tp * 0.9
	**/

	return &snap{jo:jc.EndJoint,
		//tpP:canVal + _tp,
		//slP:canVal - _tp,
		tpP:jc.EndJoint.Last.Cans[0].GetMidAverage(),
		slP:jc.EndJoint.Cans[0].GetMidAverage(),
		val:canVal,
		tp:_tp,
		sl:_sl,
		ca:lastCan,
		_ca:cache.GetlastCan(),
		cache:cache,
		start:true,
		par:self}


}

func (self *Signal) ShowMsg() string {

	pv:=math.Pow10(int(self.InsCache.GetDisplayPrecision()))
	sum  := self.MsgBox[0] + self.MsgBox[1] + self.MsgBox[2]
	//pv_3 := (pv*self.MsgBox[3])/self.MsgBox[4]
	pv_3 := (pv*self.MsgBox[3]) / sum
	//return fmt.Sprintf("%.0f %.2f %.2f %.2f",self.MsgBox[:2],self.MsgBox[0]/self.MsgBox[1],pv_2/self.MsgBox[0],pv_3/self.MsgBox[1])
	return fmt.Sprintf("%.0f %.2f %.2f %.2f",self.MsgBox,self.MsgBox[0]/self.MsgBox[1],pv_3, (self.MsgBox[6]*pv)/self.MsgBox[7])


}

func (self *Signal) UpdateJoint( ca config.Cache ) {

	k := ca.GetScale()
	j := self.Joint.Get(k)
	if j == nil {
		j = NewJointCache(self)
		self.Joint.Set(k,j)
	}
	j.Add(ca)

	if self.InsCache.GetBaseCache() != ca {
		return
	}
	_can := ca.GetlastCan()
	Time := time.Unix(_can.GetTime(),0)
	if self.LastTime.Unix() < Time.Unix() {
		if Time.Month()!= self.LastTime.Month() {
			msg := fmt.Sprintf("%s %s",self.LastTime,self.ShowMsg())
			f,err := os.OpenFile(filepath.Join(logPath,self.InsCache.GetInsName()),os.O_APPEND|os.O_CREATE|os.O_RDWR|os.O_SYNC,0777)
			if err != nil {
				panic(err)
			}
			f.WriteString(msg+"\r\n")
			f.Close()
			fmt.Println(self.InsCache.GetInsName(),msg)

			self.MsgBox = [8]float64{0,0,0,0,0,self.MsgBox[5],self.MsgBox[6]+self.MsgBox[3],self.MsgBox[7]+self.MsgBox[4]}
		}
		self.LastTime = Time
		//fmt.Printf("%s %s %s\r",self.InsCache.GetInsName(),self.LastTime,self.ShowMsg())
	}else{
		panic("_can time")
	}
	self.CheckTmpSnap(_can)

}

func (self *Signal) AddTmpSnap_(_sn *snap) {

	//if _sn.tp >0 {
	//	self.MsgBox[0]++
	//	self.MsgBox[2] += math.Abs(_sn.sl)
	//}else{
	//	self.MsgBox[1]++
	//	self.MsgBox[3] += math.Abs(_sn.sl)
	//}
	//self.MsgBox[0]++
	//self.MsgBox[2] += math.Abs(_sn.tp)
	//self.MsgBox[3] += math.Abs(_sn.sl)
	self.Snaps = append(self.Snaps,_sn)


}

func (self *Signal) AddTmpSnap(_sn *snap) {
	self.MsgBox[5]++
	if self.Snap == nil {
		self.Snap = _sn
		return
	}
	if (self.Snap.tp>0) == (_sn.tp>0) {
		self.Snap.UpdateInfo(_sn)
		return
	}
	if self.Snap.start {
		self.Snap.updateDiff(_sn.val)
		if self.Snap.gamePl >0 {
			self.MsgBox[0]++
		}else{
			self.MsgBox[1]++
		}
		self.MsgBox[3] += self.Snap.gamePl
		self.MsgBox[4]++
	}
	self.Snap = _sn
}

func (self *Signal) AddTmpSnap__(_sn *snap) {

	self.MsgBox[5]++

	if self.Snap == nil {
		self.Snap = _sn
		return
	}
	if (self.Snap.tp>0) == (_sn.tp>0) {

		if !self.Snap.start {
			_sn.start = true
			self.PostOrder(_sn)
			_sn.lastSn = self.Snap
			self.Snap = _sn
		}
		self.Snap.UpdateInfo(_sn)
		self.UpdateOrder(self.Snap)
		return

	}
	if self.Snap.start {
		self.ClearOrder()
		var ediff float64
		//fmt.Println("clear")
		//valdiff:= self.Snap.val
		self.Snap.ReadAll(func(sn *snap){
			//fmt.Println(sn.cache.GetName(),sn.tp,sn.val - valdiff)
			//valdiff = sn.val
			if !sn.start {
				return
			}
			self.MsgBox[4]++
			sn.updateDiff(_sn.val)
			ediff += sn.gamePl
			id,err :=strconv.Atoi(sn.GetResId())
			if err == nil {
				rp := self.TmpSnap.Read(id)
				if rp != nil {
					rp.GamePl = sn.gamePl
					go self.TmpSnap.Close(rp)
					//self.TmpSnap.Update(rp)
				}
			}
			//sn.End = true

		})
		if ediff == 0 {
			self.MsgBox[2]++
		}else if ediff >0 {
			self.MsgBox[0]++
		}else{
			self.MsgBox[1]++
		}
		self.MsgBox[3] += ediff
		//fmt.Println(ediff)
	//}else{
	//	self.MsgBox[4]++
	}
	self.Snap = _sn

}
func (self *Signal) CheckTmpSnap_(can config.Candles){
	le := len(self.Snaps)
	if  le== 0 {
		return
	}

	//NSnaps := make([]*snap,le)
	//i :=0
	//for _,sn := range self.Snaps {
	//	if sn.jo.Next != nil {
	//		self.MsgBox[0]++
	//		self.MsgBox[2] += math.Abs(sn.tp)
	//		self.MsgBox[3] += math.Abs(sn.sl)
	//	}else if sn.jo.Cans == nil {
	//		self.MsgBox[1]++
	//		self.MsgBox[2] += math.Abs(sn.tp)
	//		self.MsgBox[3] += math.Abs(sn.sl)
	//	}else{
	//		NSnaps[i] = sn
	//		i++
	//	}
	//}
	//self.Snaps = NSnaps[:i]

	val :=can.GetMidAverage()
	NSnaps := make([]*snap,le)
	i :=0
	for _,sn := range self.Snaps {
		sn.updateDiff(val)
		if !sn.check() {
			NSnaps[i] = sn
			i++
		}else{
			if sn.gamePl > 0 {
				self.MsgBox[0]++
			}else{
				self.MsgBox[1]++
			}
			self.MsgBox[3] += sn.gamePl
			self.MsgBox[2] += math.Abs(sn.tp)
			//self.MsgBox[3] += math.Abs(sn.sl)
		}

	}
	self.Snaps = NSnaps[:i]
}
func (self *Signal) CheckTmpSnap(can config.Candles){
	//return
	if self.Snap == nil {
		return
	}
	val :=can.GetMidAverage()
	self.Snap.updateDiff(val)
	if self.Snap.check() {

		if self.Snap.start {
			if self.Snap.gamePl >0{
				self.MsgBox[0]++
			}else{
				self.MsgBox[1]++
			}
			self.MsgBox[3] += self.Snap.gamePl
			self.MsgBox[4]++
		}
		self.Snap = nil
	}

}

func (self *Signal) CheckTmpSnap__(can config.Candles){
	//return
	if self.Snap == nil {
		return
	}
	//if !self.Snap.start{
	//	return
	//}
	val :=can.GetMidAverage()
	self.Snap.updateDiff(val)
	var ediff float64
	if self.Snap.check() {
		//ediff += self.Snap.gamePl
		//var st []float64
		//if self.Snap.lastSn != nil {
		self.ClearOrder()
		self.Snap.ReadAll(func(sn *snap){
			self.MsgBox[4]++
			if !sn.start{
				return
			}
			sn.updateDiff(val)
			ediff += sn.gamePl
			//st = append(st,sn.gamePl)
			//sn.End = true

			id,err :=strconv.Atoi(sn.GetResId())
			if err != nil {
				return
			}
			//id := sn.GetResId()
			//if id != "" {
			rp := self.TmpSnap.Read(id)
			if rp != nil {
				rp.GamePl = sn.gamePl
				go self.TmpSnap.Close(rp)
				//self.TmpSnap.Update(rp)
			}
		})
		//}
		//fmt.Println(st)
		if ediff != 0 {
			if ediff == 0 {
				self.MsgBox[2]++
			}else if ediff >0 {
				self.MsgBox[0]++
			}else{
				self.MsgBox[1]++
			}
			self.MsgBox[3] += ediff
		}
		self.Snap = nil
		//fmt.Println(ediff)
	}

}

func (self *Signal) ClearOrder(){
	if !self.Snap.CheckOrder() {
		return
	}
	//_,err := server.ClosePosition(self.InsCache.GetInsName(),"ALL",self.InsCache.GetAccountID())
	//if err == nil {
	//	return
	//}
	//log.Println("close",err)
	path:= server.AccountsMap[self.InsCache.GetAccountID()].GetAccountPath()+"/positions/"+self.InsCache.GetInsName()+"/close"
	//var res server.PositionRes
	//err :=  server.ClientDo(path,func(body io.Reader) error{
	//	return json.NewDecoder(body).Decode(&res)
	//})
	//if res.Position.UnrealizedPL.GetFloat() == 0 {
	//	return
	//}

	da, err := json.Marshal(map[string]string{"longUnits":"ALL"})
	if err != nil {
		panic(err)
	}
	err =  server.ClientIn(path,da,"PUT",200,nil)
	if err != nil {
		log.Println(err)
		//self.ClearOrder()
	}

}

func (self *Signal) UpdateOrder(_sn *snap) {

	_sn.ReadAll(func(sn *snap){
		if sn.mr == nil {
			return
		}
		mr,err :=server.HandleTrades(
			self.InsCache.GetStandardPrice(sn.tpP),
			self.InsCache.GetStandardPrice(sn.slP),
			sn.GetResId(),
			self.InsCache.GetAccountID())
		if err != nil {
			log.Println(err)
		}else{
			log.Println(mr)
		}
	})

}

func (self *Signal) PostOrder(_sn *snap) {

	if !config.Conf.Price {
		return
	}

	if math.Abs(_sn.tp) < self.InsCache.GetPriceDiff()*2 {
		return
	}
	if !self.Snap.CheckOrder() {
	//if !self.InsCache.CheckOrderFill(){
		if !_sn.cache.GetOnline(){
			//log.Println("online")
			return
		}
		now := self.InsCache.GetPriceTime()
		if now == 0 {
			now = time.Now().Unix()
		}
		if math.Abs(float64(_sn._ca.GetTime()+_sn.cache.GetScale() - self.InsCache.GetPriceTime()))>float64(20){
			//log.Println("time out")
			return
		}
	}

	units := config.Conf.Units
	//var sl_,tp_ float64
	if _sn.tp<0 {
		units = -units
	}

	//sl := self.InsCache.GetStandardPrice(_sn.slP)
	//tp := self.InsCache.GetStandardPrice(_sn.tpP)
	mr,err := server.HandleOrder(
		self.InsCache.GetInsName(),
		units,"",self.InsCache.GetStandardPrice(_sn.tpP),self.InsCache.GetStandardPrice(_sn.slP),self.InsCache.GetAccountID())
	if err != nil {
		log.Println(err)
		return
	}
	log.Println(mr)
	_sn.mr = &mr
	rf := _sn.NewReportForm()
	if rf == nil {
		_sn.mr = nil
		return
	}
	path :=fmt.Sprintf("%s/trades/%d", server.AccountsMap[rf.Acc].GetAccountPath(),rf.Id)
	var res server.TradeRes
	err = server.ClientDo(path,func(body io.Reader)error {
		return json.NewDecoder(body).Decode(&res)
	})
	if err != nil {
		_sn.mr = nil
		return
	}
	if (res.Trade.State == "OPEN") {
		self.TmpSnap.add(rf)
		return
	}
	_sn.mr = nil
	return

}

