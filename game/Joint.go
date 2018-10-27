package game
import(
	"github.com/zaddone/analog/config"
	"sync"
	"math"

)
type JointMap struct {
	sync.RWMutex
	jointMap map[int64] *JointCache
}
func NewJointMap () *JointMap {
	return &JointMap{jointMap:map[int64]*JointCache{}}
}
func (self *JointMap) Read(f func(int64,*JointCache)bool){

	self.RLock()
	for k,jc := range self.jointMap {
		if f(k,jc) {
			return
		}
	}
	self.RUnlock()

}
func (self *JointMap) Set(k int64,v *JointCache) {
	self.Lock()
	self.jointMap[k] = v
	self.Unlock()
}
func (self *JointMap) Get(k int64) (j *JointCache){
	self.RLock()
	j = self.jointMap[k]
	self.RUnlock()
	return
}
type JointCache struct {

	EndJoint  *Joint
	//BeginJoint *Joint
	IsSplit bool
	par *Signal


}

func NewJointCache(par *Signal) *JointCache {

	jc := JointCache{EndJoint:NewJoint(nil),
			par:par}
	//jc.BeginJoint = jc.EndJoint
	return &jc

}
func (self *JointCache) Add(ca config.Cache){
	self.EndJoint, self.IsSplit = self.EndJoint.Append(ca.GetlastCan())
	if !self.IsSplit {
		return
	}
	self.ClearJoint()
}
func (self *JointCache) ClearJoint(){

	if self.EndJoint.Last == nil {
		return
	}
	if self.EndJoint.Last.Last == nil {
		return
	}
	if self.EndJoint.Last.Last.Last == nil {
		return
	}else{
		self.EndJoint.Last.Last.Last = nil
	}
}
type Joint struct{

	Cans    []config.Candles
	Last    *Joint
	Next    *Joint
	SumLong float64
	Diff    float64
	MaxDiff float64
	MaxCan  config.Candles
	LastCan config.Candles
	//Num	int
	//Stop    chan bool
	//Msg  []float64

}
func NewJoint(cans []config.Candles) (jo *Joint) {
	jo = new(Joint)
	//jo.Stop = nil
	le := len(cans)
	if le > 0 {
		jo.Cans = make([]config.Candles, le)
		for i:=0;i<le;i++{
			jo.LastCan = cans[i]
			jo.SumLong += jo.LastCan.GetMidLong()
			jo.Cans[i] = jo.LastCan
		}
		jo.Diff = jo.LastCan.GetMidAverage() - cans[0].GetMidAverage()
	}
	return jo

}
func (self *Joint) merge(jo *Joint) {

	self.Cans = append(self.Cans,jo.Cans...)
	jo.Cans = nil
	self.SumLong += jo.SumLong
	self.MaxDiff = 0
	//if jo.Msg != nil{
	//	jo.Msg[1] ++
	//}

	//jo.Stop = make(chan bool)
	//go func (_jo *Joint){
	//	if _jo.Stop != nil {
	//		_jo.Stop <-false
	//		//close(_jo.Stop)
	//		//_jo.Stop = nil
	//	}
	//}(jo)

}

func (self *Joint) Append(can config.Candles) (jo *Joint, update bool) {

	canVal := can.GetMidAverage()
	self.Cans = append(self.Cans, can)
	self.SumLong += can.GetMidLong()
	self.Diff = canVal - self.Cans[0].GetMidAverage()
	jo = self
	if self.Last != nil {
		if (self.Last.Diff>0) == (self.Diff>0) {
			jo = self.Last
			jo.merge(self)
			return
		}
	}
	le := len(self.Cans)
	le--
	if le < 3 {
		return
	}
	self.MaxDiff = 0

	ave := self.GetLongAve()
	var dif float64 = 0
	var maxId int = -1
	for i := 1; i < le; i++ {
		dif = canVal - self.Cans[i].GetMidAverage()
		if (dif > 0) != (self.Diff > 0) {
			dif = math.Abs(dif)
			if (dif > self.MaxDiff) {
				self.MaxDiff = dif
				maxId = i
				self.MaxCan = self.Cans[i]
			}
		}
	}
	if ave > self.MaxDiff {
		return
	}
	jo = self.split(maxId)
	update = true
	return

}

func (self *Joint) GetLongAve() float64 {
	if self.SumLong == 0 {
		for _,can := range self.Cans {
			self.SumLong += can.GetMidLong()
		}
	}
	return self.SumLong / float64(len(self.Cans))
}

func (self *Joint) split(id int) (jo *Joint) {

	jo = NewJoint(self.Cans[id:])
	jo.Last = self


	self.Reload(self.Cans[:id])
	self.Next = jo
	self.Diff = jo.Cans[0].GetMidAverage() - self.Cans[0].GetMidAverage()
	//jo.Num = self.Num+1
	//if (jo.Diff>0) == (self.Diff>0) {
	//	panic("jo f diff")
	//}
	//if self.Msg != nil{
	//	self.Msg[0] ++
	//}

	//go func (_jo *Joint){
	//	if _jo.Stop != nil {
	//		_jo.Stop <-true
	//		//close(_jo.Stop)
	//		//_jo.Stop = nil
	//	}
	//}(self)
	return jo

}
func (self *Joint) Reload(cans []config.Candles) {

	le := len(cans)
	self.Cans = make([]config.Candles, le)
	self.SumLong = 0
	for i:=0;i<le;i++{
		self.LastCan = cans[i]
		self.SumLong += self.LastCan.GetMidLong()
		self.Cans[i] = self.LastCan
	}
	self.Diff = self.LastCan.GetMidAverage() - cans[0].GetMidAverage()
	//self.Dur = nil

}
