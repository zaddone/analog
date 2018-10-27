package game
import (
	"github.com/zaddone/analog/replay"
	"github.com/zaddone/analog/config"
	//"github.com/zaddone/analog/server"
	"sync"
	"fmt"
	"strings"
	"os"
	"path/filepath"
	"time"
)
var (
	SnapMap *MapSnaps = NewMapSnaps()
	logPath string = filepath.Join(config.Conf.TrLogPath,time.Now().Format("20060102T15_04_05"))

)
func init(){
	//replay.SignalGroup = append(replay.SignalGroup, NewGame())
	_,err := os.Stat(logPath)
	i:=0
	for err == nil {
		i++
		logPath =fmt.Sprintf("%s_%d",logPath,i)
		_,err = os.Stat(logPath)
	}
	err = os.MkdirAll(logPath,0777)
	if err != nil {
		panic(err)
	}
	fmt.Println(logPath)
	go replay.LoadSignal(NewGame())

}

type Game struct{

	msgMap map[string]*Signal
	sync.RWMutex
	NowSignal *Signal
	//sync.Mutex

}
func NewGame() *Game {
	return &Game{msgMap:map[string]*Signal{}}
}
func (self *Game) read(f func(k string,si *Signal)){
	self.RLock()
	for k,si := range self.msgMap {
		f(k,si)
	}
	self.RUnlock()
}
func (self *Game) get(k string) (s *Signal) {
	self.RLock()
	s = self.msgMap[k]
	self.RUnlock()
	return
}
func (self *Game) set(k string,s *Signal) {
	self.Lock()
	self.msgMap[k] = s
	self.Unlock()
}
func GetKeyForInsCache(insc config.Instrument) string {
	return fmt.Sprintf("%s_%d",insc.GetInsName(),insc.GetInsId())
}
func (self *Game) Update(ca config.Cache) {

	key := GetKeyForInsCache(ca.GetInsCache())
	sig := self.get(key)
	if sig == nil {
		sig = NewSignal(ca.GetInsCache())
		self.set(key,sig)
	}
	sig.UpdateJoint(ca)

}

func (self *Game) Check (InsCache config.Instrument){

	//key := InsCache.GetInsName()
	key := GetKeyForInsCache(InsCache)
	sig := self.get(key)
	if sig == nil {
		return
	}

	ids := sig.CheckSample()
	if len(ids)  == 0 {
		return
	}

	lastCan :=InsCache.GetBaseCache().GetlastCan()
	var sn *snap
	for _,id := range ids {
		sn = sig.NewSnap(id,lastCan)
		if sn != nil{
			sig.AddTmpSnap(sn)
			return
		}
	}

}

func (self *Game) Report(InsCache config.Instrument,f func(config.SignalForm)error) error{

	key :=GetKeyForInsCache(InsCache)
	signal := self.get(key)
	if signal == nil {
		return fmt.Errorf("signal == nil")
	}
	return f(signal.TmpSnap)

}
func (self *Game) GetSiForm() config.SignalForm {
	return SnapMap
}

func (self *Game) Show(cmd string){
	if self.NowSignal== nil {
		self.NowSignal = self.get(strings.ToUpper(cmd))
		if self.NowSignal == nil {
			self.read(func(k string,si *Signal){
				fmt.Println(k)
			})
		}else{
			fmt.Println("NewSignal",self.NowSignal.InsCache.GetInsName())
		}
		return
	}
	switch cmd {
	case "clear":
		self.NowSignal = nil
		fmt.Println("clear")
	case "test":
		fmt.Println(self.NowSignal.InsCache.GetInsName(),"test")
	default:
		rf := self.NowSignal.TmpSnap.GetReportForms()
		for _,r := range rf {
			fmt.Println(r)
		}
		fmt.Println("default")

	}
}
