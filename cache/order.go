package cache
import(
//	"github.com/zaddone/operate/oanda"
//	"github.com/zaddone/analog/config"
//	"os"
//	//"sync"
//	"time"
//	"path/filepath"
//	"math"
//	"fmt"
)

//type order struct{
//
//	e element
//	tp float64
//	sl float64
//	orderId string
//	ins *oanda.Instrument
//	f *os.File
//	//sync.Mutex
//	le *level
//	k float64
//	path string
//
//}
//
//func NewOrder(e element,ins *oanda.Instrument,le *level,tp,sl float64) (o *order) {
//	o = &order{
//		e:e,
//		ins:ins,
//		le:le,
//		tp:tp,
//		sl:sl,
//		k:math.Pow10(int(ins.DisplayPrecision)),
//	}
//	le.lastOrder = o
//	o.openFile()
//	o.load()
//	//go o.postOrder()
//	return o
//
//}
//
//func (self *order) close(e element){
//	old := self.f.Name()
//	self.f.Close()
//	if err := os.Rename(old,fmt.Sprintf("%s_%d",old,int(self.k*e.Middle()))); err != nil {
//		fmt.Println(err)
//	}
//}
//func (self *order) openFile(){
//
//	self.path = filepath.Join(
//		config.Conf.LogPath,
//		self.ins.Name,
//		time.Unix(self.e.DateTime(),0).Format("200601"),
//	)
//	_,err := os.Stat(self.path)
//	if err != nil {
//		err = os.MkdirAll(self.path,0700)
//		if err != nil {
//			panic(err)
//		}
//	}
//	self.f,err = os.OpenFile(
//		filepath.Join(
//			self.path,
//			fmt.Sprintf("%d_%d_%d",
//				self.le.par.tag,
//				int(self.tp*self.k),
//				int(self.sl*self.k),
//			),
//		),
//		os.O_APPEND|os.O_CREATE|os.O_RDWR|os.O_SYNC,
//		0700,
//	)
//	if err != nil {
//		panic(err)
//	}
//	fmt.Println(self.f.Name())
//
//}
//func (self *order) save(e element){
//
//	self.f.WriteString(fmt.Sprintf("%d %d %d\n",int(self.k*e.Middle()),int(self.k*e.Diff()),e.DateTime()))
//
//}
//func (self *order) load(){
//	for _,li := range self.le.par.list{
//		li.Read(func(_e interface{}){
//			__e := _e.(element)
//			self.f.WriteString(fmt.Sprintf("%d %d %d\n",int(self.k*__e.Middle()),int(self.k*__e.Diff()),__e.DateTime()))
//		})
//	}
//	for _,li := range self.le.list{
//		li.Read(func(_e interface{}){
//			__e := _e.(element)
//			self.f.WriteString(fmt.Sprintf("%d %d %d\n",int(self.k*__e.Middle()),int(self.k*__e.Diff()),__e.DateTime()))
//		})
//	}
//	self.f.WriteString("end\n")
//
//}
//func (self *order) check(e element) (b bool) {
//
//	self.save(e)
//	if self.tp>self.sl {
//		if (e.Middle() > self.tp) || (e.Middle() < self.sl) {
//			self.close(e)
//			return true
//		}
//	}else{
//		if (e.Middle() < self.tp) || (e.Middle() > self.sl) {
//			self.close(e)
//			return true
//		}
//	}
//	return false
//
//}
