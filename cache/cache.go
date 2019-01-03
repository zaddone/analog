package cache
import(
	"github.com/zaddone/operate/oanda"
	"github.com/zaddone/analog/config"
	"github.com/zaddone/analog/snap"
	"time"
	"fmt"
	"sync"
	//"math"
)
type Cache struct {

	Ins *oanda.Instrument
	part *level

	priceChan chan config.Element
	stop chan bool

	LastE config.Element
	//InsCaches sync.Map

	Cshow [2]float64
	samples map[string]*snap.Sample

	setPool *snap.SetPool

}

func NewCache(ins *oanda.Instrument) (c *Cache) {
	c = &Cache{
		//InsCaches:insC,
		Ins:ins,
		priceChan:make(chan config.Element,5000),
		samples:make(map[string]*snap.Sample),
		setPool:snap.NewSetPool(ins.Name),
	}
	c.part = NewLevel(0,c,nil)
	//go ReadCandles(c.Ins.Name,5,func(can *Candles){
	//	c.addToChan(can)
	//})
	return c
}
func (self *Cache) Close(){
	close(self.stop)
	close(self.priceChan)
	self.setPool.Close()
}

func (self *Cache) findDurationSame (dur int64) (l *level,min int64) {

	var v int64
	self.part.readUp(func(_l *level){
		v = func(_v int64 ) int64{
			if _v<0 {
				return -_v
			}
			return _v
		}(dur - _l.duration())
		if  (min == 0) ||
			(v < min) {
			min = v
			l = _l
		}
	})
	return

}
func (self *Cache) Show(){
	e := self.GetLastElement()
	if e != nil {
		fmt.Println(time.Unix(e.DateTime(),0), self.Ins.Name,self.Cshow,self.Cshow[0]/self.Cshow[1])
	}else{
		fmt.Println(self.Ins.Name,self.Cshow,self.Cshow[0]/self.Cshow[1])
	}
}

func (self *Cache) Follow(t int64,w *sync.WaitGroup){

	if self.LastE != nil {
		if self.LastE.DateTime()<= t {
			self.AddPrice(self.LastE)
			self.LastE = nil
		}else{
			w.Done()
			return
		}
	}
	for{
		e :=<-self.priceChan
		if e.DateTime()<= t {
			self.AddPrice(e)
		}else{
			self.LastE = e
			w.Done()
			return
		}
	}

}

func (self *Cache) Run(hand func(t int64)){
	go ReadCandles(self.Ins.Name,5,func(can *Candles) bool{
		select{
		case <-self.stop:
			return false
		default:
			self.addToChan(can)
			return true
		}
	})
	for{
		e :=<-self.priceChan
		//fmt.Println(time.Unix(e.DateTime(),0))
		self.AddPrice(e)
		hand(e.DateTime())
	}
}
func (self *Cache) addToChan(e config.Element) {

	xin := self.Ins.Integer()
	self.priceChan<-&eNode{
		middle:e.Middle()*xin,
		diff:e.Diff()*xin,
		dateTime:e.DateTime(),
		duration:e.Duration(),
	}
}

func (self *Cache) GetLastElement() config.Element {

	le := len(self.part.list)
	if le == 0 {
		return nil
	}
	return self.part.list[le-1]

}

func (self *Cache) AddPrice(p config.Element) {
	var diff float64
	for k,sa := range self.samples {
		diff = sa.Check(p)
		if diff != 0 {
			delete(self.samples,k)
			self.setPool.Add(sa)
		}
	}
	if e := self.GetLastElement(); (e!= nil) && ((p.DateTime() - e.DateTime()) >300) {
		self.part = NewLevel(0,self,nil)
	}
	self.part.add(p,self.Ins)
}
