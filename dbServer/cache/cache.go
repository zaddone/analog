package cache
import(
	"github.com/zaddone/operate/oanda"
	"github.com/zaddone/analog/config"
	cluster "github.com/zaddone/analog/telecar"
	//"encoding/binary"
	//"github.com/boltdb/bolt"
	//"io"
	"os"
	"fmt"
	"net"
	"time"
	"math"
	"sync"
	"github.com/zaddone/analog/dbServer/proto"
	"path/filepath"
	//"bytes"
)

type CacheList interface{
	ReadCa(int) interface{}
	Read(func(int,interface{}))
	Len() int
	//HandMap([]byte,func(interface{},byte))
	//HandMapBlack([]byte,func(interface{},byte)bool)
	//Show() interface{}
	//SetCShow(int,float64)
}
type CacheInterface interface {

	GetLastElement() config.Element
	FindSample(*cluster.Sample)(*Level,*cluster.Sample)
	//FindSampleTmp(*cluster.Sample)*cluster.Sample
	Pool() *cluster.Pool
	InsName() string
	GetI() int
	AddOrder(*OrderInfo)
	//ClearOrder(*OrderInfo)
	SetCShowF(i int,n float64)

}
type OrderInfo struct {
	c CacheInterface
	//c_ []CacheInterface
	sa *cluster.Sample
	e config.Element
	End bool
	Start bool

}

func NewOrderInfo(c CacheInterface,sa *cluster.Sample) (o *OrderInfo) {
	o = &OrderInfo{
		c:c,
		sa:sa,
	}
	o.e = c.GetLastElement()
	//l.Order = o
	c.AddOrder(o)
	return
}

func (self *OrderInfo) GetDiff() float64 {
	d := self.c.GetLastElement().Middle() - self.e.Middle()
	var dif float64 = 0
	//dif := (math.Abs(self.e.Diff()) + math.Abs(self.c.GetLastElement().Diff()))/2
	if self.sa.DisU() == (d>0) {
		return math.Abs(d)-dif
	}
	return -(math.Abs(d)) - dif
}

func (self *OrderInfo) Marge(New *OrderInfo) bool {

	if self.End {
		return true
	}
	if self.sa.DisU() != New.sa.DisU() {
		self.Clear()
		New.End = true
		return false
	}
	New.End = true
	return false

}
//func (self *OrderInfo) Marge(old *OrderInfo){
//
//	if old.End {
//		return
//	}
//	if self.sa.DisU() != old.sa.DisU() {
//		old.Clear()
//		self.End = true
//		return
//	}
//	self.e = old.e
//	//fmt.Println(self.c.InsName(),self.e.DateTime() - old.e.DateTime())
//	old.End = true
//
//}

func (self *OrderInfo) Clear(){
	if !self.End{
		d := self.GetDiff()
		self.c.SetCShowF(7,d)
		if d >0{
			self.c.SetCShowF(2,1)
		}
		self.c.SetCShowF(3,1)
		//self.c.ClearOrder(self)
		self.End = true
	}
}


type Cache struct {

	ins *oanda.Instrument
	part *Level
	eleChan chan config.Element
	pool *cluster.Pool
	//lastDateTime int64
	stop chan bool
	Cl CacheList
	Cshow [8]float64
	//LogDB *bolt.DB
	sync.Mutex
	I int
	m sync.Mutex
	last config.Element
	Order  *OrderInfo

}

func (self *Cache) ClearOrderAll(){
	//self.Lock()
	self.ReadLevel(func(l *Level)bool{
		l.ClearOrder()
		return true
	})
	//self.Unlock()
}

func (self *Cache) AddOrder(o *OrderInfo) {
	if self.Order == nil {
		self.Order = o
		return
	}
	if self.Order.Marge(o) {
		self.Order = o
	}
	//o.Marge(self.Order)
	//self.Order = o
}

func (self *Cache) SetI(i int) {
	self.I = i
}

func (self *Cache) GetI () int {
	return self.I
}


//func (self *Cache) HandMapBlack(m []byte,hand func(interface{},byte)bool){
//	self.Cl.HandMapBlack(m,hand)
//}
//func (self *Cache) HandMap(m []byte,hand func(interface{},byte)){
//	self.Cl.HandMap(m,hand)
//}

func (self *Cache) Ins() *oanda.Instrument {
	return self.ins
}

func (self *Cache) Add(e config.Element){
	self.eleChan <- e
}

func NewCache(ins *oanda.Instrument) (c *Cache) {
	c = &Cache {
		ins:ins,
		eleChan:make(chan config.Element,1),
		stop:make(chan bool),
		//Order:new(sync.Map),
	}
	c.part = NewLevel(0,c,nil)
	return c
}

func (self *Cache) InsName() string {
	return self.ins.Name
}

func (self *Cache) SetPool(){
	self.pool = cluster.NewPool(self.InsName(),self)
}


func (self *Cache) ReadLevel(h func(*Level)bool){

	self.Lock()
	defer self.Unlock()
	l := self.part
	for{
		if l == nil {
			return
		}
		if !h(l){
			return
		}
		l = l.par
	}

}

//func (self *Cache) CheckValBak(b int64,tag byte) (max config.Element,min config.Element){
	//self.ReadLevel(func(l *level)bool{
	//	l.sample
	//})
//}
//func (self *Cache) CheckVal(b int64,tag byte) (max config.Element,min config.Element){
//
//	//var li config.Element
//	self.Lock()
//	defer self.Unlock()
//	var minL *level = nil
//	var I int
//	self.ReadLevel(func(l *level)bool{
//		for i:= len(l.list)-1;i>=0;i-- {
//			if l.list[i].DateTime()<=b{
//				minL = l
//				max = l.list[i]
//				min = max
//				I = i
//				return false
//			}
//		}
//		return true
//	})
//	if minL == nil {
//		return nil,nil
//	}
//	for{
//		for _,l := range minL.list[I:]{
//			l.Read(func(e config.Element)bool{
//				d := e.Middle()
//				if (d > max.Middle()) {
//					max = e
//				}
//				if (d < min.Middle()) {
//					min = e
//				}
//				return true
//			})
//		}
//		if minL.child==nil {
//			return
//		}
//		minL = minL.child
//		I = 0
//	}
//	return
//
//}

func (self *Cache) SyncRun(cl CacheList){

	self.Cl = cl
	self.SetPool()
	go self.syncAddPrice()
	begin := self.getLastTime()
	if begin == 0 {
		begin = config.GetFromTime()
	}
	fmt.Println(self.ins.Name,time.Unix(begin,0))
	self.read(fmt.Sprintf("%s_%s",config.Conf.Local,self.ins.Name),begin,time.Now().Unix(),func(e config.Element){
		self.eleChan <- e
	})
	fmt.Println(self.ins.Name,"over")
	close(self.stop)

}

func (self *Cache) SyncInit(cl CacheList){
	self.Cl = cl
	self.SetPool()
	go self.syncAddPrice()

}

func (self *Cache) Init(cl CacheList){
	self.Cl = cl
	self.SetPool()
}
func (self *Cache) ReadAll(h func(int64)){
	begin := self.getLastTime()
	if begin == 0 {
		begin = config.GetFromTime()
	}
	var end int64
	//fmt.Println(self.ins.Name,time.Unix(begin,0))
	v := config.Conf.DateUnixV
	if v == 0 {
		v =1
		end = time.Now().Unix()
	}else{
		end = time.Now().UnixNano()
		begin = time.Unix(begin,0).UnixNano()
	}
	self.read(fmt.Sprintf("%s_%s",config.Conf.Local,self.ins.Name),begin,end,func(p config.Element){
		da := p.DateTime()
		h(da)
		da /=v
		if e := self.getLastElement();(e!= nil) && ((da - e.DateTime()/v) >100) {
			self.ClearOrderAll()
			self.part = NewLevel(0,self,nil)
		}
		self.last = p
		self.Lock()
		self.Add(p)
		//self.part.add(p)
		self.Unlock()
		//self.eleChan <- e
	})
	fmt.Println(self.ins.Name,"over")
}
func (self *Cache) Pool() *cluster.Pool {
	return self.pool
}

func (self *Cache) syncAddPrice(){
	var begin,da,v int64

	v = config.Conf.DateUnixV
	if v == 0 {
		v =1
	}
	//var last int64
	p := <-self.eleChan
	begin = p.DateTime()/v
	self.part.add(p)
	for{
		select{
		case p = <-self.eleChan:
		//p := <-self.eleChan
			da = p.DateTime()/v
			if (da - begin) > 604800 {
				self.SaveTestLog(da)
				begin = da
			}
			if e := self.getLastElement();(e!= nil) && ((da - e.DateTime()/v) >100) {
				self.part = NewLevel(0,self,nil)
			}
			self.part.add(p)
		case <-self.stop:
			return

		}
	}
}


func (self *Cache) GetLastElement() config.Element {
	return self.getLastElement()
}
func (self *Cache) getLastElement() config.Element {
	return self.last
	//if self.part == nil {
	//	return nil
	//}
	//le := len(self.part.list)
	//if le == 0 {
	//	return nil
	//}
	//return self.part.list[le-1]
}

func (self *Cache) getLastTime() int64 {

	//if self.pool != nil {
	//	return self.pool.GetLastTime()
	//}
	return 0

}

func (self *Cache) read(local string,begin,end int64,hand func(e config.Element)){
	p := &proto.Proto{Ins:self.ins.Name,B:begin,E:end}
	lAddr, err := net.ResolveUnixAddr("unixgram", p.GetTmpPath())
	if err != nil {
		panic(err)
	}
	rAddr, err := net.ResolveUnixAddr("unixgram", local)
	if err != nil {
		panic(err)
	}
	c,err := net.DialUnix("unixgram",lAddr,rAddr)
	if err != nil {
		//fmt.Println(lAddr,rAddr)
		panic(err)
	}
	//c.SetReadBuffer(1048576)
	//fmt.Println(c.LocalAddr(),c.RemoteAddr())
	//defer c.Close()
	_,err = c.Write(p.ToByte())
	if err != nil {
		panic(err)
	}
	var db [16]byte
	var n int
	G:
	for{
		n,err = c.Read(db[:])
		if err != nil {
			panic(err)
		}
		//if n == 0 {
		//	break
		//}
		switch n {
		case 0:
			break G
		case 12 :
			hand(proto.NewCandlesMin(db[:4],db[4:n]))
		case 16:
			hand(proto.NewEasyPriceDB(db[:n]))
		default:
			panic(n)
		}

	}
	//fmt.Println(lAddr.String())
	c.Close()
	os.Remove(p.GetTmpPath())
}

func (self *Cache) SetCShowF(i int,n float64) {
	//if self.Cl != nil {
	//	self.Cl.SetCShow(i,n)
	//	return
	//}
	self.m.Lock()
	self.Cshow[i] += n
	self.m.Unlock()
}

func (self *Cache) SetCShow(i int,n int) {
	self.m.Lock()
	self.Cshow[i] += float64(n)
	self.m.Unlock()
}

//func (self *Cache) FindSampleTmp(se *cluster.Sample) *cluster.Sample {
//
//	dur := se.Duration()
//	var diff,minDiff int64
//	var minl *level
//	self.ReadLevel(func(l *level)bool{
//		if l.sample == nil {
//			return true
//		}
//		//if l.AbsMax !=0 {
//		//	return true
//		//}
//		//_e := cluster.NewSample(append(l.par.list,NewbNode(l.list...)),self.GetSumLen())
//		el:= l.list[len(l.list)-1]
//		d := el.DateTime()+el.Duration()-l.par.list[0].DateTime()
//		diff = d - dur
//		if diff<0 {
//			diff = -diff
//		}
//		if (diff < minDiff) || (minDiff==0){
//			minDiff = diff
//			minl = l
//			//minSa = l.sample
//		}
//		return true
//	})
//	if minl == nil {
//		return nil
//	}
//	no := NewbNode(minl.list...)
//	//if math.Abs(self.GetLastElement().Middle() - minl.b.Middle()) < math.Abs(no.Diff()) {
//	//	return nil
//	//}
//	e := cluster.NewSample(append(minl.par.list,no),self.GetSumLen())
//	if (e.GetTag() &^ 2) != (se.GetTag() &^2) {
//		return nil
//	}
//
//	e.SetTestMap(minl.sample.GetCaMap()[0])
//	return e
//
//}
//
func (self *Cache) FindSample(se *cluster.Sample) (minL *Level,minSa *cluster.Sample) {

	dur := se.Duration()
	var diff,minDiff int64
	//var minL *Level
	self.ReadLevel(func(l *Level)bool{
		if l.sample == nil {
			return true
		}
		diff = l.sample.Duration() - dur
		if diff <0 {
			diff = -diff
		}
		if (diff < minDiff) || (minDiff==0){
			minDiff = diff
			minSa = l.sample
			minL = l
		}
		return true
	})
	if minSa == nil {
		return
	}
	//if minSa.Long != se.Long {
	//	return nil
	//}
	if (minSa.GetTag() &^ 2) != (se.GetTag() &^2) {
		return nil, nil
	}
	//d := (self.GetLastElement().Middle() - minL.b.Middle())
	//if (d >0) != minSa.DisU() {
	//	return nil
	//}

	//if math.Abs(d) < (math.Abs(self.GetLastElement().Diff()) + math.Abs(minL.b.Diff()))/2 {
	//	return nil
	//}
	return

}

func (self *Cache) GetSumLen() (n int) {
	if self.Cl == nil {
		return 0
	}
	l := self.Cl.Len()
	n = l/4
	if n%4 >0 {
		n++
	}
	return
}

func (self *Cache) CheckOrder(l *Level,node config.Element,sumdif float64){

	if (l.par.par == nil) ||
	(self.pool == nil) ||
	(self.Cl == nil) {
		return
	}
	ea := cluster.NewSample(append(l.par.list, node),self.GetSumLen())
	self.pool.Add(ea)
	if (l.sample == nil) {
		//ea.SetCaMapF(2,nil)
		l.sample = ea
		return
	}
	pli := l.par.list[len(l.par.list)-1]
	if (l.sample.GetLastElement() == pli ){
		l.sample.Long = math.Abs(node.Diff())>math.Abs(pli.Diff())
	}
	l.sample.GetCaMap(2,func(b []byte){
		ea.SetCaMapF(0,b)
	})
	l.sample.GetCaMap(1,func(b []byte){
		ea.SetCaMapF(0,b)
	})

	//ea.GetCaMap(0,func(b []byte){
	//	for i,m := range b {
	//		if m ==0 || m == 255 {
	//			continue
	//		}
	//	}
	//})

	I_ := self.GetI()*2
	tn := l.sample.GetTag()>>1

	self.Cl.Read(func(i int,_c interface{}){
		if i == self.GetI() {
			return
		}
		func(_i int,c CacheInterface){
			_,_e := c.FindSample(l.sample)
			if _e == nil {
				return
			}
			n := byte(1)
			if tn == (_e.GetTag()>>1) {
				n = 2
			}
			//_l.AddOtherLevel(self.GetI(),NewLevelInfo(l,n))
			//l.AddOrderLevel(_i,NewLevelInfo(_l,n))
			_e.SetCaMapV(1,I_,n)
			l.sample.SetCaMapV(1,_i,n)

		}(i*2,_c.(CacheInterface))
	})

	//ea.SetCaMapF(0,nb)
	var nb []byte
	l.sample.GetCaMap(1,func(b []byte){
		ea.SetCaMap(2,b)
		//ea.SetCaMapF(0,b)
		nb = b
	})
	func(_nb []byte,e_ *cluster.Sample){
		//e_.Wait()
		var j uint
		t := ^byte(3)
		e_.GetCaMap(0,func(b []byte){
			//fmt.Println(b)
			for i,m := range b{
				if (m == 255) || (m ==0){
					continue
				}
				for j=0;j<8;j+=2 {
					if ((m>>j) &^ t) != 3 {
						self.SetCShowF(1,1)
					}
				}
				m |= ^_nb[i]
				for j=0;j<8;j+=2 {
					if ((m>>j) &^ t) != 3 {
						self.SetCShowF(0,1)
					}
				}
			}
		})
	}(nb,l.sample)

	l.sample = ea

}

//func (self *Cache) CheckOrderBak(l *level,node config.Element,sumdif float64){
//
//	if (l.par.par == nil) ||
//	(self.pool == nil) ||
//	(self.Cl == nil) {
//		return
//	}
//	//l.ClearOrder()
//	ea := cluster.NewSample(append(l.par.list, node),self.GetSumLen())
//	//self.pool.Add(ea)
//	//self.SetCShowF(7,1)
//
//	self.SetCShowF(7,1)
//	if (l.sample == nil) {
//		ea.SetTestMap(ea.GetCaMap()[1])
//		l.sample = ea
//		return
//	}
//
//	//d := self.getLastElement().Middle() - l.b.Middle()
//	//if (d>0) == l.sample.DisU() {
//	//	l.sample.Long = true
//	//	self.SetCShowF(6,1)
//	//}
//
//	pli := l.par.list[len(l.par.list)-1]
//	if (l.sample.GetLastElement() == pli ){
//		l.sample.Long = math.Abs(node.Diff())>math.Abs(pli.Diff())
//		if l.sample.Long {
//			self.SetCShowF(6,1)
//			if l.sample.Check(){
//				self.SetCShowF(int(l.sample.GetTag()&^2)*2,1)
//			}
//		}else{
//			ea.SetCheck(true)
//			self.SetCShowF(int(ea.GetTag()&^2)*2+1,1)
//		}
//	}
//	//go func(_e *cluster.Sample){
//	//	_e.Wait()
//	//	if _e.Check() && _e.Long{
//	//		//self.SetCShowF(int(_e.GetTag()>>1)*2,1)
//	//		self.SetCShowF(0,1)
//	//	}
//	//}(l.sample)
//
//	l.sample = ea
//	return
//
//	//if self.Pool().CheckSample(ea) {
//	//if ((ea.GetTag() &^ 2) == 1) || self.Pool().CheckSample(ea) {
//	//if ((ea.GetTag() &^ 2) == 1) {
//	//	self.SetCacheMapSync(l.sample,nil)
//	//	ea.SetTestMap(l.sample.GetCaMap()[1])
//	//	l.sample = ea
//	//	return
//	//}
//	//I_1 := self.GetI()*2/8
//	//I_2 := uint(self.GetI()*2%8)
//	t := ^ byte(3)
//	vote := make([]int,self.Cl.Len())
//	voteDB := make([]*cluster.Sample,0,self.Cl.Len())
//	self.SetCacheMapSync(l.sample,func(_i int,_e *cluster.Sample){
//		//if ((_e.GetCaMap()[2][I_1]>>I_2) &^ t)
//		voteDB=append(voteDB,_e)
//	})
//	var j uint
//	cm := l.sample.GetCaMap()
//	cms := make([]byte,len(cm[0]))
//	for i,m := range cm[1] {
//		cms[i]= ((^m) | cm[2][i])
//	}
//	var _m byte
//	for _,_e := range voteDB {
//		for i,m := range _e.GetCaMap()[2]{
//			_m = cms[i] | m
//			if _m == 255 {
//				continue
//			}
//			for j=0;j<8;j+=2 {
//				if (_m>>j &^ t) != 3 {
//					vote[(i*4 + int(j/2))]++
//				}
//			}
//		}
//	}
//	var Maxv int
//	var I []int
//	for i,v := range vote {
//		if v> Maxv {
//			Maxv = v
//			I = []int{i}
//		}else if (v!=0) && (v == Maxv) {
//			I = append(I,i)
//		}
//	}
//	//if len(I) < 2 {
//	//	return
//	//}
//	//var or []*OrderInfo
//	for _,_i := range I {
//		c := self.Cl.ReadCa(_i).(CacheInterface)
//		_e := c.FindSampleTmp(ea)
//		if _e != nil {
//			if c.Pool().CheckSampleP(_e,self.GetI()*2) {
//				l.AddOrder(NewOrderInfo(c,_e))
//				//or = append(or,NewOrderInfo(c,_e))
//				//l.AddOrder(NewOrderInfo(c,_e))
//			}
//		}
//	}
//	//if len(or)<2 {
//	//	return
//	//}
//	//for _,o:= range or {
//	//	l.AddOrder(o)
//	//	o.c.AddOrder(o)
//	//}
//
//	ea.SetTestMap(l.sample.GetCaMap()[1])
//	l.sample = ea
//
//}


//
//func (self *Cache) CheckCaMap(l *level,se *cluster.Sample){
//
//
//	//n := int(se.GetTag()>>1)
//	//self.mu[n].RLock()
//	//t := self.pool.FindMinSet(se,n)
//	//self.mu[n].RUnlock()
//	//if t == nil {
//	//	return
//	//}
//	//if !t.s.checkSample(se){
//	//	return
//	//}
//	//t.s.SetTMap(se)
//
//
//	//cam := se.GetCaMap()[2]
//	T := ^byte(3)
//	I := self.GetI()*2
//	I_1 := I/8
//	I_2 := uint(I%8)
//	//chanDB := make(chan int,10)
//	var w sync.WaitGroup
//	//w_.Add(1)
//	//go func(){
//	//	for i := range chanDB {
//	//		//I := c.GetI()*2
//	//		//se.m.
//	//		cam[i/8] |= 3<<uint(i%8)
//	//	}
//	//	w_.Done()
//	//}()
//	self.HandMap(se.GetCaMap()[2],func(_c interface{},t byte){
//		//c:= _c.(CacheInterface)
//		w.Add(1)
//		go func(c CacheInterface,_t byte){
//			_e := c.FindSampleTmp(se)
//			if func()bool{
//				if _e == nil {
//					return false
//				}
//				if !c.Pool().CheckSample(_e){
//					return false
//				}
//				return ((_e.GetCaMap()[2][I_1] >> I_2) &^ T) == _t
//			}() {
//				//chanDB <- c.GetI()*2
//				l.AddOrder(NewOrderInfo(c,_e))
//			}
//			w.Done()
//			//return
//		}(_c.(CacheInterface),t)
//	})
//	w.Wait()
//	//close(chanDB)
//	//w_.Wait()
//
//}
//
//func (self *Cache) SetDifShow(src []byte,dis []byte)(c_1,c_2 int){
//
//	T := ^byte(3)
//	var s byte
//	var c int
//	var j uint
//
//	for i,m := range dis{
//		if m == 255 {
//			continue
//		}
//		c = 0
//		for j=0;j<8;j+=2{
//			s = ((m>>j) &^ T)
//			if s == 3 {
//				continue
//			}
//			c ++
//		}
//		c_1+=c
//		//fmt.Println(m,src[i])
//		_m := m | (^src[i])
//		//fmt.Println(m,_m)
//		if _m == m {
//			c_2 += c
//			continue
//		}
//		c = 0
//		for j=0;j<8;j+=2{
//			s = ((_m>>j) &^ T)
//			if s == 3 {
//				continue
//			}
//			c ++
//		}
//		c_2 += c
//	}
//	return
//
//}

//func (self *Cache) SetCacheMapSync(se *cluster.Sample,h func(int,*cluster.Sample)) {
//	if self.Cl == nil {
//		return
//	}
//	//I_ := self.GetI()*2
//
//	//I_1 := I_/8
//	//I_2 := uint(I_%8)
//	//t := ^ byte(3)
//
//	//tn := se.GetTag()>>1
//	//var ses []*cluster.Sample
//	self.Cl.Read(func(i int,_c interface{}){
//		if i == self.GetI() {
//			return
//		}
//		func(_i int,c CacheInterface){
//			_e := c.FindSample(se)
//			if _e == nil {
//				//w.Done()
//				return
//			}
//			//isS := true
//			//_e.GetCaMap(2,func(b []byte){
//			//	//fmt.Println(b)
//			//	k := ((b[I_1] >> I_2) &^ t)
//			//	if (k==0) || (k==3) {
//			//		isS = false
//			//	}
//			//})
//			//if !isS{
//			//	return
//			//}
//			//if h != nil {
//			h(_i,_e)
//			//}
//			//ses = append(ses,_e)
//
//			//n := byte(1)
//			//if tn == (_e.GetTag()>>1) {
//			//	n = 2
//			//}
//
//			//se.SetCaMapV(1,_i,n)
//			//_e.SetCaMapV(1,I_,n)
//
//			//se.GetCaMap()[1][_i/8] |= n<< uint(_i%8)
//			//_e.GetCaMap()[1][I_1] |= n<< I_2
//			//chanDB <- &tmpDB{i:_i,e:_e,n:n}
//			//w.Done()
//			//I := c.GetI()*2
//			//se.GetCaMap()[1][I/8] |= n<< uint(I%8)
//			//_e.GetCaMap()[0][I_1] |= n<< I_2
//
//		}(i*2,_c.(CacheInterface))
//	})
//
//}
//func (self *Cache) SetCacheMapSyncB(se *cluster.Sample) {
//	if self.Cl == nil {
//		return
//	}
//
//	I_ := self.GetI()*2
//	I_1 := I_/8
//	I_2 := uint(I_%8)
//
//	tn := se.GetTag()>>1
//	//var ses []*cluster.Sample
//	self.Cl.Read(func(i int,_c interface{}){
//		if i == self.GetI() {
//			return
//		}
//		func(_i int,c CacheInterface){
//			_e := c.FindSample(se)
//			if _e == nil {
//				//w.Done()
//				return
//			}
//			//ses = append(ses,_e)
//			n := byte(1)
//			if tn == (_e.GetTag()>>1) {
//				n = 2
//			}
//
//			se.GetCaMap()[1][_i/8] |= n<< uint(_i%8)
//			_e.GetCaMap()[0][I_1] |= n<< I_2
//			//chanDB <- &tmpDB{i:_i,e:_e,n:n}
//			//w.Done()
//			//I := c.GetI()*2
//			//se.GetCaMap()[1][I/8] |= n<< uint(I%8)
//			//_e.GetCaMap()[0][I_1] |= n<< I_2
//
//		}(i*2,_c.(CacheInterface))
//	})
//}
//func (self *Cache) SetCacheMapSyncBak(se *cluster.Sample) {
//
//	if self.Cl == nil {
//		return
//	}
//
//	I_ := self.GetI()*2
//	I_1 := I_/8
//	I_2 := uint(I_%8)
//
//	tn := se.GetTag()>>1
//	type tmpDB struct {
//		e *cluster.Sample
//		i int
//		n byte
//	}
//	chanDB := make(chan *tmpDB,10)
//	var w,w_ sync.WaitGroup
//	w_.Add(1)
//	go func (){
//		for d := range chanDB {
//			//I := d.i*2
//			se.GetCaMap()[1][d.i/8] |= d.n<< uint(d.i%8)
//			d.e.GetCaMap()[0][I_1] |= d.n<< I_2
//		}
//		w_.Done()
//	}()
//	w.Add(self.Cl.Len())
//	self.Cl.Read(func(i int,_c interface{}){
//		go func(_i int,c CacheInterface){
//			_e := c.FindSample(se)
//			if _e == nil {
//				w.Done()
//				return
//			}
//			n := byte(1)
//			if tn == (_e.GetTag()>>1) {
//				n = 2
//			}
//			chanDB <- &tmpDB{i:_i,e:_e,n:n}
//			w.Done()
//			//I := c.GetI()*2
//			//se.GetCaMap()[1][I/8] |= n<< uint(I%8)
//			//_e.GetCaMap()[0][I_1] |= n<< I_2
//
//		}(i*2,_c.(CacheInterface))
//	})
//	w.Wait()
//	close(chanDB)
//	w_.Wait()
//
//}

func (self *Cache) TmpCheck(begin,end int64) (min,max config.Element){

	//l := NewLevel(0,nil,nil)
	//var min,max config.Element
	self.read(config.Conf.Local,begin,end,func(_e config.Element){
		if (max == nil) || (_e.Middle() > max.Middle()) {
			max = _e
		}
		if (min == nil) || (_e.Middle() < min.Middle())  {
			min = _e
		}
		//l.add(_e)
	})
	return

}
func (self *Cache) SaveTestLog(from int64){

	p := filepath.Join(config.Conf.ClusterPath,self.ins.Name)
	_,err := os.Stat(p)
	if err != nil{
		if err = os.MkdirAll(p,0700);err != nil {
			panic(err)
		}
	}

	str := fmt.Sprintf(
			"%s %s %.2f %.2f %.2f %.2f %.0f %d\r\n",
			time.Now().Format(config.TimeFormat),
			time.Unix(from,0).Format(config.TimeFormat),
			self.Cshow[0]/self.Cshow[1],
			self.Cshow[2]/self.Cshow[3],
			self.Cshow[4]/self.Cshow[5],
			self.Cshow[6]/self.Cshow[7],
			self.Cshow,
			self.pool.ShowPoolNum(),
		)
		//return
	f,err := os.OpenFile(
	filepath.Join(p,"log"),
	os.O_APPEND|os.O_CREATE|os.O_RDWR|os.O_SYNC,
	0700,)
	if err != nil {
		fmt.Println(self.ins.Name,str)
		return
		//panic(err)
	}
	f.WriteString(str)
	f.Close()
	//self.Cshow[7] = 0
	//self.Cshow[6] = 0
	//self.Cshow = [8]float64{self.Cshow[0],self.Cshow[1],0,0,0,0,self.Cshow[6],self.Cshow[7]}

}
