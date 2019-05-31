package cache
import(
	"github.com/zaddone/operate/oanda"
	"github.com/zaddone/analog/config"
	//cluster "github.com/zaddone/analog/telecar"
	cluster "github.com/zaddone/analog/pool"
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
}


//type sample interface{
//	Duration() int64
//	SetCaMapF(int,[]byte)
//	SetCaMap(int,[]byte)
//	GetCaMap(int,func([]byte))
//	GetLastElement() config.Element
//
//	SetPar(interface{})
//	GetPar() interface{}
//
//	SetChild(interface{})
//
//	GetLong() bool
//	SetLong(bool)
//	GetTag() byte
//}
//type pool_ interface{
//	Add([]sample)
//	ShowPoolNum() []float64
//}

type CacheInterface interface {

	GetLastElement() config.Element
	FindSample(*cluster.Sample)(*Level,*cluster.Sample)
	Pool() *cluster.Pools
	InsName() string
	GetI() int
	SetCShow(i int,n float64)

}

type Cache struct {

	ins *oanda.Instrument
	part *Level
	eleChan chan config.Element
	pool *cluster.Pools
	//lastDateTime int64
	stop chan bool
	Cl CacheList
	Cshow [11]float64
	//LogDB *bolt.DB
	sync.RWMutex
	I int
	m sync.Mutex
	last config.Element
	Order  *Order

	//addSample []*cluster.Sample

}
func (self *Cache) PostOrder(diff float64){
	if self.Order == nil {
		self.Order = NewOrder(self)
	}
}

func (self *Cache) GetCacheLen() int {
	return self.Cl.Len()
}

func (self *Cache) SetI(i int) {
	self.I = i
}

func (self *Cache) GetI () int {
	return self.I
}

func (self *Cache) Ins() *oanda.Instrument {
	return self.ins
}

func (self *Cache) Add(e config.Element){
	self.eleChan <- e
}

func NewCache(ins *oanda.Instrument) (c *Cache) {
	c = &Cache {
		ins:ins,
		eleChan:make(chan config.Element,10),
		stop:make(chan bool),
	}
	c.Order = NewOrder(c)
	c.part = NewLevel(0,c,nil)
	return c
}

func (self *Cache) InsName() string {
	return self.ins.Name
}

func (self *Cache) SetPool(){
	self.pool = cluster.NewPools(self)
}

func (self *Cache) ReadLevel(h func(*Level)bool){

	self.RLock()
	defer self.RUnlock()
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
	if v == 1 {
		//v =1
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
			//self.ClearOrderAll()
			//fmt.Println(time.Unix(e.DateTime()/v,0),"new")
			self.part.ClearLevel()

			self.part = NewLevel(0,self,nil)
		}

		self.last = p
		//self.Add(p)

		if (da - begin) > 3600*24*7 {
			self.SaveTestLog(da)
			begin = da
		}
		self.Lock()
		if e := self.getLastElement();(e!= nil) && ((da - e.DateTime()/v) >100) {
			self.part = NewLevel(0,self,nil)
		}

		self.part.add(p)
		self.Unlock()

		//self.eleChan <- e
	})
	if self.GetLastElement() == nil{
		fmt.Println(self.ins.Name,"over")
	}else{
		fmt.Println(self.ins.Name,"over",time.Unix(self.getLastElement().DateTime()/v,0))
	}
}
func (self *Cache) Pool() *cluster.Pools {
	return self.pool
}

func (self *Cache) syncAddPrice(){
	return
	var begin,da,v int64

	v = config.Conf.DateUnixV
	if v == 0 {
		v =1
	}
	//var last int64
	p := <-self.eleChan
	begin = p.DateTime()/v

	self.Lock()
	self.part.add(p)
	self.Unlock()
	for{
		select{
		case p = <-self.eleChan:
		//p := <-self.eleChan
			da = p.DateTime()/v
			if (da - begin) > 3600*24*7 {
				self.SaveTestLog(da)
				begin = da
			}
			self.Lock()
			if e := self.getLastElement();(e!= nil) && ((da - e.DateTime()/v) >100) {
				self.part = NewLevel(0,self,nil)
			}
			//self.SetCShowF(2,1)
			self.part.add(p)
			self.Unlock()
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

func (self *Cache) SetCShow(i int,n float64) {

	self.m.Lock()
	self.Cshow[i] += n
	self.m.Unlock()
}

func (self *Cache) SetCShowInt(i int,n int) {
	self.m.Lock()
	self.Cshow[i] += float64(n)
	self.m.Unlock()
}

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

	if (minSa.GetTag() &^ 2) != (se.GetTag() &^2) {
		return nil, nil
	}

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

func (self *Cache) CheckOrder(l *Level, ea *cluster.Sample, sumdif float64){

	if (l.par.par == nil) ||
	(self.pool == nil) ||
	(self.Cl == nil) {
		return
	}
	//self.pool.Add(ea)
	//ea.SetSnap()
	//l.sample = ea
	//return
	l.par.addSample = append(l.par.addSample,ea)

	if (l.sample == nil) {
		ea.SetCaMapF(0,nil)
		//self.pool.Add(ea)
		l.sample = ea
		return
	}
	l.sample.SetLong(math.Abs(ea.GetLastElement().Diff()) > math.Abs(l.sample.GetLastElement().Diff()))

	l.sample.SetChild(ea)
	ea.SetPar(l.sample)
	////}
	//if l.sample.Check() {
	//	if l.sample.Long {
	//		self.SetCShow(4+int(l.sample.GetTag()&^2) *2,1)
	//		self.SetCShow(int(l.sample.GetTag()&^2) *2,1)
	//	}else{
	//		self.SetCShow(1+int(l.sample.GetTag()&^2) *2,1)
	//	}
	//}

	//ea.SetCheckBak(true)
	//if !l.sample.GetLong() {
		ea.SetCheck(true)
		ea.SetCheckBak(true)
	//}
	ea.SetBegin(self.getLastElement())

	p := l.sample.GetPar()
	if (p!=nil) {
		//if (p.GetLong() == l.sample.GetLong()){
			//l.sample.SetPar(nil)
			//if !l.sample.GetLong(){
			//	ea.SetCheckBak(true)
			//	//ea.SetBegin(self.getLastElement())
			//}
		//}else{
		//	if ea.GetTag()&^2==0 {
		//		ea.SetCheckBak(false)
		//	}
		//}
		//p.SetChild(l.sample)
	}

	//l.sample.GetCaMap(2,func(b []byte){
	//	ea.SetCaMapF(0,b)
	//})

	l.sample.GetCaMap(1,func(b []byte){
		ea.SetCaMapF(0,b)
	})
	I_ := self.GetI()*2
	t := ^byte(3)
	tn := l.sample.GetTag()>>1
	self.Cl.Read(func(i int,_c interface{}){
		if i == self.GetI() {
			return
		}
		func(_i int,c CacheInterface){
			n := byte(0)
			_,_e := c.FindSample(l.sample)
			if _e == nil {
				return
			}
			vl := l.sample.GetCaMapVal(1,_i)
			if vl != 0 {
				return
			}
			vl = _e.GetCaMapVal(1,I_)
			if vl != 0 {
				return
			}

			if tn == (_e.GetTag()>>1) {
				n = 2
			}else{
				n = 1
			}
			_e.SetCaMapV(1,I_,n)
			l.sample.SetCaMapV(1,_i,n)
		}(i*2,_c.(CacheInterface))
	})
	var nb_0,nb_1 []byte
	l.sample.GetCaMap(1,func(b []byte){
		ea.SetCaMap(2,b)
		nb_1 = b
		//ea.SetCaMapF(0,b)
	})
	var j uint
	l.sample.GetCaMap(0,func(b []byte){
		nb_0 = b
		for i,m := range b{
			if (m == 255){
				continue
			}
			if (m ==0){
				nb_0[i] = 255
			}
			for j=0;j<8;j+=2 {
				t := ((m>>j) &^ t)
				if t ==3 {
					continue
				}
				if t ==0 {
					nb_0[i] |= byte(3)<<j
					continue
				}
				self.SetCShow(9,1)
			}
		}
	})
	l.sample.SetCaMap(3,nb_0)
	l.sample.SetCaMapF(3,nb_1)
	l.sample.GetCaMap(3,func(b []byte){
		for _,m := range b{
			if (m == 255) || (m ==0){
				continue
			}
			for j=0;j<8;j+=2 {
				t := ((m>>j) &^ t)
				if (t!=3) && (t!=0) {
					self.SetCShow(8,1)
				}
			}
		}
	})
	//var ls []*Level

	count:=0
	//var c_1,c_2 int

	ea.GetCaMap(0,func(b []byte){
		var j uint
		for i,m := range b {
			if m == 255 {
				continue
			}
			for j = 0;j<8;j+=2 {
				n := ((m>>j) &^ t)
				if n == 3 {
					continue
				}
				_,_e := self.Cl.ReadCa((i*8+int(j))/2).(CacheInterface).FindSample(ea)
				if _e == nil {
					b[i] |= 3<<j
					continue
				}
				if (_e.GetCaMapVal(2,I_)^3) != n {
					b[i] |= 3<<j
					continue
				}
				//if ea.GetCheckBak(){
				//	if _e.Check(){
				//		c_1++
				//	}else{
				//		c_2++
				//	}
				//	//ea.SetCheckBak(false)
				////	//self.SetCShow(5+int(ea.GetTag()&^2) *2,1)
				//}
				//ls = append(ls,_l)
				count++
			}
		}
	})
	//if c_2 > c_1 {
	//	ea.SetCheckBak(false)
	//}
	//if ea.Check(){
	self.pool.Check(ea)
	//	if count==0 {
	//		ea.SetCheck(false)
	//	//}else{
	//	//	self.SetCShow(5+int(ea.GetTag()&^2) *2,1)
	//	}
	//}
	l.sample = ea

}

func (self *Cache) SaveTestLog(from int64) {

	p := filepath.Join(config.Conf.ClusterPath,self.ins.Name)
	_,err := os.Stat(p)
	if err != nil{
		if err = os.MkdirAll(p,0700);err != nil {
			panic(err)
		}
	}

	str := fmt.Sprintf(
			"%s %s %.2f %.2f %.2f %.2f %.2f %.0f %f\r\n",
			time.Now().Format(config.TimeFormat),
			time.Unix(from,0).Format(config.TimeFormat),
			self.Cshow[0]/self.Cshow[1],
			self.Cshow[2]/self.Cshow[3],
			self.Cshow[4]/self.Cshow[5],
			self.Cshow[6]/self.Cshow[7],
			self.Cshow[8]/self.Cshow[9],
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
	//self.Cshow = [11]float64{0,0,0,0,0,0,0,0,0,0,0}

}
