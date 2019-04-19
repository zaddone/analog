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
	Read(func(int,interface{}))
	Len() int
	HandMap([]byte,func(interface{},byte))
	HandMapBlack([]byte,func(interface{},byte)bool)
	Show() int
}
type CacheInterface interface {
	FindSample(*cluster.Sample)*cluster.Sample
	InsName() string
	GetI() int

}



type Cache struct {

	ins *oanda.Instrument
	part *level
	eleChan chan config.Element
	pool *cluster.Pool
	//lastDateTime int64
	stop chan bool
	Cl CacheList
	Cshow [8]float64
	//LogDB *bolt.DB
	sync.Mutex
	I int

}

func (self *Cache) SetI(i int) {
	self.I = i
}

func (self *Cache) GetI () int {
	return self.I
}


func (self *Cache) HandMapBlack(m []byte,hand func(interface{},byte)bool){
	self.Cl.HandMapBlack(m,hand)
}
func (self *Cache) HandMap(m []byte,hand func(interface{},byte)){
	self.Cl.HandMap(m,hand)
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
		eleChan:make(chan config.Element,1000),
		stop:make(chan bool),
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


func (self *Cache) ReadLevel(h func(*level)bool){

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
func (self *Cache) CheckVal(b int64,tag byte) (max config.Element,min config.Element){

	//var li config.Element
	self.Lock()
	defer self.Unlock()
	var minL *level = nil
	var I int
	self.ReadLevel(func(l *level)bool{
		for i:= len(l.list)-1;i>=0;i-- {
			if l.list[i].DateTime()<=b{
				minL = l
				max = l.list[i]
				min = max
				I = i
				return false
			}
		}
		return true
	})
	if minL == nil {
		return nil,nil
	}
	for{
		for _,l := range minL.list[I:]{
			l.Read(func(e config.Element)bool{
				d := e.Middle()
				if (d > max.Middle()) {
					max = e
				}
				if (d < min.Middle()) {
					min = e
				}
				return true
			})
		}
		if minL.child==nil {
			return
		}
		minL = minL.child
		I = 0
	}
	return

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
	//fmt.Println(self.ins.Name,time.Unix(begin,0))
	v := config.Conf.DateUnixV
	if v == 0 {
		v =1
	}
	self.read(fmt.Sprintf("%s_%s",config.Conf.Local,self.ins.Name),begin,time.Now().Unix(),func(p config.Element){
		da := p.DateTime()
		h(da)
		da /=v
		if e := self.getLastElement();(e!= nil) && ((da - e.DateTime()/v) >100) {
			self.part = NewLevel(0,self,nil)
		}
		self.Lock()
		self.part.add(p)
		self.Unlock()
		//self.eleChan <- e
	})
	fmt.Println(self.ins.Name,"over")
}

func (self *Cache) syncAddPrice(){
	var begin,da,v int64
	//var last int64
	p := <-self.eleChan
	if len(fmt.Sprintf("%d",p.DateTime())) == 19{
		v = 1000000000
	}else{
		v = 1
	}
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

func (self *Cache) getLastElement() config.Element {
	if self.part == nil {
		return nil
	}
	le := len(self.part.list)
	if le == 0 {
		return nil
	}
	return self.part.list[le-1]
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
	var db [12]byte
	var n int
	for{
		n,err = c.Read(db[:])
		if err != nil {
			panic(err)
		}
		if n == 0 {
			break
		}
		hand(proto.NewCandlesMin(db[:4],db[4:]))
	}
	//fmt.Println(lAddr.String())
	c.Close()
	os.Remove(p.GetTmpPath())
}

func (self *Cache) SetCShow(i int,n int) {
	self.Cshow[i] += float64(n)
}

func (self *Cache) FindSample(se *cluster.Sample) (minSa *cluster.Sample) {
	dur := se.Duration()
	var diff,minDiff int64
	self.ReadLevel(func(l *level)bool{
		if l.sample != nil {
			diff = l.sample.Duration() - dur
			if diff <0 {
				diff = -diff
			}
			if (diff < minDiff) || (minDiff==0){
				minDiff = diff
				minSa = l.sample
			}
		}
		return true
	})
	if minSa == nil {
		return
	}
	if (minSa.GetTag() &^ 2) != (se.GetTag() &^2) {
		return nil
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

func (self *Cache) CheckOrder(l *level,node config.Element,sumdif float64){

	if (l.par.par == nil) ||
	(self.pool == nil) ||
	(self.Cl == nil) {
		return
	}
	ea := cluster.NewSample(append(l.par.list, node),self.GetSumLen())
	self.pool.Add(ea)
	if (l.sample == nil) {
		ea.SetTestMap(ea.GetCaMap()[2])
		l.sample = ea
		return
	}

	pli := l.par.list[len(l.par.list)-1]
	if (l.sample.GetLastElement() == pli ){
		l.sample.Long = math.Abs(node.Diff())>math.Abs(pli.Diff())
	}
	self.SetCacheMapSync(l.sample)
	ea.SetTestMap(l.sample.GetCaMap()[1])
	go func(_e *cluster.Sample){
		_e.Wait()
		c1,c2 := self.SetDifShow(_e.GetCaMap()[1],_e.GetCaMap()[2])
		self.Cshow[0]+=float64(c2)
		self.Cshow[1]+=float64(c1)
	}(l.sample)
	l.sample = ea

}

func (self *Cache) SetDifShow(src []byte,dis []byte)(c_1,c_2 int){

	T := ^byte(3)
	var s byte
	var c int
	var j uint

	for i,m := range dis{
		if m == 255 {
			continue
		}
		c = 0
		for j=0;j<8;j+=2{
			s = ((m>>j) &^ T)
			if s == 3 {
				continue
			}
			c ++

		}
		c_1+=c
		_m := m | src[i]
		if _m == m {
			c_2 += c
			continue
		}
		c = 0
		for j=0;j<8;j+=2{
			s = ((_m>>j) &^ T)
			if s == 3 {
				continue
			}
			c ++
		}
		c_2 += c
	}
	return

}

func (self *Cache) SetCacheMapSync(se *cluster.Sample) {

	if self.Cl == nil {
		return
	}
	self.Cl.Read(func(i int,_c interface{}){
		go func(c CacheInterface){
			_e := c.FindSample(se)
			if _e == nil {
				return
			}
			n := byte(1)
			if (se.GetTag()>>1) == (_e.GetTag()>>1) {
				n = 2
			}
			I := c.GetI()*2
			se.GetCaMap()[1][I/8] |= n<< uint(I%8)

			I = self.GetI()*2
			_e.GetCaMap()[0][I/8] |= n<< uint(I%8)

		}(_c.(CacheInterface))
	})

}

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
