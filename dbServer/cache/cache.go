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
	Show() int
}
type CacheInterface interface {
	TmpCheck(int64,int64) (config.Element,config.Element)
	//CheckVal(int64,int64) (config.Element)
	//Ins() *oanda.Instrument
	//InsName() string
	//Add(config.Element)
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
	self.pool = cluster.NewPool(self.ins.Name,self)
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
func (self *Cache) CheckVal(b int64) (max config.Element,min config.Element){

	//var li config.Element
	var minL *level = nil
	var diff,_diff int64
	self.ReadLevel(func(l *level)bool{
		diff  = l.duration() - b
		if diff > 0 {
			if diff < _diff {
				minL = l
			}
			return false
		}
		minL = l
		_diff = -diff
		return true
	})
	if minL == nil {
		return nil,nil
	}
	max = minL.list[0]
	min = max
	for _,e := range minL.list[1:] {
		d := e.Middle()
		if d > max.Middle() {
			max = e
		}
		if d < min.Middle() {
			min = e
		}
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
		self.part.add(p)
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

func (self *Cache) CheckOrder(l *level,node config.Element,sumdif float64){

	if (l.par.par == nil) ||
	(self.pool == nil) ||
	(self.Cl == nil) {
		return
	}
	ea := cluster.NewSample(append(l.par.list, node))
	//ea.SetFlag(l.tag)
	//if math.Abs(node.Diff()) < sumdif {
	//	ea = nil
	//	//l.sample = ea
	//	//return
	//	//self.Cshow[6]++
	//}
	//self.Cshow[7]++
	//self.Cl.HandMap(self.pool.GetSetMap(ea),func(_ca interface{},t byte){
	//	//l.post = append(l.post,NewPostDB(_ca.(*Cache),t,self.getLastElement().DateTime()))
	//	//self.ca.Cshow[5]++
	//	//isa = true
	//})
	//self.Cshow[((ea.GetTag()>>1) * 2) +1]++
	//self.Cshow[7]++
	self.pool.Add(ea)
	if (l.sample == nil) {
		l.sample = ea
		return
	}
	pli := l.par.list[len(l.par.list)-1]
	if (l.sample.GetLastElement() == pli ){
		l.sample.Long = math.Abs(node.Diff())>math.Abs(pli.Diff())
		func (_e *cluster.Sample,_node config.Element,e int64){
			_e.SetCaMap(self.GetCacheMap(_node.DateTime(),e,_node),self)
			//self.pool.Add(_e)
		}(l.sample,node,self.getLastElement().DateTime())
		//go func(e *cluster.Sample){
		//	e.Wait()
		//	if e.GetCheck() {
		//		t := int(e.GetTag()>>1*4)
		//		if e.Long {
		//			self.SetCShow(t + int(e.GetTag() &^ 2)*2)
		//		}else{
		//			self.SetCShow(t + int(e.GetTag() &^ 2)*2+1)
		//		}
		//	}
		//}(l.sample)

		//l.sample.Long = (node.Diff()>0)==((self.getLastElement().Middle()-l.b.Middle())>0)
		//if l.sample.Long{
		//	self.Cshow[4]++
		//}
		//self.Cshow[5]++
	}
	//self.Cshow[6]++

	//go l.sample.SetCaMap(
	//self.GetCacheMap(
	//	node.DateTime(),
	//	//l.b.DateTime(),
	//	self.getLastElement().DateTime(),
	//))

	//go func(e *cluster.Sample){
	//	e.Wait()
	//	if e.GetCheck() {
	//		n := ((e.GetTag() &^ 2) * 2)
	//		if e.Long {
	//			self.Cshow[n]++
	//		}else{
	//			self.Cshow[n+1]++
	//		}
	//	}
	//}(l.sample)

	l.sample = ea

}

func (self *Cache) GetCacheMap(begin,end int64,node config.Element) (caMap [2][]byte) {

	//return nil
	if self.Cl == nil {
		return
	}
	le := self.Cl.Len()
	sumlen := le/4
	if le%4 >0 {
		sumlen++
	}
	caMap = [2][]byte{make([]byte,sumlen),make([]byte,sumlen)}

	type tmpdb struct{
		n int
		t byte
		i int
	}
	chanTmp := make(chan *tmpdb,le)
	var w,w_ sync.WaitGroup
	w_.Add(1)
	go func(){
		for d :=range chanTmp {
			caMap[d.n][d.i] |= d.t
		}
		w_.Done()
	}()
	w.Add(le)
	//fmt.Println(diff,long,dv)
	self.Cl.Read(func(i int,_c interface{}){
		go func(I int,c CacheInterface){
			//fmt.Println(c.InsName())
			defer w.Done()
			if c == self {
				return
			}
			max,min := c.TmpCheck(begin,end)
			if max == min {
				return
			}
			t := &tmpdb{i:I/8}
			var End int64
			var Long float64
			if max.DateTime() > min.DateTime() {
				End=(max.DateTime()+max.Duration())-begin
				Long=max.Middle()-min.Middle()
			}else{
				End=(min.DateTime()+min.Duration())-begin
				Long=min.Middle()-max.Middle()
			}
			if End > node.Duration() {
				t.n = 1
			}
			if (Long>0) != (node.Diff()>0) {
				t.t = 1 << uint(I%8)
			}else{
				t.t = 2 << uint(I%8)
			}
			chanTmp <- t

		}(i*2,_c.(CacheInterface))
	})
	w.Wait()
	close(chanTmp)
	w_.Wait()
	//for i,m := range caMap[0]{
	//	if 255 != ((^m) | (^(caMap[1][i]))) {
	//		panic(0)
	//	}
	//}
	//fmt.Println(caMap,count)
	return caMap


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
