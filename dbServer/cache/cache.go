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
type Cache struct {
	ins *oanda.Instrument
	part *level
	eleChan chan config.Element
	pool *cluster.Pool
	//lastDateTime int64
	//stop chan bool
	Cl CacheList
	Cshow [8]float64

}

func NewCache(ins *oanda.Instrument) (c *Cache) {
	c = &Cache {
		ins:ins,
		eleChan:make(chan config.Element,1000),
		//stop:make(chan bool),
	}
	c.part = NewLevel(0,c,nil)
	return c
}

func (self *Cache) SetPool(){
	self.pool = cluster.NewPool(self.ins.Name,self)
}

func (self *Cache) SyncRun(cl CacheList){

	self.Cl = cl
	self.SetPool()
	go self.syncAddPrice()
	begin := self.getLastTime()
	if begin == 0 {
		begin = config.GetFromTime()
	}
	fmt.Println(time.Unix(begin,0))

	self.read(fmt.Sprintf("%s_main",config.Conf.Local),begin,time.Now().Unix(),func(e config.Element){
		self.eleChan <- e
	})

}

func (self *Cache) syncAddPrice(){
	var begin int64
	for{
		//select{
		//case p := <-self.eleChan:
		p := <-self.eleChan
		da := p.DateTime()
		if e := self.getLastElement();(e!= nil) && ((da - e.DateTime()) >100) {
			self.part = NewLevel(0,self,nil)
		}
		if da - begin > 604800 {
			self.SaveTestLog(da)
			begin = da
		}
		self.part.add(p,self.ins)
		//case self.stop<-true:

		//}
	}
}
func (self *Cache) getLastElement() config.Element {
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
func (self *Cache) SetCShow(i int) {
	self.Cshow[i]++
}

func (self *Cache) CheckOrder(l *level,node config.Element,sumdif float64){

	if (l.par.par == nil) ||
	(self.pool == nil) ||
	(self.Cl == nil) {
		return
	}

	ea := cluster.NewSample(append(l.par.list, node))
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
		l.sample.Long = ((pli.Diff()>0) != (node.Diff()>0)) && (math.Abs(node.Diff()) > math.Abs(pli.Diff()))
		go func(e *cluster.Sample){
			e.Wait()
			if e.GetCheck() {
				t := int(e.GetTag()>>1*4)
				if e.Long {
					self.SetCShow(t + int(e.GetTag() &^ 2)*2)
				}else{
					self.SetCShow(t + int(e.GetTag() &^ 2)*2+1)
				}
			}
		}(l.sample)
		//l.sample.Long = (node.Diff()>0)==((self.getLastElement().Middle()-l.b.Middle())>0)
		//if l.sample.Long{
		//	self.Cshow[4]++
		//}
		//self.Cshow[5]++
	}
	//self.Cshow[6]++

	//go l.sample.SetCaMap(
	//self.GetCacheMap(
	//	self.list[0].DateTime(),
	//	//l.b.DateTime(),
	//	self.getLastElement().DateTime(),
	//	node.Diff(),
	//	sumdif,
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

func (self *Cache) GetCacheMap(begin,end int64,diff,long float64) (caMap []byte) {

	//return nil

	if self.Cl == nil {
		return nil
	}
	absdiff := math.Abs(diff)
	//if long > absdiff {
	//	return nil
	//}
	dv := absdiff/long

	le := self.Cl.Len()
	sumlen := le/4
	if le%4 > 0 {
		sumlen++
	}
	caMap = make([]byte,sumlen)
	type tmpdb struct{
		t byte
		i int
	}
	chanTmp := make(chan *tmpdb,le)

	var w,w_ sync.WaitGroup
	w_.Add(1)
	//count :=0
	go func(){
		for d :=range chanTmp {
			//fmt.Println(len(caMap),d.i)
			caMap[d.i] |= d.t
		}
		w_.Done()
	}()
	w.Add(le)
	//fmt.Println(diff,long,dv)
	self.Cl.Read(func(i int,_c interface{}){
		go func(I int,c *Cache){
			chanTmp <- &tmpdb{
			t:func()(n byte){
				if c == self {
					return 0
				}
				d,l := c.TmpCheck(begin,end)
				if d != 0 {
					if math.Abs(d)/l > dv {
						n  = 2
					}
				}
				if (d>0) == (diff>0) {
					n ++
				}
				return
			}() << uint(I%8),
			i:I/8,
			}
			w.Done()
		}(i*2,_c.(*Cache))
	})
	w.Wait()
	close(chanTmp)
	w_.Wait()
	//fmt.Println(caMap,count)
	return caMap


}
func (self *Cache) TmpCheck(begin,end int64) (float64,float64){

	l := NewLevel(0,nil,nil)
	self.read(config.Conf.Local,begin,end,func(_e config.Element){
		l.add(_e,self.ins)
	})
	var li []config.Element
	for{
		if l.par == nil {
			break
		}
		li = l.list
		l = l.par
	}
	if len(l.list) == 0 {
		return 0,0
	}
	if len(li) == 0 {
		li = l.list
	}else{
		li = append(l.list,NewbNode(li...))
	}
	var diffSum float64
	for _,n := range li {
		diffSum += math.Abs(n.Diff())
	}
	return NewbNode(li...).Diff(),(diffSum/float64(len(li)))
	//return NewbNode(li...).Diff()

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
