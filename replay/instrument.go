package replay

import (
	"github.com/zaddone/analog/server"
	"github.com/zaddone/analog/config"
	"log"
	"path/filepath"
	"sync"
	"net/url"
	"io"
	"bufio"
	"encoding/json"
	"fmt"
	"context"
	//"reflect"
	//"strings"
	//"strconv"
	//"os"
	//"time"
)

type InstrumentCache struct {

	cacheLen	int
	cacheList       []config.Cache
	//ServerChanMap   *ServerChanMap
	w		sync.WaitGroup
	Ins		*server.Instrument
	Price		server.Price
	ctx		context.Context
	cancel		context.CancelFunc
	//OrderFillChan	chan config.Response
	//OrderFill	config.Response
	//Transactions	request.TransactionsList

}
func (self *InstrumentCache) Close() error{
	self.cancel()
	return nil

}
func (self *InstrumentCache) GetPriceDiff() float64 {
	return self.Price.GetDiff()
}
func (self *InstrumentCache) GetPriceTime() int64 {
	return self.Price.Time.TimeU()
}
func (self *InstrumentCache) GetAccountID() int {
	return self.Ins.Uid
}

func NewInstrumentCache(Ins *server.Instrument) (inc *InstrumentCache) {

	inc = &InstrumentCache{
		Ins:Ins,
		cacheList:make([]config.Cache,len(config.Conf.Granularity))}
	i := 0

	inc.ctx,inc.cancel = context.WithCancel(context.Background())
	for k, v := range config.Conf.Granularity {
		ca := NewCache(k, int64(v), inc)
		var path string
		if config.Conf.DbPath == "" {
			path = ""
		}else{
			path = filepath.Join(config.Conf.DbPath,Ins.Name, ca.Name)
		}
		ctx,_ :=context.WithCancel(inc.ctx)
		go ca.Load(ctx,Ins.Name,path)
		//inc.CacheList = append(inc.CacheList, ca)
		inc.cacheList[i] = ca
		inc.sort(i)
		i++
	}
	inc.GetCacheList(func(i int,c config.Cache){
		c.SetId(i)
	})
	return

}

func (self *InstrumentCache) GetCacheLen() int {
	return len(self.cacheList)
}
func (self *InstrumentCache) GetCache(id int) config.Cache {
	return self.cacheList[id]
}
func (self *InstrumentCache) GetCacheList (f func (int,config.Cache)) {
	for i,c := range self.cacheList {
		f(i,c)
	}
}
func (self *InstrumentCache)GetStandardPrice(p float64) string {
	return self.Ins.StandardPrice(p)
}
func (self *InstrumentCache) GetEndCache() config.Cache {
	return self.cacheList[len(self.cacheList)-1]
}
func (self *InstrumentCache) GetWait() *sync.WaitGroup{
	return &self.w
}
func (self *InstrumentCache) GetDisplayPrecision() float64 {
	return self.Ins.DisplayPrecision
}
func (self *InstrumentCache) GetInsStopDis() float64 {
	return self.Ins.MinimumTrailingStopDistance
}
func (self *InstrumentCache) GetInsId() int {
	if self.Ins == nil {
		return 0
	}
	return self.Ins.GetId()
}
func (self *InstrumentCache) GetInsName() string{
	if self.Ins == nil {
		return "test"
	}
	return self.Ins.Name
}
func (self *InstrumentCache) sort(i int){
	if i == 0 {
		return
	}
	I := i - 1
	if self.cacheList[I].GetScale() > self.cacheList[i].GetScale() {
		self.cacheList[I], self.cacheList[i] = self.cacheList[i], self.cacheList[I]
		self.sort(I)
	}
}

func (self *InstrumentCache) Run(){
	self.run()
}

func (self *InstrumentCache) run(){

	cas:=self.cacheList[1:]
	for _, _ca := range cas {
		//fmt.Println(_ca.GetScale())
		ctx ,_ := context.WithCancel(self.ctx)
		go _ca.SyncRun(ctx,_ca.UpdateJoint)
	}
	ctx ,_ := context.WithCancel(self.ctx)
	go self.cacheList[0].Sensor(ctx,cas)

	if config.Conf.Price {
		ctx ,_ := context.WithCancel(self.ctx)
		go self.SyncPriceUpdate(ctx)
	}

}

func (self *InstrumentCache) GetBaseCache() config.Cache {
	return self.cacheList[0]
}

func (self *InstrumentCache) GetHight(ca config.Cache) config.Cache {
	le := len(self.cacheList)-1
	for i:=0;i<le;i++ {
		if self.cacheList[i] == ca {
			return self.cacheList[i+1]
		}
	}
	return nil
}
func (self *InstrumentCache) TestOrder(un int,h func(interface{})) error {
	var tp,sl float64
	diff := self.Price.GetDiff()+self.Ins.MaximumTrailingStopDistance
	if un >0 {
		price := self.Price.CloseoutBid.GetPrice()
		tp = price+diff
		sl = price-diff
	}else if un<0 {
		price := self.Price.CloseoutAsk.GetPrice()
		tp = price - diff
		sl = price + diff
	}else{
		return fmt.Errorf("un == 0")
	}
	req,err := server.HandleOrder(
		self.Ins.Name,
		un,"",
		self.Ins.StandardPrice(tp),
		self.Ins.StandardPrice(sl),
		self.Ins.Uid)
	if err != nil {
		return err
	}
	h(req)
	return nil
}
func (self *InstrumentCache) SyncPriceUpdate(ctx context.Context){
	path := server.AccountsMap[self.Ins.Uid].GetStreamAccountPath() +"/pricing/stream?" + url.Values{"instruments":[]string{self.Ins.Name},"snapshot":[]string{"true"}}.Encode()
	var err error
	var by []byte
	for{
		err = server.ClientDo(path,func(body io.Reader)error {
			read := bufio.NewReader(body)
			for{
				select{
				case <-ctx.Done():
					return io.EOF
				default:
					by,err = read.ReadSlice('\n')
					if err != nil {
						return err
					}
					err = json.Unmarshal(by,&self.Price)
					if err != nil {
						panic(err)
						return err
					}
				}

			}
		})
		if err != nil {
			if err == io.EOF {
				return
			}
			log.Println(err)
			continue
		}
	}
}
