package replay

import (
	//"github.com/zaddone/RoutineWork/request"
	"github.com/zaddone/analog/server"
	"github.com/zaddone/analog/config"
	"fmt"
	"os"
	"path/filepath"
	"time"
	"context"
	"io"
	"log"
)

type Cache struct {
	id int
	Scale       int64
	Name        string
	FutureChan  chan *CacheFile
	EndtimeChan chan int64
	endTime  int64
	LastCan   config.Candles
	par       config.Instrument
	OnlineCF  *CacheFile

}
func (self *Cache) GetEndTime() int64{
	return self.endTime
}
func NewCache(name string, scale int64, p *InstrumentCache) (ca *Cache) {

	ca = &Cache{
		Name:        name,
		Scale:       scale,
		FutureChan:  make(chan *CacheFile, 10),
		EndtimeChan: make(chan int64, 5),
		par:         p}
	return ca

}

func (self *Cache) GetId()int{
	return self.id
}

func (self *Cache) SetId(i int){
	self.id = i
}
func (self *Cache) GetOnline() bool {
	if self.OnlineCF == nil {
		return false
	}
	return len(self.OnlineCF.Can) == 0

}
func (self *Cache) GetName() string {
	return self.Name
}
func (self *Cache) GetlastCan() config.Candles {
	return self.LastCan
}

func (self *Cache) GetInsCache() config.Instrument {
	return self.par
}

func (self *Cache) GetMinCache() config.Cache {
	return self.GetInsCache().GetBaseCache()
}

func (self *Cache) GetMinCan() config.Candles {
	return self.GetMinCache().GetlastCan()
}

func (self *Cache) GetInsName() string {
	return self.GetInsCache().GetInsName()
}


//func (self *Cache) Init(name string, scale int64, p *InstrumentCache) {
//	self.Name = name
//	self.Scale= scale
//	self.FutureChan = make(chan *CacheFile, 10)
//	self.EndtimeChan= make(chan int64, 1)
//	self.par = p
//}

func (self *Cache) GetScale() int64 {

	return self.Scale

}
func (self *Cache) Show() string {
	return fmt.Sprintf("f %d",len(self.FutureChan))
}

func (self *Cache) Load(ctx context.Context,name, path string) {

	f, err := os.Stat(path)
	var cf *CacheFile
	if err == nil && f.IsDir() {
		err = filepath.Walk(path, func(pa string, fi os.FileInfo, er error) error {
			if fi.IsDir() {
				return er
			}
			//fmt.Println(pa,len(self.FutureChan),"open")
			cf,er = NewCacheFile(ctx,name, pa, fi, int(86400/self.Scale+1)*2)
			if er == nil {
				self.FutureChan <-cf
			} else {
				fmt.Println(er)
			}
			//fmt.Println(pa,"end")
			return er
		})
		//fmt.Println(name,"load over-------------------",err)
		if err != nil {
			if err == io.EOF {
				return
			}else{
				panic(err)
			}
		}

	}
	if !config.Conf.Server{
		return
	}
	var begin int64
	if cf == nil {
		beginT, err := time.Parse(config.TimeFormat, config.Conf.BEGINTIME)
		if err != nil {
			panic(err)
		}
		begin = beginT.UTC().Unix()
	} else {
		begin = cf.EndCan.GetTime() + self.Scale
	}
	self.OnlineCF = &CacheFile{Can:make(chan config.Candles, 1000)}
	self.FutureChan <- self.OnlineCF
	server.DownCandles(name, begin, 0, self.Scale/2, self.Name, func(can *server.Candles) error {

		select{
		case <-ctx.Done():
			return io.EOF
		default:
			can.SetScale(self.Scale)
			self.OnlineCF.Can <- can
		}
		return nil
	})

	fmt.Println("over load")

}

func (self *Cache) CheckUpdate(can config.Candles) bool {

	if can.GetMidLong() == 0 {
		return false
	}
	if self.LastCan != nil {
		if self.LastCan.GetTime() >= can.GetTime() {
			return false
		}
	}
	self.LastCan = can
	//fmt.Println(can)
	return true

}
func (self *Cache) SetEndTimeChan(ti int64) {
	//self.IsSplit = false
	select{
	case self.EndtimeChan <- ti:
		return
	default:
		<-self.EndtimeChan
		self.SetEndTimeChan(ti)
	}

}
func (self *Cache) Sensor(ctx context.Context,cas []config.Cache) {
	calen := len(cas)
	//self.IsUpdate = false
	//var date string = ""
	//fmt.Println("sensor")
	self.Read(func(can config.Candles) (err error) {
		//fmt.Println(time.Unix(can.GetTime(),0),"---",can.GetScale(),self.GetInsName())
		select{
		case <-ctx.Done() :
			return io.EOF
		default:
			self.endTime = can.GetTime() + self.Scale
			if !self.UpdateJoint(can) {
				return
			}
			self.par.GetWait().Add(calen)
			for _, ca := range cas {
				ca.SetEndTimeChan(self.endTime)
			}
			self.par.GetWait().Wait()

			runSignal(func(s Signal){
				s.Check(self.GetInsCache())
			})
			return
		}
	})
}

func (self *Cache) SyncRun(ctx context.Context,hand func(can config.Candles) bool) {
	select{
	case <-ctx.Done() :
		return
	case self.endTime = <-self.EndtimeChan:
		self.Read(func(can config.Candles) (err error) {
			NowTime := can.GetTime() + self.Scale
			//fmt.Println(self.GetInsName(),NowTime)
			G:
			for {
				if NowTime <= self.endTime {
					hand(can)
					return
				}
				self.par.GetWait().Done()
				select{
				case <-ctx.Done() :
					return io.EOF
				case self.endTime = <-self.EndtimeChan:
					//fmt.Println(self.GetInsName(),NowTime)
					continue G
				}
			}

		})
	}
}

func (self *Cache) UpdateJoint(can config.Candles) ( bool) {

	if !self.CheckUpdate(can) {
		return false
	}
	//self.GetInsCache().Monitor(self,can)
	//self.JointLib.Update(can)
	runSignal(func(s Signal){
		s.Update(self)
	})
	return true

//	self.EndJoint, self.IsSplit = self.EndJoint.Append(can)
//	//fmt.Println(len(self.par.SplitCache))
//	if self.IsSplit {
//		if self.LastCache == nil {
//			j :=0
//			self.EndJoint.ReadLast(func(jo *Joint) bool {
//				j++
//				if j < 4 {
//					return false
//				}
//				jo.Cut()
//				self.BeginJoint = jo
//				//fmt.Println("end cache",self.Name,time.Unix(jo.Cans[0].Time,0))
//				return true
//			})
//		} else {
//			//self.par.SplitCache <- self
//			if len(self.LastCache.BeginJoint.Cans) > 0 {
//				endTime := self.LastCache.BeginJoint.Cans[0].Time
//				self.BeginJoint.ReadNext(func(jo *Joint) bool {
//					if len(jo.Cans) > 0 && jo.Cans[0].Time < endTime {
//						return false
//					}
//					if self.BeginJoint != jo {
//						self.BeginJoint = jo
//						jo.Cut()
//						//fmt.Println(self.Name,self.BeginJoint.Cans[0].Time,endTime)
//					}
//					return true
//				})
//			}
//		}
//	}


}

func (self *Cache) Read(Handle func(can config.Candles) error ) {

	var err error
	for {
		cf := <-self.FutureChan
		if cf == nil {
			panic("cf == nil")
			break
		}
		//fmt.Println(self.GetScale())
		for {
			can := <-cf.Can
			if can == nil {
				//panic("can == nil")
				break
			}
			//fmt.Println(time.Unix(can.GetTime(),0),self.GetScale())
			err = Handle(can)
			if err != nil {
				if err == io.EOF {
					return
				}
				log.Println(err)
			}
		}
	}

}

