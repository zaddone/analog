package config
import(
	"sync"
	"time"
	"context"
)

type Candles interface {

	Load(string) error
	GetTime() int64
	GetMidLong() float64
	GetMidAverage() float64
	CheckSection(float64) bool
	GetScale() int64
	SetScale(int64)

}

type Cache interface {

	GetId() int
	SetId(int)
	GetName() string
	GetEndTime() int64
	GetScale() int64
	Load(ctx context.Context, name, path string)
	SyncRun(ctx context.Context,hand func(Candles) bool)
	Sensor(ctx context.Context,cas []Cache)
	GetlastCan() Candles
	GetInsCache() Instrument
	SetEndTimeChan(int64)
	UpdateJoint(Candles) bool
	GetOnline() bool
	Show() string

}

type Reduce interface{

	GetId() string
	GetPl() float64

}

type Response interface {
	GetType() string
	GetTime() time.Time
	String() string
	GetId() string
	Save(string)
	GetReason() string
	GetTradesClosed(func(Reduce) bool)
}
type Form interface {
	ToMap() map[string]interface{}
	GetBegin() int64
	GetEnd()  int64
	GetPl() float64
	GetGamePl() float64
}
type SignalForm interface {
	//Read(id_ string) Form
	//ReadAll(int,func(Form)bool)
	ReadDB(int,int,int,func(Form)bool)
	ReadInsList(func(int,int))
}

type Instrument interface {

	//CheckOrderFill() bool
	//SetResponse(Response)
	//GetResponse() Response
	//SetPrice(interface{})
	GetAccountID() int
	GetInsId() int
	GetInsName() string
	GetInsStopDis() float64
	GetDisplayPrecision() float64
	GetPriceTime() int64
	GetPriceDiff() float64
	GetStandardPrice(float64) string
	GetCacheLen() int
	//GetPriceVal() float64

	GetBaseCache() Cache
	GetEndCache() Cache

	GetCacheList (func(int,Cache))
	GetCache(id int) Cache
	GetWait() *sync.WaitGroup
	//Monitor(Cache,Candles)
	Run()

}
