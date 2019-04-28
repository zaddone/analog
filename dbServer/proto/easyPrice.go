package proto
import(
	"time"
	"encoding/binary"
	"github.com/zaddone/operate/oanda"
	"github.com/zaddone/analog/config"
)
type EasyPrice struct {
	dateTime int64
	bid int32
	ask int32
}

func NewEasyPrice(p *oanda.Price,integer float64)*EasyPrice{
	return &EasyPrice{
		dateTime:time.Unix(p.DateTime(),p.DateTimeMin()).UnixNano(),
		bid:int32(p.Bid()*integer),
		ask:int32(p.Ask()*integer),
	}
}
func NewEasyPriceDB(db []byte) (p *EasyPrice){
	p = &EasyPrice{}
	p.Load(db[:8],db[8:])
	return
}
func (self *EasyPrice) ToByte()(k []byte,v []byte){
	k = make([]byte,8)
	binary.BigEndian.PutUint64(k,uint64(self.dateTime))
	v = make([]byte,8)
	binary.BigEndian.PutUint32(v,uint32(self.bid))
	binary.BigEndian.PutUint32(v[4:],uint32(self.ask))
	return
}
func (self *EasyPrice) Load(k []byte,v []byte){
	self.dateTime = int64(binary.BigEndian.Uint64(k))
	self.bid = int32(binary.BigEndian.Uint32(v[:4]))
	self.ask = int32(binary.BigEndian.Uint32(v[4:]))
}

func (self *EasyPrice) Readf(hand func(config.Element)bool)bool{
	return hand(self)
}
func (self *EasyPrice) Read(hand func(config.Element)bool)bool{
	return hand(self)
}
func (self *EasyPrice) DateTime() int64 {
	return self.dateTime
}
func (self *EasyPrice) Diff() float64 {
	return float64(self.ask - self.bid)
}
func (self *EasyPrice) Middle() float64 {
	return float64(self.ask+self.bid)/2
}
func (self *EasyPrice) Duration() int64 {
	return 0
}
