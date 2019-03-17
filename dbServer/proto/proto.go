package proto
import(
	//"io"
	"time"
	"fmt"
	"encoding/binary"
	//"encoding/gob"
	//"bytes"
)
var (
	Buff [1024]byte
)
type Proto struct {
	Ins string
	B,E int64
	tmp string
}
func (self *Proto) ToByte()[]byte{
	b:=make([]byte,16)
	binary.BigEndian.PutUint64(b[:8],uint64(self.B))
	binary.BigEndian.PutUint64(b[8:],uint64(self.E))
	return append(b,[]byte(self.Ins)...)
}
func NewProto(buff []byte) (p *Proto) {
	p = &Proto{}
	p.Load(buff)
	return p
}

func (self *Proto)Load(Buff []byte) {
	self.Ins = string(Buff[16:])
	self.B = int64(binary.BigEndian.Uint64(Buff[0:8]))
	self.E = int64(binary.BigEndian.Uint64(Buff[8:16]))
}

func (self *Proto) GetTmpPath() string {

	if self.tmp == "" {
		self.tmp = fmt.Sprintf("/tmp/%s_%d_%d_%d.sock",self.Ins,self.B,self.E,time.Now().UnixNano())
	}
	return self.tmp

}
