package main
import(
	"encoding/gob"
	"encoding/binary"
	"fmt"
	"bytes"
	"reflect"
	"io"

)
var (
	buffer [8192]byte
	FuncList map[string]interface{}
)
type Protocal struct{
	Func string
	Args []interface{}
}

func (self *Protocal) Apply(f interface{}, args []interface{})([]reflect.Value){
	fun := reflect.ValueOf(f)
	in := make([]reflect.Value, len(args))
	for k,param := range args{
		in[k] = reflect.ValueOf(param)
	}
	r := fun.Call(in)
	
	return r
}
func (self *Protocal) Encoder(buf io.Writer) error {
	var b bytes.Buffer
	err = gob.NewEncoder(&b).Encode(self)
	if err != nil {
		return err
	}

	tmpBuf := make([]byte,8)
	db := b.Bytes()
	l  := len(db)
	binary.BigEndian.PutUint64(tmpBuf,uint64(l))
	tmpBuf = append(tmpBuf,db)
	_,err = buf.Write(append(tmpBuf,db))
	return err
}
func (self *Protocal) Decoder(buf io.Reader) error {

	var tmpBuf []byte
	n,err := buf.Read(buffer[:])
	if err != nil {
		return err
	}
	if (n <9){
		return io.EOF
	}
	le :=int(binary.BigEndian.Uint64(buffer[:8]))
	tmpBuf = buffer[8:n]
	for{
		if len(tmpBuf) >= le {
			break
		}
		n,err = buf.Read(buffer[:])
		if err != nil {
			return err
		}
		tmpBuf = append(tmpBuf,buffer[:n])
	}
	return gob.NewDecoder(bytes.NewBuffer(tmpBuf[:le])).Decode(self)

}
func main() {

	var b bytes.Buffer
	err := gob.NewEncoder(&b).Encode(
		&Protocal{
			Func:"read",
			Args:[]interface{}{1,"t"},
		})
	if err != nil {
		panic(err)
	}
	pr := &Protocal{}
	gob.NewDecoder(&b).Decode(pr)
	fmt.Println(pr)

}

