package main
import(
	"fmt"
	"time"
	"net"
	"os"
)
func main(){

	os.Remove("test.sock")
	laddr,err := net.ResolveUnixAddr("unixgram","test.sock")
	if err != nil {
		panic(err)
	}
	l,err := net.ListenUnixgram("unixgram",laddr)
	if err != nil {
		panic(err)
	}
	var buf [1024]byte

	for{
		n,raddr,err := l.ReadFromUnix(buf[:])
		if err != nil {
			panic(err)
		}
		fmt.Println(buf[:n],raddr.String())
		go func(addr *net.UnixAddr){
			var i byte
			for i=0;i<10;i++{
				time.Sleep(time.Second)
				l.WriteToUnix([]byte{i},addr)
			}
			l.WriteToUnix(nil,raddr)
		}(raddr)
		//l.CloseWrite()
	}


}

