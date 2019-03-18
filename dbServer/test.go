package main
import(
	"fmt"
	//"time"
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
	l.SetWriteBuffer(0)
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
				fmt.Println(i)
				l.WriteToUnix([]byte{i,255,255},addr)
			}
			l.WriteToUnix(nil,raddr)
		}(raddr)
		//l.CloseWrite()
	}


}

