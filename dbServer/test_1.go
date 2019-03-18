package main
import(
	"fmt"
	"net"
	"os"
	"time"
	"sync"
)

func main(){
	var w sync.WaitGroup
	w.Add(10)
	for i:=0;i<10;i++{
		go func(){
			run()
			w.Done()
		}()
	}
	w.Wait()
}
func run(){

	addr := fmt.Sprintf("test_%d.sock",time.Now().UnixNano())
	raddr,err := net.ResolveUnixAddr("unixgram","test.sock")
	if err != nil {
		panic(err)
	}
	laddr,err := net.ResolveUnixAddr("unixgram",addr)
	if err != nil {
		panic(err)
	}
	c,err := net.DialUnix("unixgram",laddr,raddr)
	if err != nil {
		panic(err)
	}
	n,err := c.Write([]byte{255,255,255,255})
	if err != nil {
		panic(err)
	}
	var buf [1024]byte

	time.Sleep(time.Second*20)
	for{
		n,err = c.Read(buf[:])
		if err != nil {
			fmt.Println(err)
			break
		}
		fmt.Println(buf[:n],n)
		if n == 0 {
			break
		}
		time.Sleep(time.Second)
	}

	os.Remove(addr)
	fmt.Println(addr)



}
