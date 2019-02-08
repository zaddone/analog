package main
import(
	"time"
	"fmt"

)

func main() {
	fmt.Println(time.Now())
	<-time.After(time.Second*5)
	fmt.Println(time.Now())

}

