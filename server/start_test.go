package server
import(
	"testing"
)
func Test_Router(t *testing.T){
	//_,err := os.Stat(ConfigDB)
	//if err == nil {
	//	err = os.Remove(ConfigDB)
	//	if err != nil {
	//		panic(err)
	//	}
	//}
	Router.Run(":8888")
}
