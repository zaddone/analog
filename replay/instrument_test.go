package replay
import (
	"testing"
	//"fmt"
	"time"
)
func Test_SaveTrLog(t *testing.T){

	ins := new(InstrumentCache)
	ins.SaveTrLog("test content",time.Now())
}
