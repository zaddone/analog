package snap
import(
	"testing"
	"fmt"
	//"math/rand"
	"time"
	"github.com/zaddone/analog/config"
	"github.com/boltdb/bolt"
)
func TestDate(t *testing.T) {
	d := time.Now()
	dt := d.AddDate(-1,0,0)
	fmt.Println(d,dt)
}

func TestKvDBFirst(t *testing.T) {
	config.UpdateKvDBWithName(
		config.Conf.SampleDbPath,
		[]byte("test"),
		func(db *bolt.Bucket)error{
			for i:=0;i<100;i++ {
				db.Put([]byte{byte(i)},[]byte{0})
			}
			c := db.Cursor()
			for{
				k,_ := c.First()
				if k == nil {
					break
				}
				fmt.Println(k)
				db.Delete(k)
			}
			return nil
		},
	)
}

