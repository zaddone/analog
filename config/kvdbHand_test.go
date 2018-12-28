package config
import(
	"testing"
	"fmt"
	"math/rand"
	"github.com/boltdb/bolt"
	"encoding/binary"
	"time"
)
func TestKeyName(t *testing.T) {

	rand.Seed(time.Now().UnixNano())
	var dur int64
	key := make([]byte,8)
	for i:=0;i<20;i++{
		dur = rand.Int63n(10000)
		//binary.LittleEndian.PutUint64(key,uint64(dur))
		binary.BigEndian.PutUint64(key,uint64(dur))
		fmt.Println(key)
		UpdateKvDBWithName("test.db",[]byte("test"),func(b *bolt.Bucket) error{
			return b.Put(key,[]byte(fmt.Sprintf("%d",dur)))
		})
	}
	dur = rand.Int63n(10000)
	binary.BigEndian.PutUint64(key,uint64(dur))
	ViewKvDBWithName("test.db",[]byte("test"),func(b *bolt.Bucket) error{
		c := b.Cursor()
		fmt.Println(key,dur,"-------------")

		for k,v := c.Seek(key);k!=nil;k,v = c.Next(){
			fmt.Println(k,string(v))
		}
		return nil
	})

}
