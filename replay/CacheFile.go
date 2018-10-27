package replay
import (
	"os"
	"io"
	"bufio"
	"context"
	"github.com/zaddone/analog/server"
	"github.com/zaddone/analog/config"
	//"fmt"
)
type CacheFile struct {
	Can    chan config.Candles
	Fi     os.FileInfo
	Path   string
	EndCan config.Candles
}
func NewCacheFile(ctx context.Context,name, path string, fi os.FileInfo, Max int) (*CacheFile,error) {

	var cf CacheFile
	return &cf,cf.Init(ctx,name,path,fi,Max)

}

func (self *CacheFile) Init(ctx context.Context,name, path string, fi os.FileInfo, Max int) (err error) {

	self.Path = path
	self.Fi = fi
	self.Can = make(chan config.Candles, Max)
	var fe *os.File
	fe, err = os.Open(path)
	if err != nil {
		return err
	}
	defer func(){
		fe.Close()
		close(self.Can)
	}()
	r := bufio.NewReader(fe)
	i:=0
	for {
		select{
		case <-ctx.Done():
			return io.EOF
		default:
			db, _, e := r.ReadLine()
			if e != nil {
				//fmt.Println(i)
				return nil
			}
			self.EndCan = new(server.Candles)
			self.EndCan.Load(string(db))
			//if i>= Max{
			//	fmt.Println(i,Max)
			//	panic(0)
			//}
			self.Can <- self.EndCan
		}
		i++
		//fmt.Printf("%s %d\r",path,i)
	}
	//fmt.Println(i)
	//close(self.Can)
	return nil

}


