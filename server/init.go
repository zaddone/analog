package server
import(
	"github.com/gin-gonic/gin"
	//"github.com/zaddone/operate/config"
	"github.com/zaddone/analog/config"
	"path/filepath"
	//"time"
	"io/ioutil"
	"os"
	"net/http"
	"strings"
	"bufio"
	//"strconv"
	"fmt"
)
var (
	Router *gin.Engine
)


func readFile(path string,hand func(string)) error {
	fi,err := os.Open(path)
	if err != nil {
		fmt.Println(path)
		return err
	}
	buf := bufio.NewReader(fi)
	for{
		li,err := buf.ReadString('\n')
		if len(li) >0 {
			hand(li[:len(li)-1])
		}
		if err != nil {
			break
		}
	}
	fi.Close()
	return nil
}
func init(){
	//if !config.Conf.Server {
	//	return
	//}
	Router = gin.Default()
	Router.Static("/static","./static")
	Router.LoadHTMLGlob(config.Conf.Templates)
	Router.GET("/",func(c *gin.Context){
		c.HTML(http.StatusOK,"index.tmpl",nil)
	})
	Router.GET("/show",func(c *gin.Context){
		c.HTML(http.StatusOK,"show.tmpl",nil)
	})
	Router.GET("/view",func(c *gin.Context){
		c.HTML(http.StatusOK,"view.tmpl",nil)
	})
	Router.GET("/log",func(c *gin.Context){
		//strings.Split(c.DefaultQuery("p",""),",")
		var finfo []interface{}
		err := ListDir(filepath.Join(config.Conf.ClusterPath,filepath.Join(strings.Split(c.DefaultQuery("p",""),",")...)),func(path string,f os.FileInfo){
			finfo = append(finfo,map[string]interface{}{"p":f.Name(),"f":f.IsDir(),"t":f.ModTime()})
		})
		if err != nil {
			c.JSON(http.StatusNotFound,err.Error())
			return
		}
		c.JSON(http.StatusOK,gin.H{"dir":finfo})
	})
	Router.GET("/open",func(c *gin.Context){
		var db []string
		pa :=filepath.Join(config.Conf.ClusterPath,filepath.Join(strings.Split(c.DefaultQuery("p",""),",")...))
		err := readFile(pa,func(_db string){
			db = append(db,_db)
		})
		if err != nil {
			c.JSON(http.StatusNotFound,err.Error())
			return
		}
		c.JSON(http.StatusOK,db)
	})
}
func ListDir(dirPth string, hand func(string,os.FileInfo)) (err error) {

	dir, err := ioutil.ReadDir(dirPth)
	if err != nil {
		return err
	}
	for _, fi := range dir {
		hand(filepath.Join(dirPth,fi.Name()),fi)
	}
	return nil

}
