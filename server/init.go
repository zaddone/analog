package server
import(
	"github.com/gin-gonic/gin"
	"github.com/zaddone/operate/config"
	"path/filepath"
	//"time"
	"io/ioutil"
	"os"
	"net/http"
	"strings"
	"bufio"
	"strconv"
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
	Router = gin.Default()
	Router.Static("/static","./static")
	Router.LoadHTMLGlob(config.Conf.Templates)
	Router.GET("/",func(c *gin.Context){
		c.HTML(http.StatusOK,"index.tmpl",nil)
	})
	Router.GET("/view",func(c *gin.Context){
		c.HTML(http.StatusOK,"view.tmpl",nil)
	})
	Router.GET("/log",func(c *gin.Context){
		//strings.Split(c.DefaultQuery("p",""),",")
		var finfo []interface{}
		err := ListDir(filepath.Join(config.Conf.LogPath,filepath.Join(strings.Split(c.DefaultQuery("p",""),",")...)),func(path string,f os.FileInfo){
			finfo = append(finfo,map[string]interface{}{"p":f.Name(),"f":f.IsDir(),"t":f.ModTime()})
		})
		if err != nil {
			c.JSON(http.StatusNotFound,err.Error())
			return
		}
		c.JSON(http.StatusOK,gin.H{"dir":finfo})
	})
	Router.GET("/openfile",func(c *gin.Context){
		var db [][2]float64
		pa :=filepath.Join(config.Conf.LogPath,filepath.Join(strings.Split(c.DefaultQuery("p",""),",")...))
		err := readFile(pa,func(_db string){
			_s := strings.Split(_db," ")
			v,er := strconv.ParseFloat(_s[0],64)
			if er != nil {
				fmt.Println(er)
				return
			}
			t,er := strconv.ParseFloat(_s[2],64)
			if er != nil {
				fmt.Println(er)
				return
			}
			db = append(db,[2]float64{v,t})
		})
		if err != nil {
			c.JSON(http.StatusNotFound,err.Error())
			return
		}
		c.JSON(http.StatusOK,gin.H{
			"db1":db,
		})
	})
	Router.GET("/open_test",func(c *gin.Context){
		var db [][2]float64
		var maxV,minV,maxT,minT float64
		pa :=filepath.Join(config.Conf.LogPath,filepath.Join(strings.Split(c.DefaultQuery("p",""),",")...))
		err := readFile(pa,func(_db string){
			_s := strings.Split(_db," ")
			v,er := strconv.ParseFloat(_s[0],64)
			if er != nil {
				fmt.Println(er)
				return
			}
			if minV == 0 || minV >v {
				minV = v
			}
			if maxV < v {
				maxV = v
			}
			t,er := strconv.ParseFloat(_s[2],64)
			if er != nil {
				fmt.Println(er)
				return
			}
			if minT == 0 || minT >t {
				minT = t
			}
			if maxT < t {
				maxT = t
			}
			db = append(db,[2]float64{t,v})
		})
		if err != nil {
			c.JSON(http.StatusNotFound,err.Error())
			return
		}
		tdiff := maxT - minT
		vdiff := maxV - minV
		for i,d := range db {
			t_ := d[0] - minT
			if t_ == 0 {
				d[0] = 0
			}else{
				d[0] = t_/tdiff
			}
			v_ := d[1] - minV
			if v_ == 0 {
				d[1] = 0
			}else{
				d[1] = v_/vdiff
			}
			db[i] = d
		}
		c.JSON(http.StatusOK,gin.H{
			"db1":db,
		})

	})
	Router.GET("/open",func(c *gin.Context){

		var db [][2]float64
		var maxV,minV,maxT,minT float64
		var endid int
		pa :=filepath.Join(config.Conf.LogPath,filepath.Join(strings.Split(c.DefaultQuery("p",""),",")...))
		err := readFile(pa,func(_db string){
			if _db ==  "end" {
				endid = len(db)
				return
			}
			_s := strings.Split(_db," ")
			v,er := strconv.ParseFloat(_s[0],64)
			if er != nil {
				fmt.Println(er)
				return
			}
			if minV == 0 || minV >v {
				minV = v
			}
			if maxV < v {
				maxV = v
			}
			t,er := strconv.ParseFloat(_s[2],64)
			if er != nil {
				fmt.Println(er)
				return
			}
			if minT == 0 || minT >t {
				minT = t
			}
			if maxT < t {
				maxT = t
			}
			db = append(db,[2]float64{t,v})
		})
		if err != nil {
			c.JSON(http.StatusNotFound,err.Error())
			return
		}
		tdiff := maxT - minT
		vdiff := maxV - minV
		for i,d := range db {
			t_ := d[0] - minT
			if t_ == 0 {
				d[0] = 0
			}else{
				d[0] = t_/tdiff
			}
			v_ := d[1] - minV
			if v_ == 0 {
				d[1] = 0
			}else{
				d[1] = v_/vdiff
			}
			db[i] = d
		}
		_,file := filepath.Split(pa)
		fil := strings.Split(file,"_")
		tp,err := strconv.ParseFloat( fil[1],64)
		if err != nil {
			panic(err)
		}
		tp = (tp-minV)/vdiff
		sl,err := strconv.ParseFloat( fil[2],64)
		if err != nil {
			panic(err)
		}
		sl = (sl-minV)/vdiff
		c.JSON(http.StatusOK,gin.H{
			"db1":db[:endid],
			"db2":db[endid:],
			"tp":[][2]float64{
				[2]float64{db[0][0],tp},
				[2]float64{db[len(db)-1][0],tp},
			},
			"sl":[][2]float64{
				[2]float64{db[0][0],sl},
				[2]float64{db[len(db)-1][0],sl},
			},
		})
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
