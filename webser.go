package main
import(
	"github.com/zaddone/analog/server"
	"github.com/zaddone/analog/config"
)
func main() {
	//select{}
	server.Router.Run(config.Conf.Port)
}
