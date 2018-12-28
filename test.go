package main
import(
	"github.com/zaddone/analog/server"
	"github.com/zaddone/operate/config"

)

func main() {
	server.Router.Run(config.Conf.Port)
}

