package main
import (
	"github.com/zaddone/analog/config"
	"github.com/zaddone/analog/server"
	"github.com/zaddone/analog/replay"
	_ "github.com/zaddone/analog/game"
	"fmt"
	//"strings"
)

func main() {

	if config.Conf.Server{
		server.Router.Run(config.Conf.Port)
	}else{
		var cmd string
		for {
			fmt.Scanf("%s\r", &cmd)
			if cmd == "show" {
				replay.Show()
			}
			//replay.RunSignal(func(s replay.Signal){
			//	s.Show(cmd)
			//})
			//cmd = ""
		}
	}

}
