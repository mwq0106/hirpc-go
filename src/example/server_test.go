package example

import (
	"flag"
	"hirpc-go/src/server"
)

var (
	addr = flag.String("addr", "localhost:8972", "server address")
)

func main() {
	flag.Parse()

	s := server.NewServer()
	//s.RegisterName("Arith", new(example.Arith), "")
	s.Register(new(Arith))
	s.Serve("tcp", *addr)
}
