package example

import (
	"context"
	"hirpc-go/src/client"
	"log"
)

func main() {
	zkDiscovery := client.NewZookeeperDiscovery([]string{"localhost:2181"})
	option := client.Option{}
	rpcClient := client.NewClient(client.Failtry, client.RandomSelect, zkDiscovery, option)
	args := &Args{
		A: 10,
		B: 20,
	}
	reply := &Reply{}
	err := rpcClient.Call(context.Background(), "com.xx.service.Arith", "Mul", args, reply)
	if err != nil {
		log.Fatalf("failed to call: %v", err)
	}
}
