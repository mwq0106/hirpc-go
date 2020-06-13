package plugin

import (
	"fmt"
	"hirpc-go/src/config"
	"testing"
)

func TestZookeeperRegistry(t *testing.T) {
	r := &ZooKeeperRegisterPlugin{}
	conf := &config.Conf{}
	err := config.GetConf("conf.yaml", conf)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(conf.Host)
	r.Start()

}
