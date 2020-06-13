package plugin

import (
	"errors"
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"hirpc-go/src/share"
	"time"
)

type ZooKeeperRegister struct {
	ServiceAddress           string
	conn                     *zk.Conn
	Address                  []string
	StateChangedCallbackFunc func(event zk.Event)
}

func (p *ZooKeeperRegister) Start() error {
	var err error
	var hosts = p.Address

	option := zk.WithEventCallback(p.StateChangedCallbackFunc)

	p.conn, _, err = zk.Connect(hosts, time.Second*5, option)
	if err != nil {
		fmt.Println(err)
		return nil
	}

	var rootPath = share.ZkRootNode
	var data = []byte("hello")
	var flags int32 = 0
	// permission
	var acls = zk.WorldACL(zk.PermAll)
	var exist bool
	exist, _, err = p.conn.Exists(rootPath)
	if !exist {
		// create
		resPath, errCreate := p.conn.Create(rootPath, data, flags, acls)
		if errCreate != nil {
			fmt.Println(errCreate)
			return nil
		}
		fmt.Println("created:", resPath)
	}

	return nil
}
func (p *ZooKeeperRegister) Stop() error {
	p.conn.Close()
	return nil
}
func (p *ZooKeeperRegister) Register(servicePath string, nodeType string, nodeData string) (err error) {
	if p.conn == nil {
		return errors.New("zookeeper connection is not established")
	}
	var data = []byte("hello")
	var flags int32 = 0
	// permission
	var acls = zk.WorldACL(zk.PermAll)
	var exist bool
	serviceFullPath := share.ZkRootNode + "/" + servicePath
	exist, _, err = p.conn.Exists(serviceFullPath)
	if !exist {
		// create
		resPath, errCreate := p.conn.Create(serviceFullPath, data, flags, acls)
		if errCreate != nil {
			fmt.Println(errCreate)
			return nil
		}
		fmt.Println("created:", resPath)
	}
	nodeTypeFullPath := serviceFullPath + "/" + nodeType
	exist, _, err = p.conn.Exists(nodeTypeFullPath)
	if !exist {
		// create
		resPath, errCreate := p.conn.Create(nodeTypeFullPath, data, flags, acls)
		if errCreate != nil {
			fmt.Println(errCreate)
			return nil
		}
		fmt.Println("created:", resPath)
	}
	//创建临时节点
	flags = 1
	nodePath := nodeTypeFullPath + "/" + nodeData
	resPath, errCreate := p.conn.Create(nodePath, data, flags, acls)
	if errCreate != nil {
		fmt.Println(errCreate)
		return nil
	}
	fmt.Println("created:", resPath)
	return nil
}
func (p *ZooKeeperRegister) Unregister(name string) (err error) {
	return nil
}
