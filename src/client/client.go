package client

import (
	"context"
	"errors"
	"hirpc-go/src/plugin"
	"net"
	"reflect"
	"sync"
	"time"
)

type Client struct {
	isShutdown bool
	servicePath  string
	failMode     FailMode
	selectMode   SelectMode
	discovery ServiceDiscovery
	option       Option

	mu        sync.RWMutex
	selector  Selector
	//已经监听的服务
	watchedService map[string]bool
	//已连接的目标服务器
	connectedConn map[string]net.Conn
	//缓存符合查询条件的服务器。外面的key是服务路径，里面的key是条件，value是符合条件的机器
	cachedServer map[string]map[string][]plugin.ServerInRegistry
	//服务路径下可用的机器
	availableServer map[string][]plugin.ServerInRegistry
}
func NewClient(failMode FailMode, selectMode SelectMode, discovery ServiceDiscovery, option Option) Client {
	client := Client{
		failMode:     failMode,
		selectMode:   selectMode,
		discovery:    discovery,
		option:       option,
	}
	client.selector = newSelector(selectMode, servers)
	return client
}
func (c *Client) Call(ctx context.Context, servicePath string, serviceMethod string, args interface{}, reply interface{}) error {
	if c.isShutdown {
		return errors.New("client is shut down")
	}
	if _, ok := c.watchedService[servicePath];!ok {
		c.watchedService[servicePath] = true
		c.discovery.WatchService(servicePath)
	}






	var err error
	k, client, err := c.selectClient(ctx, c.servicePath, serviceMethod, args)
	if err != nil {
		if c.failMode == Failfast {
			return err
		}
	}

	var e error
	switch c.failMode {
	case Failtry:
		retries := c.option.Retries
		for retries >= 0 {
			retries--

			if client != nil {
				err = c.wrapCall(ctx, client, serviceMethod, args, reply)
				if err == nil {
					return nil
				}
				if _, ok := err.(ServiceError); ok {
					return err
				}
			}

			if uncoverError(err) {
				c.removeClient(k, client)
			}
			client, e = c.getCachedClient(k)
		}
		if err == nil {
			err = e
		}
		return err
	case Failover:
		retries := c.option.Retries
		for retries >= 0 {
			retries--

			if client != nil {
				err = c.wrapCall(ctx, client, serviceMethod, args, reply)
				if err == nil {
					return nil
				}
				if _, ok := err.(ServiceError); ok {
					return err
				}
			}

			if uncoverError(err) {
				c.removeClient(k, client)
			}
			//select another server
			k, client, e = c.selectClient(ctx, c.servicePath, serviceMethod, args)
		}

		if err == nil {
			err = e
		}
		return err
	case Failbackup:
		ctx, cancelFn := context.WithCancel(ctx)
		defer cancelFn()
		call1 := make(chan *Call, 10)
		call2 := make(chan *Call, 10)

		var reply1, reply2 interface{}

		if reply != nil {
			reply1 = reflect.New(reflect.ValueOf(reply).Elem().Type()).Interface()
			reply2 = reflect.New(reflect.ValueOf(reply).Elem().Type()).Interface()
		}

		_, err1 := c.Go(ctx, serviceMethod, args, reply1, call1)

		t := time.NewTimer(c.option.BackupLatency)
		select {
		case <-ctx.Done(): //cancel by context
			err = ctx.Err()
			return err
		case call := <-call1:
			err = call.Error
			if err == nil && reply != nil {
				reflect.ValueOf(reply).Elem().Set(reflect.ValueOf(reply1).Elem())
			}
			return err
		case <-t.C:

		}
		_, err2 := c.Go(ctx, serviceMethod, args, reply2, call2)
		if err2 != nil {
			if uncoverError(err2) {
				c.removeClient(k, client)
			}
			err = err1
			return err
		}

		select {
		case <-ctx.Done(): //cancel by context
			err = ctx.Err()
		case call := <-call1:
			err = call.Error
			if err == nil && reply != nil && reply1 != nil {
				reflect.ValueOf(reply).Elem().Set(reflect.ValueOf(reply1).Elem())
			}
		case call := <-call2:
			err = call.Error
			if err == nil && reply != nil && reply2 != nil {
				reflect.ValueOf(reply).Elem().Set(reflect.ValueOf(reply2).Elem())
			}
		}

		return err
	default: //Failfast
		err = c.wrapCall(ctx, client, serviceMethod, args, reply)
		if err != nil {
			if uncoverError(err) {
				c.removeClient(k, client)
			}
		}

		return err
	}
}
func NewZookeeperDiscovery(zkAddr []string) ServiceDiscovery {
	zkDiscovery := &ZookeeperDiscovery{addr:zkAddr}
	go zkDiscovery.watch()
	return zkDiscovery
}
func (c *Client) selectClient(ctx context.Context, servicePath, serviceMethod string, args interface{}) (string, RPCClient, error) {
	c.mu.Lock()
	k := c.selector.Select(ctx, servicePath, serviceMethod, args)
	c.mu.Unlock()
	if k == "" {
		return "", nil, ErrXClientNoServer
	}
	client, err := c.getCachedClient(k)
	return k, client, err
}
type ServiceDiscovery interface {
	//根据服务路径获取正在提供服务器的机器
	GetServers(servicePath string) []string
	//监听服务路径下机器节点的变化
	WatchService(servicePath string) (chan plugin.ServerInRegistry,error)
}
type ZookeeperDiscovery struct {
	addr []string
	watchedService map[string][] plugin.ServerInRegistry
}

func (zk *ZookeeperDiscovery) GetServers(servicePath string) []string {

}
func (zk *ZookeeperDiscovery) WatchService(servicePath string) (chan plugin.ServerInRegistry,error){
	if _,ok := zk.watchedService[servicePath];!ok {
		zk.watchedService[servicePath] =
	}

}
//监听所有的服务路径
func (zk *ZookeeperDiscovery) watch(){
	for _, ch := range zk.chans {
		ch := ch
	}
}
type Option struct {
	Group string
	Retries int
}