package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	proto "github.com/gogo/protobuf/proto"
	pb "github.com/golang/protobuf/proto"
	"github.com/samuel/go-zookeeper/zk"
	"hirpc-go/src/codec"
	"hirpc-go/src/config"
	"hirpc-go/src/exchange"
	"hirpc-go/src/log"
	"hirpc-go/src/plugin"
	"hirpc-go/src/protocol"
	com_hirpc_entity "hirpc-go/src/protocol/protobuf"
	"hirpc-go/src/share"
	"io"
	"math"
	"net"
	"os"
	"os/signal"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// ErrServerClosed is returned by the Server's Serve, ListenAndServe after a call to Shutdown or Close.
var ErrServerClosed = errors.New("http: Server closed")

const (
	// ReaderBuffsize is used for bufio reader.
	ReaderBuffsize = 1024
	// WriterBuffsize is used for bufio writer.
	WriterBuffsize = 1024
)

// contextKey is a value for use with context.WithValue. It's used as
// a pointer so it fits in an interface{} without allocation.
type contextKey struct {
	name string
}

var (
	// RemoteConnContextKey is a context key. It can be used in
	// services with context.WithValue to access the connection arrived on.
	// The associated value will be of type net.Conn.
	RemoteConnContextKey = &contextKey{"remote-conn"}
	// StartRequestContextKey records the start time
	StartRequestContextKey = &contextKey{"start-parse-request"}
	// StartSendRequestContextKey records the start time
	StartSendRequestContextKey = &contextKey{"start-send-request"}
	// TagContextKey is used to record extra info in handling services. Its value is a map[string]interface{}
	TagContextKey = &contextKey{"service-tag"}
)

type Server struct {
	ln           net.Listener
	readTimeout  time.Duration
	writeTimeout time.Duration

	serviceMapMu sync.RWMutex
	serviceMap   map[string]*service

	mu         sync.RWMutex
	activeConn map[net.Conn]struct{}
	doneChan   chan struct{}
	seq        uint64

	inShutdown int32
	onShutdown []func(s *Server)

	// BlockCrypt for kcp.BlockCrypt
	options map[string]interface{}

	Plugins PluginContainer

	// AuthFunc can be used to auth.
	AuthFunc func(ctx context.Context, req *protocol.Message, token string) error

	handlerMsgNum int32
	zk            plugin.ZooKeeperRegister
	conf          config.Conf
}

// NewServer returns a server.
func NewServer(options ...OptionFn) *Server {
	s := &Server{
		Plugins:    &pluginContainer{},
		options:    make(map[string]interface{}),
		activeConn: make(map[net.Conn]struct{}),
		doneChan:   make(chan struct{}),
		serviceMap: make(map[string]*service),
	}

	for _, op := range options {
		op(s)
	}

	return s
}
func (s *Server) reRegisterServiceToZookeeper(event zk.Event) {
	fmt.Println(">>>>>>>>>>>>>>>>>>>")
	fmt.Println("path:", event.Path)
	fmt.Println("type:", event.Type.String())
	fmt.Println("state:", event.State.String())
	fmt.Println("<<<<<<<<<<<<<<<<<<<")
	conf := s.conf
	serverInRegistry := plugin.ServerInRegistry{}
	serverInRegistry.Host = conf.Host
	serverInRegistry.Port = conf.Port
	serverInRegistry.ApplicationName = conf.ApplicationName
	serverInRegistry.Pid = string(os.Getpid())
	serverInRegistry.Timestamp = string(time.Now().Unix())

	for serviceName, service := range s.serviceMap {
		serverInRegistry.Version = service.version
		data, _ := json.Marshal(serverInRegistry)
		fmt.Println(data)
		s.zk.Register(serviceName, share.ZkConsumerNode, string(data))
	}
}

// Serve starts and listens RPC requests.
// It is blocked until receiving connectings from clients.
func (s *Server) Serve(conf config.Conf) (err error) {
	zk := plugin.ZooKeeperRegister{
		StateChangedCallbackFunc: s.reRegisterServiceToZookeeper,
		Address:                  strings.Split(conf.RegistryAddress, ","),
	}
	s.zk = zk
	s.conf = conf
	zk.Start()
	serverInRegistry := plugin.ServerInRegistry{}
	serverInRegistry.Host = conf.Host
	serverInRegistry.Port = conf.Port
	serverInRegistry.ApplicationName = conf.ApplicationName
	serverInRegistry.Pid = string(os.Getpid())
	serverInRegistry.Timestamp = string(time.Now().Unix())

	for serviceName, service := range s.serviceMap {
		serverInRegistry.Version = service.version
		data, _ := json.Marshal(serverInRegistry)
		fmt.Println(data)
		zk.Register(serviceName, share.ZkConsumerNode, string(data))
	}

	s.startShutdownListener()
	var ln net.Listener
	ln, err = s.makeListener(network, address)
	if err != nil {
		return
	}

	return s.serveListener(ln)
}

// serveListener accepts incoming connections on the Listener ln,
// creating a new service goroutine for each.
// The service goroutines read requests and then call services to reply to them.
func (s *Server) serveListener(ln net.Listener) error {

	var tempDelay time.Duration

	s.mu.Lock()
	s.ln = ln
	s.mu.Unlock()

	for {
		conn, e := ln.Accept()
		if e != nil {
			select {
			case <-s.getDoneChan():
				return ErrServerClosed
			default:
			}

			if ne, ok := e.(net.Error); ok && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}

				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}

				log.Errorf("rpcx: Accept error: %v; retrying in %v", e, tempDelay)
				time.Sleep(tempDelay)
				continue
			}
			return e
		}
		tempDelay = 0

		if tc, ok := conn.(*net.TCPConn); ok {
			tc.SetKeepAlive(true)
			tc.SetKeepAlivePeriod(3 * time.Minute)
			tc.SetLinger(10)
		}

		conn, ok := s.Plugins.DoPostConnAccept(conn)
		if !ok {
			closeChannel(s, conn)
			continue
		}

		s.mu.Lock()
		s.activeConn[conn] = struct{}{}
		s.mu.Unlock()

		go s.serveConn(conn)
	}
}
func (s *Server) serveConn(conn net.Conn) {
	defer func() {
		if err := recover(); err != nil {
			const size = 64 << 10
			buf := make([]byte, size)
			ss := runtime.Stack(buf, false)
			if ss > size {
				ss = size
			}
			buf = buf[:ss]
			log.Errorf("serving %s panic error: %s, stack:\n %s", conn.RemoteAddr(), err, buf)
		}
		s.mu.Lock()
		delete(s.activeConn, conn)
		s.mu.Unlock()
		conn.Close()

		s.Plugins.DoPostConnClose(conn)
	}()

	if isShutdown(s) {
		closeChannel(s, conn)
		return
	}

	//r := bufio.NewReaderSize(conn, ReaderBuffsize)
	//缓存
	var requestBuf = new(bytes.Buffer)
	for {
		if isShutdown(s) {
			closeChannel(s, conn)
			return
		}

		t0 := time.Now()
		if s.readTimeout != 0 {
			conn.SetReadDeadline(t0.Add(s.readTimeout))
		}

		ctx := share.WithValue(context.Background(), RemoteConnContextKey, conn)

		req, err := s.readRequest(ctx, conn, requestBuf)
		if err != nil {
			if err == io.EOF {
				log.Infof("client has closed this connection: %s", conn.RemoteAddr().String())
			} else if strings.Contains(err.Error(), "use of closed network connection") {
				log.Infof("rpcx: connection %s is closed", conn.RemoteAddr().String())
			} else {
				log.Warnf("rpcx: failed to read request: %v", err)
			}
			return
		}

		if s.writeTimeout != 0 {
			conn.SetWriteDeadline(t0.Add(s.writeTimeout))
		}

		ctx = share.WithLocalValue(ctx, StartRequestContextKey, time.Now().UnixNano())
		var closeConn = false
		if !req.IsHeartbeat() {
			err = s.auth(ctx, req)
			closeConn = err != nil
		}

		if err != nil {
			if !req.IsOneway() {
				res := &com_hirpc_entity.ResponseInner{}
				errorString := err.Error()
				res.Error = &errorString
				protoBufCodec := share.Codecs[exchange.ProtoBuffer]
				//handleError(res, err)
				data, _ := protoBufCodec.Encode(res)

				s.Plugins.DoPreWriteResponse(ctx, req, res)
				conn.Write(data)
				s.Plugins.DoPostWriteResponse(ctx, req, res, err)
				//protocol.FreeMsg(res)
			} else {
				s.Plugins.DoPreWriteResponse(ctx, req, nil)
			}
			//protocol.FreeMsg(req)
			// auth failed, closed the connection
			if closeConn {
				log.Infof("auth failed for conn %s: %v", conn.RemoteAddr().String(), err)
				return
			}
			continue
		}
		go func() {
			atomic.AddInt32(&s.handlerMsgNum, 1)
			defer atomic.AddInt32(&s.handlerMsgNum, -1)

			if req.IsHeartbeat() {
				//req.SetMessageType(protocol.Response)
				//data := req.Encode()
				//conn.Write(data)
				return
			}

			resMetadata := make(map[string]string)
			newCtx := share.WithLocalValue(share.WithLocalValue(ctx, share.ReqMetaDataKey, req.Metadata),
				share.ResMetaDataKey, resMetadata)

			s.Plugins.DoPreHandleRequest(newCtx, req)

			res, err := s.handleRequest(newCtx, req)

			if err != nil {
				log.Warnf("rpcx: failed to handle request: %v", err)
			}

			s.Plugins.DoPreWriteResponse(newCtx, req, res)
			if !req.IsOneway() {
				if len(resMetadata) > 0 { //copy meta in context to request
					meta := res.Metadata
					if meta == nil {
						res.Metadata = resMetadata
					} else {
						for k, v := range resMetadata {
							if meta[k] == "" {
								meta[k] = v
							}
						}
					}
				}

				if len(res.Payload) > 1024 && req.CompressType() != protocol.None {
					res.SetCompressType(req.CompressType())
				}
				data := res.Encode()
				conn.Write(data)
			}
			s.Plugins.DoPostWriteResponse(newCtx, req, res, err)

			//protocol.FreeMsg(req)
			//protocol.FreeMsg(res)
		}()
	}
}
func (s *Server) handleRequest(ctx context.Context, req *exchange.Request) (res *exchange.Response, err error) {
	serviceName := req.ServicePath
	methodName := req.ServiceName

	res = exchange.NewResponse()

	s.serviceMapMu.RLock()
	service := s.serviceMap[serviceName]
	s.serviceMapMu.RUnlock()
	if service == nil {
		err = errors.New("rpcx: can't find service " + serviceName)
		return handleError(res, err)
	}
	mtype := service.method[methodName]
	if mtype == nil {
		if service.function[methodName] != nil { //check raw functions
			return s.handleRequestForFunction(ctx, req)
		}
		err = errors.New("rpcx: can't find method " + methodName)
		return handleError(res, err)
	}

	var argv = argsReplyPools.Get(mtype.ArgType)

	codec := share.Codecs[req.SerializeType()]
	if codec == nil {
		err = fmt.Errorf("can not find codec for %d", req.SerializeType())
		return handleError(res, err)
	}

	err = codec.Decode(req.Payload, argv)
	if err != nil {
		return handleError(res, err)
	}

	replyv := argsReplyPools.Get(mtype.ReplyType)

	if mtype.ArgType.Kind() != reflect.Ptr {
		err = service.call(ctx, mtype, reflect.ValueOf(argv).Elem(), reflect.ValueOf(replyv))
	} else {
		err = service.call(ctx, mtype, reflect.ValueOf(argv), reflect.ValueOf(replyv))
	}

	argsReplyPools.Put(mtype.ArgType, argv)
	if err != nil {
		argsReplyPools.Put(mtype.ReplyType, replyv)
		return handleError(res, err)
	}

	if !req.IsOneway() {
		data, err := codec.Encode(replyv)
		argsReplyPools.Put(mtype.ReplyType, replyv)
		if err != nil {
			return handleError(res, err)

		}
		res.Payload = data
	} else if replyv != nil {
		argsReplyPools.Put(mtype.ReplyType, replyv)
	}

	return res, nil
}
func (s *Server) handleRequestForFunction(ctx context.Context, req *protocol.Message) (res *protocol.Message, err error) {
	res = req.Clone()

	res.SetMessageType(protocol.Response)

	serviceName := req.ServicePath
	methodName := req.ServiceMethod
	s.serviceMapMu.RLock()
	service := s.serviceMap[serviceName]
	s.serviceMapMu.RUnlock()
	if service == nil {
		err = errors.New("rpcx: can't find service  for func raw function")
		return handleError(res, err)
	}
	mtype := service.function[methodName]
	if mtype == nil {
		err = errors.New("rpcx: can't find method " + methodName)
		return handleError(res, err)
	}

	var argv = argsReplyPools.Get(mtype.ArgType)

	codec := share.Codecs[req.SerializeType()]
	if codec == nil {
		err = fmt.Errorf("can not find codec for %d", req.SerializeType())
		return handleError(res, err)
	}

	err = codec.Decode(req.Payload, argv)
	if err != nil {
		return handleError(res, err)
	}

	replyv := argsReplyPools.Get(mtype.ReplyType)

	err = service.callForFunction(ctx, mtype, reflect.ValueOf(argv), reflect.ValueOf(replyv))

	argsReplyPools.Put(mtype.ArgType, argv)

	if err != nil {
		argsReplyPools.Put(mtype.ReplyType, replyv)
		return handleError(res, err)
	}

	if !req.IsOneway() {
		data, err := codec.Encode(replyv)
		argsReplyPools.Put(mtype.ReplyType, replyv)
		if err != nil {
			return handleError(res, err)

		}
		res.Payload = data
	} else if replyv != nil {
		argsReplyPools.Put(mtype.ReplyType, replyv)
	}

	return res, nil
}

func handleError(res *protocol.Message, err error) (*protocol.Message, error) {
	res.SetMessageStatusType(protocol.Error)
	if res.Metadata == nil {
		res.Metadata = make(map[string]string)
	}
	res.Metadata[protocol.ServiceError] = err.Error()
	return res, err
}
func (s *Server) auth(ctx context.Context, req *protocol.Message) error {
	if s.AuthFunc != nil {
		token := req.Metadata[share.AuthKey]
		return s.AuthFunc(ctx, req, token)
	}

	return nil
}
func (s *Server) readRequest(ctx context.Context, conn net.Conn, buffer *bytes.Buffer) (req *exchange.Request, err error) {
	err = s.Plugins.DoPreReadRequest(ctx)
	if err != nil {
		return nil, err
	}

	req = exchange.NewRequest()
	bufferLen := buffer.Len()
	if bufferLen != 0 {
		_, err = buffer.Read(req.Header[0:])
	}
	if bufferLen < exchange.HeaderLength {
		_, err = conn.Read(req.Header[bufferLen:])
	}
	if err != nil {
		return nil, err
	}
	if req.Header[0] != exchange.MagicHigh || req.Header[1] != exchange.MagicLow {
		for i := 1; i < exchange.HeaderLength-1; i++ {
			if req.Header[i] == exchange.MagicHigh && req.Header[i+1] == exchange.MagicLow {
				buffer.Write(req.Header[i:])
				break
			}
		}
		return nil, errors.New("unknown data when read a request")
	}
	bodyLength := req.BodyLength()
	bodyBytes := make([]byte, bodyLength)
	_, err = conn.Read(bodyBytes)

	protoBufCodec := share.Codecs[exchange.ProtoBuffer]

	var body1Length int32
	binary.Read(bytes.NewBuffer(bodyBytes[0:4]), binary.BigEndian, &body1Length)
	requestInner := &com_hirpc_entity.RequestInner{}
	err = protoBufCodec.Decode(bodyBytes[4:body1Length], requestInner)
	readIndex := body1Length
	req.ParameterType = make([]reflect.Type, len(requestInner.ParameterType))
	req.ParameterValue = make([]interface{}, len(requestInner.ParameterType))
	for i := 0; i < len(requestInner.ParameterType); i++ {
		var parameterLength int32
		binary.Read(bytes.NewBuffer(bodyBytes[readIndex:readIndex+4]), binary.BigEndian, &parameterLength)
		parameterType := (*PbTypeMap)[requestInner.ParameterType[i]]
		//TODO 为何需要加*?
		var parameterValue = argsReplyPools.Get(parameterType)
		err = protoBufCodec.Decode(bodyBytes[readIndex+4:readIndex+4+parameterLength], parameterValue)
		req.ParameterType[i] = parameterType

		req.ParameterValue[i] = parameterValue
		readIndex += 4 + parameterLength
	}

	perr := s.Plugins.DoPostReadRequest(ctx, req, err)
	if err == nil {
		err = perr
	}
	return req, err
}
func isShutdown(s *Server) bool {
	return atomic.LoadInt32(&s.inShutdown) == 1
}
func closeChannel(s *Server, conn net.Conn) {
	s.mu.Lock()
	delete(s.activeConn, conn)
	s.mu.Unlock()
	conn.Close()
}
func (s *Server) getDoneChan() <-chan struct{} {
	return s.doneChan
}
func (s *Server) startShutdownListener() {
	go func(s *Server) {
		log.Info("server pid:", os.Getpid())
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGTERM)
		si := <-c
		if si.String() == "terminated" {
			if nil != s.onShutdown && len(s.onShutdown) > 0 {
				for _, sd := range s.onShutdown {
					sd(s)
				}
			}
		}
	}(s)
}
