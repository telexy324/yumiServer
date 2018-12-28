package myserverframework

import (
	"sync"
	"time"
	"github.com/leesper/holmes"
	"net"
	"context"
)

const (
	// MessageTypeBytes is the length of type header.
	MessageTypeBytes = 4
	// MessageLenBytes is the length of length header.
	MessageLenBytes = 4
	// MessageMaxBytes is the maximum bytes allowed for application data.
	MessageMaxBytes = 1 << 23 // 8M
)

// MessageHandler is a combination of message and its handler function.
type MessageHandler struct {
	message Message
	handler HandlerFunc
}

// WriteCloser is the interface that groups Write and Close methods.
type WriteCloser interface {
	Write(Message) error
	Close()
}

// 代表连接到server的一个链接
type ServerConn struct {
	netid   int64
	belong  *Server // 代表连接到哪个server
	rawConn net.Conn

	once      *sync.Once
	wg        *sync.WaitGroup
	sendCh    chan []byte
	handlerCh chan MessageHandler
	timerCh   chan *OnTimeOut

	mu      sync.Mutex // guards following
	name    string
	heart   int64
	pending []int64
	ctx     context.Context
	cancel  context.CancelFunc
}

//创建新的serverconn
func NewServerConn(id int64, s *Server, c net.Conn) *ServerConn {
	sc := &ServerConn{
		netid:     id,
		belong:    s,
		rawConn:   c,
		once:      &sync.Once{},
		wg:        &sync.WaitGroup{},
		sendCh:    make(chan []byte, s.opts.bufferSize),
		handlerCh: make(chan MessageHandler, s.opts.bufferSize),
		timerCh:   make(chan *OnTimeOut, s.opts.bufferSize),
		heart:     time.Now().UnixNano(), //每个连接有一个心跳
	}
	sc.ctx, sc.cancel = context.WithCancel(context.WithValue(s.ctx, serverCtx, s))  //由server的context派生出的context,包含本次连接所属server的信息
	sc.name = c.RemoteAddr().String()
	sc.pending = []int64{}
	return sc
}

// 返回context中记录的server信息
func ServerFromContext(ctx context.Context) (*Server, bool) {
	server, ok := ctx.Value(serverCtx).(*Server)
	return server, ok
}

// NetID returns net ID of server connection.
func (sc *ServerConn) NetID() int64 {
	return sc.netid
}

// SetName sets name of server connection.
func (sc *ServerConn) SetName(name string) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.name = name
}

// Name returns the name of server connection.
func (sc *ServerConn) Name() string {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	name := sc.name
	return name
}

// SetHeartBeat sets the heart beats of server connection.
func (sc *ServerConn) SetHeartBeat(heart int64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.heart = heart
}

// HeartBeat returns the heart beats of server connection.
func (sc *ServerConn) HeartBeat() int64 {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	heart := sc.heart
	return heart
}

// SetContextValue sets extra data to server connection.
func (sc *ServerConn) SetContextValue(k, v interface{}) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.ctx = context.WithValue(sc.ctx, k, v)
}

// ContextValue gets extra data from server connection.
func (sc *ServerConn) ContextValue(k interface{}) interface{} {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.ctx.Value(k)
}

// Start starts the server connection, creating go-routines for reading,
// writing and handlng.
func (sc *ServerConn) Start() {
	holmes.Infof("conn start, <%v -> %v>\n", sc.rawConn.LocalAddr(), sc.rawConn.RemoteAddr())
	onConnect := sc.belong.opts.onConnect //调用注册的onconnect方法
	if onConnect != nil {
		onConnect(sc)
	}

	loopers := []func(WriteCloser, *sync.WaitGroup){readLoop, writeLoop, handleLoop}
	for _, l := range loopers {
		looper := l
		sc.wg.Add(1)
		go looper(sc, sc.wg) //为每个连接起3个go程，由serverconn的context控制
	}
}

// 优雅关闭，等待3个go程都退出再退出，实现了writecloser的方法
func (sc *ServerConn) Close() {
	sc.once.Do(func() {
		holmes.Infof("conn close gracefully, <%v -> %v>\n", sc.rawConn.LocalAddr(), sc.rawConn.RemoteAddr())

		// callback on close
		onClose := sc.belong.opts.onClose
		if onClose != nil {
			onClose(sc)
		}

		// 服务器的总连接数减1
		sc.belong.conns.Delete(sc.netid)
		addTotalConn(-1)

		// close net.Conn, any blocked read or write operation will be unblocked and
		// return errors.
		if tc, ok := sc.rawConn.(*net.TCPConn); ok {
			// avoid time-wait state
			tc.SetLinger(0)  //立即结束（不发送fin包）清空缓存，默认是发送fin包，得到确认后再清理
		}
		sc.rawConn.Close()

		// cancel readLoop, writeLoop and handleLoop go-routines.
		sc.mu.Lock()
		sc.cancel() //取消3个go程
		//TODO
		pending := sc.pending //pending表示有多少个待处理的定时任务？
		sc.pending = nil //退出时清空pengding
		sc.mu.Unlock()

		// clean up pending timers
		for _, id := range pending {
			sc.CancelTimer(id) //TODO
		}

		// wait until all go-routines exited.
		sc.wg.Wait()

		// close all channels and block until all go-routines exited.
		close(sc.sendCh)
		close(sc.handlerCh)
		close(sc.timerCh)

		// tell server I'm done :( .
		sc.belong.wg.Done()
	})
}

// AddPendingTimer adds a timer ID to server Connection.
func (sc *ServerConn) AddPendingTimer(timerID int64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.pending != nil {
		sc.pending = append(sc.pending, timerID)
	}
}

// Write writes a message to the client. 实现了writecloser的方法
func (sc *ServerConn) Write(message Message) error {
	return asyncWrite(sc, message)
}

// RunAt runs a callback at the specified timestamp.
func (sc *ServerConn) RunAt(timestamp time.Time, callback func(time.Time, WriteCloser)) int64 {
	id := runAt(sc.ctx, sc.netid, sc.belong.timing, timestamp, callback)
	if id >= 0 {
		sc.AddPendingTimer(id)
	}
	return id
}

// RunAfter runs a callback right after the specified duration ellapsed.
func (sc *ServerConn) RunAfter(duration time.Duration, callback func(time.Time, WriteCloser)) int64 {
	id := runAfter(sc.ctx, sc.netid, sc.belong.timing, duration, callback)
	if id >= 0 {
		sc.AddPendingTimer(id)
	}
	return id
}

// RunEvery runs a callback on every interval time.
func (sc *ServerConn) RunEvery(interval time.Duration, callback func(time.Time, WriteCloser)) int64 {
	id := runEvery(sc.ctx, sc.netid, sc.belong.timing, interval, callback)
	if id >= 0 {
		sc.AddPendingTimer(id)
	}
	return id
}

func runAt(ctx context.Context, netID int64, timing *TimingWheel, ts time.Time, cb func(time.Time, WriteCloser)) int64 {
	timeout := NewOnTimeOut(NewContextWithNetID(ctx, netID), cb)
	return timing.AddTimer(ts, 0, timeout)
}

func runAfter(ctx context.Context, netID int64, timing *TimingWheel, d time.Duration, cb func(time.Time, WriteCloser)) int64 {
	delay := time.Now().Add(d)
	return runAt(ctx, netID, timing, delay, cb)
}

func runEvery(ctx context.Context, netID int64, timing *TimingWheel, d time.Duration, cb func(time.Time, WriteCloser)) int64 {
	delay := time.Now().Add(d)
	timeout := NewOnTimeOut(NewContextWithNetID(ctx, netID), cb)
	return timing.AddTimer(delay, d, timeout)
}

func asyncWrite(c interface{}, m Message) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = ErrServerClosed
		}
	}()

	var (
		pkt    []byte
		sendCh chan []byte
	)
	switch c := c.(type) {
	case *ServerConn:
		pkt, err = c.belong.opts.codec.Encode(m)
		sendCh = c.sendCh

	case *ClientConn:
		pkt, err = c.opts.codec.Encode(m)
		sendCh = c.sendCh
	}

	if err != nil {
		holmes.Errorf("asyncWrite error %v\n", err)
		return
	}

	select {
	case sendCh <- pkt: //能正确编码消息，这里一定是能发送的
		err = nil
	default:
		err = ErrWouldBlock
	}
	return
}