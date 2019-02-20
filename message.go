package myserverframework

import (
	"context"
	"bytes"
	"fmt"
	"encoding/binary"
	"github.com/leesper/holmes"
	"io"
	"net"
)

const (
	// HeartBeat is the default heart beat message number.
	HeartBeat = 0
)

//负责处理消息
type Handler interface {
	Handle(context.Context,interface{})
}

//定义handlerfunc函数类型，需要能够处理writecloser，writecloser字面意义即为含有写和关闭方法，就是服务端或客户端的conn
type HandlerFunc func(context.Context, WriteCloser)

//handlerFunc的handle方法，即是调用自己
func (f HandlerFunc) Handle(ctx context.Context, c WriteCloser) {
	f(ctx, c)
}

//定义反序列化函数类型，能够反序列化字节流为message
type UnmarshalFunc func([]byte) (Message, error)

// handlerUnmarshaler既能handle conn又能反序列化字节流，注册消息时需要注册这两个函数
type handlerUnmarshaler struct {
	handler     HandlerFunc
	unmarshaler UnmarshalFunc
}

var (
	buf *bytes.Buffer
	//全局的消息类型注册表，需要注册handle和反序列化两个函数
	messageRegistry map[int32]handlerUnmarshaler
)

//func init() {
//	messageRegistry = map[int32]handlerUnmarshaler{}
//	buf = new(bytes.Buffer)
//}

//注册消息，一个消息类型只能注册一次，由一个int32的消息ID来标识
func Register(msgType int32, unmarshaler func([]byte) (Message, error), handler func(context.Context, WriteCloser)) {
	if _, ok := messageRegistry[msgType]; ok {
		panic(fmt.Sprintf("trying to register message %d twice", msgType))
	}

	messageRegistry[msgType] = handlerUnmarshaler{
		unmarshaler: unmarshaler,
		handler:     HandlerFunc(handler),
	}
}

//由全局消息类型表，通过消息类型ID获取反序列化函数
func GetUnmarshalFunc(msgType int32) UnmarshalFunc {
	entry, ok := messageRegistry[msgType]
	if !ok {
		return nil
	}
	return entry.unmarshaler
}

// 由全局消息类型表，通过消息类型ID获取handle函数
func GetHandlerFunc(msgType int32) HandlerFunc {
	entry, ok := messageRegistry[msgType]
	if !ok {
		return nil
	}
	return entry.handler
}

// 消息接口 消息要有消息ID以及序列化方法
type Message interface {
	MessageNumber() int32
	Serialize() ([]byte, error)
}

// 心跳消息，维护一个时间戳
type HeartBeatMessage struct {
	Timestamp int64
}

// Serialize serializes HeartBeatMessage into bytes.
func (hbm HeartBeatMessage) Serialize() ([]byte, error) {
	buf.Reset()
	err := binary.Write(buf, binary.LittleEndian, hbm.Timestamp)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

//实现messageNumber
func (hbm HeartBeatMessage) MessageNumber() int32 {
	return HeartBeat
}

//实现心跳消息的反序列化，只是具有相同的结构即可
func DeserializeHeartBeat(data []byte) (message Message, err error) {
	var timestamp int64
	if data == nil {
		return nil, ErrNilData
	}
	buf := bytes.NewReader(data)
	err = binary.Read(buf, binary.LittleEndian, &timestamp)
	if err != nil {
		return nil, err
	}
	return HeartBeatMessage{
		Timestamp: timestamp,
	}, nil
}

// 处理心跳消息，消息全部存在于context中，区分服务与客户端
func HandleHeartBeat(ctx context.Context, c WriteCloser) {
	msg := MessageFromContext(ctx)
	switch c := c.(type) {
	case *ServerConn:
		c.SetHeartBeat(msg.(HeartBeatMessage).Timestamp)
	case *ClientConn:
		c.SetHeartBeat(msg.(HeartBeatMessage).Timestamp)
	}
}

//编解码接口
type Codec interface {
	Decode(net.Conn) (Message, error)
	Encode(Message) ([]byte, error)
}

//tlv编码
type TypeLengthValueCodec struct{}

//tlv解码方法
func (codec TypeLengthValueCodec) Decode(raw net.Conn) (Message, error) {
	byteChan := make(chan []byte)
	errorChan := make(chan error)

	//异步解出消息类型T
	go func(bc chan []byte, ec chan error) {
		typeData := make([]byte, MessageTypeBytes)
		_, err := io.ReadFull(raw, typeData)  //readfull返回buf长度的数据（4）
		if err != nil {
			ec <- err
			close(bc)
			close(ec)
			holmes.Debugln("go-routine read message type exited")
			return
		}
		bc <- typeData
	}(byteChan, errorChan)

	var typeBytes []byte

	select {
	case err := <-errorChan:
		return nil, err

	case typeBytes = <-byteChan:
		if typeBytes == nil {
			holmes.Warnln("read type bytes nil")
			return nil, ErrBadData
		}
		typeBuf := bytes.NewReader(typeBytes)
		var msgType int32
		//读取出具体的type,注意这里不是使用conn.read,需要自己实现
		if err := binary.Read(typeBuf, binary.LittleEndian, &msgType); err != nil {
			return nil, err
		}

		lengthBytes := make([]byte, MessageLenBytes) //读取length
		_, err := io.ReadFull(raw, lengthBytes)
		if err != nil {
			return nil, err
		}
		lengthBuf := bytes.NewReader(lengthBytes)
		var msgLen uint32
		if err = binary.Read(lengthBuf, binary.LittleEndian, &msgLen); err != nil {
			return nil, err
		}
		if msgLen > MessageMaxBytes {  //防止过长的消息
			holmes.Errorf("message(type %d) has bytes(%d) beyond max %d\n", msgType, msgLen, MessageMaxBytes)
			return nil, ErrBadData
		}

		// 读取具体的内容，然后再经由消息自己的反序列化函数反序列化
		msgBytes := make([]byte, msgLen)
		_, err = io.ReadFull(raw, msgBytes)
		if err != nil {
			return nil, err
		}

		// deserialize message from bytes
		unmarshaler := GetUnmarshalFunc(msgType)
		if unmarshaler == nil {
			return nil, ErrUndefined(msgType)
		}
		return unmarshaler(msgBytes)
	}
}

// Encode encodes the message into bytes data.
func (codec TypeLengthValueCodec) Encode(msg Message) ([]byte, error) {
	data, err := msg.Serialize() // 由定义消息时的序列化方法来序列化
	if err != nil {
		return nil, err
	}
	buf := new(bytes.Buffer) //编码tlv格式
	binary.Write(buf, binary.LittleEndian, msg.MessageNumber())
	binary.Write(buf, binary.LittleEndian, int32(len(data)))
	buf.Write(data)
	packet := buf.Bytes()
	return packet, nil
}

// ContextKey is the key type for putting context-related data.
type contextKey string

// 数据全部由context传递，共有三种类型，消息，服务器，客户端的ID
const (
	messageCtx contextKey = "message"
	serverCtx  contextKey = "server"
	netIDCtx   contextKey = "netid"
)

// 由现有context分出新的context，携带消息
func NewContextWithMessage(ctx context.Context, msg Message) context.Context {
	return context.WithValue(ctx, messageCtx, msg)
}

// 从context中解出消息
func MessageFromContext(ctx context.Context) Message {
	return ctx.Value(messageCtx).(Message)
}

// 由现有context分出新的context，携带ID
func NewContextWithNetID(ctx context.Context, netID int64) context.Context {
	return context.WithValue(ctx, netIDCtx, netID)
}

// 从context中解出ID
func NetIDFromContext(ctx context.Context) int64 {
	return ctx.Value(netIDCtx).(int64)
}
