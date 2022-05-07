package GRING

import (
	"bufio"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	//"encoding/hex"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"io"
	"net"
	"sync"
	"time"
	"math"
)

type clientSide bool

var starttime time.Time
var duration time.Duration
var datasize int

const (
	clientSideInbound  clientSide = false
	clientSideOutbound clientSide = true
	NO_OP = 0xFF
)

func (c clientSide) String() string {
	if c {
		return "outbound"
	}

	return "inbound"
}

// Client represents an pooled inbound/outbound connection under some node. Should a client successfully undergo
// GRING's protocol handshake, information about the peer representative of this client, such as its ID is available.
//
// A clients connection may be closed through (*Client).Close, through the result of a failed handshake, through
// exceeding the max inbound/outbound connection count configured on the clients associated node, through a node
// gracefully being stopped, through a Handler configured on the node returning an error upon recipient of some data,
// or through receiving unexpected/suspicious data.
//
// The lifecycle of a client may be controlled through (*Client).WaitUntilReady, and (*Client).WaitUntilClosed. It
// provably has been useful in writing unit tests where a client instance is used under high concurrency scenarios.
//
// A client in total has two goroutines associated to it: a goroutine responsible for handling writing messages, and a
// goroutine responsible for handling the recipient of messages.
type Client struct {
	node *Node

	id ID

	addr string
	side clientSide

	suite cipher.AEAD

	logger struct {
		sync.RWMutex
		*zap.Logger
	}

	conn net.Conn

	reader *bufio.Reader
	writer *bufio.Writer

	readerBuf []byte
	writerBuf []message

	writerCond   sync.Cond
	writerClosed bool

	requests *requestMap
	requests_recv *requestMap

	ready      chan struct{}
	readerDone chan struct{}
	writerDone chan struct{}
	clientDone chan struct{}

	err struct {
		sync.Mutex
		error
	}

	closeOnce sync.Once
}

func newClient(node *Node) *Client {
        //fmt.Printf("new client\n")
	c := &Client{
		node: node,

		requests: newRequestMap(),
		requests_recv: newRequestMap(),

		readerBuf: make([]byte, 4+node.maxRecvMessageSize),

		ready:      make(chan struct{}),
		readerDone: make(chan struct{}),
		writerDone: make(chan struct{}),

		clientDone: make(chan struct{}),
	}

	c.writerCond.L = &sync.Mutex{}

	c.SetLogger(node.logger)

	return c
}

// ID returns an immutable copy of the ID of this client, which is established once the client has successfully
// completed the handshake protocol configured from this clients associated node.
//
// ID may be called concurrently.
func (c *Client) ID() ID {
	return c.id
}

// Logger returns the underlying logger associated to this client. It may optionally be set via (*Client).SetLogger.
//
// Logger may be called concurrently.
func (c *Client) Logger() *zap.Logger {
	c.logger.RLock()
	defer c.logger.RUnlock()

	return c.logger.Logger
}

// SetLogger updates the logger instance of this client.
//
// SetLogger may be called concurrently.
func (c *Client) SetLogger(logger *zap.Logger) {
	c.logger.Lock()
	defer c.logger.Unlock()

	c.logger.Logger = logger
}

// Close asynchronously kills the underlying connection and signals all goroutines to stop underlying this client.
//
// Close may be called concurrently.
func (c *Client) Close() {
	c.close()
}

// WaitUntilReady pauses the goroutine to which it was called within until/unless the client has successfully
// completed/failed the handshake protocol configured under the node instance to which this peer was derived from.
//
// It pauses the goroutine by reading from a channel that is closed when the client has successfully completed/failed
// the aforementioned handshake protocol.
//
// WaitUntilReady may be called concurrently.
func (c *Client) WaitUntilReady() {
	c.waitUntilReady()
}

// WaitUntilClosed pauses the goroutine to which it was called within until all goroutines associated to this client
// has been closed. The goroutines associated to this client would only close should:
//
// 1) handshaking failed/succeeded,
// 2) the connection was dropped, or
// 3) (*Client).Close was called.
//
// WaitUntilReady may be called concurrently.
func (c *Client) WaitUntilClosed() {
	c.waitUntilClosed()
}

// Error returns the very first error that has caused this clients connection to have dropped.
//
// Error may be called concurrently.
func (c *Client) Error() error {
	c.err.Lock()
	defer c.err.Unlock()

	return c.err.error
}

func (c *Client) reportError(err error) {
	c.err.Lock()
	defer c.err.Unlock()

	if c.err.error == nil {
		c.err.error = err
	}
}

func (c *Client) close() {
	c.closeOnce.Do(func() {
		c.writerCond.L.Lock()
		c.writerClosed = true
		c.writerCond.Signal()
		c.writerCond.L.Unlock()

		if c.conn != nil {
			c.conn.Close()
		}
	})
}

func (c *Client) waitUntilReady() {
	<-c.ready
}

func (c *Client) waitUntilClosed() {
	<-c.clientDone
}

func (c *Client) outbound(ctx context.Context, addr string, opcode ...byte) {
	c.addr = addr
	c.side = clientSideInbound

	defer func() {
		c.node.outbound.remove(addr)
		close(c.clientDone)
	}()

	var dialer net.Dialer

	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		c.reportError(err)
		close(c.ready)
		close(c.writerDone)
		close(c.readerDone)
		return
	}

	c.reader = bufio.NewReader(conn)
	c.writer = bufio.NewWriter(conn)
	c.conn = conn

	c.handshakeOutbound(opcode...)

	go c.writeLoop()
	c.recvLoop()
	c.close()

	//fmt.Printf("JHJH %s outbound Peer connection closed.\n",c.id.Address)
	//c.Logger().Debug("Peer connection closed.")

	for _, protocol := range c.node.protocols {
		if protocol.OnPeerDisconnected == nil {
			continue
		}

		protocol.OnPeerDisconnected(c)
	}
}

func (c *Client) inbound(conn net.Conn, addr string) {
	c.addr = addr
	c.side = clientSideOutbound

	defer func() {
		c.node.inbound.remove(addr)
		close(c.clientDone)
	}()

	c.reader = bufio.NewReader(conn)
	c.writer = bufio.NewWriter(conn)
	c.conn = conn

	c.handshakeInbound()

	if c.Error() != nil {
	        //fmt.Printf("JHJH inbound bread1 Err: %v\n",  c.Error())
		c.close()
		return
	}

	go c.writeLoop()
	c.recvLoop()
	c.close()

	//fmt.Printf("JHJH %s inbound Peer connection closed. %s\n",c.id.Address, c.addr)

	for _, protocol := range c.node.protocols {
		if protocol.OnPeerDisconnected == nil {
			continue
		}

		protocol.OnPeerDisconnected(c)
	}
}

func (c *Client) read() ([]byte, error) {
	if c.node.idleTimeout > 0 {
		if err := c.conn.SetReadDeadline(time.Now().Add(c.node.idleTimeout)); err != nil {
			return nil, err
		}
	}

	if _, err := io.ReadFull(c.reader, c.readerBuf[:4]); err != nil {
		return nil, err
	}

	size := binary.BigEndian.Uint32(c.readerBuf[:4])

	if c.node.maxRecvMessageSize > 0 && size > c.node.maxRecvMessageSize {
		//fmt.Printf("JHJH got %d bytes, limit is set to %d", size, c.node.maxRecvMessageSize)
		return nil, fmt.Errorf("got %d bytes, but limit is set to %d: %w", size, c.node.maxRecvMessageSize, ErrMessageTooLarge)
	}

	if _, err := io.ReadFull(c.reader, c.readerBuf[4:size+4]); err != nil {
		return nil, err
	}

	if c.suite == nil {
		return c.readerBuf[4 : size+4], nil
	}

	buf, err := decryptAEAD(c.suite, c.readerBuf[4:size+4])
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func (c *Client) write(data []byte) error {
	if c.node.idleTimeout > 0 {
		if err := c.conn.SetWriteDeadline(time.Now().Add(c.node.idleTimeout)); err != nil {
			return err
		}
	}

	if c.suite != nil {
		var err error

		if data, err = encryptAEAD(c.suite, data); err != nil {
			return err
		}
	}

	data = append(make([]byte, 4), data...)
	binary.BigEndian.PutUint32(data[:4], uint32(len(data)-4))

	if _, err := c.writer.Write(data); err != nil {
		return err
	}

	return c.writer.Flush()
}

func (c *Client) send(nonce uint64, recv_nonce uint64, data []byte, length uint32) error {
	c.writerCond.L.Lock()
	//c.writeSingle(message{nonce: nonce, length: length, data: data})
	c.writerBuf = append(c.writerBuf, message{length: length, nonce: nonce, recv_nonce: recv_nonce, data: data})
	//fmt.Printf("writeBuf message len=%d\n",len(c.writerBuf))
	c.writerCond.Signal()

	c.writerCond.L.Unlock()

	return nil
}

func (c *Client) request(ctx context.Context, data []byte, length uint32) (message, error) {
	// Figure out an available request nonce.

	ch, nonce, err := c.requests.nextNonce()
	if err != nil {
                fmt.Printf("request nonce error\n")
		return message{}, err
	}

	// Send request.
	if err := c.send(nonce, math.MaxUint64, data, length); err != nil {
                fmt.Printf("request send error\n")
		c.requests.markRequestFailed(nonce)
		return message{}, err
	}

        //fmt.Printf("wait for ACK\n")

	var msg message

	select {
	case msg = <-ch:
		if msg.nonce == 0 {
                        fmt.Printf("request: receive ACK nonce %d\n",msg.nonce)
			return message{}, io.EOF
		}
	case <-ctx.Done():
                //fmt.Printf("ctx done\n")
		return message{}, ctx.Err()
	}

	return msg, nil
}

// inbound doesn't have opcode in function argument
func (c *Client) handshakeInbound() {
	defer close(c.ready)

	var opcode []byte

	// Generate Ed25519 ephemeral keypair to perform a Diffie-Hellman handshake.

	pub, sec, err := GenerateKeys(nil)
	if err != nil {
		c.reportError(err)
		return
	}

	// Send our Ed25519 ephemeral public key and signature of the message '.__GRING_handshake'.

	signature := sec.Sign([]byte(".__GRING_handshake"))

	if err := c.write(append(pub[:], signature[:]...)); err != nil {
		c.reportError(fmt.Errorf("failed to send session handshake: %w", err))
		return
	}

	// Read from our peer their Ed25519 ephemeral public key and signature of the message '.__GRING_handshake'.

	data, err := c.read()
	if err != nil {
		c.reportError(err)
		return
	}

	if len(data) != SizePublicKey+SizeSignature {
		c.reportError(fmt.Errorf("received invalid number of bytes opening a session: expected %d byte(s), but got %d byte(s)",
			SizePublicKey+SizeSignature,
			len(data),
		))

		return
	}

	var peerPublicKey PublicKey
	copy(peerPublicKey[:], data[:SizePublicKey])

	// Verify ownership of our peers Ed25519 public key by verifying the signature they sent.

	if !peerPublicKey.Verify([]byte(".__GRING_handshake"), UnmarshalSignature(data[SizePublicKey:SizePublicKey+SizeSignature])) {
		c.reportError(errors.New("could not verify session handshake"))
		return
	}

	// Transform all Ed25519 points to Curve25519 points and perform a Diffie-Hellman handshake
	// to derive a shared key.

	shared, err := ECDH(sec, peerPublicKey)
	if err != nil {
		c.reportError(err)
		return
	}

	// Use the derived shared key from Diffie-Hellman to encrypt/decrypt all future communications
	// with AES-256 Galois Counter Mode (GCM).

	core, err := aes.NewCipher(shared[:])
	if err != nil {
		c.reportError(fmt.Errorf("could not instantiate aes: %w", err))
		return
	}

	suite, err := cipher.NewGCM(core)
	if err != nil {
		c.reportError(fmt.Errorf("could not instantiate aes-gcm: %w", err))
		return
	}

	c.suite = suite

	// Send to our peer our overlay ID.

	buf := c.node.id.Marshal()
	signature = c.node.Sign(append(buf, shared...))
	buf = append(buf, signature[:]...)

	if err := c.write(buf); err != nil {
		c.reportError(fmt.Errorf("failed to send session handshake: %w", err))
		return
	}

	// Read and parse from our peer their overlay ID.

	data, err = c.read()
	if err != nil {
		c.reportError(fmt.Errorf("failed to read overlay handshake: %w", err))
		return
	}

	id, err := UnmarshalID(data)
	if err != nil {
		c.reportError(fmt.Errorf("failed to parse peer id while handling overlay handshake: %w", err))
		return
	}

	// Validate the peers ownership of the overlay ID.

	buf = make([]byte, id.Size())
	copy(buf, data)

	if len(data) != len(buf)+SizeSignature {
		c.reportError(fmt.Errorf("received invalid number of bytes handshaking: expected %d byte(s), got %d byte(s)",
			len(buf)+SizeSignature,
			len(data),
		))

		return
	}

	if !id.ID.Verify(append(buf, shared...), UnmarshalSignature(data[len(buf):len(buf)+SizeSignature])) {
		c.reportError(errors.New("overlay handshake signature is malformed"))
		return
	}

	c.id = id
/*
	c.SetLogger(c.Logger().With(
		zap.String("peer_id", id.ID.String()),
		zap.String("peer_addr", id.Address),
		zap.String("remote_addr", c.conn.RemoteAddr().String()),
		zap.String("session_key", hex.EncodeToString(shared[:])),
	))
*/
	//fmt.Printf("JHJH handshakeinbound peer_addr %s\n remote_addr %s\n",id.Address,c.conn.RemoteAddr().String())

	// Read from our peer their opcode
	data, err = c.read()
	if err != nil {
                fmt.Printf("handshake : recv opcode error\n")
		// if nothing received, we do nothing here
		// if this is outbound, simply we use opcode in the argument
		// if this is inbound and doesn't have opcode received, counterpart outbound didn't send the opcode
	}else{
		// if there is an opcode we received, we pass this opcode
		opcode = data
                //fmt.Printf("handshake : receive protocol opcode : %d\n",opcode)
		var op uint16
		op = binary.BigEndian.Uint16(data[:2])
		//fmt.Printf("handshake : receive protocol opcode : %d\n",op)
		if op == uint16(NO_OP) {
		    //fmt.Printf("handshake : receive protocol opcode : NO_OP %d\n",int(op))
                    opcode = nil
                }else{
	            opcode = data[:2]
	        }
	}


	c.Logger().Debug("Peer connection opened.")

        for _, protocol := range c.node.protocols {
		if protocol.OnPeerConnected == nil {
			continue
		}

		protocol.OnPeerConnected(true, c, opcode...)
        }
}


// outbound could have opcode in function arguement
func (c *Client) handshakeOutbound(opcode ...byte) {
	defer close(c.ready)

	// Generate Ed25519 ephemeral keypair to perform a Diffie-Hellman handshake.

	pub, sec, err := GenerateKeys(nil)
	if err != nil {
		//fmt.Printf("JHJH handshakeoutbound fail to gen key: %w\n", err)
		c.reportError(err)
		return
	}

	// Send our Ed25519 ephemeral public key and signature of the message '.__GRING_handshake'.

	signature := sec.Sign([]byte(".__GRING_handshake"))

	if err := c.write(append(pub[:], signature[:]...)); err != nil {
		//fmt.Printf("JHJH handshakeoutbound failed to send session handshake: %w", err)
		c.reportError(fmt.Errorf("failed to send session handshake: %w", err))
		return
	}

	// Read from our peer their Ed25519 ephemeral public key and signature of the message '.__GRING_handshake'.

	data, err := c.read()
	if err != nil {
		//fmt.Printf("JHJH handshakeoutbound failed to read pub key: %w", err)
		c.reportError(err)
		return
	}

	if len(data) != SizePublicKey+SizeSignature {
		c.reportError(fmt.Errorf("received invalid number of bytes opening a session: expected %d byte(s), but got %d byte(s)",
			SizePublicKey+SizeSignature,
			len(data),
		))

		return
	}

	var peerPublicKey PublicKey
	copy(peerPublicKey[:], data[:SizePublicKey])

	// Verify ownership of our peers Ed25519 public key by verifying the signature they sent.

	if !peerPublicKey.Verify([]byte(".__GRING_handshake"), UnmarshalSignature(data[SizePublicKey:SizePublicKey+SizeSignature])) {
		c.reportError(errors.New("could not verify session handshake"))
		return
	}

	// Transform all Ed25519 points to Curve25519 points and perform a Diffie-Hellman handshake
	// to derive a shared key.

	shared, err := ECDH(sec, peerPublicKey)
	if err != nil {
		c.reportError(err)
		return
	}

	// Use the derived shared key from Diffie-Hellman to encrypt/decrypt all future communications
	// with AES-256 Galois Counter Mode (GCM).

	core, err := aes.NewCipher(shared[:])
	if err != nil {
		c.reportError(fmt.Errorf("could not instantiate aes: %w", err))
		return
	}

	suite, err := cipher.NewGCM(core)
	if err != nil {
		c.reportError(fmt.Errorf("could not instantiate aes-gcm: %w", err))
		return
	}

	c.suite = suite

	// Send to our peer our overlay ID.

	buf := c.node.id.Marshal()
	signature = c.node.Sign(append(buf, shared...))
	buf = append(buf, signature[:]...)

	if err := c.write(buf); err != nil {
		c.reportError(fmt.Errorf("failed to send session handshake: %w", err))
		return
	}

	// Read and parse from our peer their overlay ID.

	data, err = c.read()
	if err != nil {
		c.reportError(fmt.Errorf("failed to read overlay handshake: %w", err))
		return
	}

	id, err := UnmarshalID(data)
	if err != nil {
		c.reportError(fmt.Errorf("failed to parse peer id while handling overlay handshake: %w", err))
		return
	}

	// Validate the peers ownership of the overlay ID.

	buf = make([]byte, id.Size())
	copy(buf, data)

	if len(data) != len(buf)+SizeSignature {
		c.reportError(fmt.Errorf("received invalid number of bytes handshaking: expected %d byte(s), got %d byte(s)",
			len(buf)+SizeSignature,
			len(data),
		))

		return
	}

	if !id.ID.Verify(append(buf, shared...), UnmarshalSignature(data[len(buf):len(buf)+SizeSignature])) {
		c.reportError(errors.New("overlay handshake signature is malformed"))
		return
	}

	c.id = id
/*
	c.SetLogger(c.Logger().With(
		zap.String("peer_id", id.ID.String()),
		zap.String("peer_addr", id.Address),
		zap.String("remote_addr", c.conn.RemoteAddr().String()),
		zap.String("session_key", hex.EncodeToString(shared[:])),
	))
*/
        //fmt.Printf("JHJH handshakeoutbound peer_addr %s remote_addr %s\n",id.Address,c.conn.RemoteAddr().String())
        if len(opcode) != 0 {
	    // send opcode to peer
            if err := c.write(opcode); err != nil {
		c.reportError(fmt.Errorf("failed to send session handshake: %w", err))
		return
	    }
	    //fmt.Printf("handshake : send protocol opcode : %d\n",opcode)
	}else{
	    buf := make([]byte, 2)
	    binary.BigEndian.PutUint16(buf[:2], NO_OP)
	    if err := c.write(buf); err != nil {
		c.reportError(fmt.Errorf("failed to send session handshake: %w", err))
                return
            }
	}

	c.Logger().Debug("Peer connection opened.")

        for _, protocol := range c.node.protocols {
		if protocol.OnPeerConnected == nil {
			continue
		}

		protocol.OnPeerConnected(false, c, opcode...)
        }
}

func (c *Client) recvBulkHandleLoop(recv_nonce uint64, first_msg message, ch chan message) {
	var msg message
        dummy := make([]byte, 1)
	nonce := first_msg.nonce

	first_msg.data = append([]byte{}, first_msg.data...)
        //fmt.Printf("start bulk loop. receive first partial. nonce:%d recv_nonce:%d legnth:%d\n",nonce,recv_nonce, first_msg.length)

        if err := c.send(nonce, recv_nonce, dummy, 0); err != nil {
            // TODO : error handle
            fmt.Printf("Error send ACK nonce:%d recv_nonce:%d length:%d dummy size:%d\n",nonce,recv_nonce,first_msg.length,len(dummy))
	    return
        }
        //fmt.Printf("sent ACK nonce:%d recv_nonce:%d length:%d\n",nonce, recv_nonce, first_msg.length)

	for {
            //fmt.Printf("wait for data nonce:%d recv_nonce:%d\n",nonce, recv_nonce)
	    select {
	        case msg = <-ch:
	            first_msg.data = append(first_msg.data, msg.data...)
		    nonce = msg.nonce
		    if msg.length == 1 {
                        //fmt.Printf("receive last partial bulk nonce:%d recv_nonce:%d legnth:%d\n",nonce, recv_nonce, msg.length)
		        // send ACK
	                if err := c.send(nonce, recv_nonce, dummy,0); err != nil {
		            // TODO : error handle
		            fmt.Printf("error on sent ACK nonce:%d recv_nonce:%d length:%d\n",nonce, recv_nonce, msg.length)
		            continue
	                }
		        //fmt.Printf("sent ACK nonce:%d recv_nonce:%d length:%d\n",nonce, recv_nonce, msg.length)
			break
		    }
                    //fmt.Printf("receive partial bulk nonce:%d recv_nonce:%d legnth:%d\n",nonce, recv_nonce, msg.length)

		    // send ACK
	            if err := c.send(nonce, recv_nonce, dummy,0); err != nil {
		        // TODO : error handle
		        fmt.Printf("error on sent ACK nonce:%d recv_nonce:%d length:%d\n",nonce, recv_nonce, msg.length)
		        continue
	            }
		    //fmt.Printf("sent ACK nonce:%d recv_nonce:%d length:%d\n",nonce, recv_nonce, msg.length)

		    continue
	    }

            //fmt.Printf("handle message nonce:%d recv_nonce:%d size:%d\n",nonce, recv_nonce, len(first_msg.data))
	    c.node.work <- HandlerContext{client: c, msg: first_msg}

	    for _, protocol := range c.node.protocols {
			if protocol.OnMessageRecv == nil {
				continue
			}

			protocol.OnMessageRecv(c)
	    }

	    close(ch)
	    dummy = nil

	    c.requests_recv.deleteRequest(recv_nonce)

	    return
	}
}

func (c *Client) recvLoop() {
	defer close(c.readerDone)

	for {
		buf, err := c.read()
		if err != nil {
			if !isEOF(err) {
				c.Logger().Warn("Got an error while sending messages.", zap.Error(err))
			}
			c.reportError(err)

			break
		}

		msg, err := unmarshalMessage(buf)
		if err != nil {
			c.Logger().Warn("Got an error while reading incoming messages.", zap.Error(err))
			c.reportError(err)

			break
		}

                if msg.length > uint32(0) { // if not a single message or request message
		    // check if it is existing bulk request
		    if ch,_ := c.requests_recv.findRequest(msg.recv_nonce); ch != nil {
		        //fmt.Printf("bulk request exists. nonce:%d recv_nonce:%d length:%d\n",msg.nonce,msg.recv_nonce, msg.length)
			// if existing, add to channel
			ch <- msg
			continue
		    }else{
	                // if new request and bulk, create new channel for this nonce
		        //fmt.Printf("bulk request doesnt exist. nonce:%d recv_nonce:%d length:%d\n",msg.nonce,msg.recv_nonce, msg.length)
	                ch, recv_nonce, err := c.requests_recv.nextNonce()
	                if err != nil {
		            fmt.Printf("error to get next nonce\n")
			    // TODO : error ACK return and continue
			    continue
	                }
		        //fmt.Printf("this is first bulk request nonce:%d register recv_nonce:%d length:%d\n",msg.nonce, recv_nonce, msg.length)
	                go c.recvBulkHandleLoop(recv_nonce, msg, ch)
		        continue
		    }
		}

		msg.data = append([]byte{}, msg.data...)

		if ch, exists := c.requests.findRequest(msg.nonce); ch != nil {
			ch <- msg
			close(ch)
			if exists {
			    c.requests.deleteRequest(msg.nonce)
			}
			continue
		}

		c.node.work <- HandlerContext{client: c, msg: msg}

		for _, protocol := range c.node.protocols {
			if protocol.OnMessageRecv == nil {
				continue
			}

			protocol.OnMessageRecv(c)
		}
	}
}

func (c *Client) writeSingle(msg message) {
	header := make([]byte, 4)
	buf := make([]byte, 0, 1024)

	buf = buf[:0]
	buf = msg.marshal(buf)

	if c.suite != nil {
		var err error

		if buf, err = encryptAEAD(c.suite, buf); err != nil {
			c.Logger().Warn("Got an error encrypting a message.", zap.Error(err))
			c.reportError(err)
			return
		}
	}

	binary.BigEndian.PutUint32(header, uint32(len(buf)))

	if _, err := c.writer.Write(header); err != nil {
		if !isEOF(err) {
			c.Logger().Warn("Got an error writing header.", zap.Error(err))
		}
		c.reportError(err)
		return
	}

	if _, err := c.writer.Write(buf); err != nil {
		if !isEOF(err) {
			c.Logger().Warn("Got an error writing a message.", zap.Error(err))
		}
		c.reportError(err)
		return
	}

        if err := c.writer.Flush(); err != nil {
	        if !isEOF(err) {
			c.Logger().Warn("Got an error flushing.", zap.Error(err))
		}
		c.reportError(err)
		return
	}

	for _, protocol := range c.node.protocols {
		if protocol.OnMessageSent == nil {
			continue
		}
		protocol.OnMessageSent(c)
	}
}

func (c *Client) writeLoop() {
	defer close(c.writerDone)

	header := make([]byte, 4)
	buf := make([]byte, 0, 1024)

Write:
	for {
		select {
		case <-c.readerDone:
			return
		case <-c.clientDone:
			return
		default:
		}

		if c.node.idleTimeout > 0 {
			if err := c.conn.SetWriteDeadline(time.Now().Add(c.node.idleTimeout)); err != nil {
				if !isEOF(err) {
					c.Logger().Warn("Got an error setting write deadline.", zap.Error(err))
				}
				c.reportError(err)

				break Write
			}
		}

		c.writerCond.L.Lock()
		for len(c.writerBuf) == 0 && !c.writerClosed {
			c.writerCond.Wait()
		}
		writerClosed := c.writerClosed

		if writerClosed {
			break Write
		}

		starttime = time.Now()
		for _, msg := range c.writerBuf {
			buf = buf[:0]
			buf = msg.marshal(buf)
		        datasize = len(buf)

			if c.suite != nil {
				var err error

				if buf, err = encryptAEAD(c.suite, buf); err != nil {
					c.Logger().Warn("Got an error encrypting a message.", zap.Error(err))
					c.reportError(err)
					break Write
				}
			}

			binary.BigEndian.PutUint32(header, uint32(len(buf)))

			if _, err := c.writer.Write(header); err != nil {
				if !isEOF(err) {
					c.Logger().Warn("Got an error writing header.", zap.Error(err))
				}
				c.reportError(err)
				break Write
			}

			if _, err := c.writer.Write(buf); err != nil {
				if !isEOF(err) {
					c.Logger().Warn("Got an error writing a message.", zap.Error(err))
				}
				c.reportError(err)
				break Write
			}
		}

		if err := c.writer.Flush(); err != nil {
			if !isEOF(err) {
				c.Logger().Warn("Got an error flushing.", zap.Error(err))
			}
			c.reportError(err)
			break Write
		}

		duration = time.Since(starttime)
	        //fmt.Printf("msg:%d I/O time: %d ns\n",datasize, duration.Nanoseconds())

		for _, protocol := range c.node.protocols {
			if protocol.OnMessageSent == nil {
				continue
			}

			protocol.OnMessageSent(c)
		}

		c.writerBuf = nil
		c.writerCond.L.Unlock()
	}
}

func isEOF(err error) bool {
	if errors.Is(err, io.EOF) {
		return true
	}

	var netErr *net.OpError

	if errors.As(err, &netErr) {
		if netErr.Err.Error() == "use of closed network connection" {
			return true
		}
	}

	return false
}