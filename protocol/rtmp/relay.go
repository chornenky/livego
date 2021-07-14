package rtmp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/chornenky/livego/av"
	"github.com/chornenky/livego/configure"
	"github.com/chornenky/livego/protocol/rtmp/core"
	log "github.com/sirupsen/logrus"
)

type RelayAuthorizer interface {
	Authorize(app, name string) (endpoint string, err error)
	LogOut(name string) error
}

type RelayServerOpts struct {
	ReadTimeout, WriteTimeout time.Duration
}

type RelayServer struct {
	ctx                       context.Context
	readTimeout, writeTimeout time.Duration
	handler                   av.Handler
	authorizer                RelayAuthorizer
}

func NewRelayServer(ctx context.Context, h av.Handler, authorizer RelayAuthorizer, opts RelayServerOpts) *RelayServer {
	return &RelayServer{
		ctx:          ctx,
		readTimeout:  opts.ReadTimeout,
		writeTimeout: opts.WriteTimeout,
		handler:      h,
		authorizer:   authorizer,
	}
}

func (s *RelayServer) Serve(listener net.Listener) (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("serve: rtmp serve panic: ", r)
		}
	}()

	for {
		var netconn net.Conn
		netconn, err = listener.Accept()
		if err != nil {
			return
		}
		conn := core.NewConn(netconn, 4*1024)
		log.Debug("serve: new client, connect remote: ", conn.RemoteAddr().String(),
			"local:", conn.LocalAddr().String())
		go s.handleConn(conn)
	}
}

func (s *RelayServer) handleConn(conn *core.Conn) error {
	closeWithLog := func() {
		if err := conn.Close(); err != nil {
			log.Error("handleConn: can't close conn err: ", err)
		}
	}

	if err := conn.HandshakeServer(); err != nil {
		closeWithLog()
		log.Error("handleConn: HandshakeServer err: ", err)
		return err
	}
	connServer := core.NewConnServer(conn)

	if err := connServer.ReadMsg(); err != nil {
		closeWithLog()
		log.Error("handleConn: read msg err: ", err)
		return err
	}

	log.Debugf("handleConn: IsPublisher=%v", connServer.IsPublisher())
	if connServer.IsPublisher() {
		// host:port/first/sec
		// appName = first, name = sec
		appName, name, _ := connServer.GetInfo()
		log.Infof("handleConn: client is establishing a connection appName=%v name=%v", appName, name)

		rtmpAddr, err := s.authorizer.Authorize(appName, name)
		if err != nil {
			errW := fmt.Errorf("invalid key, %w", err)
			closeWithLog()
			log.Errorf("handleConn: checkKey failed err=%v", errW)
			return errW
		}

		log.Infof("handleConn: client successfully connected appName=%v name=%v", appName, name)

		connServer.PublishInfo.Name = name
		if pushlist, ret := configure.GetStaticPushUrlList(appName); ret && (pushlist != nil) {
			log.Debugf("handleConn: GetStaticPushUrlList: %v", pushlist)
		}

		reader := NewVirReader(connServer)
		s.handler.HandleReader(reader)
		log.Debugf("handleConn: new publisher: %+v", reader.Info())
		cc := core.NewConnClient()
		if err = cc.Start(rtmpAddr, "publish"); err != nil {
			log.Debugf("connectClient.Start url=%v error", rtmpAddr)
			closeWithLog()
			return err
		}
		log.Debug("handleConn: static publish is starting....")

		wr := NewRelayWriter(cc, reader, optsRelayWriter{
			readTimeout:  s.readTimeout,
			writeTimeout: s.writeTimeout,
			onCloseFunc: func() {
				log.Infof("do logout... stream %v", name)
				if err = s.authorizer.LogOut(name); err != nil {
					log.Warn("logOut err", err)
				}
				log.Infof("close streamer connection %v", name)
				closeWithLog()
			},
		})
		s.handler.HandleWriter(wr)
	} else {
		log.Error("handleConn: server doesn't support play method")
		closeWithLog()
	}

	return nil
}

type optsRelayWriter struct {
	readTimeout  time.Duration
	writeTimeout time.Duration
	onCloseFunc  func()
}

type RelayWriter struct {
	readTimeout  time.Duration
	writeTimeout time.Duration
	onCloseFunc  func()
	closed       bool
	av.RWBaser

	dstConn   *core.ConnClient
	srcReader *VirReader

	packetQueue chan *av.Packet
}

func NewRelayWriter(dstConn *core.ConnClient, srcReader *VirReader, opts optsRelayWriter) *RelayWriter {
	wt := opts.writeTimeout
	if wt == 0 {
		wt = time.Second * time.Duration(writeTimeout)
	}

	to := opts.readTimeout
	if to == 0 {
		to = time.Second * time.Duration(readTimeout)
	}

	ret := &RelayWriter{
		readTimeout:  to,
		writeTimeout: wt,
		onCloseFunc:  opts.onCloseFunc,
		RWBaser:      av.NewRWBaser(wt),
		dstConn:      dstConn,
		srcReader:    srcReader,
		packetQueue:  make(chan *av.Packet, maxQueueNum),
	}

	go ret.Check()
	go func() {
		err := ret.SendPacket()
		if err != nil {
			log.Warning(err)
			if ret.closed {
				log.Infof("stop forwarding, client is disconnected %v", ret.srcReader.Info().URL)
				return
			} else {
				ret.Close(err)
			}
		}
	}()

	return ret
}

func (v *RelayWriter) Check() {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for range t.C {
		if !v.Alive() || !v.srcReader.Alive() {
			err := errors.New("r/w timeout, probably the client is disconnected or remote server's closed the connection")
			v.Close(err)
			return
		}
	}
}

func (v *RelayWriter) DropPacket(pktQue chan *av.Packet, info av.Info) {
	log.Warningf("[%v] packet queue max!!!", info)
	for i := 0; i < maxQueueNum-84; i++ {
		tmpPkt, ok := <-pktQue
		// try to don't drop audio
		if ok && tmpPkt.IsAudio {
			if len(pktQue) > maxQueueNum-2 {
				log.Debug("drop audio pkt")
				<-pktQue
			} else {
				pktQue <- tmpPkt
			}
		}

		if ok && tmpPkt.IsVideo {
			videoPkt, ok := tmpPkt.Header.(av.VideoPacketHeader)
			// dont't drop sps config and dont't drop key frame
			if ok && (videoPkt.IsSeq() || videoPkt.IsKeyFrame()) {
				pktQue <- tmpPkt
			}
			if len(pktQue) > maxQueueNum-10 {
				log.Debug("drop video pkt")
				<-pktQue
			}
		}
	}
	log.Debug("packet queue len: ", len(pktQue))
}

func (v *RelayWriter) Write(p *av.Packet) error {
	v.RWBaser.SetPreTime()

	var err error = nil
	if v.closed {
		err = fmt.Errorf("RelayWriter closed")
		return err
	}
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("RelayWriter has already been closed: %v", e)
		}
	}()

	if len(v.packetQueue) >= maxQueueNum-24 {
		v.DropPacket(v.packetQueue, v.Info())
	} else {
		v.packetQueue <- p
	}

	return err
}

func (v *RelayWriter) SendPacket() error {
	var cs core.ChunkStream
	for {
		p, ok := <-v.packetQueue
		if !ok {
			return fmt.Errorf("data channel is closed")
		}

		cs.Data = p.Data
		cs.Length = uint32(len(p.Data))
		cs.StreamID = v.dstConn.GetStreamId()
		cs.Timestamp = p.TimeStamp

		if p.IsVideo {
			cs.TypeID = av.TAG_VIDEO
		} else {
			if p.IsMetadata {
				cs.TypeID = av.TAG_SCRIPTDATAAMF0
			} else {
				cs.TypeID = av.TAG_AUDIO
			}
		}

		if err := v.dstConn.Write(cs); err != nil {
			v.closed = true
			log.Errorf("can't write packet to destination for stream %v, err %v", v.srcReader.Info().URL, err)
			return err
		}
	}
}

func (v *RelayWriter) Info() av.Info {
	return v.srcReader.Info()
}

func (v *RelayWriter) Close(err error) {
	log.Warningf("publisher %v is closed due err %v", v.Info(), err.Error())
	if !v.closed {
		close(v.packetQueue)
	}
	v.closed = true
	v.onCloseFunc()
	v.dstConn.Close(err)
}
