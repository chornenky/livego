package rtmp

import (
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/chornenky/livego/av"
	"github.com/chornenky/livego/configure"
	"github.com/chornenky/livego/protocol/amf"
	"github.com/chornenky/livego/protocol/rtmp/core"
	"github.com/chornenky/livego/utils/uid"
	log "github.com/sirupsen/logrus"
)

type RelayAuthorizer interface {
	Authorize(app, name string) (endpoint string, err error)
	LogOut(name string) error
}

type RelayServer struct {
	handler    av.Handler
	authorizer RelayAuthorizer
}

func NewRelayServer(h av.Handler, authorizer RelayAuthorizer) *RelayServer {
	return &RelayServer{
		handler:    h,
		authorizer: authorizer,
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

		wr := NewRelayWriter(reader.Info(), cc)
		s.handler.HandleWriter(wr)
	} else {
		log.Error("handleConn: server doesn't support play method")
		closeWithLog()
	}

	return nil
}

type RelayWriter struct {
	Uid    string
	closed bool
	av.RWBaser
	conn         *core.ConnClient
	packetQueue  chan *av.Packet
	info         av.Info
	lastPacketTS time.Time
}

func NewRelayWriter(info av.Info, conn *core.ConnClient) *RelayWriter {
	ret := &RelayWriter{
		Uid:         uid.NewId(),
		conn:        conn,
		RWBaser:     av.NewRWBaser(time.Second * time.Duration(writeTimeout)),
		packetQueue: make(chan *av.Packet, maxQueueNum),
		info:        info,
	}

	go ret.Check()
	go func() {
		err := ret.SendPacket()
		if err != nil {
			log.Warning(err)
			if ret.closed {
				log.Info("stop forwarding, client is disconnected")
				return
			}
		}
	}()

	return ret
}

const (
	readTimeOut = time.Second * 5
)

func (v *RelayWriter) Check() {
	//TODO made proper closed connection checker
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for range t.C {
		if v.lastPacketTS.Add(readTimeOut).Before(time.Now()) {
			err := errors.New("read timeout, probably client is disconnected")
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
	v.lastPacketTS = time.Now()

	typeID := av.TAG_VIDEO
	if !p.IsVideo {
		if p.IsMetadata {
			var err error
			typeID = av.TAG_SCRIPTDATAAMF0
			p.Data, err = amf.MetaDataReform(p.Data, amf.DEL)
			if err != nil {
				return err
			}
		} else {
			typeID = av.TAG_AUDIO
		}
	}

	v.RWBaser.SetPreTime()
	timestamp := p.TimeStamp
	timestamp += v.BaseTimeStamp()
	v.RWBaser.RecTimeStamp(timestamp, uint32(typeID))

	var err error = nil
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("RelayWriter has already been closed:%v", e)
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
	//if !self.startflag {
	//	return
	//}
	var cs core.ChunkStream

	for {
		p, ok := <-v.packetQueue
		if !ok {
			return fmt.Errorf("data channel is closed\n")
		}

		cs.Data = p.Data
		cs.Length = uint32(len(p.Data))
		cs.StreamID = v.conn.GetStreamId()
		cs.Timestamp = p.TimeStamp
		//cs.Timestamp += v.BaseTimeStamp()

		//log.Printf("Static sendPacket: rtmpurl=%s, length=%d, streamid=%d",
		//	self.RtmpUrl, len(p.Data), cs.StreamID)
		if p.IsVideo {
			cs.TypeID = av.TAG_VIDEO
		} else {
			if p.IsMetadata {
				cs.TypeID = av.TAG_SCRIPTDATAAMF0
			} else {
				cs.TypeID = av.TAG_AUDIO
			}
		}

		if err := v.conn.Write(cs); err != nil {
			log.Errorf("can't write packet to connection %v", err)
			return err
		}
	}
}

func (v *RelayWriter) Info() av.Info {
	return v.info
}

func (v *RelayWriter) Close(err error) {
	log.Warning("publisher ", v.Info(), "closed: "+err.Error())
	if !v.closed {
		close(v.packetQueue)
	}
	v.closed = true
	v.conn.Close(err)
}
