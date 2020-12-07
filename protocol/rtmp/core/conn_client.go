package core

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	neturl "net/url"
	"strings"

	"github.com/gwuhaolin/livego/av"
	"github.com/gwuhaolin/livego/protocol/amf"

	log "github.com/sirupsen/logrus"
)

var (
	respResult     = "_result"
	respError      = "_error"
	onStatus       = "onStatus"
	publishStart   = "NetStream.Publish.Start"
	playStart      = "NetStream.Play.Start"
	connectSuccess = "NetConnection.Connect.Success"
	onBWDone       = "onBWDone"
)

var (
	ErrFail = fmt.Errorf("respone err")
)

type ConnClient struct {
	done       bool
	transID    int
	url        string
	tcurl      string
	app        string
	title      string
	query      string
	curcmdName string
	streamid   uint32
	conn       *Conn
	encoder    *amf.Encoder
	decoder    *amf.Decoder
	bytesw     *bytes.Buffer
}

func NewConnClient() *ConnClient {
	return &ConnClient{
		transID: 1,
		bytesw:  bytes.NewBuffer(nil),
		encoder: &amf.Encoder{},
		decoder: &amf.Decoder{},
	}
}

func (connClient *ConnClient) DecodeBatch(r io.Reader, ver amf.Version) (ret []interface{}, err error) {
	vs, err := connClient.decoder.DecodeBatch(r, ver)
	return vs, err
}

func (connClient *ConnClient) readRespMsg() error {
	var err error
	var rc ChunkStream
	for {
		if err = connClient.conn.Read(&rc); err != nil {
			return err
		}
		if err != nil && err != io.EOF {
			return err
		}
		switch rc.TypeID {
		case 20, 17:
			r := bytes.NewReader(rc.Data)
			vs, _ := connClient.decoder.DecodeBatch(r, amf.AMF0)

			log.Debugf("readRespMsg: vs=%v", vs)
			for k, v := range vs {
				switch v.(type) {
				case string:
					switch connClient.curcmdName {
					case cmdConnect, cmdCreateStream:
						if v.(string) != respResult {
							return fmt.Errorf(v.(string))
						}

					case cmdPublish:
						if v.(string) != onStatus {
							return ErrFail
						}
					}
				case float64:
					switch connClient.curcmdName {
					case cmdConnect, cmdCreateStream:
						id := int(v.(float64))

						if k == 1 {
							if id != connClient.transID {
								return ErrFail
							}
						} else if k == 3 {
							connClient.streamid = uint32(id)
						}
					case cmdPublish:
						if int(v.(float64)) != 0 {
							return ErrFail
						}
					}
				case amf.Object:
					objmap := v.(amf.Object)
					switch connClient.curcmdName {
					case cmdConnect:
						code, ok := objmap["code"]
						if ok && code.(string) != connectSuccess {
							return ErrFail
						}
					case cmdPublish:
						code, ok := objmap["code"]
						if ok && code.(string) != publishStart {
							return ErrFail
						}
					}
				}
			}

			return nil
		}
	}
}

func (connClient *ConnClient) readConnRespMsg() (error, string) {
	var err error
	var rc ChunkStream
	for {
		if err = connClient.conn.Read(&rc); err != nil {
			return err, ""
		}
		if err != nil && err != io.EOF {
			return err, ""
		}
		switch rc.TypeID {
		case 20, 17:
			r := bytes.NewReader(rc.Data)
			vs, _ := connClient.decoder.DecodeBatch(r, amf.AMF0)

			log.Debugf("readRespMsg: vs=%v", vs)
			for _, v := range vs {
				switch v.(type) {
				case amf.Object:
					objmap := v.(amf.Object)
					switch connClient.curcmdName {
					case cmdConnect:
						code, ok := objmap["code"]
						if ok && code.(string) != connectSuccess {
							var descr string
							if d, ok := objmap["description"]; ok {
								descr = d.(string)
							}
							return ErrFail, descr
						}
					}
				}
			}

			return nil, ""
		}
	}
}

func (connClient *ConnClient) writeMsg(args ...interface{}) error {
	connClient.bytesw.Reset()
	for _, v := range args {
		if _, err := connClient.encoder.Encode(connClient.bytesw, v, amf.AMF0); err != nil {
			return err
		}
	}
	msg := connClient.bytesw.Bytes()
	c := ChunkStream{
		Format:    0,
		CSID:      3,
		Timestamp: 0,
		TypeID:    20,
		StreamID:  connClient.streamid,
		Length:    uint32(len(msg)),
		Data:      msg,
	}
	connClient.conn.Write(&c)
	return connClient.conn.Flush()
}

type connectAuth struct {
	stage                   int
	salt, challenge, opaque string
}

func (connClient *ConnClient) writeConnectMsg(ca connectAuth) (error, string) {
	event := make(amf.Object)
	event["app"] = connClient.app
	event["type"] = "nonprivate"
	event["flashVer"] = "FMLE/3.0 (compatible; Lavf58.45.100)"
	event["tcUrl"] = connClient.tcurl
	connClient.curcmdName = cmdConnect

	var usr *neturl.Userinfo
	if ca.stage > 0 {
		u, err := neturl.Parse(connClient.url)
		if err != nil {
			return err, ""
		}
		if u.User == nil {
			return errors.New("connect url has no user info, but it must have"), ""
		}
		usr = u.User
	}

	if ca.stage == 1 {
		authStr := "?authmod=adobe&user=" + usr.Username()
		event["app"] = connClient.app + authStr
		event["tcUrl"] = connClient.tcurl + authStr
	} else if ca.stage == 2 {
		pwd, _ := usr.Password()
		authStr := adobeAuth(usr.Username(), pwd, ca.salt, ca.opaque, ca.challenge)
		event["app"] = connClient.app + "?" + authStr
		event["tcUrl"] = connClient.tcurl + "?" + authStr
	}

	log.Debugf("writeConnectMsg: connClient.transID=%d, event=%v", connClient.transID, event)
	if err := connClient.writeMsg(cmdConnect, connClient.transID, event); err != nil {
		return err, ""
	}

	return connClient.readConnRespMsg()
}

func adobeAuth(user, password, salt, opaque, challenge string) string {
	h := md5.New()
	io.WriteString(h, user)
	io.WriteString(h, salt)
	io.WriteString(h, password)

	hashStr := base64.StdEncoding.EncodeToString(h.Sum(nil))

	challenge2 := fmt.Sprintf("%08x", rand.Uint32())

	h = md5.New()
	io.WriteString(h, hashStr)
	if opaque != "" {
		io.WriteString(h, opaque)
	} else if challenge != "" {
		io.WriteString(h, challenge)
	}
	io.WriteString(h, challenge2)

	hashStr = base64.StdEncoding.EncodeToString(h.Sum(nil))

	return fmt.Sprintf("authmod=%s&user=%s&challenge=%s&response=%s&opaque=%s", "adobe", user, challenge2, hashStr, opaque)
}

func (connClient *ConnClient) writeCreateStreamMsg() error {
	connClient.transID++
	connClient.curcmdName = cmdCreateStream
	log.Debugf("writeCreateStreamMsg: connClient.transID=%d", connClient.transID)
	if err := connClient.writeMsg(cmdCreateStream, connClient.transID, nil); err != nil {
		return err
	}

	for {
		err := connClient.readRespMsg()
		if err == nil {
			return err
		}

		if err == ErrFail {
			log.Debugf("writeCreateStreamMsg readRespMsg err=%v", err)
			return err
		}
	}

}

func (connClient *ConnClient) writePublishMsg() error {
	connClient.transID++
	connClient.curcmdName = cmdPublish
	if err := connClient.writeMsg(cmdPublish, connClient.transID, nil, connClient.title, publishLive); err != nil {
		return err
	}
	return connClient.readRespMsg()
}

func (connClient *ConnClient) writePlayMsg() error {
	connClient.transID++
	connClient.curcmdName = cmdPlay
	log.Debugf("writePlayMsg: connClient.transID=%d, cmdPlay=%v, connClient.title=%v",
		connClient.transID, cmdPlay, connClient.title)

	if err := connClient.writeMsg(cmdPlay, 0, nil, connClient.title); err != nil {
		return err
	}
	return connClient.readRespMsg()
}

func (connClient *ConnClient) Start(url string, method string) error {
	u, err := neturl.Parse(url)
	if err != nil {
		return err
	}
	connClient.url = url
	path := strings.TrimLeft(u.Path, "/")
	ps := strings.SplitN(path, "/", 2)
	if len(ps) != 2 {
		return fmt.Errorf("u path err: %s", path)
	}
	connClient.app = ps[0]
	connClient.title = ps[1]
	connClient.query = u.RawQuery
	connClient.tcurl = "rtmp://" + u.Host + ":1935" + "/" + connClient.app
	port := ":1935"
	host := u.Host
	localIP := ":0"
	var remoteIP string
	if strings.Index(host, ":") != -1 {
		host, port, err = net.SplitHostPort(host)
		if err != nil {
			return err
		}
		port = ":" + port
	}
	ips, err := net.LookupIP(host)
	log.Debugf("ips: %v, host: %v", ips, host)
	if err != nil {
		log.Warning(err)
		return err
	}
	remoteIP = ips[rand.Intn(len(ips))].String()
	if strings.Index(remoteIP, ":") == -1 {
		remoteIP += port
	}

	local, err := net.ResolveTCPAddr("tcp", localIP)
	if err != nil {
		log.Warning(err)
		return err
	}
	log.Debug("remoteIP: ", remoteIP)
	remote, err := net.ResolveTCPAddr("tcp", remoteIP)
	if err != nil {
		log.Warning(err)
		return err
	}
	conn, err := net.DialTCP("tcp", local, remote)
	if err != nil {
		log.Warning(err)
		return err
	}

	log.Debug("connection:", "local:", conn.LocalAddr(), "remote:", conn.RemoteAddr())

	connClient.conn = NewConn(conn, 4*1024)

	log.Debug("HandshakeClient....")
	if err := connClient.conn.HandshakeClient(); err != nil {
		return err
	}
	connClient.conn.Flush()

	log.Debug("writeConnectMsg....")
	errC, descr := connClient.writeConnectMsg(connectAuth{})
	if errC != nil && descr == "" {
		return errC
	}

	//do auth
	if errC != nil && descr != "" {
		spl := strings.Split(descr, ":")

		if len(spl) < 2 {
			return errC
		}

		if u.User == nil {
			return errors.New("connect url has no user info, but it must have")
		}

		if strings.Contains(spl[1], "code=403") {
			if !strings.Contains(spl[1], "adobe") {
				return errors.New("unknown authmod =" + spl[1])
			}

			log.Debug("stage 1")

			conn, err := net.DialTCP("tcp", local, remote)
			if err != nil {
				log.Warning(err)
				return err
			}
			connClient.conn = NewConn(conn, 4*1024)
			log.Debug("HandshakeClient....")
			if err := connClient.conn.HandshakeClient(); err != nil {
				return err
			}

			log.Debug("writeConnectMsg....")
			errC, descr := connClient.writeConnectMsg(connectAuth{
				stage: 1,
			})
			if errC != nil && descr == "" {
				return errC
			}

			if errC != nil && descr != "" {

				spl = strings.Split(descr, ":")
				fmt.Printf("%#v\n", spl)

				if len(spl) < 2 {
					return errC
				}
			}

			if !strings.Contains(spl[1], "adobe") {
				return errors.New("unknown auth mode")
			}
			var salt, user, challenge, opaque string

			log.Debug("stage 2")
			q, err := neturl.ParseQuery(strings.TrimSpace(spl[2]))
			if err != nil || q == nil {
				return errors.New("can't parse auth url from server")
			}

			user = q.Get("user")
			if user == "" || user != u.User.Username() {
				return errors.New("user is empty or doesn't equal to expected")
			}
			fixPlus := func(s string) string {
				return strings.ReplaceAll(s, " ", "+")
			}
			salt = fixPlus(q.Get("salt"))
			if salt == "" {
				return errors.New("can't get salt")
			}

			challenge = fixPlus(q.Get("challenge"))
			if challenge == "" {
				// can be empty
			}

			opaque = fixPlus(q.Get("opaque"))
			if opaque == "" {
				// can be empty
			}

			conn, err = net.DialTCP("tcp", local, remote)
			if err != nil {
				log.Warning(err)
				return err
			}
			connClient.conn = NewConn(conn, 4*1024)
			log.Debug("HandshakeClient....")
			if err := connClient.conn.HandshakeClient(); err != nil {
				return err
			}

			log.Debug("writeConnectMsg....")
			errC, descr = connClient.writeConnectMsg(connectAuth{
				stage:     2,
				salt:      salt,
				challenge: challenge,
				opaque:    opaque,
			})
			if errC != nil && descr == "" {
				return errC
			}

			log.Infof("writeConnectMsg.... descr %v", descr)

		} else {
			return errC
		}
	}

	log.Debug("writeCreateStreamMsg....")
	if err := connClient.writeCreateStreamMsg(); err != nil {
		log.Debug("writeCreateStreamMsg error", err)
		return err
	}

	log.Debug("method control:", method, av.PUBLISH, av.PLAY)
	if method == av.PUBLISH {
		if err := connClient.writePublishMsg(); err != nil {
			return err
		}
	} else if method == av.PLAY {
		if err := connClient.writePlayMsg(); err != nil {
			return err
		}
	}

	return nil
}

func (connClient *ConnClient) Write(c ChunkStream) error {
	if c.TypeID == av.TAG_SCRIPTDATAAMF0 ||
		c.TypeID == av.TAG_SCRIPTDATAAMF3 {
		var err error
		if c.Data, err = amf.MetaDataReform(c.Data, amf.ADD); err != nil {
			return err
		}
		c.Length = uint32(len(c.Data))
	}
	return connClient.conn.Write(&c)
}

func (connClient *ConnClient) Flush() error {
	return connClient.conn.Flush()
}

func (connClient *ConnClient) Read(c *ChunkStream) (err error) {
	return connClient.conn.Read(c)
}

func (connClient *ConnClient) GetInfo() (app string, name string, url string) {
	app = connClient.app
	name = connClient.title
	url = connClient.url
	return
}

func (connClient *ConnClient) GetStreamId() uint32 {
	return connClient.streamid
}

func (connClient *ConnClient) Close(err error) {
	connClient.conn.Close()
}
