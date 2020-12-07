package main

import (
	"crypto/md5"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"

	"github.com/chornenky/livego/protocol/rtmp"
	log "github.com/sirupsen/logrus"
)

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

type auth struct{}

func (a auth) Authorize(app, name string) (endpoint string, err error) {
	if app == "12" {
		return "rtmp://127.0.0.1/live/live3", nil
	}

	return "", errors.New("unknown app")
}

func (a auth) LogOut(name string) error {
	return nil
}

func main() {

	rtmpAddr := ":1934"
	//pwd, _ := os.Getwd()
	//fmt.Println(pwd)

	log.SetLevel(log.DebugLevel)

	/*	room := "12"
		msg, err := configure.RoomKeys.GetKey(room)
		if err != nil {
			log.Errorf(err.Error())
			return
		}
		log.Printf(msg)
	*/

	stream := rtmp.NewRtmpStream()
	rtmpServer := rtmp.NewRelayServer(stream, &auth{})
	rtmpListen, err := net.Listen("tcp", rtmpAddr)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		if r := recover(); r != nil {
			log.Error("RTMP server panic: ", r)
		}
	}()
	log.Info("RTMP Listen On ", rtmpAddr)
	if err := rtmpServer.Serve(rtmpListen); err != nil {
		panic(err)
	}
}
