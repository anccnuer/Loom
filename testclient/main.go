// 模拟 EaglercraftX 客户端，验证代理的 MOTD 查询与握手流程。
package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
)

var wsURL = "ws://127.0.0.1:8080/"

func ascii(b []byte, s string) []byte {
	if len(s) > 255 {
		s = s[:255]
	}
	return append(append(b, byte(len(s))), s...)
}

func sh(b []byte, v int) []byte { return binary.BigEndian.AppendUint16(b, uint16(v)) }

func loginPacket(gameProto int) []byte {
	var p []byte
	p = append(p, 0x01) // CLIENT_VERSION
	p = append(p, 2)    // legacy 协议版本 2 (V3+)
	// eagler 网络协议列表: [2, 3, 4]
	p = sh(p, 3)
	p = sh(p, 2)
	p = sh(p, 3)
	p = sh(p, 4)
	// minecraft 游戏协议列表
	p = sh(p, 1)
	p = sh(p, gameProto)
	// brand + version
	p = ascii(p, "EaglercraftX")
	p = ascii(p, "0.80")
	p = append(p, 0) // authFlag = false
	p = ascii(p, "") // authUsername
	return p
}

func usernamePacket(name, server string) []byte {
	var p []byte
	p = append(p, 0x04)
	p = ascii(p, name)
	p = ascii(p, server)
	p = append(p, 0) // password len 0
	p = append(p, 0) // cookies false
	p = append(p, 0) // cookie len 0
	return p
}

func clientFinish() []byte { return []byte{0x08} }

func readID(c *websocket.Conn) (byte, []byte, error) {
	for {
		op, data, err := c.ReadMessage()
		if err != nil {
			return 0, nil, err
		}
		if op != websocket.BinaryMessage || len(data) == 0 {
			continue
		}
		return data[0], data[1:], nil
	}
}

func main() {
	log.SetFlags(0)
	mode := "handshake"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}
	if mode == "motd" {
		testMOTD()
		return
	}
	gameProto := 47
	if len(os.Args) > 2 {
		gameProto, _ = strconv.Atoi(os.Args[2])
	}
	testHandshake(gameProto, mode == "play")
}

func dial() *websocket.Conn {
	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	c, resp, err := dialer.Dial(wsURL, http.Header{})
	if err != nil {
		if resp != nil {
			b, _ := io.ReadAll(resp.Body)
			log.Fatalf("拨号失败: %v (http %d): %s", err, resp.StatusCode, string(b))
		}
		log.Fatalf("拨号失败: %v", err)
	}
	return c
}

func testMOTD() {
	c := dial()
	defer c.Close()
	c.SetReadDeadline(time.Now().Add(5 * time.Second))
	if err := c.WriteMessage(websocket.TextMessage, []byte("accept: motd")); err != nil {
		log.Fatalf("写入失败: %v", err)
	}
	op, data, err := c.ReadMessage()
	if err != nil {
		log.Fatalf("读取失败: %v", err)
	}
	if op != websocket.TextMessage {
		log.Fatalf("期望 TEXT 帧，收到 op=%d", op)
	}
	var obj map[string]any
	if err := json.Unmarshal(data, &obj); err != nil {
		log.Fatalf("JSON 解析失败: %v (%s)", err, data)
	}
	if obj["type"] != "motd" {
		log.Fatalf("type 字段应为 motd，实际 %v", obj["type"])
	}
	fmt.Printf("MOTD OK: %s\n", data)
}

func testHandshake(gameProto int, play bool) {
	c := dial()
	defer c.Close()
	c.SetReadDeadline(time.Now().Add(5 * time.Second))

	// 1) LOGIN
	if err := c.WriteMessage(websocket.BinaryMessage, loginPacket(gameProto)); err != nil {
		log.Fatalf("发送 LOGIN 失败: %v", err)
	}
	id, body, err := readID(c)
	if err != nil {
		log.Fatalf("等待 IDENTIFY 失败: %v", err)
	}
	if id != 0x02 {
		log.Fatalf("期望 IDENTIFY(0x02)，收到 0x%02x", id)
	}
	if len(body) < 4 {
		log.Fatalf("IDENTIFY 太短")
	}
	proto := binary.BigEndian.Uint16(body[0:2])
	game := binary.BigEndian.Uint16(body[2:4])
	fmt.Printf("IDENTIFY OK: netProto=%d gameProto=%d\n", proto, game)

	// 2) USERNAME
	if err := c.WriteMessage(websocket.BinaryMessage, usernamePacket("TestPlayer", "proxy")); err != nil {
		log.Fatalf("发送 USERNAME 失败: %v", err)
	}
	id, body, err = readID(c)
	if err != nil {
		log.Fatalf("等待 SYNC_UUID 失败: %v", err)
	}
	if id == 0x06 { // DENY
		ln := binary.BigEndian.Uint16(body[0:2])
		log.Fatalf("服务器拒绝登录: %s", body[2:2+ln])
	}
	if id != 0x05 {
		log.Fatalf("期望 SYNC_UUID(0x05)，收到 0x%02x", id)
	}
	// body = [byte nameLen][name][16 uuid]
	nl := int(body[0])
	name := string(body[1 : 1+nl])
	uuidB := body[1+nl:]
	if len(uuidB) != 16 {
		log.Fatalf("SYNC_UUID 长度应为 16，实际 %d", len(uuidB))
	}
	fmt.Printf("SYNC_UUID OK: name=%s uuid=%x\n", name, uuidB)

	// 3) CLIENT_FINISH_LOGIN
	if err := c.WriteMessage(websocket.BinaryMessage, clientFinish()); err != nil {
		log.Fatalf("发送 CLIENT_FINISH 失败: %v", err)
	}
	id, _, err = readID(c)
	if err != nil {
		log.Fatalf("等待 SERVER_FINISH 失败: %v", err)
	}
	if id != 0x09 {
		log.Fatalf("期望 SERVER_FINISH(0x09)，收到 0x%02x", id)
	}
	fmt.Println("SERVER_FINISH OK —— 握手完整通过")

	if play {
		testPlay(c, gameProto)
	}
}

// testPlay 握手后模拟真实 MC 客户端：不自行发送 Handshake/LoginStart（代理已代发起），
// 仅持续读取代理回传的 play 包，验证从 JoinGame 起的原始透传。
func testPlay(c *websocket.Conn, gameProto int) {
	fmt.Println("等待代理代发起登录后回传的 play 包……")
	c.SetReadDeadline(time.Now().Add(6 * time.Second))
	seenJoinGame := false
	for i := 0; i < 15; i++ {
		op, data, err := c.ReadMessage()
		if err != nil {
			fmt.Printf("读取结束: %v\n", err)
			break
		}
		if op != websocket.BinaryMessage || len(data) == 0 {
			continue
		}
		id, _ := binary.Uvarint(data)
		fmt.Printf("  <- 包 id=0x%02x (%d) 帧长=%d\n", id, id, len(data))
		if id == 0x23 {
			seenJoinGame = true
			fmt.Println("JoinGame 收到")
		}
	}
	if seenJoinGame {
		fmt.Println("OK: 客户端从 JoinGame 起正常收到 play 包")
	} else {
		fmt.Println("警告: 未收到 JoinGame")
	}
}

func vi(b []byte, x int) []byte {
	u := uint64(x)
	for u >= 0x80 {
		b = append(b, byte(u)|0x80)
		u >>= 7
	}
	return append(b, byte(u))
}

func pstr(b []byte, s string) []byte {
	b = vi(b, len(s))
	return append(b, s...)
}
