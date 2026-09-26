package main

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	_ "image/png"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// Eaglercraft relay 协议（对齐 EaglerXBungee HttpWebSocketHandler，legacy 版本 2 / V3+）
const (
	eaglerNetVer = 4 // 我们支持并协商 Eagler 网络协议 2/3/4

	pktLogin     = 0x01 // C->S CLIENT_VERSION
	pktIdentify  = 0x02 // S->C SERVER_VERSION
	pktUsername  = 0x04 // C->S CLIENT_REQUEST_LOGIN
	pktSyncUUID  = 0x05 // S->C SERVER_ALLOW_LOGIN
	pktDenyLogin = 0x06 // S->C SERVER_DENY_LOGIN
	pktSetSkin   = 0x07 // C->S CLIENT_PROFILE_DATA
	pktClientRdy = 0x08 // C->S CLIENT_FINISH_LOGIN
	pktSvrRdy    = 0x09 // S->C SERVER_FINISH_LOGIN
	pktKick      = 0x40 // S->C play 状态断开
	pktError     = 0xFF // S->C SERVER_ERROR
)

var nameReg = regexp.MustCompile(`^[A-Za-z0-9_]{3,16}$`)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	// 内网环境：不校验 Origin
	CheckOrigin: func(r *http.Request) bool { return true },
}

// ---- 在线玩家表（用于 MOTD 人数与重名检查） ----

type playerEntry struct {
	name string
	uuid string
}

var (
	playersMu sync.Mutex
	players   = map[string]playerEntry{} // key: 小写用户名
)

func playerCount() int {
	playersMu.Lock()
	defer playersMu.Unlock()
	return len(players)
}

func playerSample(n int) []string {
	playersMu.Lock()
	defer playersMu.Unlock()
	out := []string{}
	for _, p := range players {
		if len(out) >= n {
			break
		}
		out = append(out, p.name)
	}
	return out
}

// ---- 会话 ----

type session struct {
	ws      *websocket.Conn
	cfg     *Config
	log     *log.Logger
	name    string
	uuid    uuid
	gameVer int
	tcp     net.Conn
	mu      sync.Mutex
	closed  bool

	compressed atomic.Bool // 后端已启用协议压缩
	playStream atomic.Bool // 已过 LoginSuccess，从 JoinGame 起对客户端做原始字节透传
	down       atomic.Bool // 会话已开始拆除（抑制另一方向的关闭噪音）

	debug   bool
	dbgIn   int // 客户端->后端 已记录帧数
	dbgOut  int // 后端->客户端 已记录帧数
}

func newSession(ws *websocket.Conn, cfg *Config, remote string) *session {
	return &session{ws: ws, cfg: cfg, log: log.New(log.Writer(), "["+remote+"] ", log.LstdFlags), debug: os.Getenv("EAGLER_DEBUG") != ""}
}

func (s *session) closeAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	s.down.Store(true)
	if s.tcp != nil {
		s.tcp.Close()
	}
	s.ws.Close()
}

// writeRawFrame 发送原始 WS 二进制帧（已含首字节）
func (s *session) writeRawFrame(frame []byte) error {
	s.ws.SetWriteDeadline(time.Now().Add(10 * time.Second))
	return s.ws.WriteMessage(websocket.BinaryMessage, frame)
}

// sendFrame 发送 [varint id][body]
func (s *session) sendFrame(id int, body []byte) error {
	buf := make([]byte, 0, 3+len(body))
	buf = append(buf, byte(id)) // id < 128
	buf = append(buf, body...)
	return s.writeRawFrame(buf)
}

// handshakeFail 以 relay 错误包断开（握手阶段用 0xff）
func (s *session) handshakeFail(reason string) {
	msg := []byte(reason)
	if len(msg) > 250 {
		msg = msg[:250]
	}
	out := []byte{pktError, 0x08} // 0x08 = SERVER_ERROR_CUSTOM_MESSAGE
	out = append(out, byte(len(msg)))
	out = append(out, msg...)
	s.writeRawFrame(out)
	s.closeAll()
}

func (s *session) kick(msg string) {
	chat, _ := json.Marshal(map[string]string{"text": msg})
	s.sendFrame(pktKick, appendString(nil, string(chat)))
	s.closeAll()
}

// ascii 字符串: 单字节长度 + ASCII（relay 握手专用）
func writeASCII(b []byte, str string) []byte {
	if len(str) > 255 {
		str = str[:255]
	}
	return append(append(b, byte(len(str))), str...)
}

func readASCII(b []byte) (string, int, error) {
	if len(b) < 1 {
		return "", 0, io.ErrShortBuffer
	}
	ln := int(b[0])
	if len(b) < 1+ln {
		return "", 0, io.ErrShortBuffer
	}
	return string(b[1 : 1+ln]), 1 + ln, nil
}

// ---- 主流程 ----

func handleWS(w http.ResponseWriter, r *http.Request, cfg *Config) {
	remote := clientIP(r)
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	s := newSession(conn, cfg, remote)
	defer func() {
		if s.name != "" {
			playersMu.Lock()
			if cur, ok := players[strings.ToLower(s.name)]; ok && cur.name == s.name {
				delete(players, strings.ToLower(s.name))
			}
			playersMu.Unlock()
			s.log.Printf("玩家 %s 断开", s.name)
		}
		s.closeAll()
	}()

	conn.SetReadLimit(8 << 20)

	op, data, err := conn.ReadMessage()
	if err != nil {
		return
	}
	if op == websocket.TextMessage {
		// "accept: motd" 服务器列表查询
		if len(data) >= 7 && strings.HasPrefix(strings.ToLower(string(data)), "accept:") {
			s.serveQuery(string(data))
			return
		}
		s.log.Printf("意外的文本帧: %q", firstN(string(data), 64))
		return
	}

	if err := s.eaglerHandshake(data); err != nil {
		s.log.Printf("握手失败: %v", err)
		return
	}

	if err := s.connectBackend(); err != nil {
		s.kick(fmt.Sprintf("§c无法连接到后端服务器:\n%s", err))
		return
	}
	s.log.Printf("玩家 %s (%s) 进入游戏 -> %s", s.name, s.uuid, cfg.Backend)
	s.pump()
}

// eaglerHandshake 完整 Eagler 握手：
// LOGIN -> IDENTIFY -> USERNAME -> SYNC_UUID -> PROFILE_DATA*/READY -> READY
func (s *session) eaglerHandshake(first []byte) error {
	s.ws.SetReadDeadline(time.Now().Add(time.Duration(s.cfg.LoginTimeout) * time.Second))

	// --- 0x01 LOGIN ---
	if len(first) == 0 || first[0] != pktLogin {
		return fmt.Errorf("首包应为 LOGIN(0x01)，实际 0x%02x", safeByte(first))
	}
	p := first[1:]
	legacy, n, err := readVarInt(p)
	if err != nil {
		return err
	}
	p = p[n:]
	if legacy == 1 {
		return fmt.Errorf("不支持的旧版客户端(legacy=1)，请更新 EaglercraftX")
	}
	if legacy != 2 {
		return fmt.Errorf("未知的 legacy 协议版本 %d", legacy)
	}
	// eagler 网络协议版本列表
	protos, p, err := readShortList(p)
	if err != nil {
		return err
	}
	// minecraft 协议版本列表
	games, p, err := readShortList(p)
	if err != nil {
		return err
	}
	brand, n1, err := readASCII(p)
	if err != nil {
		return err
	}
	p = p[n1:]
	clVer, n2, err := readASCII(p)
	if err != nil {
		return err
	}
	p = p[n2:]
	if len(p) < 1 {
		return io.ErrShortBuffer
	}
	p = p[1:] // 客户端 auth 标志（不使用认证，忽略）
	authName, _, err := readASCII(p)
	if err != nil {
		return err
	}
	s.log.Printf("客户端 %s %q 网络版本=%v 游戏版本=%v auth名=%q", brand, clVer, protos, games, authName)

	// 版本协商
	negotiated := -1
	for _, v := range protos {
		if v >= 2 && v <= 4 && v > negotiated {
			negotiated = v
		}
	}
	if negotiated == -1 {
		s.sendVersionMismatch(protos)
		return fmt.Errorf("客户端 Eagler 网络版本不兼容: %v", protos)
	}
	gameVer := -1
	for _, v := range games {
		if v >= s.cfg.MinGameProtocol && v <= s.cfg.MaxGameProtocol && v > gameVer {
			gameVer = v
		}
	}
	if gameVer == -1 {
		s.sendVersionMismatch(protos)
		return fmt.Errorf("客户端游戏版本 %v 不在允许范围 [%d,%d] 内", games, s.cfg.MinGameProtocol, s.cfg.MaxGameProtocol)
	}
	s.gameVer = gameVer

	// --- 0x02 IDENTIFY ---
	body := binary.BigEndian.AppendUint16(nil, uint16(negotiated))
	body = binary.BigEndian.AppendUint16(body, uint16(gameVer))
	body = writeASCII(body, s.cfg.Brand)
	body = writeASCII(body, versionString)
	body = append(body, 0x00) // authMode = 0 无需认证
	body = binary.BigEndian.AppendUint16(body, 0)
	if err := s.sendFrame(pktIdentify, body); err != nil {
		return err
	}

	// --- 0x04 USERNAME ---
	op2, data2, err := s.readBinary()
	if err != nil {
		return err
	}
	_ = op2
	id2, body2, err := splitID(data2)
	if err != nil {
		return err
	}
	if id2 != pktUsername {
		return fmt.Errorf("期望 USERNAME(0x04)，收到 0x%02x", id2)
	}
	username, m, err := readASCII(body2)
	if err != nil {
		return err
	}
	body2 = body2[m:]
	serverName, m, err := readASCII(body2)
	if err != nil {
		return err
	}
	_ = serverName // 内网单后端，忽略 requestedServer

	if !nameReg.MatchString(username) {
		s.sendDeny("用户名非法（3-16 位字母数字下划线）")
		return fmt.Errorf("非法用户名 %q", username)
	}
	if len(username) > 16 || len(username) < 3 {
		s.sendDeny("用户名长度必须为 3-16 位")
		return fmt.Errorf("用户名长度非法 %q", username)
	}
	s.name = username
	s.uuid = offlineUUID(username)

	// --- 0x05 SYNC_UUID ---
	uuidBody := writeASCII(nil, username)
	uuidBody = append(uuidBody, s.uuid[:]...)
	if err := s.sendFrame(pktSyncUUID, uuidBody); err != nil {
		return err
	}

	// --- 0x07 PROFILE_DATA（0 至多个）与 0x08 FINISH ---
	sawReady := false
	for !sawReady {
		_, data, err := s.readBinary()
		if err != nil {
			return err
		}
		id, _, err := splitID(data)
		if err != nil {
			return err
		}
		switch id {
		case pktSetSkin:
			// 皮肤/披风上传数据，内网环境忽略
		case pktClientRdy:
			sawReady = true
		default:
			return fmt.Errorf("握手期意外包 0x%02x", id)
		}
	}

	// 注册玩家 / 去重 / 容量检查
	playersMu.Lock()
	if len(players) >= s.cfg.MaxPlayers {
		playersMu.Unlock()
		s.sendDeny("代理已满，请稍后再试")
		return fmt.Errorf("代理已满")
	}
	key := strings.ToLower(username)
	if _, ok := players[key]; ok {
		playersMu.Unlock()
		s.sendDeny("同名用户已在线")
		return fmt.Errorf("重名 %s", username)
	}
	players[key] = playerEntry{name: username, uuid: s.uuid.String()}
	playersMu.Unlock()

	// --- 0x09 SERVER_FINISH_LOGIN ---
	if err := s.sendFrame(pktSvrRdy, nil); err != nil {
		return err
	}
	return nil
}

func (s *session) readBinary() (int, []byte, error) {
	for {
		op, data, err := s.ws.ReadMessage()
		if err != nil {
			return 0, nil, err
		}
		if op == websocket.BinaryMessage && len(data) > 0 {
			return op, data, nil
		}
	}
}

func (s *session) sendVersionMismatch(protos []int) {
	// 0x03: short protoCnt, short[] serverProtos, short gameCnt, short[] gameProtos, byte msgLen + ascii
	var out []byte
	out = append(out, 0x03)
	out = binary.BigEndian.AppendUint16(out, 3)
	out = binary.BigEndian.AppendUint16(out, 2)
	out = binary.BigEndian.AppendUint16(out, 3)
	out = binary.BigEndian.AppendUint16(out, 4)
	out = binary.BigEndian.AppendUint16(out, 2)
	out = binary.BigEndian.AppendUint16(out, uint16(s.cfg.MinGameProtocol))
	out = binary.BigEndian.AppendUint16(out, uint16(s.cfg.MaxGameProtocol))
	msg := "Version mismatch: client/server protocols not compatible"
	out = append(out, byte(len(msg)))
	out = append(out, msg...)
	s.writeRawFrame(out)
}

func (s *session) sendDeny(reason string) {
	msg := []byte(reason)
	out := []byte{pktDenyLogin}
	out = binary.BigEndian.AppendUint16(out, uint16(len(msg)))
	out = append(out, msg...)
	s.writeRawFrame(out)
	s.closeAll()
}

func splitID(data []byte) (int, []byte, error) {
	if len(data) < 1 {
		return 0, nil, io.ErrShortBuffer
	}
	return int(data[0]), data[1:], nil
}

func readShortList(p []byte) ([]int, []byte, error) {
	if len(p) < 2 {
		return nil, nil, io.ErrShortBuffer
	}
	cnt := int(binary.BigEndian.Uint16(p))
	p = p[2:]
	if cnt < 0 || cnt > 16 {
		return nil, nil, fmt.Errorf("短整数列表长度异常: %d", cnt)
	}
	out := make([]int, 0, cnt)
	for i := 0; i < cnt; i++ {
		if len(p) < 2 {
			return nil, nil, io.ErrShortBuffer
		}
		out = append(out, int(binary.BigEndian.Uint16(p)))
		p = p[2:]
	}
	return out, p, nil
}

func safeByte(b []byte) byte {
	if len(b) == 0 {
		return 0
	}
	return b[0]
}

// ---- 后端连接与双向转发 ----

func (s *session) connectBackend() error {
	c, err := net.DialTimeout("tcp", s.cfg.Backend, 10*time.Second)
	if err != nil {
		return err
	}
	s.tcp = c

	// 代理代表客户端发起 Minecraft 登录（真实 Eagler 客户端不会自己发握手/登录包）
	host, portStr, err := net.SplitHostPort(s.cfg.Backend)
	if err != nil {
		host = s.cfg.Backend
		portStr = "25565"
	}
	// Handshake: [0x00][varint protocol][string host][u16 port][varint 2=login]
	hs := []byte{0x00}
	hs = appendVarInt(hs, s.gameVer)
	hs = appendString(hs, host)
	hs = binary.BigEndian.AppendUint16(hs, uint16(atoiPort(portStr)))
	hs = appendVarInt(hs, 2)
	// LoginStart: [0x00][string username]（1.8.9–1.18；1.19+ 若后端严格校验属性链需另行处理）
	ls := []byte{0x00}
	ls = appendString(ls, s.name)

	c.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := c.Write(append(appendVarInt(nil, len(hs)), hs...)); err != nil {
		return err
	}
	if _, err := c.Write(append(appendVarInt(nil, len(ls)), ls...)); err != nil {
		return err
	}
	c.SetDeadline(time.Time{})
	return nil
}

// pump：WS 帧 = [varint id][body...]；TCP 包 = [varint len][frame]
// 若后端在登录阶段下发 Set Compression，则本代理转为"压缩端点"：对后端解/加压缩，对客户端始终走不压缩帧。
func (s *session) pump() {
	s.ws.SetReadDeadline(time.Time{}) // 游戏阶段不设超时

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.tcpToWS()
	}()
	s.wsToTCP()
	s.closeAll()
	<-done
}

func (s *session) wsToTCP() {
	for {
		_, frame, err := s.ws.ReadMessage()
		if err != nil {
			if !s.down.Load() && !isNormalWSClose(err) {
				s.log.Printf("客户端异常断开: %s", wsCloseReason(err))
			}
			return
		}
		if len(frame) == 0 {
			continue
		}
		if s.debug && s.dbgIn < 12 {
			s.dbgIn++
			s.log.Printf("DBG C->S #%d compressed=%v play=%v frame(%d)=% x", s.dbgIn, s.compressed.Load(), s.playStream.Load(), len(frame), firstBytes(frame, 32))
		}
		// 登录阶段由代理全权发起，客户端此时不应发 MC 包；进入 play 前丢弃
		if !s.playStream.Load() {
			continue
		}
		// 拦截 Eagler 插件通道（皮肤 UUID 查询等），不透传给后端
		if isEaglerChannelClient(frame) {
			continue
		}
		// 后端启用了压缩时，客户端帧需加上 dataLength=0 前缀（小帧不压缩，符合协议）
		body := frame
		if s.compressed.Load() {
			body = appendVarInt(nil, 0)
			body = append(body, frame...)
		}
		tcpFrame := appendVarInt(nil, len(body))
		tcpFrame = append(tcpFrame, body...)
		s.tcp.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if _, err := s.tcp.Write(tcpFrame); err != nil {
			if !s.down.Load() {
				s.log.Printf("转发到后端失败: %v", err)
			}
			return
		}
	}
}

func (s *session) tcpToWS() {
	rd := bufio.NewReaderSize(s.tcp, 64*1024)
	for {
		body, err := readTCPFramed(rd)
		if err != nil {
			if !s.down.Load() {
				s.log.Printf("后端断开: %v", err)
			}
			return
		}
		if len(body) == 0 {
			continue
		}
		if s.debug && s.dbgOut < 12 {
			s.dbgOut++
			s.log.Printf("DBG S->C #%d compressed=%v play=%v body(%d)=% x", s.dbgOut, s.compressed.Load(), s.playStream.Load(), len(body), firstBytes(body, 32))
		}

		// 若已启用压缩：body = [varint dataLength][payload]，dataLength=0 表示未压缩。
		// 先解出逻辑包 pkt = [varint id][fields...]。
		pkt := body
		if s.compressed.Load() {
			pkt, err = uncompressFrame(body)
			if err != nil {
				s.log.Printf("压缩帧处理失败: %v", err)
				return
			}
		}

		// 登录阶段：吞掉 Set Compression / LoginSuccess，从 JoinGame 起透传
		if !s.playStream.Load() {
			id, _, err := readVarInt(pkt)
			if err != nil {
				s.log.Printf("登录包解析失败: %v", err)
				return
			}
			switch id {
			case 0x03: // Set Compression（此时 pkt 仍为未压缩）
				s.compressed.Store(true)
				s.log.Printf("后端启用协议压缩，代理将以压缩端点转发")
				continue
			case 0x02: // LoginSuccess：登录完成，下一条即 JoinGame
				s.playStream.Store(true)
				continue
			case 0x00: // Login Disconnect：把原因转成 Eagler kick 发给客户端
				s.handleLoginDisconnect(pkt)
				return
			default:
				// 其它登录期包（如 EncryptionRequest）不转发
				s.log.Printf("登录期未知包 id=0x%02x，忽略", id)
				continue
			}
		}

		// play 阶段：原始透传（帧内首字节为 varint packetId）
		if err := s.writeRawFrame(pkt); err != nil {
			if !s.down.Load() {
				s.log.Printf("转发到客户端失败: %v", err)
			}
			return
		}
	}
}

// uncompressFrame 把压缩 TCP 帧体 [varint dataLength][payload] 解成逻辑包 [varint id][fields]
func uncompressFrame(body []byte) ([]byte, error) {
	dataLen, n, err := readVarInt(body)
	if err != nil {
		return nil, err
	}
	payload := body[n:]
	if dataLen == 0 {
		return payload, nil
	}
	return zlibDecompress(payload, dataLen)
}

// handleLoginDisconnect 解析 login 期 Disconnect(0x00): [id][varint-len string reason(JSON)]，转成 Eagler kick
func (s *session) handleLoginDisconnect(pkt []byte) {
	reason := string(pkt[1:])
	// 尝试从 JSON chat 里抽纯文本
	var obj map[string]any
	if json.Unmarshal(pkt[1:], &obj) == nil {
		if t, ok := obj["text"].(string); ok {
			reason = t
		}
	}
	s.log.Printf("后端登录拒绝: %s", firstN(reason, 120))
	s.kick("§c" + firstN(reason, 300))
}

// readTCPFramed 读取 [varint len][data] 的一个 MC 包，返回 data（不含长度前缀）
func readTCPFramed(rd *bufio.Reader) ([]byte, error) {
	var length, shift int
	for {
		b, err := rd.ReadByte()
		if err != nil {
			return nil, err
		}
		length |= int(b&0x7f) << shift
		if b&0x80 == 0 {
			break
		}
		shift += 7
		if shift > 28 {
			return nil, fmt.Errorf("varint 头过长")
		}
	}
	if length < 1 || length > 2<<20 {
		return nil, fmt.Errorf("包过大: %d", length)
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(rd, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// zlibDecompress 解出期望长度 expectLen 的原始包
func zlibDecompress(payload []byte, expectLen int) ([]byte, error) {
	if expectLen > 8<<20 {
		return nil, fmt.Errorf("解压尺寸过大 %d", expectLen)
	}
	zr, err := zlib.NewReader(bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	out := make([]byte, 0, expectLen)
	buf := make([]byte, 32*1024)
	for {
		n, err := zr.Read(buf)
		out = append(out, buf[:n]...)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(out) > expectLen+1 {
			return nil, fmt.Errorf("解压超出预期长度")
		}
	}
	return out, nil
}

// isEaglerChannelClient 检查 C->S 0x17 plugin message 的通道名是否以 "EAG|" 开头。
// 1.8.9 C->S plugin message = 0x17: [varint id][string channel][data]
func isEaglerChannelClient(frame []byte) bool {
	if frame[0] != 0x17 {
		return false
	}
	ch, _, err := readStringBuf(frame[1:], 128)
	if err != nil {
		return false
	}
	return strings.HasPrefix(ch, "EAG|") || strings.HasPrefix(ch, "MC|")
}

// ---- MOTD 查询（"accept: motd" / "accept: cache.motd"） ----

func (s *session) serveQuery(req string) {
	s.ws.SetReadDeadline(time.Now().Add(10 * time.Second))
	s.ws.SetWriteDeadline(time.Now().Add(10 * time.Second))
	lower := strings.ToLower(strings.TrimSpace(req))
	noIcon := strings.Contains(lower, "noicon")

	// 拉取后端真实状态（失败则用配置兜底）
	backend := s.queryBackend()

	motdLine1, motdLine2 := s.cfg.motdLines()
	if backend != nil {
		motdLine1 = backend.MOTD
		if i := strings.IndexByte(motdLine1, '\n'); i >= 0 {
			motdLine1, motdLine2 = motdLine1[:i], motdLine1[i+1:]
			motdLine1 = strings.ReplaceAll(motdLine1, "§r", "")
			motdLine2 = strings.ReplaceAll(motdLine2, "§r", "")
		}
	}
	l1 := motdLine1
	if l1 == "" {
		l1 = "§bEaglercraftX " + s.cfg.Brand
	}
	l2 := motdLine2
	if l2 == "" {
		l2 = "§7" + s.cfg.Backend
	}

	iconB64 := ""
	if backend != nil && backend.Favicon != "" && !noIcon {
		iconB64 = backend.Favicon
	}

	out := map[string]any{
		"type":  "motd",
		"brand": s.cfg.Brand,
		"name":  s.cfg.Brand,
		"vers":  versionString,
		"uuid":  s.cfg.serverUUID(),
		"time":  time.Now().UnixMilli(),
		"secure": false,
		"cracked": true,
		"data": map[string]any{
			"online":  playerCount(),
			"max":     s.cfg.MaxPlayers,
			"motd":    []string{l1, l2},
			"icon":    iconB64 != "",
			"cache":   false,
			"players": playerSample(5),
		},
	}
	b, err := json.Marshal(out)
	if err != nil {
		return
	}
	if err := s.ws.WriteMessage(websocket.TextMessage, b); err != nil {
		return
	}
	if iconB64 != "" {
		if raw, err := decodeFavicon(iconB64); err == nil {
			s.ws.WriteMessage(websocket.BinaryMessage, raw)
		}
	}
	s.closeAll()
}

// statusResponse 是 1.8 status ping 的解析子集
type statusResponse struct {
	Version struct {
		Name     string `json:"name"`
		Protocol int    `json:"protocol"`
	} `json:"version"`
	Players struct {
		Online int `json:"online"`
		Max    int `json:"max"`
	} `json:"players"`
	Description json.RawMessage `json:"description"`
	Favicon     string          `json:"favicon"`

	MOTD string `json:"-"`
}

func (s *session) queryBackend() *statusResponse {
	host, portStr, err := net.SplitHostPort(s.cfg.Backend)
	if err != nil {
		return nil
	}
	c, err := net.DialTimeout("tcp", s.cfg.Backend, 3*time.Second)
	if err != nil {
		return nil
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(3 * time.Second))

	// handshake: id0 varint <proto> string host short port varint1
	hs := []byte{0x00}
	hs = appendVarInt(hs, s.cfg.StatusProtocol)
	hs = appendString(hs, host)
	hs = binary.BigEndian.AppendUint16(hs, uint16(atoiPort(portStr)))
	hs = appendVarInt(hs, 1)
	if _, err := c.Write(append(appendVarInt(nil, len(hs)), hs...)); err != nil {
		return nil
	}
	// status request: id0x00（varint 0x00）
	if _, err := c.Write([]byte{0x01, 0x00}); err != nil {
		return nil
	}

	rd := bufio.NewReader(c)
	// 读 varint 包长
	var length, shift int
	for {
		b, err := rd.ReadByte()
		if err != nil {
			return nil
		}
		length |= int(b&0x7f) << shift
		if b&0x80 == 0 {
			break
		}
		shift += 7
		if shift > 28 {
			return nil
		}
	}
	if length < 2 || length > 5<<20 {
		return nil
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(rd, buf); err != nil {
		return nil
	}
	// buf = [varint id=0][varint jsonLen][json]
	_, n, err := readVarInt(buf)
	if err != nil {
		return nil
	}
	_, n2, err := readVarInt(buf[n:])
	if err != nil {
		return nil
	}
	jsonBytes := buf[n+n2:]
	var st statusResponse
	if err := json.Unmarshal(jsonBytes, &st); err != nil {
		return nil
	}
	st.MOTD = flattenChat(st.Description)
	return &st
}

// decodeFavicon 把 "data:image/png;base64,..." 转成 Eagler 需要的 RGBA 像素（64x64，每像素 [R,G,B,A]）
func decodeFavicon(b64 string) ([]byte, error) {
	i := strings.Index(b64, "base64,")
	if i >= 0 {
		b64 = b64[i+7:]
	}
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return nil, err
	}
	img, _, err := image.Decode(bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	b := img.Bounds()
	out := make([]byte, 64*64*4)
	for y := 0; y < 64; y++ {
		for x := 0; x < 64; x++ {
			px := b.Min.X + x*max(1, b.Dx()/64)
			py := b.Min.Y + y*max(1, b.Dy()/64)
			r, g, bb, a := img.At(px, py).RGBA()
			o := (y*64 + x) * 4
			out[o] = byte(r >> 8)
			out[o+1] = byte(g >> 8)
			out[o+2] = byte(bb >> 8)
			out[o+3] = byte(a >> 8)
		}
	}
	return out, nil
}

func atoiPort(s string) int {
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return 25565
		}
		n = n*10 + int(c-'0')
	}
	if n == 0 || n > 65535 {
		return 25565
	}
	return n
}

// ---- 工具 ----

// flattenChat 把 1.8 MOTD（字符串或 JSON chat 组件）压成带 § 颜色码的纯文本
func flattenChat(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	var str string
	if json.Unmarshal(raw, &str) == nil {
		return str
	}
	var obj map[string]any
	if json.Unmarshal(raw, &obj) != nil {
		return ""
	}
	return flattenChatObj(obj, "")
}

func flattenChatObj(obj map[string]any, inherit string) string {
	var b strings.Builder
	b.WriteString(inherit)
	if c, ok := obj["color"].(string); ok {
		b.WriteString(colorToLegacy(c))
	}
	if v, ok := obj["text"].(string); ok {
		b.WriteString(v)
	}
	if v, ok := obj["extra"].([]any); ok {
		for _, e := range v {
			if em, ok := e.(map[string]any); ok {
				b.WriteString(flattenChatObj(em, ""))
			}
		}
	}
	return b.String()
}

var colorCodes = map[string]string{
	"black": "§0", "dark_blue": "§1", "dark_green": "§2", "dark_aqua": "§3",
	"dark_red": "§4", "dark_purple": "§5", "gold": "§6", "gray": "§7",
	"dark_gray": "§8", "blue": "§9", "green": "§a", "aqua": "§b",
	"red": "§c", "light_purple": "§d", "yellow": "§e", "white": "§f",
}

func colorToLegacy(c string) string {
	if v, ok := colorCodes[c]; ok {
		return v
	}
	return ""
}

func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func firstN(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func firstBytes(b []byte, n int) []byte {
	if len(b) <= n {
		return b
	}
	return b[:n]
}

// isNormalWSClose 判断是否为"正常离开"级别的关闭（关页面/退出游戏/无状态）
func isNormalWSClose(err error) bool {
	var ce *websocket.CloseError
	if errors.As(err, &ce) {
		switch ce.Code {
		case websocket.CloseNormalClosure, websocket.CloseGoingAway, websocket.CloseNoStatusReceived, websocket.CloseAbnormalClosure:
			return true
		}
	}
	return false
}

func wsCloseReason(err error) string {
	var ce *websocket.CloseError
	if errors.As(err, &ce) {
		return fmt.Sprintf("code=%d text=%q", ce.Code, ce.Text)
	}
	return err.Error()
}
