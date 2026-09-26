// 探测后端 MC 服务器登录阶段返回的原始字节，判断是否启用了压缩。
package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"time"
)

func vi(x int) []byte {
	var b []byte
	u := uint64(x)
	for u >= 0x80 {
		b = append(b, byte(u)|0x80)
		u >>= 7
	}
	return append(b, byte(u))
}
func str(s string) []byte { return append(vi(len(s)), s...) }

func pkt(body []byte) []byte { return append(vi(len(body)), body...) }

func main() {
	addr := "192.168.1.10:25565"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}
	proto := 340
	if len(os.Args) > 2 {
		proto, _ = strconv.Atoi(os.Args[2])
	}
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		fmt.Println("dial:", err)
		return
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(8 * time.Second))

	host, portStr, _ := net.SplitHostPort(addr)
	port, _ := strconv.Atoi(portStr)

	// handshake (login state): id 0x00, protocol, server, port(short), next=2
	hs := []byte{0x00}
	hs = append(hs, vi(proto)...)
	hs = append(hs, str(host)...)
	var pb [2]byte
	binary.BigEndian.PutUint16(pb[:], uint16(port))
	hs = append(hs, pb[:]...)
	hs = append(hs, 0x02)
	c.Write(pkt(hs))

	// LoginStart: id 0x00, username
	ls := []byte{0x00}
	ls = append(ls, str("ProbeTester")...)
	c.Write(pkt(ls))

	// dump responses
	rd := bufioReader(c)
	for i := 0; i < 8; i++ {
		b, err := readOnePacket(rd)
		if err != nil {
			fmt.Printf("[%d] read err: %v\n", i, err)
			return
		}
		if len(b) == 0 {
			fmt.Printf("[%d] empty\n", i)
			continue
		}
		id, _ := binary.Uvarint(b)
		hexn := len(b)
		if hexn > 40 {
			hexn = 40
		}
		fmt.Printf("[%d] len=%d packetId=%d head=% x\n", i, len(b), id, b[:hexn])
		if id == 0x03 {
			// Set Compression: field 0 = threshold varint
			thr, _ := binary.Uvarint(b[1:])
			fmt.Printf("      => SET COMPRESSION threshold=%d (%s)\n", thr, map[bool]string{true: "DISABLED", false: "ENABLED"}[thr == 0xFFFFFFFF || int64(thr) == -1])
		}
	}
}

func bufioReader(c net.Conn) io.Reader { return c }

// readOnePacket reads [varint len][data]
func readOnePacket(r io.Reader) ([]byte, error) {
	var length int
	var shift uint
	for {
		var b [1]byte
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return nil, err
		}
		length |= int(b[0]&0x7f) << shift
		if b[0]&0x80 == 0 {
			break
		}
		shift += 7
		if shift > 28 {
			return nil, fmt.Errorf("varint too long")
		}
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}
