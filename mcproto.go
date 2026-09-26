package main

import (
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// ---- Minecraft 基础类型读写（wiki.vg: VarInt=LEB128, Short=大端） ----

func readVarInt(b []byte) (value, size int, err error) {
	var x uint64
	for i := 0; i < 5; i++ {
		if i >= len(b) {
			return 0, 0, io.ErrShortBuffer
		}
		byt := b[i]
		x |= uint64(byt&0x7f) << (7 * uint(i))
		if byt&0x80 == 0 {
			if x > 0x7fffffff {
				return 0, 0, errors.New("varint too large")
			}
			return int(x), i + 1, nil
		}
	}
	return 0, 0, errors.New("varint overflow")
}

func varIntLen(x int) int {
	u := uint64(x)
	switch {
	case u < 1<<7:
		return 1
	case u < 1<<14:
		return 2
	case u < 1<<21:
		return 3
	case u < 1<<28:
		return 4
	default:
		return 5
	}
}

func appendVarInt(buf []byte, x int) []byte {
	u := uint64(x)
	for u >= 0x80 {
		buf = append(buf, byte(u)|0x80)
		u >>= 7
	}
	return append(buf, byte(u))
}

// writeVarIntBuf 返回仅含 varint 的新切片
func writeVarIntBuf(x int) []byte { return appendVarInt(nil, x) }

func readShort(b []byte) (int, error) {
	if len(b) < 2 {
		return 0, io.ErrShortBuffer
	}
	return int(binary.BigEndian.Uint16(b)), nil
}

func writeShort(v int) []byte {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, uint16(v))
	return b
}

// readStringBuf 读取 MC String（VarInt 长度 + UTF-8），返回内容和消耗的字节数
func readStringBuf(b []byte, maxLen int) (string, int, error) {
	ln, sz, err := readVarInt(b)
	if err != nil {
		return "", 0, err
	}
	if ln < 0 || ln > maxLen || len(b) < sz+ln {
		return "", 0, fmt.Errorf("bad string length %d", ln)
	}
	return string(b[sz : sz+ln]), sz + ln, nil
}

func appendString(b []byte, s string) []byte {
	b = appendVarInt(b, len(s))
	return append(b, s...)
}

// ---- Offline UUID（Minecraft 正版验证关闭时的离线 UUID：MD5("OfflinePlayer:"+name) v3） ----

type uuid [16]byte

func offlineUUID(name string) uuid {
	sum := md5.Sum([]byte("OfflinePlayer:" + name))
	var u uuid
	copy(u[:], sum[:])
	u[6] = (u[6] & 0x0f) | 0x30
	u[8] = (u[8] & 0x3f) | 0x80
	return u
}

func (u uuid) String() string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:16])
}
