package main

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"os"

	"github.com/BurntSushi/toml"
)

// Config 使用 TOML 格式，全部字段可省略，缺省值见 defaultConfig
type Config struct {
	Listen     string `toml:"listen"`      // 监听地址，如 ":8080"
	Backend    string `toml:"backend"`     // 后端 MC 服务器 host:port
	Brand      string `toml:"brand"`       // 握手时发给客户端的品牌名
	Version    string `toml:"version"`     // 展示版本串
	ServerUUID string `toml:"server_uuid"` // 用于 MOTD 的服务端 UUID（留空自动生成）
	MaxPlayers int    `toml:"max_players"` // 代理同时最大玩家数
	MOTD       []string `toml:"motd"`      // 后端无响应时显示的 MOTD 行（最多两行）

	// 游戏协议版本协商范围（对应 Minecraft 版本，见 wiki.vg Protocol：47=1.8.9、109=1.12、762=1.19、767=1.20.2、770=1.21.2...）
	// 客户端 LOGIN 里的游戏版本列表与此范围取交集，最大者胜出。
	MinGameProtocol int `toml:"min_game_protocol"`
	MaxGameProtocol int `toml:"max_game_protocol"`

	// 状态 ping 时上报给后端的协议号（仅影响 MOTD 查询；很多服务端不校验此项）
	StatusProtocol int `toml:"status_protocol"`

	LoginTimeout int `toml:"login_timeout_seconds"` // 握手超时

	// 可选 TLS（wss://）。留空=不启用
	TLSCertFile string `toml:"tls_cert_file"`
	TLSKeyFile  string `toml:"tls_key_file"`
}

func defaultConfig() *Config {
	return &Config{
		Listen:          ":8080",
		Backend:         "127.0.0.1:25565",
		Brand:           "Loom",
		Version:         versionString,
		MaxPlayers:      100,
		MOTD:            []string{"§bEaglercraft 服务器", "§7Via Loom"},
		MinGameProtocol: 4,    // 1.7.10
		MaxGameProtocol: 770,  // 1.21.x（EaglercraftX IR/WASM 支持到的话就可以）
		StatusProtocol:  47,
		LoginTimeout:    30,
	}
}

func loadConfig(path string) (*Config, error) {
	cfg := defaultConfig()
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil // 无配置文件时使用默认值
		}
		return nil, err
	}
	if err := toml.Unmarshal(b, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c *Config) serverUUID() string {
	if c.ServerUUID != "" {
		return c.ServerUUID
	}
	// 由品牌名派生一个稳定的展示用 UUID（仅用于 MOTD，非安全用途）
	sum := md5.Sum([]byte(c.Brand))
	h := hex.EncodeToString(sum[:])
	c.ServerUUID = fmt.Sprintf("%s-%s-4%s-8%s-%s", h[0:8], h[8:12], h[13:16], h[17:20], h[20:32])
	return c.ServerUUID
}

func (c *Config) motdLines() (string, string) {
	l1, l2 := "", ""
	if len(c.MOTD) > 0 {
		l1 = c.MOTD[0]
	}
	if len(c.MOTD) > 1 {
		l2 = c.MOTD[1]
	}
	return l1, l2
}
