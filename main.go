package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

const versionString = "1.0.0"

const appName = "Loom"

func main() {
	cfgPath := flag.String("config", "config.toml", "配置文件路径")
	showVer := flag.Bool("version", false, "显示版本并退出")
	flag.Parse()

	if *showVer {
		fmt.Println(strings.ToLower(appName), versionString)
		return
	}

	cfg, err := loadConfig(*cfgPath)
	if err != nil {
		log.Fatalf("读取配置失败: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if !isWSUpgrade(r) {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusUpgradeRequired)
			fmt.Fprintf(w, "%s %s 运行中。\n在 EaglercraftX 中使用 ws://%s 连接（Direct Connect）。\n",
				appName, versionString, hostFor(r))
			return
		}
		handleWS(w, r, cfg)
	})

	srv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	log.Printf("%s %s 启动，监听 %s，后端 %s", appName, versionString, cfg.Listen, cfg.Backend)
	useTLS := cfg.TLSCertFile != "" && cfg.TLSKeyFile != ""
	if useTLS {
		log.Printf("TLS 已启用 (证书 %s)", cfg.TLSCertFile)
	}
	if err := listenAndServe(srv, cfg, useTLS); err != nil {
		log.Fatalf("服务器退出: %v", err)
		os.Exit(1)
	}
}

func listenAndServe(srv *http.Server, cfg *Config, useTLS bool) error {
	if useTLS {
		return srv.ListenAndServeTLS(cfg.TLSCertFile, cfg.TLSKeyFile)
	}
	return srv.ListenAndServe()
}

func isWSUpgrade(r *http.Request) bool {
	for _, v := range r.Header["Upgrade"] {
		if equalFoldASCII(v, "websocket") {
			return true
		}
	}
	return false
}

func equalFoldASCII(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]
		if 'A' <= ca && ca <= 'Z' {
			ca += 32
		}
		if 'A' <= cb && cb <= 'Z' {
			cb += 32
		}
		if ca != cb {
			return false
		}
	}
	return true
}

func hostFor(r *http.Request) string {
	h := r.Host
	if h == "" {
		return "127.0.0.1"
	}
	return h
}
