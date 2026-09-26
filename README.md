# Loom

> 把 EaglercraftX 的 WebSocket 帧，一针一线织进 Minecraft Java 的 TCP 字节流。

Loom 是用 **Go** 编写的 **EaglercraftX ⇄ Minecraft Java 版** 代理。它监听一个 WebSocket
端口，接收 EaglercraftX（浏览器 / WASM 客户端）发来的二进制帧，再把它们转发给一台普通的
Minecraft Java 服务端（如 Cuberite、Vanilla、Paper……），从而让网页端的 EaglercraftX 玩家
进入常规局域网服务器。

单个静态二进制即可部署，无运行时依赖。

---

## 核心特性

- **纯 Go 单文件部署**，无外部运行时；可选 TLS（`wss://`）。
- **协议版本区间协商**：`min_game_protocol ~ max_game_protocol` 与客户端上报的版本列表取交集，
  最大者胜出，覆盖 1.7.10 ~ 1.21.x 区间内可协商的任意版本。
- **作为压缩端点**：吸收后端的 `Set Compression`（Login 0x03）与 `LoginSuccess`（Login 0x02），
  对 WebSocket 侧输出无长度前缀的原始逻辑包。
- **代理主动登录**：真实 EaglercraftX 客户端在握手中继完成后**不会**自行发送 MC
  `Handshake`/`LoginStart`，由 Loom 代表客户端向后端发起。
- **离线模式（LAN）**：使用 `OfflinePlayer:<name>` 的 MD5 派生 UUID，不做在线验证。
- **MOTD 文本帧查询**：首帧 WebSocket 文本以 `accept:` 开头时返回 JSON 状态。
- **简洁的 TOML 配置**，所有字段可省略并带默认值。

---

## 工作原理

### 帧格式差异（关键点）

| 通道 | 包结构 |
| --- | --- |
| Minecraft TCP | `[varint 总长][varint packetId][fields]`；启用压缩后为 `[varint 总长][varint dataLength][payload]` |
| EaglercraftX WebSocket | `[varint packetId][fields]` —— **没有** 长度前缀 |

依据来自 Eaglercraft 官方网关解码器 `EaglerMinecraftDecoder.java`：每个 WS 二进制帧先读
`VarInt packetId`，其后即为字段，不含长度。Loom 因此在两个方向上做 framing 的转换，并在
后端开启压缩时充当"压缩端点"。

### 中继握手包 ID（EaglercraftX relay handshake）

| ID | 方向 | 含义 |
| --- | --- | --- |
| `0x01` | C→S | CLIENT_VERSION（LOGIN） |
| `0x02` | S→C | SERVER_VERSION（IDENTIFY） |
| `0x03` | — | VERSION_MISMATCH |
| `0x04` | C→S | CLIENT_REQUEST_LOGIN（USERNAME） |
| `0x05` | S→C | SERVER_ALLOW_LOGIN（SYNC_UUID） |
| `0x06` | S→C | SERVER_DENY_LOGIN |
| `0x07` | C→S | CLIENT_PROFILE_DATA（皮肤等，可 0..n 帧） |
| `0x08` | C→S | CLIENT_FINISH_LOGIN |
| `0x09` | S→C | SERVER_FINISH_LOGIN |
| `0x40` | S→C | play 阶段断开（KICK） |
| `0xFF` | S→C | SERVER_ERROR |

握手完成后进入 play 透传：后端 TCP 包解除压缩 / 去掉长度前缀后原样写 WS；客户端 WS 帧
补上 `varint 0`（压缩占位）与 `varint 总长` 后写 TCP，并过滤 `EAG|` / `MC|` 等插件通道。

---

## 快速开始

### 构建

```bash
go build -o loom .
```

### 运行

```bash
./loom -config config.toml
```

启动横幅示例：

```
Loom 1.0.0 启动，监听 :8080，后端 192.168.1.10:25565
```

用浏览器直接访问监听端口会看到落地页提示；在 EaglercraftX 客户端里选择
**Direct Connect**，填入 `ws://<本机IP>:8080` 即可连接。

---

## 配置

`config.toml`（所有字段可省略，缺省值见 `config.go`）：

```toml
listen    = ":8080"                       # WebSocket 监听地址
backend   = "192.168.1.10:25565"          # 后端 Minecraft Java 服务器 host:port
brand     = "Loom"                        # 握手时上报给客户端的品牌名
version   = "1.0.0"                       # 展示版本串
max_players = 100                         # 并发玩家上限
motd      = ["§bEaglercraft 服务器", "§7Powered by Loom"]

min_game_protocol   = 4                   # 游戏协议协商下界（4 = 1.7.10）
max_game_protocol   = 770                 # 游戏协议协商上界（770 = 1.21.x）
status_protocol     = 110                 # MOTD 查询上报给后端的协议号

login_timeout_seconds = 30                # 握手超时
# tls_cert_file = "cert.pem"              # 可选：填写后启用 wss://
# tls_key_file  = "key.pem"
```

常用协议号对照（见 wiki.vg）：`47 = 1.8.9`，`110 = 1.12.2`，`770 = 1.21.2`。

---

## 项目结构

```
Loom/
├── main.go        # HTTP/WS 服务入口、落地页、启动横幅
├── session.go     # 核心：中继握手、双向 pump、压缩/登录状态机
├── mcproto.go     # VarInt / String / UUID 等 MC 协议编解码
├── config.go      # TOML 配置与默认值
├── config.toml    # 示例配置
├── testclient/    # 独立调试模块：模拟 EaglercraftX 客户端（motd / handshake / play）
└── probe/         # 独立调试模块：向后端裸发 MC 登录包并 dump 前若干响应
```

`testclient` 与 `probe` 各自是独立 Go module，不参与主程序构建：

```bash
cd testclient && go run . motd
cd testclient && go run . play 47
cd probe      && go run .
```

---

## 参照仓库

本项目在协议实现上参考并核对了以下上游代码，**特此致谢**：

1. **eaglerproxy**（TypeScript）
   - 本地路径：`eaglerproxy-master/`（`package.json` name 为 `eaglerproxy`）
   - 作者：WorldEditAxe、qix3；许可证：MIT
   - 用途：代理整体行为、中继握手与逐包处理逻辑的参考实现。
     尤其是 `src/proxy/Player.ts` 印证了"代理需代表客户端主动发起 MC 登录"这一关键点。

2. **eaglercraft-1.8 / EaglercraftXBungee**（Java，官方网关插件）
   - 上游仓库：<https://github.com/Git-Eaglercraft-rip/eaglercraft-1.8>
   - 本地为 blobless 稀疏克隆，路径 `eaglercraft-ref/`，仅取用：
     `gateway/EaglercraftXBungee`、`gateway/EaglercraftXVelocity`、`sources/main`、`sources/protocol-game`
   - 用途：**权威的 WS 帧格式**依据 ——
     `.../gateway_bungeecord/server/EaglerMinecraftDecoder.java` 明确了
     "WS 二进制帧 = `[VarInt packetId][fields]`，无长度前缀"，以及各中继包 ID。

3. **wiki.vg — Minecraft Protocol**
   - <https://wiki.vg/Protocol>
   - 用途：TCP 包 framing、协议版本号、`Set Compression` / `LoginSuccess` 等登录阶段包定义。

> 注：`eaglerproxy` 的 TypeScript 参考源码在本仓库以本地目录形式提供，其包元数据中未声明
> 公开仓库地址，故此处不臆测其 URL。

---

## 说明与免责声明

- 面向 **局域网 / 离线** 部署：不做 Mojang 在线验证，客户端身份由用户名派生。
- Eaglercraft、EaglercraftX、Minecraft 及相关商标为其各自持有者所有，本项目与之无隶属关系。
- 参考实现的版权与许可证以各上游仓库声明为准。
