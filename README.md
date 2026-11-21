# xiaozhi-server-rs

小智 ESP32 的轻量级 Rust 服务端。语音识别（ASR）、大模型（LLM）和语音合成（TTS）全部调用云端接口（豆包 / 火山方舟），本地不运行任何推理模型，单机开箱即用。

## 1. 环境依赖

- **Rust**：1.88 或更高版本
- **Opus**：通常随 `audiopus` 自动编译；若编译失败，请安装系统库：
  - Ubuntu / Debian：`sudo apt install libopus-dev cmake build-essential`
  - macOS：`brew install opus cmake`

## 2. 快速搭建与运行

### 第一步：准备配置文件

```bash
cp -n config.example.toml config.local.toml
```

编辑 `config.local.toml`，填入以下必填凭证：

| 配置项 | 说明 | 获取位置 |
|---|---|---|
| `llm.api_key` / `llm.model` | 火山方舟 API Key 与推理接入点 ID（如 `ep-xxxx`） | [火山方舟控制台](https://console.volcengine.com/ark) |
| `speech.api_key` | 豆包语音项目 API Key（ASR 与 TTS 共用） | [豆包语音控制台](https://console.volcengine.com/speech/app) |
| `tts.voice` | 音色 ID（示例为 `zh_female_xiaohe_uranus_bigtts`） | 豆包语音控制台 |

> 提示：如果不填写 `[speech]`，也可沿用旧版 `[asr]` / `[tts]` 中的 `appid` 和 `access_token`。

### 第二步：编译并启动

```bash
cargo build --release
./target/release/xiaozhi-server-rs -c config.local.toml
```

启动后默认监听两个端口：
- **HTTP（OTA 与配置页）**：`8000`
- **WebSocket（设备音频通信）**：`26003`

### 第三步：连接设备与网页调参

- **设备端**：将固件 OTA 地址设为 `http://<服务器局域网IP>:8000/xiaozhi/ota/`，设备会自动获取 WebSocket 地址 `ws://<服务器IP>:26003/xiaozhi/v1/`。
- **网页配置**：在运行服务的本机浏览器打开 `http://127.0.0.1:8000/config`，可直接修改并校验保存配置（仅限本机访问；修改端口需重启生效）。

## 3. 常用配置说明

`config.local.toml` 中其余项均有默认值，按需调整即可：

```toml
[server]
http_port = 8000           # OTA 与管理页面端口
ws_port = 26003            # 设备 WebSocket 端口

[vad]
rms_threshold = 500        # 拾音能量门限：环境嘈杂调高，说话不触发调低
start_frames = 2           # 连续多少帧有声音算开始说话（60ms/帧）
end_frames = 6             # 静音多少帧算停顿（约 360ms）

[log]
level = "info"             # trace / debug / info / warn / error
dir = "logs"
```

## 4. 测试与排查

### 运行测试

```bash
cargo test --locked
```

### 测试 OTA 接口

```bash
curl -s -X POST http://127.0.0.1:8000/xiaozhi/ota/ \
  -H "Host: 192.168.1.100:8000" \
  -H "device-id: aa:bb:cc:dd:ee:ff" \
  -H "client-id: test" \
  -H "device-version: 1.0.0" \
  -d '{"board":{"type":"bread-compact-wifi"}}'
```

### 常见问题

| 现象 / 日志 | 排查方向 |
|---|---|
| LLM 返回 `401 Unauthorized` | 检查 `llm.api_key` 是否正确、模型是否已开通 |
| LLM 返回 `404` / `model not found` | `llm.model` 需填写方舟推理接入点 ID（`ep-xxxxxxxx`） |
| 语音接口报错 `401` / `45000010` / `45000001` | 核对 `speech.api_key` 及对应 ASR / TTS 资源开通状态 |
| 设备说话无反应 | 调低 `vad.rms_threshold`，或确认设备 OTA 地址是否指向本机 |

## 5. 目录结构

- `src/main.rs` / `src/lib.rs`：服务入口与路由（OTA、WebSocket、配置页）
- `src/session/`：WebSocket 连接生命周期、VAD 音频输入、对话回合与 TTS 推流
- `src/mcp/`：设备 MCP 客户端、工具发现与只读数据校验
- `src/asr/`、`src/tts/`、`src/llm.rs`：豆包 ASR / TTS 与火山方舟 LLM 客户端
- `src/config.rs` / `src/webcfg.rs`：TOML 配置加载与本机 Web 配置页
- `src/audio.rs` / `src/vad.rs` / `src/protocol.rs` / `src/ota.rs`：Opus 编解码、能量 VAD 与小智协议封装
