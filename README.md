# xiaozhi-server-rs

小智 ESP32 服务端的 **Rust 重写版**，只保留“基本功能够用”的对话链路，**所有模型能力全部走云端（豆包）**，不在本地跑任何推理模型。

> 参考实现：同级目录 `../xiaozhi-esp32-server`（Python 版）。本项目的字段、协议、消息格式均与 Python 版逐字节/逐字段对齐，便于对比调试。

## 1. 当前完成度

| 能力 | 状态 | 说明 |
|---|---|---|
| OTA `POST /xiaozhi/ota/` | ✅ | 返回 `server_time` / `firmware` / `websocket`，字段与 Python 版一致 |
| WebSocket `/xiaozhi/v1/` | ✅ | `device-id` 头识别、连接生命周期、出站消息统一队列 |
| `hello` → `welcome` | ✅ | 透传设备 `audio_params`，下发 `session_id` |
| `listen` start/stop/detect | ✅ | start 打断并重开 ASR；stop 触发服务端出最终结果 |
| `ping` → `pong` | ✅ | |
| `abort` 打断 | ✅ | 取消 LLM/TTS 任务（`CancellationToken`）+ 丢弃未发送音频 |
| 上行音频解码 | ✅ | Opus 16k 单声道 → PCM16（`audiopus`） |
| 下行音频编码 | ✅ | PCM 24k → Opus 60ms 帧，定速推送（`Instant` 补偿抖动） |
| 拾音门控 VAD | ✅ | 纯能量门限（无需 Silero/ONNX），`vad.rms_threshold` 可调 |
| ASR | ✅ | 豆包流式 ASR v3（`bigmodel_async`，二进制帧 + gzip） |
| LLM | ✅ | 火山方舟 Ark（OpenAI 兼容，SSE 流式） |
| TTS | ✅ | 豆包语音合成 HTTP v1（整句返回，`pcm`/`wav` 自动处理） |
| 上下文 | ✅ | 保留最近 N 轮（`llm.history_rounds`） |
| MCP / IoT 指令 | ❌ 未实现 | 收到后仅打印日志，不影响对话 |
| 情绪 emoji（`type: llm`） | ❌ 未实现 | 需要的话在 `run_turn` 里补一条消息即可 |
| 双向流式 TTS | ❌ 未实现 | 当前是 HTTP 整句合成（S 档），延迟略高于流式 |
| 多用户/智控台/记忆/声纹 | ❌ 未实现 | 定位就是“单机自用” |

## 2. 依赖

- Rust 1.80+（本机 1.95）
- 系统 `libopus`（`audiopus` 会自带源码编译，通常无需额外安装；如失败则 `apt install libopus-dev`）
- 不需要：Python / torch / ffmpeg / MySQL / Redis / 任何本地模型文件

## 3. 配置

编辑 `config.yaml`，只有 3 组值必填（**豆包是两套独立凭证**）：

| 位置 | 填什么 | 从哪拿 |
|---|---|---|
| `llm.api_key` | 火山方舟 API Key | https://console.volcengine.com/ark → API Key |
| `asr.appid` / `asr.access_token` | 语音识别 APPID + Token | https://console.volcengine.com/speech/service/10038（开通 seed-asr 2.0） |
| `tts.appid` / `tts.access_token` | 语音合成 APPID + Token | https://console.volcengine.com/speech/service/10035（开通 seed-tts 2.0 并购买音色） |

`tts.voice` 必须是**已购买**的音色 id，例如 `zh_female_xiaohe_uranus_bigtts`（2.0）或 `BV001_streaming`（普通音色）。

其余都有默认值，只有调手感时才动：

```yaml
log:
  level: info             # trace/debug/info/warn/error；RUST_LOG 可覆盖
  dir: logs               # 相对启动目录，自动创建
  keep: 2                 # 最多保留几个日志文件
  max_mb: 50              # 单文件上限，超过就切片
server:
  port: 8000              # websocket
  http_port: 8003         # OTA + 配置页
vad:
  rms_threshold: 500      # 噪声大造成误触发调高，说话识别不到调低
  start_frames: 2         # 连续多少帧有声音算开口（60ms/帧）
  end_frames: 6           # 静音多少帧算说完（约 400ms）
asr:
  resource_id: volc.seedasr.sauc.duration   # 并发版换成 volc.seedasr.sauc.concurrent
```

### 浏览器改配置

打开 `http://<服务器IP>:8003/config` 可以直接编辑 `config.yaml` 全文并保存：

- **没有密码**，能访问到这个端口就能改配置，只适合局域网（公网请套反向代理加认证）
- 保存前按 YAML 解析校验，写坏了不会覆盖当前文件；注释原样保留
- 保存后对**新连接**生效，正在通话的设备重连后生效
- 带 Origin 校验，挡掉别的网站发起的跨站提交

## 4. 运行与自测

```bash
# 填完 config.yaml 后先自检 3 组豆包凭证（不启动服务，失败会指出是哪一项）
cargo run -- --check config.yaml

cargo build --release
./target/release/xiaozhi-server-rs config.yaml     # 默认读取 ./config.yaml

# 协议自测（hello / listen / ping / abort，不需要真实 key）
python3 tests/check_ws.py
# OTA 自测
# websocket 地址按 Host 头自动组装；从 PC 自测时带上局域网 IP 才是设备会拿到的地址
curl -s -X POST http://127.0.0.1:8003/xiaozhi/ota/ \
  -H "Host: 192.168.1.100:8003" \
  -H "device-id: aa:bb:cc:dd:ee:ff" -H "client-id: test" \
  -H "device-version: 1.0.0" -d '{"board":{"type":"bread-compact-wifi"}}'
# 单元测试（Opus 编解码往返、wav 剥头）
cargo test
```

设备端：HTTP OTA 地址填 `http://<服务器IP>:8003/xiaozhi/ota/`，设备会自动拿到 `ws://<IP>:8000/xiaozhi/v1/`。

## 5. 代码结构（与 Python 版的对应关系）

| 文件 | 作用 | Python 对应 |
|---|---|---|
| `src/main.rs` | axum 路由：OTA + WS 升级，双端口监听 | `app.py` / `core/http_server.py` / `core/websocket_server.py` |
| `src/config.rs` | 配置结构与 welcome 组装、本机 IP 探测 | `config/config_loader.py` / `core/utils/util.py` |
| `src/session.rs` | 连接状态机、VAD 驱动、回合编排、定速推帧、打断 | `core/connection.py` / `core/handle/*.py` |
| `src/protocol.rs` | `tts` / `stt` / `pong` 消息格式 | `core/handle/sendAudioHandle.py` |
| `src/audio.rs` | Opus 编解码、wav 剥头 | `core/utils/opus_encoder_utils.py` |
| `src/vad.rs` | 能量门限 VAD | `core/providers/vad/silero.py`（改为无模型方案） |
| `src/asr.rs` | 豆包流式 ASR v3 二进制帧 | `core/providers/asr/doubao_stream.py` |
| `src/llm.rs` | Ark SSE 流式 + 切句 + TTS 文本清洗 | `core/providers/llm/openai/*` / `core/utils/textUtils.py` |
| `src/tts.rs` | 豆包 HTTP v1 合成 | `core/providers/tts/doubao.py` |

约 1,300 行，对比 Python 版 `core` 的 22,762 行。

## 6. 联调排查顺序

1. `python3 tests/check_ws.py` 通过 → 协议层 OK
2. 启动日志出现 `ASR 连接已建立` → 豆包 ASR 凭证 OK
3. 日志出现 `ASR 识别: xxx` → 麦克风/Opus/VAD 链路 OK
4. 日志出现 `用户: xxx` 且设备出声 → LLM + TTS 全链路 OK

常见报错：

| 日志 | 原因 |
|---|---|
| `Ark LLM 请求失败 401` | `llm.api_key` 错误或未开通模型 |
| `Ark LLM 请求失败 404` / `model not found` | `llm.model` 换成方舟的推理接入点 ID（`ep-xxxxxxxx`） |
| `ASR 初始化失败 ... 45000001` | `appid`/`access_token` 或 `resource_id` 与开通的服务不匹配 |
| `TTS 返回异常 ... "code":3001` | 音色未购买，或 `cluster` 与音色类型不匹配 |
| 设备一直没反应 | VAD 门限过高（降低 `rms_threshold`），或设备 OTA 未指向本机 |

## 7. 已知取舍

- **TTS 用 HTTP 整句合成**（S 档）：实现简单、稳定，代价是句间停顿比双向流式多 0.3–0.8s。要最低延迟可另加 `huoshan_double_stream` 模块（协议与 `asr.rs` 同为二进制帧，约 +300 行）。
- **VAD 用能量门限**：安静环境够用；噪声环境误触发时可换 `ort` + silero.onnx（违背“零本地模型”原则，故未做）。
- **单设备友好**：每连接独立任务与编解码器，多设备可用，但没做并发上限与限流。
- **空闲回收**：120 秒没有任何消息就断开该连接（正在回话时不计），设备会自己重连，对唤醒流程透明。
- **结束会话**：识别到「退下 / 退下吧 / 再见 / 拜拜」时回显 `stt`、下发 `goodbye` 并断开，不再走 LLM。
