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
| TTS | ✅ | 豆包语音合成 V3 HTTP 单向流式接口，拼接分块 PCM 后按句播放 |
| 上下文 | ✅ | 保留最近 6 个完整回合（含 assistant tool_calls / tool results）；取消回合不写历史 |
| 设备 MCP + LLM tool calling | ✅ | `rmcp` 客户端；时间/缓存天气/本地传感器三个只读读数工具固定 schema + 硬兜底，其余设备标准工具（音量/亮度/主题/拍照/板级动作）透传；见第 8 节 |
| IoT 指令 | ❌ 未实现 | 不执行设备任意指令 |
| 情绪 emoji（`type: llm`） | ❌ 未实现 | 需要的话在 `run_turn` 里补一条消息即可 |
| 双向流式 TTS | ❌ 未实现 | 当前是 HTTP 整句合成（S 档），延迟略高于流式 |
| 多用户/智控台/记忆/声纹 | ❌ 未实现 | 定位就是“单机自用” |

## 2. 依赖

- Rust 1.88+（`rmcp 3.10.0` 的最低要求；此次验证使用 1.94.1）
- 系统 `libopus`（`audiopus` 会自带源码编译，通常无需额外安装；如失败则 `apt install libopus-dev`）
- 不需要：Python / torch / ffmpeg / MySQL / Redis / 任何本地模型文件

## 3. 配置

将 [config.example.toml](config.example.toml) 复制为本地配置，再填入凭证和模型 ID。`config.local.toml` 已被 Git 忽略。

```bash
cp -n config.example.toml config.local.toml
```

| 位置 | 填什么 | 从哪拿 |
|---|---|---|
| `llm.api_key` / `llm.model` | 方舟 API Key 和已开通的文本模型 ID | [火山方舟控制台](https://console.volcengine.com/ark) |
| `speech.api_key` | 豆包语音项目 API Key，供 ASR 和 TTS 共用 | 已开通流式语音识别 2.0 和语音合成 2.0 的项目。[豆包语音控制台指引](https://docs.volcengine.com/docs/doubao-voice/console-guidance?lang=zh) |

`tts.voice` 填当前项目可用的音色 ID。示例使用小何 2.0（`zh_female_xiaohe_uranus_bigtts`）。

旧配置省略 `[speech]` 时，ASR 使用 `[asr]` 的 `appid` 和 `access_token`，TTS 使用 `[tts]` 的 `appid`、`access_token` 和 `cluster` 调用 V1。V1 需要选用支持的旧版音色。

其余都有默认值，只有调手感时才动：

```toml
[log]
level = "info"             # trace/debug/info/warn/error；RUST_LOG 可覆盖
dir = "logs"               # 相对启动目录，自动创建
keep = 2                   # 最多保留几个日志文件
max_mb = 50                # 单文件上限，超过就切片

[server]
ws_port = 26003            # WebSocket
http_port = 8000            # OTA + 配置页

[vad]
rms_threshold = 500        # 噪声大造成误触发调高，说话识别不到调低
start_frames = 2           # 连续多少帧有声音算开口（60ms/帧）
end_frames = 6             # 静音多少帧算停顿（约 360ms）。停顿只停止上传尾音，不结束整轮

[asr]
resource_id = "volc.seedasr.sauc.duration"  # 并发版换成 volc.seedasr.sauc.concurrent

[tts]
resource_id = "seed-tts-2.0"
voice = "zh_female_xiaohe_uranus_bigtts"
```

服务端口明确区分为 `server.ws_port`（WebSocket）和 `server.http_port`（HTTP）。旧字段 `server.port` 仍作为 `ws_port` 的兼容别名，但不能与 `ws_port` 同时填写。

### 浏览器改配置

在运行服务的电脑上打开 `http://127.0.0.1:8000/config`，可编辑并保存本次启动使用的配置文件。监听端口不在重载时改变：保存后重启才换端口，重启前 OTA 仍下发当前实际监听的端口。两个端口都在启动时绑定，任一被占用则启动失败并报出端口号。

- 配置页仅允许本机回环连接和本机 Host。设备的 OTA 与 WebSocket 仍可从局域网访问。反向代理应排除 `/config`。
- 保存前按 TOML 解析校验，写坏了不会覆盖当前文件；注释原样保留
- 模型等配置保存后对**新连接**生效，正在通话的设备重连后生效
- 带 Origin 校验，挡掉别的网站发起的跨站提交

## 4. 运行与自测

```bash
# 自检方舟请求、ASR 模型初始化和 TTS 音频合成
cargo run -- --check config.local.toml

cargo build --release
./target/release/xiaozhi-server-rs config.local.toml

# 协议自测（hello / listen / ping / abort，不需要真实 key）
python3 tests/check_ws.py
# OTA 自测
# websocket 地址按 Host 头自动组装；从 PC 自测时带上局域网 IP 才是设备会拿到的地址
curl -s -X POST http://127.0.0.1:8000/xiaozhi/ota/ \
  -H "Host: 192.168.1.100:8000" \
  -H "device-id: aa:bb:cc:dd:ee:ff" -H "client-id: test" \
  -H "device-version: 1.0.0" -d '{"board":{"type":"bread-compact-wifi"}}'
# 全量测试（包括本地假设备/假 LLM；不需要云凭据、硬件或 Python）
cargo test
```

`tests/check_ws.py` 是可选的 Python 冒烟测试，需安装 `websockets`（本轮使用 15.0.1）；`cargo test` 的真实 WS 回归不需要 Python。

设备端：HTTP OTA 地址填 `http://<服务器IP>:8000/xiaozhi/ota/`，设备会自动拿到 `ws://<IP>:26003/xiaozhi/v1/`。两个端口都被占用时服务不会半启动：启动日志会报出绑定失败的端口。`8000` 与 Python 版默认的 WebSocket 端口相同，两套服务不要同时跑在同一台机器的同一网络上。

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
| `src/llm.rs` | OpenAI-compatible SSE、工具增量解析、ID 校验与有界调用循环；返回最终文本和完整历史 | `core/providers/llm/openai/*` |
| `src/mcp.rs` | rmcp 客户端、XiaoZhi envelope、发现/工具开放规则/读数校验；`ToolTurn` 集中固定提示、不可用兜底及缓存警告 | — |
| `tests/support/device_tools.rs` | 无云假设备 / 假 LLM / 真实 WS reader 回归（由 main 的测试模块引入） | — |
| `src/tts.rs` | 豆包 V3 HTTP 合成和 V1 兼容 | `core/providers/tts/doubao.py` |

MCP 复用官方 Rust SDK `rmcp`（Apache-2.0），SSE 帧解析复用 `eventsource-stream`（MIT/Apache-2.0）；版本由 `Cargo.lock` 锁定，不在项目里重写通用 MCP 或云端 SDK。

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
| 语音接口返回 `401` / `45000010` | 核对 `speech.api_key` 和模型开通的语音项目 |
| `ASR 初始化失败 ... 45000001` | 核对 ASR 资源 ID 及该资源的开通状态 |
| `TTS 返回错误 code=...` | 核对 TTS 资源 ID、音色和项目权限，记录响应中的 LogID |
| 设备一直没反应 | VAD 门限过高（降低 `rms_threshold`），或设备 OTA 未指向本机 |

## 7. 已知取舍

- **TTS 用 HTTP 整句合成**（S 档）：实现简单、稳定，代价是句间停顿比双向流式多 0.3–0.8s。要最低延迟可另加 `huoshan_double_stream` 模块（协议与 `asr.rs` 同为二进制帧，约 +300 行）。
- **VAD 用能量门限**：安静环境够用；噪声环境误触发时可换 `ort` + silero.onnx（违背“零本地模型”原则，故未做）。
- **单设备友好**：每连接独立任务与编解码器，多设备可用，但没做并发上限与限流。
- **空闲回收**：120 秒没有任何消息就断开该连接（正在回话时不计），设备会自己重连，对唤醒流程透明。
- **结束会话**：识别到「退下 / 退下吧 / 再见 / 拜拜」时回显 `stt`、下发 `goodbye` 并断开，不再走 LLM。
- **免唤醒追问**：固件默认开启，可在配网页关闭。设备在同一 WebSocket 上用现有 `listen start/stop` 发起下一轮，服务端复用最近的会话上下文。VAD 和设备 stop 均可结束音频输入，每轮接受一次收尾。ASR 与回复绑定同一个取消令牌，新一轮或 abort 会取消旧输入及回复。服务端先完成历史提交，再发送该轮的 `tts stop`。
- **延迟诊断**：Debug 日志记录 ASR 从 flush 到最终文本、LLM 与工具合计耗时、TTS 整句合成耗时，以及文本轮次到首音频写入确认的耗时。


## 8. ESP32 设备 MCP 与 LLM 工具调用

### 连接与协议

设备继续使用 `espressif/esp_xiaozhi` + `mcp-c-sdk`，hello 声明 `features.mcp=true`。服务端先发 welcome，再在独立任务中执行：

1. `initialize`：请求 `protocolVersion="2024-11-05"`、`capabilities={}`、服务端的 `clientInfo`。接受已知的 initialize 版本 2024-11-05 / 2025-03-26 / 2025-06-18 / 2025-11-25，要求响应 capabilities 包含 tools。
2. `notifications/initialized`（通知，没有 id）。
3. `tools/list`；有 `nextCursor` 就继续以 `params.cursor` 请求，直到结束，再一次性发布可用工具。
4. LLM 发起工具调用时发送 `tools/call`，`params={"name":"self.…","arguments":{}}`。SDK 可能附带标准 `_meta.progressToken`，它不是工具参数，设备不应把它当额外 arguments。

每个 payload 都由 `rmcp` 生成/解析并关联 JSON-RPC id，仅包上一层：

```json
{"type":"mcp","session_id":"welcome 返回的当前连接 ID","payload":{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"self.get_current_time","arguments":{}}}}
```

设备响应保留相同 id，payload 内放 `result` 或标准 `error`；envelope 必须带当前 `session_id`。错误 session 被忽略，未知/迟到 id 不会满足新请求。不要把 JSON-RPC payload 再编码成字符串。设备侧 `esp_xiaozhi` 已负责这些封装，不需另写 WebSocket/MCP 协议。

所有上行 WS 消息（包含完整 envelope，不只是 payload）限制为 **64 KiB**，由 axum/tungstenite 在 JSON 解析前执行，分片按重组后的累计长度计算；超限关闭连接。MCP reader 不再为了量长度重新序列化 JSON，也不再先复制整个 `Value` 才交给 rmcp 反序列化。

**SDK 适配评估**：`rmcp::Transport<RoleClient>` 支持独立 send/receive，能直接复用现有 WS writer 队列和有界 reader 转交通道。`serve_with_ct` 提供标准 initialize 生命周期，`list_tools`/`call_tool` 提供请求关联及放弃请求时的取消通知，因此没有降级为自写 JSON-RPC/MCP 实现。局部代码只做 envelope、设备字段校验、分页资源上限和工具开放规则。已只读核对两份 ESP 源码：`esp_xiaozhi_mcp.c`（自写设备）将 manager 响应解析为 payload 对象并附当前 session_id；`esp_mcp_tool.c` 对空 properties 输出 object / additionalProperties=false，且不输出空 required 数组；上游 `xiaozhi-esp32` 的 `mcp_server.cc` 同样输出空 schema 并支持带参工具。此为源码兼容性核对，不代替实机互通验证。

### 工具开放规则与模型别名

设备 `tools/list` 返回的标准工具分两类处理；`withUserTools` 从不请求，所以设备把重启、刷机、截屏上传这类用户权限动作藏在用户工具里，模型看不到也调不到。

1. **只读数据工具**（仅这三个）使用服务端固定描述与固定空参数 schema，设备端 schema 必须精确为下列形式，否则该工具不开放：

| MCP 名称 | OpenAI 函数名 |
|---|---|
| `self.get_current_time` | `get_current_time` |
| `self.get_weather_forecast` | `get_weather_forecast` |
| `self.get_sensor_reading` | `get_sensor_reading` |

```json
{"type":"object","properties":{},"additionalProperties":false}
```

使用别名是因为许多 OpenAI-compatible 提供商不接受函数名中的点；设备端名称不变。

2. **其余标准工具**（`self.get_device_status`、`self.audio_speaker.set_volume`、`self.screen.set_brightness`、`self.screen.set_theme`、`self.camera.take_photo`，以及 `self.otto.*` 这类板级动作）按设备声明的 description 与 `inputSchema` 透传。函数名把点号等非法字符换成下划线（`self.audio_speaker.set_volume` → `self_audio_speaker_set_volume`）；超过 64 字节、重名或与只读别名冲突的直接丢弃。schema 必须是 `type: object`，且不加 `strict`（设备生成的 schema 常带可选参数，严格模式会被提供商拒绝）。

服务端不再维护第二份工具名清单：设备作为标准工具暴露什么，模型就能调什么（`self.reboot` 这类默认藏在用户工具里，只有显式请求 `withUserTools` 才会出现，本服务端从不请求）。重复名称、无效/循环 cursor、发现超时则整次发现失败。

执行时仍按发现结果匹配函数名：只读工具只接受 JSON 空对象 `{}`，`null`、空字符串、数组或任何参数键都拒绝；动作工具接受任意 JSON 对象并原样作为 `arguments` 转发。两者都只接受一个 text content；动作工具的 JSON 结果会被解析，纯文本（如 `true`）按字符串交给模型。动作工具调用失败（包括模型编造的工具名）时用固定文案替换本轮回答，避免模型声称“已完成”。

### 只读工具的返回约定

只读工具返回的城市、天气缓存、时间与传感器数据会作为 tool message 上传到配置的 LLM 提供商；这不是纯本地问答。不要在工具结果里混入密码或其他秘密。

成功返回一个 JSON **文字** content，`isError` 可省略或为 false：

```json
{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"{\"synchronized\":false}"}],"isError":false}}
```

只读工具只接受一个 text content，text 内必须为 JSON object。`isError=true`、RPC error、非 JSON、非 object、字段不完整都作为不可用数据，不制造默认读数。不要返回图片、资源链接或多个文字块。

字段约定（字段名区分大小写）：

- **时间**：`{"synchronized":false}`；已对时则为 `{"synchronized":true,"local_time":"2026-11-17T16:30:00+08:00"}`。`local_time` 须为带 UTC 偏移的 RFC3339 / ISO 本地时间。ESP32 目前保守地只在**显式 SNTP 成功且已知天气 UTC 偏移**时声明 synchronized=true；将来若支持 OTA 对时，由设备依据验证成功状态决定。Rust 仅信任设备显式状态，不从系统时钟、hello、天气抓取时间或服务器时间推断已对时。
- **传感器**：`{"available":false}`；可用时为 `{"available":true,"temperature_c":23.5,"humidity_percent":58}`。必须是设备本地实际采样，数值有限，湿度 0..100；不能借用天气 API 的温湿度。
- **天气**：无缓存时 `{"available":false}`；有缓存时字段如下（`available:true` 可省略）：

```text
{
  "available": true,
  "city": "杭州",
  "utc_offset_seconds": 28800,
  "fetched_at": "2026-11-17T15:00:00+08:00",
  "fetched_at_known": true,
  "age": 5400,
  "stale": true,
  "retrying": true,
  "days": [设备最近成功快照的全部 5 条日预报],
  "hours": [同一快照的全部 21 条逐时预报]
}
```

`fetched_at_known=true` 时，`fetched_at` 为带偏移的 RFC3339 字符串或正 Unix 秒数；兼容旧设备省略 fetched_at_known 但返回有效时间戳。抓取时尚未对时则返回 **`"fetched_at":null,"fetched_at_known":false`**，天气缓存仍可用；缺字段、错误标志类型、null 搭配 true/省略标志、false 搭配非 null 时间戳均拒绝。不要伪造 Unix timestamp=0，服务端也拒绝这个哨兵值。`age` 为来自 `esp_timer` 单调时钟的非负秒数，与墙上时钟/SNTP 无关，不能用服务器时间减 age 来补造 fetched_at。`stale` / `retrying` 为布尔值，UTC 偏移为整数秒。日/小时条目内部字段原样保留供模型理解，例如日期/小时、天气码、摄氏温度、降水概率；服务端检查数量和元数据，不重复实现 ESP32 已维护的天气业务校验。这里是返回结构说明，不是可直接发送的 JSON 演示数据。

固定天气工具描述承载静态语义，不要求设备在每次结果中重复 `age_unit`、`weather_code_system`、`precip_probability_unit`、`hours_time_basis`：`age` 单位为秒，来自设备单调时钟；天气码采用 WMO；降水概率单位为百分比（0..100），不是实时降水观测；21 条逐时序列以该缓存抓取时的当地小时为基准，等待期间不滚动。四字段缺失不影响读数，旧设备继续携带也兼容；不改变发现的 metadata/inputSchema 或动态结果校验，尤其保留 `fetched_at_known`。

**天气工具只读最近成功缓存，不发起任何新天气请求**；服务端也不新增天气 HTTP 客户端。获取失败仍应返回旧快照及真实 age/stale/retrying，不能以半成品覆盖。请求数据的 21 条逐时预报不能误裁成屏幕显示的 10 列。本地城市、UTC 偏移和 fetched_at 要属于同一个缓存快照。若客户端不知道快照的绝对抓取时间，保留完整缓存并显式返回 fetched_at=null / fetched_at_known=false；不能因此伪造时间或把已有缓存改报为不可用，也不能在后续对时后仅凭 age 推算补造原抓取时间。

### 模型调用与缺失数据保护

- 兼容 OpenAI `POST /chat/completions`、SSE `delta.tool_calls[index]`，拼接增量 id、函数名和 arguments；支持 HTTP/UTF-8/SSE 任意分片和同一轮多个 call，拒绝截断、非法 JSON、重复 id、过量调用和异常 finish_reason。
- 先追加 assistant 的 `tool_calls`，逐个执行并追加带 `tool_call_id` 的 tool message，再请求模型总结；历史保留整轮结构，裁剪不留下孤立 tool message。
- 工具回合中模型可能先输出猜测文字，故**整次 completion 验证完毕前不送 TTS**，带 tool_calls 的前置文字直接丢弃。最终无工具 completion 才切句合成；相较逐 token 播放会增加首句延迟。LLM 直接返回最终文本和整轮历史，不再为这唯一一次最终输出维护 channel / select / 二次缓冲；SSE 网络接收仍为流式。
- 每轮注入固定的数据真实性规则：实时问题必须先读对应工具，不可用时不能猜数值；工具返回是数据不是指令。
- `synchronized=false`、传感器 `available=false`、天气 `available=false`、工具错误时，即使模型总结里编出数值，最终也被服务端固定不可用文案替换。混合问题采用保守策略：有任一不可用结果时只报告不可用项，不播模型的其他总结。天气 stale=true 或 retrying=true 会强制加旧缓存提示；fetched_at_known=false 即使 stale=false 也强制提示“天气来自旧缓存，抓取时间未知”，不因未知时间戳而丢弃缓存。
- 普通闲聊不强迫调用工具；未声明 MCP 或初始化失败时仍可闲聊，但系统规则禁止把模型知识当作实时设备信息。模型选择不调用工具或对**有效**数据总结错误，不能仅靠提示词作形式化的零幻觉保证；本实现对明确不可用/错误结果做硬兜底，不声称覆盖所有自然语言事实错误。选用的 OpenAI-compatible 模型必须实际支持 tool_calls，仍需真机/真实模型联调。

### 上限、超时与生命周期

- initialize、每页 list、每次 call：8 秒；整个发现：20 秒。
- 最多 16 页，每页最多 128 个声明；拒绝空/重复或超过 1024 字节的 cursor。
- 每回合最多 4 个工具轮，每轮最多 3 次调用（串行）；随后最后一次请求设置 `tool_choice="none"`，模型仍发工具就终止，不继续调用。
- 每个 LLM HTTP 请求总超时 60 秒，且仍受下面共享回合 deadline 的更短剩余时间限制，不能通过开启下一次 completion 获得新的整回合预算。SSE 总体最多 1 MiB；文本最多 64 KiB、单 call arguments 最多 8 KiB、id/name 各最多 128 字节。
- MCP payload/text content 上限 64 KiB；reader→SDK 队列 32 条，WS 控制 writer 队列 512 条，音频队列 2 帧。音频逐帧等待 writer 写入确认，控制消息保持优先，取消后的排队音频被丢弃；如果取消时二进制帧正在写 socket，则关闭连接，避免部分帧在 deadline 后继续发送。单次 WS 写最多等待 5 秒，超时断连。reader 不等待任何 RPC 或完整 LLM/TTS；ASR/手动文本也用有界非阻塞转交。队列溢出关闭连接，不无限积压或让 reader 等死。
- abort / 新一轮 listen start 会取消当前回合，丢弃 LLM/HTTP/TTS future，并由 rmcp 对已发出、未完成的工具请求尽力发送 `notifications/cancelled`。断连同时结束 MCP、回合和 writer；链路已断时不保证取消通知可送达。设备只读同步操作可能已完成，取消不能撤销已执行的读取。
- MCP 每条 WS 连接只初始化一次：每次唤醒重新 hello 都返回同一 session_id，discovery 进行中或已完成时均不重入 initialize/tools/list，也不重置服务端 RPC ID 序列。ESP 的 hello 处理只清重复请求 ID 记录，不清已注册工具和初始化状态。握手失败后本连接不自动重试；重连再发现。当前工具集合按连接固定，不动态处理 tools/list_changed 重新授权；设备增加/减少工具后重连。没有 MQTT transport、MCP sampling/roots/elicitation 授权，也没有新增云 SDK、传感器驱动或 ESP32 固件改动。
- 仍使用现有 TOML `server.ws_port` / `server.http_port`，不增加必填配置。以上上限是代码常量，不是新增 TOML 字段。

### 跨端首音频与整回合 deadline

无需新增用户配置；常量在 `src/session.rs`。时钟使用 Tokio 单调时间，**不因工具进度、下一次 completion、TTS 状态或下一句语音而重置**。

- 起点：服务端接收并开始处理一条非空最终识别文本/手动文本的回合。之前的录音与 ASR 识别不是这个文本回合的一部分，客户端原有 15 秒录音限制不变；客户端从停止录音后的首音频等待中需另给 ASR/网络余量。
- **正常首音频截止 65 秒**：统一涵盖等待 MCP discovery、所有 LLM completion、所有 tools/call、首次 TTS 合成、Opus 编码、队列背压及首帧 WS 写入确认。`tts:start` / `sentence_start`、SSE 文本、MCP 响应以及只排队未写出的音频，均不能解除这个截止时间。
- **首音频阶段硬上限 75 秒**：正常阶段超时会丢弃在途 HTTP/LLM/TTS future 并取消 MCP 请求，最多用保留的 10 秒尝试固定短句“回复超时，请稍后再试。”；失败兜底 TTS 也受同一个绝对截止时间限制，不会另开一份 15/60 秒超时。正常流程其他错误使用简短“暂时无法获取可靠回复，请稍后再试。”。空 TTS 音频视为错误，不当作成功出音。
- 兜底失败、无音频、或截止时间用尽，仍发送明确失败文字和 `tts:stop`；abort/disconnect 优先且可随时取消正常流程或兜底。失败/取消回合不写入对话历史，迟到的云回复不会恢复播放。
- **整回合硬上限 180 秒**：从同一起点计算，首帧成功只解除 65 秒的初始限制，不解除总限制。后续合成、定速播放以及错误恢复都不得越过 180 秒；总截止到达后仅发送失败文字/stop，不再发起兜底 TTS。
- 首音频成功按服务端 WS writer 完成首个二进制帧写入确认计，不是客户端已经解码/播放的保证；断网不能保证失败语音或 stop 到达，写阻塞会断连。客户端拟采用 `VOICE_FIRST_AUDIO_TIMEOUT_MS=90000`，为上述最多 75 秒的服务端首音频阶段留余量；开始播放后的 30 秒断流等待仍独立保留。服务端不会靠重复 `tts:start` 或 keepalive 延长客户端等待。
- deadline 和音频写确认均在独立回合/writer 任务内等待，WS reader 继续响应 MCP、ping、abort 和新唤醒 hello，不同步等待模型或 TTS。

SSE 兼容 `delta.function_call` 缺失或为 null（普通文本和 tool_calls 均可）；只有非 null 的旧式 function_call 被拒绝，不会错误拒绝提供商填充的合法 null 字段。

### 无云回归

```bash
cargo test
cargo build
# 只跑 MCP / 假 LLM / 真实 WS 集成回归
cargo test device_tools
```

测试只绑定本机随机端口，使用本地 HTTP SSE/空 PCM TTS、假设备 JSON-RPC 响应；不访问豆包或其他云，也不使用真实密钥。覆盖完整初始化/分页/三工具读取、额外工具及非法 schema、RPC/工具错误、数据缺项、超时/断连/取消/迟到回复、有界工具轮、分片/截断 SSE、缺失读数硬兜底，以及真实 WS 上工具等待期间 ping/abort/下一轮继续工作。可控 paused-time + 延迟假 LLM/TTS 额外覆盖两次 completion 总耗时超 30 秒但 49 秒成功出音、跨轮累计超时、TTS/失败恢复的 75 秒硬停止、工具取消、音频写入背压及首帧之后的 180 秒总上限。不代表已验证 ESP32 实机、麦克风、云模型 tool calling 或音频听感。


### 精简实验与验证

本轮逐项消融、失败反证、基线/after 行数及构建限制见 [ABLATION.md](ABLATION.md)。未引入 trait、manager 或泛化工具框架，也未修改 ESP32。`ToolTurn` 只封装设备数据策略，不接管通用 SSE、工具回合或历史结构。

语音追问联调用例使用真实 Opus 和本地 ASR WebSocket 模拟服务，覆盖取消等待中的旧 ASR、重复 stop、同连接追问复用上下文，以及「退下」后关闭连接。
