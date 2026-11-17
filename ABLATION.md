# MCP / LLM 实证精简（2026-11-17）

## 基线与方法

- 只改本服务端；所有终端命令的 cwd 都是 `weather-esp32`，Cargo 通过绝对 `--manifest-path` 定位服务端。未改 ESP32、Cargo 依赖/锁文件、`config.toml` 或其他既有 Rust 改动，没有 checkout/reset。
- 本轮开始状态（包括未提交的 MCP、TOML、测试改动）完整复制到 **`target/mcp-ablation/server-baseline/`**：`src/`、`tests/`、Cargo 文件、配置、README、git diff/status、SHA256 清单。最终源码/测试另存于 `target/mcp-ablation/server-after/`。
- Rust/Cargo 1.94.1，`rmcp=3.10.0`。最先运行未经改动的 `cargo test --locked`：**48 passed / 0 failed**；日志 `target/mcp-ablation/baseline-test.log`。
- 独立删减变体位于 `target/mcp-ablation/variants/`，不覆盖工作树；每个变体保存源码和 `test.log`。首批共享 target 误复用产物的结果已废弃；有效结果均在每个变体前 `cargo clean -p xiaozhi-server-rs` 后重建。清理的是本包构建产物，不是快照/源码。
- 控制测试的 **51 项**为原 48 项加版本/capability、发现重名、单 text/空参数三项防护。此阶段仅跳过尚未实现的新天气描述测试。最终 **56 项全部运行，无跳过**。

## 独立删除实验：必要的约束不为减行而删

下表行数是变体涉及文件的完整物理行数，含空行/注释；均从同一控制源码独立删减，不是累加结果。

| 候选 | 基线 → 删除变体 | 测试结果 | 最终决定 / 实证必要性 |
|---|---:|---|---|
| initialize 支持版本列表 | `mcp.rs` 397 → 390 | 0 passed / 1 failed | **保留**。2099-01-01 仍进入 tools/list；SDK legacy initialize 不做项目支持版本列表检查 |
| tools capability 检查 | `mcp.rs` 397 → 393 | 0 / 1 failed | **保留**。无 tools capability 仍进入发现 |
| 执行时空对象参数检查 | `mcp.rs` 397 → 393 | 0 / 1 failed | **保留**。非法 `[]` 参数仍触发实际 tools/call，不能只信模型 schema |
| 恰好一个 text content | `mcp.rs` 397 → 393 | 0 / 1 failed | **保留**。空 content 导致越界 panic；多块也不可擅取第一块 |
| 发现重名检测 / seen_names | `mcp.rs` 397 → 392 | 0 / 1 failed | **保留**。先报不合规 schema、再报同名合规 schema，不再整体拒绝 |
| 不可用固定回答分支（条件禁用） | `llm.rs` 304 → 304 | 0 / 1 failed | **保留**。模型虚构“16点、99度”不再被可靠拦截；这是禁用行为的实验，不伪报删行 |
| JSON text 的二次长度检查 | `mcp.rs` 397 → 396 | 51 / 0 failed | **移除**。内层 JSON text 不可能比承载它的已受限消息更长 |
| 内层 JSON object 显式检查 | `mcp.rs` 397 → 396 | 51 / 0 failed | **移除**。仅三个白名单分支可到达；每个都要求对象成员/布尔标志，非对象已被必需字段校验拒绝 |

失败测试具体名称、命令筛选及完整错误见 `candidate-results.json` 和各变体 `test.log`。不把 rmcp 只负责外层 MCP/JSON-RPC 当作它也验证了设备内部读数。

## 累积实施：不仅移动文件

以下为每一步完成时的文件行数；步骤日志在 `target/mcp-ablation/NN-*.log`，前六步另有 `steps/` 源码快照。最后新增区域的局部格式化计入下方最终计数。

| 步骤 | 本步行数 before → after | 测试 | 处理 / 收益 |
|---|---|---|---|
| 01 隐含校验 | MCP 397 → 395 | 51 pass | 删除上面两个已证明冗余的检查，保留所有必需字段/时间戳/只读校验 |
| 02 JSON 树复制 | MCP 395 → 397 | 51 pass | `ServerJsonRpcMessage::deserialize(&Value)` 代替 clone 后 from_value；直接比较 schema map，一次构造固定空 schema；增加两行是为了移除实际树复制，不冒充减行 |
| 03 调用 ID 状态 | LLM 304 → 303 | 51 pass | completion 校验复用回合级 HashSet，删除每个 completion 的第二集合和执行循环重复插入；重复 ID 在本 completion 任何工具执行前拒绝 |
| 04 设备策略归位 | MCP 397 → 463；LLM 303 → 253 | 51 pass | `ToolTurn` 集中固定 policy、读数可用性、失败结果、最终兜底和缓存提示；两个 warning bool 合为一个 Option，未知抓取时间优先；这是职责收拢，单独净增 16 行，不算减行成绩 |
| 05 写而不读的状态 | session 661 → 656 | 51 pass | 删 `Shared.speaking` 字段、初始化及三个原子 store；保留真正被读取的 first_audio 和 turn_cancel |
| 06 最终输出 channel | LLM 253 → 250；session 656 → 641 | 51 pass | 直接返回 `(最终文本, 整轮历史)`；删除容量 64 的 channel、pin/select 收集循环、尾部 drain 和拼接第二 String。SSE 接收仍流式，仍完成验证后才播报 |
| 07 SDK 消息长度限制 | MCP 463 → 462；session 641 → 641 | 55 pass | 用 axum/tungstenite 的重组消息上限代替 payload.to_string 量长度；同时保证新 helper 无警告时直接返回原 String，不额外 format 复制 |
| 08 固定天气描述 | MCP 同行替换 | 56 pass | 静态语义写入可信描述；不要求每份结果重复四个常量字段，保留动态 `fetched_at_known` 和原 schema |
| 09 移交 transcript | LLM 同行替换 | 56 pass | `split_off(initial_len)` 移交 Value 所有权，代替 to_vec 深复制完整工具历史/天气 JSON 内容 |

### 结构与运行步骤的实际变化

- `llm.rs` 的生产代码不含设备工具名称、中文数据真实性 policy、不可用项集合或缓存规则；只保留 SSE、工具增量、ID、上限、通用消息结构与取消。仍具体使用 MCP，没有新增 trait / manager / 泛化框架。
- `mcp.rs::ToolTurn` 只有构造、执行/记录、最终回答三个入口，承载真实业务逻辑，不是透传文件。BTreeSet 留在 MCP 内做不可用说明去重/稳定排序，不把一个小集合换成新 manager 或手写位图。
- 每条入站 MCP：额外 JSON 序列化 **1 → 0**、中间整棵 Value clone **1 → 0**；每个 completion 的局部 ID 集合 **1 → 0**；最终语音 channel **1 → 0**；transcript 最终深复制 **1 → 0**。
- **没有测量延迟、峰值内存或云端 token 成本**。上述数字是源码可核对的运行步骤数量，不冒称性能基准。

## 最终精确行数

`splitlines()` 统计完整文件，含空行/注释/内联测试；不把测试增长隐藏在源码减行里。

| 文件 / 范围 | 本轮 baseline | after | 差值 |
|---|---:|---:|---:|
| `src/llm.rs` | 304 | 251 | −53 |
| `src/mcp.rs` | 397 | 465 | +68 |
| `src/session.rs` | 661 | 643 | −18 |
| 上述三个源码文件 | 1362 | 1359 | −3 |
| 全部 `src/*.rs` | 3079 | 3076 | −3 |
| 全部源码非空行 | 2849 | 2842 | −7 |
| `tests/support/device_tools.rs` | 1347 | 1582 | +235 |
| 测试数 | 48 | 56 | +8 |

三文件分别经同一 `rustfmt --edition 2021 --emit stdout` 规范化后的合计是 **1362 → 1362**。因此本轮**不声称大幅减行**：可证明的精简是减少运行步骤/冗余状态，代价是为独立 MCP 策略增加必要接口。详细口径见 `line-counts.json`。

## 不变的行为与新增回归

- 三工具白名单、可信固定 description、精确空 schema、执行时只接受 `{}`、拒绝指令工具、RPC/单 JSON text/元数据校验均保留；`fetched_at_known` 继续上 wire，标准 SDK `_meta` 不变。
- 不请求新天气；5 天 + 21 小时完整缓存、未对时/无传感器/无天气说明、未知抓取时间和 stale/retrying 警告均保留。已知旧设备缺 fetched_at_known 但提供合法时间戳的兼容路径不变。
- `age` 是设备单调时钟秒数；WMO 天气码；百分比是预报概率，不是实时降水观测；21 小时以缓存抓取时当地小时为起点，等待不滚动。四个可不再重复发送的常量字段是 `age_unit/weather_code_system/precip_probability_unit/hours_time_basis`，不是动态读数，也未改工具 metadata/inputSchema。
- 混合问题继续保守拒答：任一所请求读数不可用就不播模型总结；可用传感器读数仍保留在工具 transcript。没有无依据改变既有播报策略。
- 取消、断连、SDK cancellation、8s RPC / 20s discovery、4 次工具往返、65s 正常首音频 / 75s 含失败提示 / 180s 总回合均未放宽；取消回合不入历史。直接 return 不再有“错误前 channel 是否空”的接口，测试改为断言返回错误/最终文本与历史一致，并保留真实 WS 音频先于 stop、预算截止后无迟到音频等回归。
- 八项新增测试：版本/capability、发现重名、空参数/单 text、描述/无常量字段、原始/分片消息边界、跨 completion ID、未知时间提示优先级、混合可用性。
- 上行限制有一项明确加强：从“解析后重编码 MCP payload 的 64 KiB”改为**所有上行完整 WS 消息原始字节 64 KiB**，包含 envelope/转义/分片累计，解析前拒绝。测试证明恰好 65536 字节可继续 ping，65537 字节关闭；旧实现对应新测试会超时失败（`07-sdk-message-bound-before.log`）。

## 最终验证与限制

均从 weather-esp32 cwd 用服务端绝对 manifest 运行，日志保存在 `target/mcp-ablation/`：

- `cargo test --locked`：**56/56**。
- `cargo build --locked`：成功。
- `CMAKE_POLICY_VERSION_MINIMUM=3.5 cargo build --locked --release`：成功。
- 同环境 `cargo test --locked --release`：**56/56**。
- `cargo clippy --locked --all-targets -- -D warnings`：baseline / after 均被既有 `src/ota.rs:56` 的 `items_after_test_module` 阻挡，未修改无关文件。
- `cargo clippy --locked --all-targets -- -D warnings -A clippy::items_after_test_module`：成功，仅豁免上述已验证的旧 lint。
- 既有 `tests/check_ws.py` 对实际 release 进程：hello/listen/ping/abort 通过；随机分配本地双端口，配置/日志只写 target，进程测试后终止。系统 Python 缺 websockets，故只在 `target/mcp-ablation/python-smoke/` venv 安装 websockets 15.0.1；未新增项目依赖。
- 首次 release 构建遇到 CMake 4 移除旧兼容策略，Opus 的旧 CMakeLists 配置失败。设置上述环境变量重跑成功，未修改下载的第三方源码。
- `git diff --check` 通过；逐文件确认其他 Rust 源码/测试、Cargo.toml、Cargo.lock、config.toml 与本轮快照逐字节一致；baseline SHA256 验证完整。

测试只使用假设备、回环 HTTP/WS、真实 rmcp 和真实本地音频编解码；没有云凭据测试、ESP32 实机 MCP/WakeNet/I²S 或真实读数验证。不把这些通过写成端到端硬件/云端保证。

## 2026-11-17 第二轮：工具透传后的消融

### 基线与方法

- 本轮对象是“修复 MCP 工具对不上上游 `xiaozhi-esp32`”那次改动：`src/mcp.rs` 575 行、`tests/support/device_tools.rs` 1639 行、57 项测试。改动前的 HEAD 为 `mcp.rs` 465 / 测试 1582 / 56 项。
- 工作树快照在 `target/ablation2/`（`baseline/`、`mcp.final.rs`）；每个变体都从同一 575 行基线独立删减，`cargo test` 全量跑，不累加。
- 判断口径沿用上一轮：**删了测试还过 ≠ 一定要删**。只有当删除不改变已声明的行为、或删除本身就是目的时才移除；删除会让整轮对话失败（而不是单个工具失败）的校验保留并在表中写明。

### 独立删除实验

| 候选 | `mcp.rs` 575 → | 测试 | 决定 / 依据 |
|---|---:|---|---|
| `MAX_TOOLS = 32` + `tools.truncate` | 572 | 57 / 0 failed | **移除**。无测试覆盖；真实设备暴露 ≤10 个工具；同样的“无限增长”论点对工具 description 并不设限，属推测性防御 |
| `Ok(data) if !reading` 分支 | 573 | 57 / 0 failed | **移除**。动作结果本来就落到 `match alias` 的 `_ => {}`，返回值逐字节相同，是纯冗余 guard |
| `complete` 标志 | 570 | 57 / 0 failed | **移除**。恢复 `None => return Ok((service, build_tools(raw)))` 早退；20 页上限仍由循环后的 `bail!` 保证 |
| `DENIED` 4 个名字 | 566 | 56 / 1 failed | **移除**。3 个是 `AddUserOnlyTool` 注册的用户权限工具，服务端从不请求 `withUserTools`，无论如何都看不到；第 4 个覆盖了板子自己“让 AI 可调”的注册意图。更关键的是它与本次 bug 同源——服务端维护第二份工具名清单。测试改为断言真正的那道门：`tools/list` 不带 `withUserTools` |
| 动作失败复用读数失败文案 | 556 | 56 / 1 failed | **保留**。`llm_unknown_tool_never_reaches_device` 断言动作/未知工具失败必须说“这个操作没有执行成功”，不能对用户说“读取失败” |
| `alias_for` 长度/首字符检查 | — | 57 / 0 failed | **保留（判断项）**。删了测试照样过；但一个超过 64 字节或首字符非法的工具名会让**每次** LLM 请求被提供商 400 拒绝，整轮对话失败。代价 1 行 |
| 动作工具 schema `type=object` 检查 | — | 57 / 0 failed | **保留（判断项）**。同上：非法 schema 会让整份 `tools` 数组被拒绝，不是只丢一个工具 |

### 结果

| 文件 | HEAD | 首次修复 | 消融后 | 消融净减 |
|---|---:|---:|---:|---:|
| `src/mcp.rs` | 465 | 575 | 556 | −19 |
| `tests/support/device_tools.rs` | 1582 | 1639 | 1632 | −7 |
| `README.md` | 279 | 285 | 285 | 0（正文替换，行数不变） |
| 测试数 | 56 | 57 | 57 | 0 |

- 首次修复净增 110 行生产代码，其中 18 行是推测性防御/冗余分支，已删除；剩下的是 `ToolSpec`、`build_tools`、别名推导、`call` 的参数/结果分支。
- 行为没有因为消融而放宽：三个只读工具仍要求精确空 schema、执行时只接受 `{}`、读数仍需 `validate_reading`；64 KiB 上行上限、8s RPC / 20s discovery、4 次工具往返、65/75/180s 预算均未改。

### 验证

- `cargo test --locked`：**57/57**；`cargo build --locked` 成功。
- `cargo clippy --locked --all-targets -- -D warnings -A clippy::items_after_test_module`：成功（豁免项与上一轮相同，仍是 `ota.rs` 的既有 lint）。
- `git diff --check` 通过。
- 仍然只有假设备 + 回环 HTTP/WS，没有上游 `xiaozhi-esp32` 实机、真实模型 tool_calls 或豆包云端验证。删除 `DENIED` 意味着：设备作为标准工具暴露什么，模型就能调什么；`self.reboot` 等是否可调由设备自己的 `AddUserOnlyTool` 决定。
