# WorkBuddy 使用指南

[← 返回 README](../README.md)

这页是 `astrbot_plugin_workbuddy` 的完整说明：安装、登录、网关接入、指令、签到、
CLI 进程、配置默认值、通知、风险与回滚。

插件只对接**一个** WorkBuddy（CN）账号。它不是多账号管理面板，也不是 WorkBuddy
客户端替身，更不是腾讯官方服务。聊天走的是官方 CodeBuddy CLI 的 ACP 通道
（JSON-RPC over stdio），不是逆向出来的私有接口。

## 一、安装

**前置条件**

- AstrBot `>=4.27.4,<5`，Python 3.12。
- 运行 AstrBot 的环境里要有 Node.js 和 npm。
- CLI 必须装在与 AstrBot **同一个容器 / 同一台机器**上。插件是 spawn 它、用标准
  输入输出跟它说话，不是隔着网络连它。

**步骤**

1. 装 CLI：

   ```bash
   npm i -g @tencent-ai/codebuddy-code
   codebuddy --version
   ```

   能打印版本就算装好了。

2. 把插件放进 `<AstrBot 根目录>/data/plugins/astrbot_plugin_workbuddy/`，或者在管理
   面板里上传 ZIP。解压后 `main.py`、`_conf_schema.json`、`metadata.yaml` **必须
   直接位于该目录根**，不要多套一层同名文件夹。

3. 依赖按 `requirements.txt` 走（`aiohttp`、`cryptography`），没有别的。

4. 在管理面板**重载插件**。

5. 打开插件设置，给 `gateway.api_key` 填一个够长的随机串。这是 AstrBot 的 Provider
   要用的密钥，留空则网关根本不监听。生成一个：

   ```bash
   python -c "import secrets; print(secrets.token_urlsafe(32))"
   ```

6. 管理员**私聊**机器人执行 `/wb login` 完成登录（见下一节）。

`<AstrBot 根目录>` 指包含 `data/` 的运行根目录，不是 `data/` 本身；容器内和宿主机
上的路径通常不一样，以 StarTools 返回的数据目录为准。

CLI 不在 PATH 里（比如装在 nvm 的版本目录下）时，把 `acp.cli_path` 填成绝对路径，
例如 `/usr/local/bin/codebuddy`。

## 二、登录（扫码）

`/wb login` 只回管理员私聊。插件拉起一条 CLI，CLI 自己把登录链接推给插件，插件把
链接回给你；如果开了管理员通知，链接也会同时私聊推一份，省得你守着窗口等。

- 链接有时效，过期了再执行一次 `/wb login` 就行。
- 插件在后台等你扫完，默认最多等 `acp.login_timeout`（120 秒）。超时按未登录处理，
  重新来一次即可，不会留下半登录状态。
- 登录成功后插件会**重启全部** CLI 进程。登录前起的那些进程手里记着"未登录"，
  不重启的话，别的模型会一直 401，而你明明已经登上了。

**凭据落在哪**

```
<插件数据目录>/cli-home/.local/share/CodeBuddyExtension/Data/Public/auth/
    Tencent-Cloud.coding-copilot.info          # 明文 JSON，auth.accessToken 即 Bearer
    Tencent-Cloud.coding-copilot.info.logged-out   # 存在 = 明确登出过
<插件数据目录>/codebuddy/                       # CLI 自己的配置目录
```

`<插件数据目录>` 通常是 `<AstrBot 根目录>/data/plugin_data/astrbot_plugin_workbuddy/`。

- 插件把 CLI 子进程的 `HOME` 指到 `<插件数据目录>/cli-home`，凭据才会落在数据目录
  里，而不是留在镜像的可写层。**`XDG_DATA_HOME` 对这个路径无效**——CLI 直接读
  `os.homedir()`，实测只有 `HOME` 说了算，别指望改 XDG 变量能把凭据挪走。
- 那个 `.info` 是明文 JSON，`auth.accessToken` 就是 Bearer。谁能读这个文件，谁就能
  花你的额度：注意目录权限和备份，**不要**把内容贴到群聊或 issue 里。
- 容器重建只要 `data/` 是挂载卷，登录就还在；`data/` 没挂卷则会丢，重新 `/wb login`。
- 同目录的 `.info.logged-out` 是"明确登出"标记，优先级高于 `.info`。

**`/wb logout` 做的事**

先关掉**全部** CLI 进程，再删上面两处目录。顺序不能反：留着进程的话它握着 token，
会把凭据原样写回去，而命令已经回了"已退出登录"。同时会停掉网关监听、停掉签到调度。

它只清本地凭据，**不等于**服务端令牌吊销。

## 三、本地网关

插件在本机开一个 OpenAI 兼容接口，给 AstrBot 的原生 Provider 用。

| 端点 | 鉴权 | 说明 |
| --- | --- | --- |
| `POST /v1/chat/completions` | 需要 | 普通 JSON 与 SSE 流式都在这里 |
| `GET /v1/models` | 需要 | 这台机器上 CLI 认的模型 |
| `GET /health`、`/healthz` | 不需要 | 只回一个状态，不含账号数据 |

除 `/health*` 外都要带 `Authorization: Bearer <gateway.api_key>`。

**Provider 怎么填**

- 类型：OpenAI 兼容。
- API Base：`http://127.0.0.1:17863/v1`（同容器时）。
- 密钥：`gateway.api_key`。
- 模型：点「获取模型列表」，看到的就是本机 CLI 的清单。开了「按顺序调用」的话，
  虚拟入口排在最后。

**模型清单从哪来**

跑一次 `codebuddy --help`，读 `--model` 那一行的 `Currently supported: (...)`；没有
那一行时退回 CLI 自己的 `models.json`，给每个 id 加 `custom-local:` 前缀。不联网、
不需要登录，所以登录之前也能看到清单。

- 名字里带 `image` 的会被滤掉：图片模型接不了 chat completions，选进来只会失败。
- 清单只存在内存里，60 秒保鲜；`/wb models` 随时可以重读。读失败保留上一次的结果，
  5 秒内不重复 spawn——`/v1/models` 会被客户端轮询，不能让它变成进程制造机。
- 请求一个清单里没有的模型，本地直接 400，不会起一条注定失败的进程。清单为空
  （还没成功读过）时跳过这个校验，否则 CLI 装晚一步就永远用不了。

**请求体上限**

`gateway.max_request_body_mb` 默认 16 MiB（1 MiB = 1024 × 1024 字节），按真实收到
的字节算，包含多字节字符和 chunked 传输。超了固定回 `413` / `request_too_large`，
不进队列、不占并发、不花每分钟额度、不碰上游。真正的 JSON 语法错误仍然是 `400`。
调大它只放宽本地这一关，跟模型上下文长度和上游限制是两回事。

**本地限速**

`gateway.concurrency` / `rpm` / `queue_size` / `queue_timeout` 默认 `1` / `6` / `2` /
`30` 秒，都是**插件自己的本地策略初始值**，不是官方配额，也不是对你账号实测能力的
结论。调大只放宽本地排队，不能提升上游限额。

- 每分钟额度是**每次真实 prompt** 花掉一个；网关那层只判断、不预扣。
- 会回 `429` 的情况：并发满且队列满、排队超时、每分钟额度用完、路由冷却。都带
  `Retry-After`。

**网络**

- AstrBot 与本插件在同容器 / 同主机时，回环地址可用。
- **跨容器**时 `127.0.0.1` 指容器自己，需要把 `gateway.host` 改成两端都能访问的私有
  地址（或 `0.0.0.0`），端口映射和访问控制自己配。
- 插件不会自动对公网暴露；TLS 和防火墙由部署环境负责。

### 主备顺序调用（可选）

`routing.enabled` 默认关闭。开启后模型列表多出一个虚拟模型「按顺序调用」，勾选顺序
就是调用顺序：第一项主用，其余依次备用，严格按顺序试，主用能用就不会碰备用。
取消勾选再重新勾选会把该项追加到末尾，插件不提供拖动排序。

- 候选来自本机 CLI 的清单。第一次用先执行 `/wb models`，然后**重新打开配置页**，
  候选才会出现。
- 已保存但当前清单里查不到的模型会保留，标成「（当前目录无此模型）」，不会被静默
  删除，也不会被当成可用。
- 保存的是纯模型 ID。旧版本带倍率后缀的别名仍可调用，插件只在真正下发时剥掉后缀。

**什么时候真的会换下一个**：只有在上游明确失败、并且还没吐出任何正文时。

- 会换：`429`、`502`、`503`、`504`。走 ACP 之后，进程中途死掉、一轮超时、这一轮被
  上游拒掉，都算在内。
- 不会换：`401`（要重新登录）、`403`、本地限流、坏帧、以及"一帧正文都没有"的空回答。
- 一旦已经发出角色、工具或思考等任一帧，这一轮就归那个模型了，后面的错误直接抛，
  绝不重播——群里出现两遍回答比失败更糟。
- 连续两个 `429`，或上游明确说是账号 / IP 级限流，整轮进入冷却：默认
  `routing.cooldown_seconds` 60 秒，上游给了 `Retry-After` 就优先用它。
- `routing.first_frame_timeout` 默认 90 秒，是**整轮**从开始到收到第一帧的总期限，
  不按候选重置；超时直接失败，不再换。
- 冷却记在 `state.json` 里，重载插件后仍然有效；到期后下一次请求自然回到主用，没有
  后台探测或预热。
- 换模型**不能**绕过账号级限流。CLI 起不来的情况也不换：`acp.cli_path` 配错时每个
  候选都会以同样方式失败，那是配置问题，不该把整条顺序挨个试一遍。

## 四、指令（仅管理员私聊）

所有指令都经过 `is_admin() and is_private_chat()` 门控：群聊或非管理员不会触发任何
上游请求，也拿不到登录链接。

| 指令 | 作用 |
| --- | --- |
| `/wb help` | 显示帮助 |
| `/wb status` | 只读总览 |
| `/wb login` | 取登录链接 |
| `/wb logout` | 退出登录并删除本地凭据 |
| `/wb models` | 列出本机 CLI 认的模型 |
| `/wb signin` | 手动签到一次 |
| `/wb signin resume` | 恢复被暂停的签到 |

`/wb status` 依次显示：就绪状态、登录态（`已登录（CLI）` / `未登录（CLI）` /
`已登录（凭据在，尚未实测）` / `未登录`）、在跑的 CLI 进程数与上限、这些进程对应的
模型、网关是否监听及端口、签到开关与暂停状态与定时与今日尝试次数、通知各项开关与
接收人、模型清单（含读取时间，过期会提示）、请求体上限、主备顺序与每个模型的冷却。

它本身不聊天、不签到、不领奖，也不发起任何上游请求；某一项取不到只影响那一行。

`/wb models` 会顺手把配置页的候选刷新一遍，刷新后需要重新打开配置页才看得到。
清单条目会写明来源和"读取于 X 分钟前"；过了保鲜期会直接提示可能与本机 CLI 现状
不符，不要拿它当权威价目或可用性承诺。

## 五、签到（每日）

签到不走 ACP：插件直接请求平台的私有接口，用从 CLI 凭据里读出来的 token。所以它
有自己的 `http.*` 超时配置，也不占 CLI 进程。

- 流程：先查状态 → 需要就领 → 再复核。已签到时不会重复领奖。
- 手动与自动**共享**每日次数上限（`signin.max_attempts` 默认 5，最大 5）。关闭自动
  签到**不会**禁用 `/wb signin`。
- 默认时间：`primary=00:10`、`recovery=12:10`、`jitter=120` 秒，时区
  `Asia/Shanghai`。抖动是叠加在时间点上的随机延迟。
- 启动时只补**当日**，不回溯补签过去日期。
- 错误处理：`401` 只刷新一次授权；`403` 暂停；`429` 按 `Retry-After` 冷却；
  网络错误和 5xx 走 60 / 300 / 1800 秒的有限退避。
- `signin resume` 只解除暂停。它不会把"未确认"当成成功，也不会抹掉服务端强制的
  429 硬冷却。

## 六、CLI 进程与模型

**一个模型一条进程。** `--model` 是进程启动参数，ACP 协议里没有"换模型"这种请求，
`session/new` 也不收模型，所以想用哪个模型就得有哪条进程。插件按需懒启动，空闲的会
被回收，正在回答的永远不会——杀掉一条正在出字的进程，比多占几十兆内存严重得多。

`acp.max_processes` 默认 2，取值 1..16，限制同时留驻的条数。每条进程几十 MB，按机器
余量调，不要拉太高。重载或停用插件时全部关闭。

**内置工具默认是关的。** `acp.tools` 留空时启动参数带 `--tools ""`，同时传输层会拒掉
所有权限请求——两道闸。群聊消息永远不该驱动服务器执行命令。只有你确实需要 CLI 自带
工具时才填这一项，填什么就开什么。

**上下文是无状态的。** 每轮新建一个 ACP 会话，历史靠 AstrBot 每轮重发的完整
`messages` 自己带着。插件不替你保存群聊记忆；要"接着上一条说"，靠的是 AstrBot 自己
的会话管理。

**几处如实说明的行为**

- 思考块会以 `reasoning_content` 下发，不影响正文位置。
- 只有思考、没有正文的一轮按失败处理，不会把空回答配个 `finish_reason` 当成功交出去。
- 只有一轮真正跑通，才认为登录有效；`/wb status` 的登录态跟着真实请求走，不是靠
  另外探测一次。

## 七、配置默认值

关键默认值以实际 `_conf_schema.json` 为准，下表是常用项：

| 配置 | 默认 | 说明 |
| --- | --- | --- |
| `gateway.enabled` / `host` / `port` | `true` / `127.0.0.1` / `17863` | 端口 1..65535；`0` 仅测试用 |
| `gateway.api_key` | `""` | 留空不监听 |
| `gateway.concurrency` / `rpm` / `queue_size` / `queue_timeout` | `1` / `6` / `2` / `30` | 本地策略，不是官方配额 |
| `gateway.max_request_body_mb` | `16` | 超出直接 413 |
| `routing.enabled` / `models` | `false` / `[]` | 开启后模型列表多一个「按顺序调用」 |
| `routing.cooldown_seconds` / `first_frame_timeout` | `60` / `90` | 秒 |
| `acp.cli_path` | `codebuddy` | 走 PATH；否则填绝对路径 |
| `acp.tools` | `""` | 留空 = 禁用全部内置工具 |
| `acp.model` | `""` | 请求没带模型时的兜底，留空用 CLI 默认 |
| `acp.max_processes` | `2` | 同时常驻的 CLI 进程数，1..16 |
| `acp.login_timeout` | `120` | 等扫码的秒数 |
| `signin.enabled` / `timezone` | `true` / `Asia/Shanghai` | |
| `signin.primary` / `recovery` / `jitter` / `max_attempts` | `00:10` / `12:10` / `120` / `5` | `max_attempts` 最大 5 |
| `http.total_timeout` / `connect_timeout` / `sock_read_timeout` | `180` / `15` / `120` | 只用于签到请求，聊天不走这里 |
| `notifications.enabled` | `false` | 总开关 |
| `notifications.signin_success` / `errors` | `true` / `true` | 分别控制 |
| `notifications.admin_qq` / `platform_id` | `""` / `""` | |
| `notifications.error_cooldown_seconds` | `1800` | 60..86400 |

### 管理员通知

在插件配置的「管理员通知」里填接收消息的管理员 QQ，再打开总开关。签到成功和异常
可以分别关掉，默认总开关关闭。通知不会为了多问一句余额而额外请求。

- 通过 AstrBot 的 QQ（aiocqhttp / OneBot V11）机器人**私聊**发送，不发群聊。
- 只有一个 QQ 机器人实例时 `platform_id` 可以留空自动选；多个实例时必须填 AstrBot
  里对应的实例 ID——那不是机器人 QQ 号。
- 接收 QQ 只决定发给谁，不会赋予 AstrBot 管理员权限。能不能投递还受好友关系、机器人
  连接和平台限制影响。
- 签到消息里的领取数量只取复核之后的领奖响应；总数是当前可用积分，不是历史总额。
  取不到就显示「未知」，不猜。
- 同类异常在 `error_cooldown_seconds` 内只提醒一次。主用失败但成功切到备用的**不会**
  提醒；整轮最终失败才提醒一次。
- 投递失败不影响签到结果，用 `/wb status` 看最近一次投递结果。

## 八、风险

- CLI 的模型清单、配额与可用性随服务端变化，插件只能按观测到的行为实现，不保证
  长期可用。
- 签到走的是**非官方私有接口**，可能随时变化；自动化行为可能与服务条款不符，也可能
  触发账号风险。请用最小频率，先手动验证。不保证拿到奖励。
- 凭据是明文 JSON，落在插件数据目录里。文件权限保护不能替代主机权限保护；备份和
  分享时请当作密码对待。`gateway.api_key` 能消耗额度，同样要保密。
- 主备顺序调用只是本地选择策略，**不能**突破账号或 IP 级的限流。
- AstrBot 或 SDK 自身的重试可能产生额外调用，建议把重试次数调低。插件不会在流已经
  输出内容之后自己重播。

## 九、回滚

- 停用 / 卸载插件会关闭监听端口、HTTP 会话、登录任务、签到调度与全部 CLI 进程。
- 把 AstrBot 里的 Provider 换回原来的即可。
- 数据默认保留：插件的 `data/` 目录不会被自动清理。
- 不影响其它插件。CLI 本身是独立安装的，卸载插件不会把它删掉。

## 十、来源与许可

签到那部分的协议行为参考了下面两个非官方开源项目（固定 commit）：

- [`workbuddy2api-panel`](https://github.com/linguo2625469/workbuddy2api-panel)（原始项目 [`workbuddy2api`](https://github.com/Sliverkiss/workbuddy2api)）：[`1b6abaa0cce4a960749494cad56d67fe87a6ee1f`](https://github.com/linguo2625469/workbuddy2api-panel/commit/1b6abaa0cce4a960749494cad56d67fe87a6ee1f)
- [`workbuddy-auto-signin`](https://github.com/88lin/workbuddy-auto-signin)：[`cb2bf1f02db8900922dc0f06090cdb7334d45ff5`](https://github.com/88lin/workbuddy-auto-signin/commit/cb2bf1f02db8900922dc0f06090cdb7334d45ff5)

本插件是独立的 Python 插件，只参考上述项目的公开协议行为，**未捆绑构建**其原源码。
MIT 许可全文与版权归因见 [THIRD_PARTY_NOTICES.md](../THIRD_PARTY_NOTICES.md)。以上
**均为非官方项目**，不暗示与腾讯有任何背书关系。
