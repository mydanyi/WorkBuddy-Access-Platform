# WorkBuddy 接入平台（astrbot_plugin_workbuddy）

> **v0.1.1 测试版**：本插件当前为公开测试版本（Pre-release），供试用与反馈。
>
> - GitHub 仓库：<https://github.com/mydanyi/WorkBuddy-Access-Platform>
> - Releases（下载插件 ZIP）：<https://github.com/mydanyi/WorkBuddy-Access-Platform/releases>

本仓库交付的是一个 **AstrBot 插件**，为**单个 WorkBuddy（CN）账号**提供以下三件事（它是插件，**不是**新的多账号管理网站，也**不是**腾讯官方服务，与腾讯无任何背书关系）：

1. **OpenAI 兼容聊天 API（本地网关）**：提供普通（一次性）响应与 **SSE 流式**两种模式，请求中的 `tools` 只做**透传**给上游，插件**不执行也不改写**；端点包括 `/v1/chat/completions`（聊天，含流式）与 `/v1/models`（模型列表），并提供 `/health`、`/healthz` 健康检查。除聊天外，同样可列模型、查余额；
2. **每日签到**：按配置时区自动 / 手动签到；
3. **成长任务一键**：管理员手动触发一轮服务端动态任务。

- 插件版本：`0.1.1`，作者 `mydanyi`。
- 仅支持 CN 单账号（不合并多账号）。
- 兼容版本要求：**AstrBot `>=4.27.4,<5`**，**Python 3.12**。
- 保持 AstrBot **原生 Provider** 接入方式：聊天请求的历史 / persona / tools 只做透传，插件**不执行也不改写**这些行为；没有网页控制面板；**不合并**参考项目里的 Go 程序。

> 本插件对接的是**非官方**的私人接口。接口形状、可用模型、任务活动都可能随服务端变化，插件只能按观测到的协议实现，不保证长期可用。

---

## 一、安装

1. 从 [Releases](https://github.com/mydanyi/WorkBuddy-Access-Platform/releases) 下载本插件的发布 ZIP，解压到：

   ```
   <AstrBot 根目录>/data/plugins/astrbot_plugin_workbuddy/
   ```

   解压后请确认 `main.py`、`_conf_schema.json`、`metadata.yaml` **都位于该目录根**（ZIP 内为平铺结构，不要多套一层同名文件夹）。

   > `<AstrBot 根目录>` 指包含 `data/` 的 AstrBot 运行根目录，不是 `data/` 本身。容器和主机上的路径可能不同，请以实际部署路径及 StarTools 返回的数据目录为准。

2. 依赖由 AstrBot 的常规插件安装流程按 `requirements.txt` 处理（`aiohttp`、`cryptography` 两项，无新增依赖）。

3. 在 AstrBot 管理面板**重载插件**。

---

## 二、主密钥（WORKBUDDY_MASTER_KEY）

> **新用户须知**：使用本插件的用户必须**自行**生成并设置主密钥 `WORKBUDDY_MASTER_KEY`，并**自行**生成并设置网关的 API 访问密钥 `gateway.api_key`；账号登录授权也由用户**在自己的浏览器**里完成。请勿把账号 Token 当作网关 API Key 使用。

凭据以 **AES-256-GCM** 加密保存到：

```
<AstrBot 根目录>/data/plugin_data/astrbot_plugin_workbuddy/
  credentials.enc    # 加密后的账号凭据
  state.json         # 非敏感的运行元数据（签到 / 成长进度等）
```

> `<AstrBot 根目录>` 是包含 `data/` 的运行根目录；实际凭据位置以 `StarTools.get_data_dir("astrbot_plugin_workbuddy")` 返回值为准。

- `credentials.enc` 的解密密钥来自环境变量 `WORKBUDDY_MASTER_KEY`（变量名本身可在配置里改）。
- 该值必须是**标准 base64、解码后恰好 32 字节**。
- `state.json` 只放非敏感元数据，不含令牌明文。

**用户自行生成密钥（占位符示例，绝非真实密钥）：**

```bash
# 在你自己的机器上运行，生成一次性随机主密钥
python -c "import os,base64; print(base64.b64encode(os.urandom(32)).decode())"
```

把输出设置到 AstrBot 进程 / 容器的环境变量（示例为占位符，请替换为你自己生成的值）：

```bash
export WORKBUDDY_MASTER_KEY="<把上面命令输出的 base64 粘贴到这里>"
```

Compose 片段（占位符）：

```yaml
services:
  astrbot:
    environment:
      - WORKBUDDY_MASTER_KEY=<把你自己生成的 base64 粘到这里>
```

**注意：**

- 请单独保存并备份该密钥；**不要把密钥或令牌发到群聊 / 聊天窗口**。
- 未设置 `WORKBUDDY_MASTER_KEY`、密钥格式非法，或 `credentials.enc` 在原密钥下**解密失败**时，插件在 `initialize` 阶段就会进入**未就绪（ready=False）**并安全停用（不监听、不发请求）；此时 `/wb login` 等指令都会被就绪门控挡下，**无法**通过 `/wb login` 重新登录。

**丢失或需要更换主密钥时的正确恢复步骤**（不要覆盖 / 删除原文件）：

1. 在 AstrBot 管理面板**停用**本插件；
2. **备份**现有的 `credentials.enc`，并把原文件**移动**到一个安全的备份位置（例如改名为 `credentials.enc.bak-<日期>`）。**不要覆盖、不要删除**原加密文件；`state.json` 不含令牌明文，可原样保留；
3. 设置**新的**主密钥（标准 base64、解码后恰好 32 字节），写入 `WORKBUDDY_MASTER_KEY`（或你在配置里改用的变量名）；
4. 在管理面板**重载**插件；
5. 由管理员**私聊**执行 `/wb login` 重新完成一次授权，生成新的 `credentials.enc`。

- **若只是 key 配错、而原 key 仍然在手**：先把环境变量**恢复为原主密钥**即可，无需移动或替换原凭据文件。
- 直接更换密钥**不能**解密原文件（AES-GCM 无原密钥不可解），所以换 key 必须走上面的重新授权流程。
- **停用 / 卸载插件不会自动删除**已保存的 `credentials.enc`、`state.json` 等数据；插件**没有**自动迁移逻辑，也**不存在**云端吊销——`/wb logout` 仅清除本地授权，不代表服务端令牌失效。
- 插件配置面板中的 secret 项**仅掩码显示**；自建的 `gateway.api_key` 是一个**独立的高熵随机 token，不是 WorkBuddy 的 accessToken**。
- 未设置 `WORKBUDDY_MASTER_KEY` 时，插件**安全地不启用**（不监听、不发请求）。
- `gateway.api_key` 留空时，**不会监听**任何端口。

---

## 三、本地网关（供 AstrBot 原生 Provider 使用）

推荐默认：

- 监听 `127.0.0.1:17863`；
- OpenAI 类型 Provider 的 **API Base**：`http://127.0.0.1:17863/v1`；
- Provider 的 **key** 填 `gateway.api_key`（即上面自建的高熵 token）；
- 点击 AstrBot 提供商的**获取模型列表**，模型 ID 后会直接显示当前积分倍率和优惠标签，例如 `deepseek-v4.1-flash [0.11x]` 或 `hy3 [0x · 限时免费]`；示例不是固定价目表。
- 网关在普通与流式调用时只去掉标准展示后缀，向上游发送原模型 ID。原来配置的纯 ID 仍可使用，保存的旧倍率名称也可继续调用，不会因此锁定旧价格。
- 倍率来自实时目录和优惠配置，按官方时区、日期和跨午夜时段判断。未提供倍率的模型标注“倍率未知”，不是免费；只有明确零倍率才展示 `0x`。仅有活动说明、没有可计算折扣时保留目录倍率，不从说明文字猜价。实际扣费以官方结算为准。

网络说明：

- AstrBot 与插件在**同一容器 / 同一主机**时，回环地址可用。
- **不同容器**时 `127.0.0.1` 指容器自己，需要用户**显式**把 `gateway.host` 配成可互通的私有网络地址（或 `0.0.0.0`）并自行配置容器端口映射与访问控制。
- 插件**不会**自动对公网暴露；TLS / 防火墙由部署环境提供。

`gateway.enabled` 只关闭网关监听，**不会**禁止由成长任务等已单独授权的聊天。

---

## 四、指令（仅管理员私聊）

所有动作都经过 `is_admin() and is_private_chat()` 门控：群聊或非管理员**不会**触发任何上游请求，也**不会**收到登录链接。OAuth 授权 URL 只发给发起登录的管理员私聊，**不往群里发**。

| 指令 | 作用 |
| --- | --- |
| `/wb help` | 显示帮助 |
| `/wb status` | 只读完整总览：登录、网关、积分、模型倍率、签到调度和成长任务 |
| `/wb login` | 获取登录链接（浏览器完成授权后插件后台 poll） |
| `/wb logout` | 退出登录（仅清除本地授权） |
| `/wb models` | 列出可用模型、倍率、优惠标签和活动说明 |
| `/wb balance` | 汇总剩余积分、已用积分、总额度和积分包数量 |
| `/wb signin` | 手动签到 |
| `/wb signin resume` | 恢复被暂停的签到 |
| `/wb tasks` | **只读**列出成长任务 |
| `/wb tasks run` | 后台执行一轮成长任务 |
| `/wb tasks claim` | 后台领取已完成任务奖励 |
| `/wb tasks status` | 查看成长任务状态 |
| `/wb tasks cancel` | 取消成长任务 |
| `/wb tasks resume` | 恢复被暂停的成长任务 |

说明：

- `login` 只在管理员私聊返回链接；登录在浏览器完成后，插件在**后台 poll** 并在成功时安全通知。**未登录不会自动签到**。
- `logout` 只做**本地授权清除**，不构成云端 token 吊销。
- 未就绪时命令只回显**已存储的固定安全原因**（缺 key / 配置错误 / 文件损坏有不同提示），不回显你的原始配置或 secret。
- 平时只需 `/wb status`：一次查看积分、全部模型倍率、自动签到开关与暂停状态、签到时间和最近结果、成长运行状态与聊天预算，以及当前服务端任务进度和领奖状态。它不会执行聊天、签到或领奖；某个查询失败会单独提示，其它区块继续显示。
- 积分按服务端有效时间筛选返回的包汇总，周期包只计算周期额度，不与普通容量重复相加；分页会继续读取。总额度与已用积分是本次返回积分包的合计，**不是月度额度或单次聊天费用**。数据缺失或无法完整读取时显示未知，不把未知当成零。

---

## 五、签到（每日）

- 流程：先 query 状态 → 需要则 claim → 再 recheck；已签到时不会重复领奖（no double）。
- 手动与自动**共享**每日次数上限（默认 5 轮）；关闭自动签到**不会**禁用 `/wb signin`。
- 默认时间：`primary=00:10`、`recovery=12:10`、`jitter=120`，时区 `Asia/Shanghai`（均以实际 schema 为准）。
- 启动时只补**当日**，**不回溯补签过去日期**。
- 错误处理：`401` 只 refresh 一次；`403` 暂停；`429` 按 `Retry-After` 冷却；网络 / 5xx 使用 60 / 300 / 1800 秒的有限退避。
- `status` 与 `resume` 的行为**严格按代码**：`resume` 只解除暂停，绝不把「未确认」当成成功，也绝不抹掉服务端强制的 429 硬冷却。

---

## 六、成长任务

- 默认**管理员手动**触发（`auto_after_signin=false`）。
- `/wb tasks` 是**只读**的动态任务列表；`run` 一键执行（serial，一次一个账号）；`claim` 只领取**已确认达标**的任务，不做对话。
- `auto_claim` 只控制 `run` 时的自动领奖；**显式** `/wb tasks claim` 属人工授权，**不受** `auto_claim=false` 影响。
- 结果分 `actions` / `progress` / `rewards` 三段展示：动作、服务端进度 `current/target`、确认奖励。
- 未知 / manual 且未完成的任务**只提示**，不承诺「全部完成」；不虚报总数。
- 已完成的任务可直接领奖；奖励**必须回读 `claimed`** 才算到账。`credit` / `energy` 未知时显示「已确认领取，数值未知」，**不虚报到账数量**。
- 失败 / 重启后**先 query**；聊天预算持久化（默认 20 次 / 天，按 Asia/Shanghai 日期）；共享 `concurrency=1` / `rpm=6` / `queue=2` / `timeout=30s`；有界进度轮询（默认 120 秒）与 `max_retries=2`；本地或上游 `429` 需等冷却，`resume` 不会绕过 `Retry-After`。
- one-click 可能耗时数分钟，或因服务端策略**部分 pending**；请按返回的 `status` 决定是否再次运行，**不鼓励无限 spam**。

### 首版动作白名单

| 任务码 | 动作 | 说明 |
| --- | --- | --- |
| `chat_5` | chat | 普通动态模型对话 + 上报 |
| `Model_chat_GLM5.2` | model_chat | **仅当**真实存在 `glm-5.2` 时使用 |
| `black_cat` | black_cat | 夜间窗口 `23:00–08:00`（Asia/Shanghai） |
| `RichMeow_Chat` | richmeow | 真实 chat 及桌面事件链 attempt（需 `allow_telemetry`） |
| `Library_read` | library | Web 阅读事件 attempt（需 `allow_telemetry`） |

以上全部**依赖实际服务端计分回读**，HTTP 200 只证明动作 attempt，不等于到账。

其它：`first_buddy` 默认禁用，需显式 `allow_adoption`（开启后含同意协议 / 领养操作）；`Hp_Appearance` 默认禁用，需显式 `allow_theme_change`（开启后会改账号主题）。这些**不代表**「全部桌面实体操作已经完成」。

`skill` / `tool` / 授权 / 捐赠 / 小程序 / 模板专家等**未实现真实动作链，一律视为人工**，插件不会伪造 `toolStatus` 成功或身份授权。`allow_telemetry=false` 会禁用指纹事件相关任务。

`allowed_tasks[]` 留空表示「全部已支持白名单」（不是不执行）；填写则仅执行列出的任务码，不限于上表。

---

## 七、配置默认值

关键默认值**以实际 `_conf_schema.json` 为准**（下表为常用项概览）：

| 配置 | 默认 |
| --- | --- |
| `gateway.enabled` / `host` / `port` | `true` / `127.0.0.1` / `17863` |
| `gateway.api_key` | `""`（留空不监听） |
| `gateway.concurrency` / `rpm` / `queue_size` / `queue_timeout` | `1` / `6` / `2` / `30` |
| `signin.enabled` / `timezone` / `primary` / `recovery` / `jitter` / `max_attempts` | `true` / `Asia/Shanghai` / `00:10` / `12:10` / `120` / `5` |
| `growth.auto_after_signin` / `auto_claim` | `false` / `true` |
| `growth.max_chat_requests` / `max_retries` | `20` / `2` |
| `growth.allow_telemetry` / `allow_theme_change` / `allow_adoption` | `true` / `false` / `false` |
| `auth.master_key_env` | `WORKBUDDY_MASTER_KEY` |

插件不安装无需要的依赖，也不提供 token 导入 / 浏览器抓包绕过登录的功能。

---

## 八、风险

- 非官方私人接口，可能随时变化；模型清单与任务活动随账号变化。
- 存在频率限制与成本 / 额度消耗；自动化行为**可能与服务条款不符或触发账号风险**。请用最小频率并先手动验证，**不保证获得奖励**。
- 凭据加密**不替代**主机权限保护。主密钥与加密文件应各自安全备份；`gateway.api_key` 能控制可消耗余额的权限，务必保密。
- 桌面事件只是**已实现链的 attempt**，并非完整客户端替身，需真实账号验证。
- 提供商自身 / SDK 的重试可能产生额外调用，建议使用较低重试数；插件**不会**在 stream 已输出内容后自己重播。

---

## 九、回滚

- 停用 / 卸载插件会关闭 listener、HTTP 会话、登录 poll、scheduler、growth；
- 手动把 AstrBot 切回原来的 Provider；
- 数据默认保留（不自动清理，也不删除旧项目）；
- 不会改变其它插件。

---

## 十、来源与许可

参考实现的协议来源与固定 commit：

- [`workbuddy2api-panel`](https://github.com/linguo2625469/workbuddy2api-panel)（原始项目 [`workbuddy2api`](https://github.com/Sliverkiss/workbuddy2api)）：[`1b6abaa0cce4a960749494cad56d67fe87a6ee1f`](https://github.com/linguo2625469/workbuddy2api-panel/commit/1b6abaa0cce4a960749494cad56d67fe87a6ee1f)
- [`workbuddy-auto-signin`](https://github.com/88lin/workbuddy-auto-signin)：[`cb2bf1f02db8900922dc0f06090cdb7334d45ff5`](https://github.com/88lin/workbuddy-auto-signin/commit/cb2bf1f02db8900922dc0f06090cdb7334d45ff5)

本插件是**独立的 Python 插件**，仅参考上述项目的公开协议行为，**未捆绑构建**其原源码。相关 MIT 许可全文与版权归因见 `THIRD_PARTY_NOTICES.md`。以上**均为非官方项目**，不暗示与腾讯有任何背书关系。
