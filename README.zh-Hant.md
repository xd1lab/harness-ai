<!-- SPDX-License-Identifier: Apache-2.0 -->

# Boltrope

**為「必須自行掌控資料、必須證明租戶隔離、需要稽核每一次執行」的團隊打造的可自架 AI agent 引擎。**

[![CI](https://github.com/xd1lab/harness-ai/actions/workflows/ci.yml/badge.svg)](https://github.com/xd1lab/harness-ai/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/xd1lab/harness-ai.svg)](https://pkg.go.dev/github.com/xd1lab/harness-ai)
[![Go Report Card](https://goreportcard.com/badge/github.com/xd1lab/harness-ai)](https://goreportcard.com/report/github.com/xd1lab/harness-ai)
[![OpenSSF Scorecard](https://api.securityscorecards.dev/projects/github.com/xd1lab/harness-ai/badge)](https://securityscorecards.dev/viewer/?uri=github.com/xd1lab/harness-ai)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

[English](README.md) · **繁體中文**

> 本文件是英文版 [README.md](README.md) 的完整繁體中文對譯，方便中文使用者閱讀。**權威來源以英文版為準**;若兩者有出入(例如英文版剛更新、中文版尚未跟上),請以英文版為主。連到 `docs/`、`examples/`、`deploy/` 的文件目前仍為英文。

Boltrope 是一個讓你跑在自己基礎設施上的 AI agent 引擎。把它指向一個託管模型(Anthropic、Google、OpenAI)或一個你自架的模型——同一套應用程式碼對任何一個都能運作。它是**純後端**的:沒有 UI、不依賴任何專有雲、沒有供應商遙測。它唯一會對話的對象,就是你指定的那些端點——你的資料庫、你的模型供應商、你的可觀測性堆疊。

貫穿其中的是一個理念:每一次執行都是 PostgreSQL 資料庫裡一筆永久、有序的紀錄,而不是放在記憶體裡的狀態。正是這筆紀錄,讓一次執行能在崩潰後續跑、之後能被重播或分叉、也能作為一份完整、可重播的紀錄交給稽核人員——也正是它,讓 agent 不會把同一個真實世界的動作做第二次。

**為什麼又一個 harness?** 許多 agent 框架把你綁在某一家模型供應商上、把一次執行的狀態放在記憶體裡(一崩潰就丟失),並讓工具以啟動它的那個行程相同的權限執行。對一個必須自架、又要面對安全審查的團隊來說,這三點都是負債。Boltrope 反其道而行:

- **不被供應商鎖定。** 單一的內部介面意味著 agent 對任何支援的模型——託管或自架——運作方式都相同;換模型不必改你的應用程式。
- **崩潰不丟東西,動作也不會重複。** 每一次執行都活在一份耐久的資料庫日誌裡,所以失敗的執行能從停下的地方續跑,而一個已經發生的真實世界動作——已寄出的 email、已扣款的卡、已寫出的檔案——不會被重做。
- **模型驅動的程式碼跑在一個上鎖的盒子裡。** 工具在一個預設無網路的每會話沙箱中執行——這是模型可影響的程式碼唯一能跑的地方,除非你允許,否則它無法對外連線。

Context 被當作有限資源——透過 token 計量、自動壓縮(compaction)與 prompt 快取來主動管理。

> **狀態:** v1,純後端(尚無 UI)。今日已能運作的:agent 迴圈、四個模型家族的支援(Anthropic、Google、OpenAI,以及自架/OpenAI 相容)、耐久的事件日誌、每會話沙箱、權限與人工核可、一個 MCP 客戶端、把 Boltrope 自己暴露為 MCP 伺服器([被呼叫端](#mcp-伺服器模式被呼叫端--callee)),以及內建的可觀測性。完整清單見[功能總覽](#功能總覽),目前刻意省略的內容見[路線圖](#路線圖與延後項目)。

---

## 目錄

- [快速開始](#快速開始) · [使用真實模型](#使用真實模型)
- [安裝 — 二進位檔與容器映像](#安裝)
- [REST API(SSE)](#rest-api) — 用 Python/curl 驅動,免 SDK
- [MCP 伺服器模式(被呼叫端)](#mcp-伺服器模式被呼叫端--callee) — 其他 agent 把工作委派給 Boltrope
- [結構化輸出](#結構化輸出) — 取得程式可解析的 JSON
- [長期記憶](#長期記憶) — 跨會話、租戶隔離的持久回想
- [規劃](#規劃) — 持久、可時光回溯的任務計畫
- [子代理](#子代理) — 把聚焦的子任務委派給深度受限的子迴圈
- [本機開發模式 boltrope-dev](#本機開發模式-boltrope-dev) — 單一執行檔,免 Docker、免金鑰
- [範例](#範例) · [Boltrope 與其他工具的比較](docs/comparison.md)
- [功能總覽](#功能總覽)
- [設定模型供應商](#設定模型供應商)
- [架構](#架構)
- [安全性](#安全性)
- [路線圖與延後項目](#路線圖與延後項目)
- [文件](#文件)
- [參與貢獻](#參與貢獻) · [社群與支援](#社群與支援)
- [開發](#開發) · [授權](#授權)

---

## 快速開始

把完整堆疊拉起來(PostgreSQL、schema migration,以及四個服務——orchestrator、model-gateway、tool-runtime、`projectord`)並跑一個任務——**免金鑰,不需要任何模型 API key**。拉起堆疊只需要裝了 Compose 外掛的 **Docker**;第 4 步的 `harnessctl` 客戶端額外需要 **Go** 來建置/執行(`go run ./cmd/harnessctl …`,或先 `go build -o bin/ ./cmd/harnessctl`)。model-gateway 預設使用內建的 `stub` 供應商(一個確定性、不連網的供應商),所以乾淨的 `docker compose up` 開箱即可跑完一個端到端任務。要對接真實模型,見下方[使用真實模型](#使用真實模型)。

```bash
# 1. 複製。
git clone https://github.com/xd1lab/harness-ai.git
cd harness-ai

# 2. 拉起 — 免金鑰。Postgres 健康 -> migrate 完成 -> grant -> 四個服務啟動。
#    model-gateway 預設 BOLTROPE_MODELGW_PROVIDER=stub,所以不需要 .env、不需要 API key。
#    --wait 會阻塞,直到每個服務的 /readyz 都回報就緒(各服務以其真實的下游依賴為門檻;見「注意事項」)。
docker compose -f deploy/docker-compose.yml up --build --wait

# 3. 確認整個堆疊就緒(各自回傳 HTTP 200)。orchestrator 的 HTTP 邊緣
#    發布在主機埠 8080,gRPC 邊緣在 9000。
curl -fsS http://localhost:8080/readyz && echo   # orchestrator: ready

# 4. 跑一個任務。harnessctl 是 gRPC 客戶端 CLI;BOLTROPE_DEV_INSECURE=1 讓它
#    透過共用種子的 dev mTLS 路徑撥接 compose 邊緣(細節見下方說明)。免金鑰的 stub
#    會回覆一個確定性的文字回合,所以這裡會串流出 assistant 文字與一個終止成功的訊框。
BOLTROPE_DEV_INSECURE=1 go run ./cmd/harnessctl --endpoint localhost:9000 \
    run "Write a hello-world Go program."
# => session: 019eb1...
#    I received your task and I am working on it.
#    [result] subtype=success turns=1 cost=0.000000 USD
```

> **免金鑰執行證明了什麼,以及之外的指令。** 它端到端地驗證了完整的分散式管線——客戶端 → orchestrator(mTLS + 事件溯源日誌)→ model-gateway,加上 orchestrator → tool-runtime 的工具公告與可續傳的串流轉送。(沙箱中的工具*執行*由 tool-runtime 整合測試套件涵蓋,而非由這個不連網的 demo 供應商驗證——對接[真實模型](#使用真實模型)才能驅動實際的工具呼叫。)實用旗標:會話的常設權限模式在建立時以 `--permission-mode default|acceptEdits|plan`(環境變數 `BOLTROPE_CTL_PERMISSION_MODE`)選定;在 `default` 模式下,真實模型的工具呼叫會停在 `[approval required]` 提示並印出一個 `call_id`——從第二個終端機以 `harnessctl … --session <id> approve <call-id>` 核可。以 `--session <id> --after-seq <n>` 重連一個斷掉的會話;以 `harnessctl … fork --at-seq <n>` 分叉一條軌跡。

> **`harnessctl` 對接 dev 邊緣。** 在 `BOLTROPE_DEV_INSECURE=1` 下,orchestrator 的 gRPC 邊緣使用**靜態憑證 mTLS**(它沒有明文監聽埠)。在 `harnessctl` 上設同樣的 `BOLTROPE_DEV_INSECURE=1`(如第 4 步)會讓它透過**共用種子的 dev CA** 撥接:CLI 會出示 orchestrator RBAC 所接納的 `spiffe://boltrope.local/edge` 身分,並釘選 `spiffe://boltrope.local/orchestrator`,對 compose 邊緣完成雙向 TLS。(以 `--trust-domain` / `--server-id` 覆寫信任網域 / 釘選 id,或在整個堆疊一致設定 `BOLTROPE_DEV_CA_SEED`。)單獨的 `--insecure` 旗標只走明文,是給一個**沒有** mTLS 啟動的本機 orchestrator 用的——它無法與 compose dev 邊緣完成握手。生產環境在邊緣使用 SPIFFE/SPIRE 的 SVID 與 OIDC。

> **注意事項。** Compose 堆疊位於 [`deploy/docker-compose.yml`](deploy/docker-compose.yml);它以 `depends_on` + healthcheck 依 [docs/architecture/00-architecture.md §10](docs/architecture/00-architecture.md) 排定 Postgres → migrate → grant → 服務的順序。`--wait` 只在每個服務的 `/readyz` 都轉綠後才回傳,而每個服務的就緒以其真實下游依賴為門檻——orchestrator 以一次 Postgres ping **加上對 model-gateway 與 tool-runtime 的 `grpc.health.v1` 探測(走它自己所服務的同一條跨服務 mTLS 通道)**,tool-runtime 額外以 `docker version`——所以 `--wait` 轉綠意味著堆疊已接好且跨服務 mTLS 確實握手成功(共用 CA 不一致會在這裡讓 `--wait` 失敗,而不是拖到第一個回合),而不只是行程啟動了而已。這是**開發 / 單租戶 / 可信程式碼**的堆疊:它以 `BOLTROPE_DEV_INSECURE=1` 執行(靜態憑證 mTLS 後備,所有服務共用一個 dev-CA 種子),並把主機的 Docker socket 掛載進 tool-runtime(docker-out-of-docker,在主機上等同 root——請閱讀該檔的註解)。生產部署使用 SPIFFE/SPIRE 簽發的 SVID 做跨服務 mTLS、在客戶端邊緣用 OIDC/bearer 認證,並使用 socket-proxy 或 microVM 的沙箱後端(見[安全性](#安全性))。

### 使用真實模型

`stub` 供應商證明接線無誤;當你想要真實輸出時,換上真實模型即可。供應商選擇是 model-gateway 的**部署層面議題**,從它的環境讀取。gateway 只儲存持有金鑰的環境變數**名稱**——值在可信邊界解析,絕不落入設定檔、事件日誌或任何回應內容。把這些設在 `deploy/.env`(已被 git 忽略;compose 會自動讀取),再重跑上面的 `up --wait`:

```bash
# deploy/.env — Anthropic Claude(可替換為 gemini / openai / openaicompat):
cat > deploy/.env <<'EOF'
BOLTROPE_MODELGW_PROVIDER=anthropic
BOLTROPE_MODELGW_API_KEY_ENV=ANTHROPIC_API_KEY
ANTHROPIC_API_KEY=sk-ant-...
EOF
```

| 供應商 | `BOLTROPE_MODELGW_PROVIDER` | 金鑰環境變數(名稱 → 填入 `BOLTROPE_MODELGW_API_KEY_ENV`) |
|---|---|---|
| Anthropic Claude | `anthropic` | `ANTHROPIC_API_KEY` |
| Google Gemini | `gemini` | `GEMINI_API_KEY` |
| OpenAI(Responses API) | `openai` | `OPENAI_API_KEY` |
| 自架 / OpenAI 相容(Ollama、vLLM、LM Studio…) | `openaicompat` | 選用——僅在端點需要時設定;把 `BOLTROPE_MODELGW_OPENAI_BASE_URL` 指向 `/v1` 網址 |

完整對照表與每 `(端點, 模型)` 的能力解析見[設定模型供應商](#設定模型供應商)。

<details>
<summary><b>範例</b> — 一個真實(自架)模型透過沙箱驅動一個工具</summary>

當 `BOLTROPE_MODELGW_PROVIDER=openaicompat` 指向本機 Ollama 服務的 `gemma4:26b`,並建立一個 `acceptEdits` 模式的會話,請 agent 寫一個檔案:

```text
$ harnessctl --endpoint localhost:9000 --session <id> \
    run "Write a hello-world Go program to hello.go, then confirm."

[tool] wrote 77 bytes to hello.go
[result] subtype=success turns=2 cost=0.000000 USD
File hello.go has been created successfully.
```

模型發出了一個真實的 `write` 工具呼叫;策略管線自動核可了它(`acceptEdits` 模式),工具在每會話的 Docker 沙箱內執行,結果被餵回做第二個回合——完整的 gather → act → verify 迴圈。事件日誌記錄了整條軌跡:

```text
seq  event                  detail
1    SessionStarted
2    MessageAppended        user task
3    TurnStarted            model=gemma4:26b
4    AssistantMessage       tool_call: write(hello.go)
5    PermissionDecided      allow — "acceptEdits mode: file edit auto-approved"
6    ToolExecutionStarted   durable intent (idempotency key)
7    ToolResult             "wrote 77 bytes to hello.go"
8    MessageAppended        tool result fed back
9    TurnStarted            model=gemma4:26b
10   AssistantMessage       confirmation text
11   TurnFinished           success — usage 1670 in / 63 out tokens
```

成本為 `$0`,因為模型是自架的;對接計量收費的供應商時,同一次執行會在 `TurnFinished` 事件上彙整真實的 token 用量與美元成本。

</details>

---

## 安裝

上面的快速開始全部從原始碼執行。要做真正的部署,請使用**發行工件**——由 [GoReleaser](.goreleaser.yaml) 在每個打了標籤的版本上產出:交叉編譯、附校驗和、附 SBOM,並以 cosign(Sigstore)做免金鑰簽章。

**容器映像**(多架構 `linux/amd64` + `arm64`,在 GHCR 上)——每個服務一個,與 `deploy/docker-compose.yml` 可釘選的映像相同:

```bash
docker pull ghcr.io/xd1lab/boltrope-orchestratord:latest
docker pull ghcr.io/xd1lab/boltrope-modelgwd:latest
docker pull ghcr.io/xd1lab/boltrope-toolruntimed:latest
docker pull ghcr.io/xd1lab/boltrope-projectord:latest
docker pull ghcr.io/xd1lab/boltrope-migrate:latest      # 一次性 schema migration
```

**二進位檔**——每個 GitHub 版本附帶兩種封存檔(透過 `checksums.txt` 以 cosign 簽章,使用前請驗證):

- `boltrope_<version>_linux_<arch>.tar.gz` — **伺服器套裝**:四個守護行程 + `boltrope-migrate`,適用 `linux/{amd64,arm64}`。
- `harnessctl_<version>_<os>_<arch>` — 單獨的**客戶端 CLI**,適用 `linux`、**macOS** 與 **Windows**(`.tar.gz`,Windows 為 `.zip`)。

**從原始碼**(Go 1.25+):

```bash
go install github.com/xd1lab/harness-ai/cmd/harnessctl@latest   # 客戶端 CLI
# 守護行程以 `spire` 標籤建置,啟用生產用的 SPIFFE/SPIRE 身分:
go build -tags spire ./cmd/...
```

> 版本由維護者推送 `vX.Y.Z` 標籤切出。`ghcr.io/xd1lab/…` 映像與 `go install` 路徑在版本發布後(`v0.1.0` 起)即可解析;在那之前,請如快速開始般從原始碼建置。

---

## REST API

你不需要 Go 或 gRPC 也能驅動 Boltrope(透過 SSE 串流):orchestrator 的 HTTP 監聽埠(compose 堆疊中為 8080,與 `/readyz`、`/metrics` 並列)在 gRPC 邊緣所用的**同一個伺服器**上,提供一個極簡的 REST/JSON 外觀(facade)——認證相同(共用的 OIDC 驗證器;dev 堆疊不需要 token)、所有權檢查相同、事件串流相同。

```bash
# 1. 建立一個會話。
SESSION=$(curl -fsS -X POST localhost:8080/v1/sessions \
    -d '{"mode":"default"}' | jq -r .sessionId)

# 2. 跑一個任務 — 回應是一條即時的 Server-Sent-Events 串流。
curl -NfsS -X POST "localhost:8080/v1/sessions/$SESSION/run" \
    -d '{"text":"Write a hello-world Go program."}'
# id: 2
# event: text_delta
# data: {"seq":"2","textDelta":{"text":"I received your task..."}}
#
# event: result
# data: {"result":{"subtype":"TERMINATION_SUBTYPE_SUCCESS","numTurns":"1",...}}

# 3. 帶外控制(核可一個待決的工具呼叫、中斷、重連):
curl -fsS -X POST "localhost:8080/v1/sessions/$SESSION/control" \
    -d '{"action":"approve","call_id":"<來自 approval_request 訊框的 call-id>"}'
```

每個 SSE 訊框都把它的耐久事件 seq 帶在 `id:` 欄位上,所以用標準的 `Last-Event-ID` 標頭(或 `{"after_seq": N}`)重連可精確續傳——不重複、不漏接。訊框內容是 `boltrope.v1.RunEvent` 訊息的正規 protojson;生產環境請送出 `Authorization: Bearer <OIDC access token>` 並在你的 ingress 終結 TLS。

**Python,零 SDK:** [examples/python/run_task.py](examples/python/run_task.py) 是一個完整的約 100 行客戶端(`pip install requests`),會建立會話、串流一次執行,並互動式回答核可提示。

路由:`POST /v1/sessions` · `GET /v1/sessions` · `GET /v1/sessions/{id}` · `GET /v1/sessions/{id}/usage` · `GET /v1/sessions/{id}/events` · `GET /v1/sessions/{id}/state` · `GET /v1/sessions/{id}/cost` · `GET /v1/cost` · `GET /v1/sessions/{id}/integrity` · `POST /v1/sessions/{id}/run`(SSE)· `POST /v1/sessions/{id}/control` · `POST /v1/sessions/{id}/fork`。

### 管理/租戶會話管理

`GET /v1/sessions` 列出你租戶的會話——可用 `?status=active&status=failed`(可重複)與半開區間 `[created_after_ms, created_before_ms)` 過濾,以不透明的 `page_token` 分頁(在 `(created_at, id)` 上 keyset 分頁,`page_size` 預設 50、上限 200),並以 `?descending=true` 由新到舊排序。它只回傳控制/血緣投影(status、mode、head_seq、血緣、時間戳),不含用量/成本,也不含任何事件酬載。`GET /v1/sessions/{id}/usage` 回傳單一會話從事件日誌折算出的累計用量/成本/回合數(被中斷之執行的部分用量已計入,絕不重複計費),並標注其來源。要**停止**一個執行中的會話,沿用既有的 control 路由——`POST /v1/sessions/{id}/control` 帶 `{"action":"interrupt"}`——它會協作式中止仍在執行的迴圈(可續傳),對已結束的會話則是冪等的 no-op;不存在另一個 kill 端點。一切皆以 RLS 限定於你的租戶:請求從不攜帶 `tenant_id` 過濾鍵,跨租戶/全域管理視圖刻意不在範圍內([ADR-0027](docs/decisions/0027-admin-session-api.md))。

### 事件日誌讀取 + 時光回溯

`GET /v1/sessions/{id}/events` 以**遮蔽後的描述符**列出會話事件——seq、型別、actor、時間戳、blob 中繼資料與一段有界摘要——以 `?after_seq=` keyset 分頁(`page_size` 預設 100、上限 1000)。敏感酬載永不外洩:`provider_raw` 與系統提示一律省略(即使 `?include_payload=true`),串流檢查點永不暴露,大型工具輸出僅回 blob 參照。`GET /v1/sessions/{id}/state?at_seq=N` 是**時光回溯**:以 Load-then-fold 重建會話在序號 `N` 的控制/帳務投影——不新建會話、不重複計費(`at_seq` 超過 head 會封頂至 head)([ADR-0025](docs/decisions/0025-event-log-read-and-time-travel.md))。

### 會話/租戶成本

`GET /v1/sessions/{id}/cost` 回傳單一會話**依模型**拆分的成本(依成本排序;無法關聯的模型落入 `unknown` 桶)加上會話總計;`GET /v1/cost` 回傳你租戶的依模型彙總、租戶總計,以及有成本紀錄的相異會話數。此上捲由 `projectord` 持久化進可重建的 `session_cost_events` 投影——對投影 cursor 冪等(以每個事件的 `global_id` 為鍵),per-model 歸因在寫入時完成關聯(`TurnStarted.Model` ⋈ 以 `TurnID` 對應的終結回合)。事件日誌仍是計費權威;投影完全可重建。兩個端點皆以 RLS 限定於你的租戶([ADR-0026](docs/decisions/0026-session-tenant-cost-read.md))。

### 防竄改稽核(Tamper-evident audit)

事件日誌不只是依慣例 append-only——它是**密碼學鏈接**的,因此任何對已儲存事件的後續竄改都**可被偵測**。在 append 時,於同一個已負責樂觀並行控制、租約圍欄(lease fencing)與 `request_id` 冪等的單寫者交易內,每個事件都會得到一個 **`content_hash`**(對該列已儲存酬載位元組的 SHA-256)與一個 per-session 的 **`chain_hash`** = `SHA256(prev_chain_hash ‖ content_hash)`,依 `seq` 順序從一個由 session 衍生的 genesis 折算而成。運行中的鏈頭存於 `sessions.chain_head`;此鏈是 **per-session** 的(對齊 seq 連續性、RLS,以及「session 即稽核單位」)([ADR-0033](docs/decisions/0033-tamper-evident-hash-chain.md))。

從任一外觀都可驗證一個會話——它會重新讀取事件、重算兩個雜湊,並與已儲存值比對:

```bash
# REST:回傳 {valid, first_bad_seq, reason, checked}。可選 ?from_seq=&to_seq=。
curl -fsS "localhost:8080/v1/sessions/$SESSION/integrity"
# => {"valid":true,"firstBadSeq":"0","reason":"","checked":"11"}
```

若有人 `UPDATE` 了某筆已儲存酬載,其 `content_hash` 便不再相符(**content mismatch**);改寫某個 `chain_hash`,該連結便無法驗證(**broken link**)——兩種情形下 `valid` 皆為 `false`,而 `first_bad_seq` 指向出問題的事件。這兩個完整性摘要也會(作為非敏感的 `content_hash`/`chain_hash` 欄位)出現在 [`GET /v1/sessions/{id}/events`](#事件日誌讀取--時光回溯) 的每個事件描述符上,無論 `include_payload` 為何。同一操作也以 gRPC `VerifySessionIntegrity` RPC 與 MCP `verify_session_integrity` 工具提供,並以 RLS 限定於你的租戶。

**forward-only,且是防竄改「可偵測」而非「不可竄改」。** 這些雜湊欄位由[遷移 0009](migrations/0009_event_hash_chain.up.sql) 新增——附加且可為 NULL,因此在它之前寫入的事件保持未鏈接,而驗證會優雅地略過該前綴。本批次交付的是偵測基底;它**並不**能阻止具備完整資料庫寫入權的攻擊者偽造一份自洽的改寫。把鏈頭錨定在**資料庫之外**——**簽章檢查點 + SIEM/WORM 匯出**——才是讓日誌「不可竄改(tamper-proof)」的後續工作,已列入[路線圖](#路線圖與延後項目)。

---

## MCP 伺服器模式(被呼叫端 / callee)

Boltrope 也把**自己**暴露為一個 [Model Context Protocol](https://modelcontextprotocol.io) 伺服器,讓任何合規的 MCP 客戶端——Claude Desktop、Cursor、其他 agent 框架——能把一整個受治理的任務委派給它:建立會話、跑 agent 任務、查狀態、核可/拒絕待決工具呼叫、fork。這就是**被呼叫端(callee)**定位([ADR-0022](docs/decisions/0022-mcp-server-mode.md)):Boltrope 是一個沙箱化、租戶隔離、可稽核、耐久、可重放的執行後端,供其他 agent 透過網路呼叫。

端點是 orchestrator HTTP 監聽埠上的 `POST /mcp`,與 REST 外觀和 `/readyz` **同一個**監聽埠。它是同一份伺服器方法之上的薄殼,因此 OIDC 認證、多租戶 RLS、核可閘門、每租戶在途上限、耐久可續傳的串流、以及 at-most-once 變更動作全部沿用——與 gRPC 和 REST 邊緣完全相同。

```bash
# 1. initialize — MCP 握手(回傳 serverInfo + capabilities{tools})。
curl -fsS -X POST localhost:8080/mcp -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"curl","version":"0"}}}'

# 2. tools/list — 12 個工具:create_session、run、get_session、control、fork、
#    list_sessions、get_session_usage(管理讀取)、list_session_events、
#    get_state_at_seq(事件讀取)、get_session_cost、get_tenant_cost(成本讀取)、
#    verify_session_integrity(防竄改稽核驗證)。
# 3. tools/call run 帶 _meta.progressToken — 回覆會以 text/event-stream 串流為
#    notifications/progress,最後送終局 result。
```

**run + 核可迴圈會讓 call 保持開啟**:`run` 的 `tools/call` 會把它的 SSE 串流維持開啟直到該次執行終結(與 REST 的 `POST .../run` 完全一樣)。撞到風險工具呼叫時,會以 in-band 的核可 `notifications/progress` 訊框(帶 `call_id`)推出;你在**另一條連線**上以**並行的** `tools/call control`(approve/deny)解除,而 run 仍保持開啟。一次需要 N 次核可的 run = 一次 `run` call 交織 N 次 `control` call;斷線重連則用耐久的 `after_seq` 游標。

**v1 只交付 Streamable HTTP**、手刻(不採 MCP SDK)、掛在既有監聽埠上。誠實延後到路線圖:stdio 傳輸、MCP `elicitation`、stateful `Mcp-Session-Id` 重送、完整 OAuth Protected-Resource-Metadata discovery,以及 `prompts`/`resources`/`sampling` capabilities——見 [ADR-0022](docs/decisions/0022-mcp-server-mode.md)。逐步示範:[**examples/mcp-server/**](examples/mcp-server/)(initialize → tools/list → create_session → run,約 50 行 POSIX shell)。

---

## 結構化輸出

需要一次執行回傳程式可以解析的 JSON、而不是一段文字嗎?在執行上設定 `output_schema`(一份 JSON Schema),Boltrope 就會要求模型遵守它。同樣兩個欄位在每個入口(gRPC、REST、MCP)都通用:

- **`output_schema`** — 最終答案必須符合的 JSON Schema **物件**(以 inline JSON 傳入;非物件會在執行開始前就以 `400` 拒絕)。
- **`strict`** — 在模型支援時,要求供應商原生的嚴格 schema 強制。

```bash
curl -NfsS -X POST "localhost:8080/v1/sessions/$SESSION/run" -d '{
  "text": "Extract the invoice as JSON.",
  "output_schema": {"type":"object","required":["total"],"properties":{"total":{"type":"number"}}},
  "strict": true
}'
```

在供應商原生支援的地方——**OpenAI(Responses)、Gemini,以及目前的 Anthropic 模型**——Boltrope 會把你的 schema 當成供應商自己的結構化輸出模式送出。其他地方(OpenAI 相容/自架端點、較舊的模型)則退回**驗證後重試**:最終答案會對 schema 檢查,不符就再問模型一次,直到上限(之後該次執行以 `error_max_structured_output_retries` 結束)。無論走哪條路,你拿到的契約都一樣——一個符合 schema 的結果,或一個明確、有紀錄的失敗([ADR-0023](docs/decisions/0023-structured-output.md))。

---

## 長期記憶

agent 能**跨會話記住事情**。記憶以三個原生工具的形式提供給模型——沒有新的 API 要呼叫、沒有 proto、沒有 facade:模型在執行過程中自行寫入與回想([ADR-0030](docs/decisions/0030-long-term-memory-via-tools.md))。

- **`memory_write`** `{namespace?, key, value, tags?}` — 為此租戶儲存一份跨會話持久的鍵/值記憶(一項事實、偏好或決策)。以 `(namespace, key)` upsert。
- **`memory_read`** `{namespace?, key}` — 依 key 回想記憶。找不到是正常的「no memory found」結果,而非錯誤。
- **`memory_search`** `{query?, tags?, limit?}` — 以**不分大小寫的子字串**比對 value 來尋找記憶,並以**所有**給定的標籤做 AND 過濾。`limit` 有上限;全空的搜尋會列出最近的項目。

**設計上即租戶隔離。** 在生產環境,記憶存放在 Postgres 資料表(`agent_memory`,migration `0008`),受與事件日誌相同的 **Row-Level Security** 保護:`FORCE ROW LEVEL SECURITY` 加上以請求租戶為鍵的逐操作策略,在租戶上下文未設定時**fail-closed**。**租戶 A 永遠無法讀取或修改租戶 B 的記憶**——這由一個對真實 Postgres 的整合測試,以及一個對記憶體儲存的單元測試證明。

**開發 vs 生產的後端。** [`boltrope-dev`](#本機開發模式-boltrope-dev) 以**記憶體**儲存(一個以租戶為鍵的 map)支撐同樣的三個工具,讓此功能在本機免 Postgres 即可運作——並透過同一個上下文接縫強制同樣的租戶隔離。生產則使用 **Postgres/RLS** 儲存。兩種實作分屬不同套件,讓 dev 執行檔維持其免 pgx 的建置期圍籬。

**刻意保持簡單——無向量/RAG。** 檢索是鍵/值 + 標籤/子字串,這是刻意的。**沒有 embedding 模型、沒有向量索引、也沒有 RAG 管線**——那種人云亦云的複雜度被明確排除在範圍外([ADR-0030](docs/decisions/0030-long-term-memory-via-tools.md))。它的任務是依 key 或標籤對事實做持久回想,而非對語料庫做語意搜尋。

---

## 規劃

agent 能透過單一工具**撰寫並追蹤多步驟計畫**——一份持久、可重播的待辦清單。與記憶一樣,沒有新的 API 要呼叫;模型在執行過程中自行規劃([ADR-0031](docs/decisions/0031-in-loop-virtual-tools-planning-and-subagents.md))。

- **`todo_write`** `{items: [{content, status}]}` — 記錄或更新目前的任務計畫。每次都送出**完整**的有序清單;它會取代先前的計畫。每個項目有 `content`(步驟)與 `status`(`pending` / `in_progress` / `completed`);讓恰好一個項目維持在 `in_progress`。空陣列會清空計畫。

**持久且可時光回溯。** 每次 `todo_write` 都會對會話的事件日誌附加一筆 `PlanUpdated` 事件。因此計畫能在重播中存活,在[事件日誌讀取 + 時光回溯 API](#事件日誌讀取--時光回溯) 中以**未遮蔽**的描述符出現(計畫文字不是機密,不像 provider raw 或系統提示),並能在任意 `GetStateAtSeq` 正確重建。它不影響計費總額。

**重新呈現給模型。** **最新的**計畫會以單一 `[current plan]` 註記重新呈現到模型的上下文視窗,讓 agent 永遠看得到自己的進度——過時的計畫更新不會堆疊。

**這不是權限模式。** 這是一個*規劃原語*,有別於會話層級的 `plan` 權限模式([ADR-0019](docs/decisions/0019-session-scoped-permission-mode.md))——後者是一道拒絕變動型工具的護欄。`todo_write` 記錄意圖;權限模式約束行動。

---

## 子代理

agent 能**把聚焦的子任務委派給子代理**,由子代理執行自己的受限迴圈並回傳一份濃縮結果([ADR-0031](docs/decisions/0031-in-loop-virtual-tools-planning-and-subagents.md))。

- **`spawn_subagent`** `{task, model?}` — 把一個自足的 `task` 交給子代理(它**看不到**父對話)。子代理執行自己的迴圈,其濃縮結果會作為工具結果回饋給父代理。`model` 可選地覆寫子代理的模型;省略則沿用父代理的。

**深度受限。** 遞迴由 `BOLTROPE_SUBAGENT_MAX_DEPTH`(預設 `2`)設限。此工具**只在限制之下才會被宣告**,因此模型永遠不會被提供一個會被拒絕的 spawn——一個已宣告的 `spawn_subagent` 呼叫一定能通過深度檢查。子代理知道自己的深度,因此孫代理的 spawn 也以同樣方式受限。

**如同任何變動般被把關。** 子代理可以做任何事,因此 `spawn_subagent` 被分類為變動型工具:它被**序列化**(永不自動平行化),並流經**完整的權限管線**——PreToolUse hooks → 政策 → 核准。被拒絕的 spawn 永不執行。

**在迴圈內,而非在執行期。** `todo_write` 與 `spawn_subagent` 兩者都是在 orchestrator 迴圈**內部**處理的**虛擬工具**,而非在工具沙箱內,因為它們需要事件日誌與子代理 spawner——而工具執行期刻意無法觸及這些。它們仍發出與真實工具相同的 `ToolExecutionStarted` + `ToolResult` 事件,因此稽核、重播與冪等性完全一致([ADR-0031](docs/decisions/0031-in-loop-virtual-tools-planning-and-subagents.md))。

---

## 本機開發模式 boltrope-dev

[快速開始](#快速開始)會拉起四個服務加 Postgres、走 mTLS。那是誠實的生產形態——但只是想*感受*一下 agent 迴圈的話,要架起這一整套太重了。`boltrope-dev` 是**30 秒上手通道**:一個純 Go 的單一執行檔,在**單一行程**裡跑**同一套** agent 迴圈——記憶體事件儲存、免金鑰的 `stub` 模型、不可執行(no-exec)的工具沙箱、明文 loopback、無 mTLS/OIDC/Postgres([ADR-0024](docs/decisions/0024-boltrope-dev-local-mode.md))。

```bash
# 建置這一個執行檔(免 Docker、免 Postgres、免金鑰)並執行。
go run ./cmd/boltrope-dev run
# 它會印出醒目的「非生產用」橫幅,然後服務:
#   gRPC     : 127.0.0.1:8089
#   REST/SSE : 127.0.0.1:8088

# 透過 REST/SSE 跑一個免金鑰任務——不需要 Authorization 標頭。
curl -s -X POST localhost:8088/v1/sessions -d '{}'          # => {"sessionId":"019e…"}
curl -s -N -X POST localhost:8088/v1/sessions/<id>/run -d '{"text":"hello"}'
# event: text_delta … "I received your task and I am working on it."
# event: result      … "subtype":"TERMINATION_SUBTYPE_SUCCESS","numTurns":"1"
```

`harnessctl --insecure --endpoint localhost:8089 …` 是對同一個執行檔的 gRPC 客戶端。它跑的是**真正的**迴圈、策略管線、唯讀-vs-變更的排程、審批閘門、串流、分叉,以及結構化輸出的驗證重試——唯一被替身化的只有網路邊緣(模型 = 免金鑰 stub;工具 = no-exec)。

**它不是、也不可能變成生產部署——這是設計上的保證:**

- **它是獨立的執行檔。** 生產映像永遠不打包 `cmd/boltrope-dev`,所以「不會不小心在生產跑」是**建置期**性質,不是執行期旗標。一個 import-graph 測試強制它**不**牽入 pgx、SPIRE/mTLS 或跨服務 gRPC 客戶端邊緣。
- **遇到生產訊號就拒絕啟動。** `KUBERNETES_SERVICE_HOST`、`BOLTROPE_POSTGRES__DSN`、`BOLTROPE_OIDC_ISSUER` 任一存在 → fail-closed 退出。它**只綁 loopback**;要綁非 loopback 需要顯眼的 `--i-understand-this-is-not-production` 旗標。
- **它很吵。** 每次啟動都印多行橫幅:`NOT FOR PRODUCTION · IN-MEMORY · NO RLS · NO mTLS · NO OIDC · LOOPBACK ONLY · NO-EXEC`。
- **它仍然跑租戶檢查。** 跳過 OIDC 時它注入一個固定的合成單租戶 principal,所以 `igrpc` 的 `authorizeTenant` 走的是同一條程式路徑——單租戶 loopback 語意*取代*多租戶 RLS,而不是刪掉檢查。

**預設範圍,誠實說:** 在**沒有任何旗標**時,會話是**記憶體**式的(不持久、退出即失),沙箱是 **no-exec**——`read`/`compute`/`sub-agent` 可用,但 `bash` 是會拒絕的佔位符(`"dev sandbox exec disabled"`),所以預設執行展示整套迴圈,但不執行任意 shell/coding 任務。**SQLite/檔案持久化**仍重新劃入路線圖,其 `--store` 旗標是**被拒絕、而非默默忽略**。見 [ADR-0024](docs/decisions/0024-boltrope-dev-local-mode.md)。

### 選擇啟用:真實本機模型 + Docker 沙箱(gemma / Ollama)

你可以讓 `boltrope-dev` 連上一個**真實的本機 OpenAI 相容模型**,並讓它**在強隔離的 Docker 沙箱中實際執行工具**——兩者都在**顯式、預設關閉(default-OFF)**的旗標之後([ADR-0029](docs/decisions/0029-boltrope-dev-real-model-and-local-exec-opt-in.md))。stub 模型 + no-exec 沙箱仍是**預設**。

```bash
# 前置需求:Docker 運行中;Ollama 提供 OpenAI 相容 API。
ollama serve                 # 提供 http://localhost:11434/v1
ollama pull gemma            # 或任何你想用的 model id

# 連上本機模型,並在 Docker 沙箱中執行工具。
go run ./cmd/boltrope-dev run \
  --model-url http://localhost:11434/v1 \  # OpenAI 相容 base URL
  --model gemma \                          # model id(預設:stub)
  --enable-local-exec \                    # 在每會話 Docker 容器中執行真實工具
  --enable-native-schema                   # 開啟原生 json_schema 結構化輸出
```

- `--model-url <base-url>` 將迴圈指向任何 **OpenAI 相容**端點(Ollama、vLLM、LM Studio、llama.cpp、TGI、LiteLLM)。未設定時使用免金鑰的 `stub` 模型。
- `--model <id>`(預設 `stub`)設定貫穿迴圈與 gRPC 預設的 model id。
- `--model-api-key-env <ENVVAR>`(選用)指定一個環境變數,其**值**作為 API key 送出——該值**絕不**被記錄或印在橫幅中(只顯示模型端點 + id)。
- `--enable-native-schema` 為該端點開啟原生 `json_schema` 結構化輸出。
- `--enable-local-exec` 以真實沙箱取代 no-exec 沙箱:每個會話跑在**自己的 Docker 容器**中,具 `--network none` + cgroup/PID 限制、預設拒絕的網路出口(所以 `webfetch`/`websearch` 被拒)、一份記憶體去重帳本(無 Postgres),以及一個位於暫存目錄的 FS blob 儲存。容器映像/二進位檔重用 `BOLTROPE_TOOLRT_IMAGE` / `BOLTROPE_TOOLRT_DOCKER_BIN`。

當 local-exec 開啟時,橫幅會以 `Sandbox     : LOCAL-EXEC ENABLED (Docker isolation: per-session container, --network none, cgroup/PID limits)` 取代 `NO-EXEC` 標記,並加上一行 `Model       : <endpoint> <id>`。**一切仍然是 NOT-FOR-PRODUCTION:** 醒目橫幅、只綁 loopback,以及生產訊號拒絕(`KUBERNETES_SERVICE_HOST` / `BOLTROPE_POSTGRES__DSN` / `BOLTROPE_OIDC_ISSUER` → fail-closed 退出)都未改變,即使帶上這些旗標仍會生效。**Docker 僅在 `--enable-local-exec` 時需要;**預設路徑不需要 Docker。

---

## 範例

[examples/](examples/) 裡有可實際執行的逐步示範——前三個對免金鑰的 dev 堆疊端到端執行(不需 API key、不需客戶端工具):

- **[curl/](examples/curl/)** — 只用 `curl` 驅動一個完整會話:建立、執行、串流 SSE、以 `Last-Event-ID` 續傳。
- **[durable-resume/](examples/durable-resume/)** — 檢視每會話的 Postgres 事件日誌,接著看一個會話**在 orchestrator 重啟後存活**(projection 從耐久日誌重建,headSeq 不變)。
- **[python/](examples/python/)** — 一個約 100 行、僅用 `requests` 的客戶端,含互動式核可。
- **[web-research/](examples/web-research/)** — 透過預設拒絕的網路出口資料通道,啟用 `webfetch`/`websearch`(需要真實模型 + 一個白名單主機)。

剛接觸、還在權衡選項?[**Boltrope 與其他工具的比較**](docs/comparison.md) 是一份把 Boltrope 與 deepagents、hive 並排檢視的誠實說明——包含它們在哪些情況下是更好的選擇。

---

## 功能總覽

支撐上述承諾——掌控你的技術堆疊、隔離每個租戶、安全地行動、稽核一切——的各項能力,在此逐一拆解。除非明確標記為 _roadmap(路線圖)_,以下一切皆已在 v1 實作。

- **Agent 迴圈** — 單執行緒的 gather → act → verify(ReAct 風格)迴圈,具回合(turn)、`max_turns` / `max_budget_usd` 上限,以及具型別的終止子類型(`success`、`error_max_turns`、`error_max_budget_usd`、`error_during_execution`、`error_max_structured_output_retries`)。協作式取消,以及限制深度的「子 agent 作為工具」。**doom-loop(卡迴圈)偵測現在會*終止*執行**——當模型連續 `DoomLoopThreshold` 次(預設 **3**,預設開啟)重複*同一個*工具呼叫(名稱與引數皆相同)時,迴圈會以 `error_doom_loop` 終止原因停止,而非空轉到 `max_turns`([ADR-0032](docs/decisions/0032-estimator-doomloop-durable-approvals.md))。
- **多 LLM、可跨供應商移植** — model-gateway 之後是單一正規化的 `Provider` 介面(Generate / Stream / CountTokens / Capabilities),具備 **Anthropic Claude**、**Google Gemini**、**OpenAI**(以 Responses API 為主、Chat Completions 為子旗標)的轉接器,以及一個涵蓋**自架**端點(vLLM、Ollama、LM Studio、llama.cpp、TGI、LiteLLM)的 **OpenAI 相容**轉接器。能力旗標按每 `(端點, 模型)` 解析,而非按供應商家族。迴圈持有**零**供應商 SDK import——新增一個供應商只動到一個轉接器套件加上一筆能力表項目。
- **事件溯源的會話,含續傳與分叉** — 一份只追加的 PostgreSQL 日誌是唯一真實來源。追加是**樂觀的**(比對 `expected_seq`)、**有柵欄的**(lease epoch),且**冪等的**(重送的 `request_id` 是 no-op,不是衝突)。崩潰之後,一次執行會從耐久日誌中恰好停下的地方續跑,而不是從頭開始;它重播已記錄的步驟,但不會重做已完成的工作。分叉可在歷史中的任意一點從會話分支出去,不動到原始會話——用於時光回溯除錯,或把一次真實執行凍結成一個測試。_(因為已完成的回合不會重跑,續傳的執行也不會為它們重複計費——這只在長時間、高成本的執行上才有意義;對短執行而言差異可忽略。)_
- **沙箱化工具** — 核心原生工具(`read`、`edit`、`write`、`glob`、`grep`、`bash`、`webfetch`、`websearch`)在 `Workspace`/`Runtime` 埠之後的每會話容器內執行。工具輸入在執行前經 JSON-Schema 驗證;錯誤以 `Observation` 呈現,絕不 panic。取消時,行程群組在 cgroup/PID-namespace 邊界被擊殺。一份耐久的去重(dedup)帳本讓會變更狀態的工具在重啟後仍為至多一次(at-most-once)。
- **權限與 human-in-the-loop** — 分層的 `deny → mode → allow → tool` 策略管線,含 `default` / `acceptEdits` / `plan` / `bypass` 模式、針對 lethal-trifecta 風險的污點追蹤網路出口閘門,以及以事件持久化的核可決定(可在重播時重新檢查)。會話的常設模式在建立時設定:`harnessctl --permission-mode default|acceptEdits|plan`(環境變數 `BOLTROPE_CTL_PERMISSION_MODE`)在 CLI 建立會話時套用;`bypass` 僅限操作者,客戶端提供的 bypass 會在伺服器端被拒(ADR-0019)。**核可可跨崩潰耐久:**工具呼叫一進入 ask 閘門,就會在迴圈阻塞*之前*寫入一筆 `ApprovalRequested` 事件,因此在 ask 進行到一半時重啟,會把同一個核可重新提給重連的操作者(以續傳逾時為界),在獲得回應後續跑執行——而非默默遺失待決的 ask([ADR-0032](docs/decisions/0032-estimator-doomloop-durable-approvals.md))。
- **MCP(客戶端)** — 透過 **stdio 或 HTTP** 連接 Model Context Protocol 伺服器,具延遲 schema 載入;每個伺服器在自己的受限沙箱中執行;首次使用的註冊需要明確的人類核可,且 MCP 工具描述被當作不可信輸入。
- **Hooks / 中介層** — `PreToolUse`、`PostToolUse`、`Stop`、`PreCompact` hooks 以主機子行程在 `CommandRunner` 埠之後執行;一個 `PreToolUse` 阻擋會阻止派發。
- **Context 管理** — 即時 token 計量、在預算門檻前自動壓縮、只追加的工具結果清除(視窗中以 stub 呈現,完整內容保留在日誌/blob 儲存)、以及租戶範圍的 prompt 快取前綴。當 model gateway 無法計算 token 時(例如自架/Ollama 端點),本地後備估算現在會計入**所有**帶 token 的內容——工具結果、工具呼叫的名稱與引數,以及 thinking 文字——而不只是純文字,因此一個有大量工具輸出的吵雜執行仍會觸發門檻壓縮([ADR-0032](docs/decisions/0032-estimator-doomloop-durable-approvals.md))。
- **可觀測性** — OpenTelemetry GenAI spans(`invoke_agent` / `chat` / `execute_tool`)帶 `gen_ai.*` 屬性,並在 gRPC 上傳播 trace context;每 RPC 的 RED 指標(錯誤按終止子類型細分)與 USE/飽和度量表(worker pool、live 沙箱、PG pool、blob 位元組、projection 落後);`slog` JSON 日誌含 `LogValuer` 機密遮蔽;gRPC health + HTTP `/livez` / `/readyz`,就緒以依賴為門檻。
- **客戶端 API** — 一個可續傳的 `Run` 伺服器串流(Last-Event-ID 語意)加上一個一元的 `Control` RPC(核可 / 拒絕 / 中斷 / 重連),同時以 **gRPC 與一個 REST/JSON + SSE 外觀**提供(`Run` 以 `text/event-stream` 串流;依建構即具相同認證與所有權檢查——外觀呼叫的是同一個伺服器)。零 SDK 的 Python 路徑見 [REST API](#rest-api) 與 [examples/python/run_task.py](examples/python/run_task.py)。
- **確定性評測 harness** — golden scenario 對一個腳本化的 fake 供應商與 fake clock 驅動真實迴圈,**不連網**;接入 CI 作為必過閘門。

---

## 設定模型供應商

Boltrope 可跨供應商移植:無論哪個模型負責某個回合,agent 迴圈都完全相同。供應商選擇是 model-gateway 的**部署層面議題**,從環境讀取。gateway 只儲存持有 API 金鑰的環境變數**名稱**——機密值在可信邊界解析,絕不落入設定檔、事件日誌或任何回應內容(依 [ADR-0013](docs/decisions/0013-security-model.md))。

在 `boltrope-modelgwd` 服務上設定這些(於 `.env` / Compose,或行程環境):

| 變數 | 含義 |
|---|---|
| `BOLTROPE_MODELGW_PROVIDER` | `anthropic` \| `gemini` \| `openai` \| `openaicompat` \| `stub`(gateway 二進位檔預設:`openaicompat`;Compose 堆疊為免金鑰路徑預設 `stub`) |
| `BOLTROPE_MODELGW_API_KEY_ENV` | 持有上游 API 金鑰的環境變數**名稱**(例如 `ANTHROPIC_API_KEY`);`stub` / 免金鑰 `openaicompat` 不使用 |
| `BOLTROPE_MODELGW_OPENAI_BASE_URL` | `openai` / `openaicompat` 的 base URL(預設 `http://localhost:11434/v1`,Ollama) |

`stub` 供應商是內建的確定性、不連網供應商,供本機 demo 與 CI 煙霧測試(它串流一個腳本化回應,不需要金鑰);它是 Compose 預設,所以堆疊能免金鑰執行。它絕不用於生產。

**Anthropic Claude**

```bash
export ANTHROPIC_API_KEY=sk-ant-...
export BOLTROPE_MODELGW_PROVIDER=anthropic
export BOLTROPE_MODELGW_API_KEY_ENV=ANTHROPIC_API_KEY
```

**Google Gemini**(使用 `google.golang.org/genai`)

```bash
export GEMINI_API_KEY=...
export BOLTROPE_MODELGW_PROVIDER=gemini
export BOLTROPE_MODELGW_API_KEY_ENV=GEMINI_API_KEY
```

**OpenAI**(預設 Responses API)

```bash
export OPENAI_API_KEY=sk-...
export BOLTROPE_MODELGW_PROVIDER=openai
export BOLTROPE_MODELGW_API_KEY_ENV=OPENAI_API_KEY
```

**自架 / OpenAI 相容**(Ollama、vLLM、LM Studio、llama.cpp、TGI、LiteLLM)——指向 `/v1` base URL;免金鑰的本機端點不需要金鑰:

```bash
export BOLTROPE_MODELGW_PROVIDER=openaicompat
export BOLTROPE_MODELGW_OPENAI_BASE_URL=http://localhost:11434/v1   # Ollama
# export BOLTROPE_MODELGW_API_KEY_ENV=MY_GATEWAY_KEY                # 若端點需要金鑰
```

能力旗標(串流工具呼叫、平行工具呼叫、視覺、thinking、伺服器端 token 計數、最大輸出 token)按每 `(端點, 模型)` 解析,並可按端點覆寫。當一個端點不支援串流工具呼叫(例如 LM Studio),gateway 會緩衝並發出完整的工具呼叫。每家族的預設見[多 LLM 支援對照表](docs/spec/00-system-specification.md#6-multi-llm-support-matrix),理由見 [ADR-0004](docs/decisions/0004-multi-llm-provider-strategy.md) / [ADR-0016](docs/decisions/0016-provider-abstraction.md)。

共用服務設定透過 `knadh/koanf` 遵循 `flags > env > file > defaults`,並在必填欄位缺失或無效時快速失敗。環境變數以 `BOLTROPE_` 為前綴,以 `__` 作為巢狀分隔符——例如 `BOLTROPE_POSTGRES__DSN`、`BOLTROPE_SERVER__GRPC_ADDR`、`BOLTROPE_OTLP__ENDPOINT`、`BOLTROPE_LOG_LEVEL`、`BOLTROPE_DEV_INSECURE`。

---

## 架構

Boltrope 是**三個長駐服務加一個 projection worker**,全部跑在單一 PostgreSQL 實例上(耐久的脊柱)。事件儲存是**orchestrator 內的行程內套件**,而非獨立服務——PostgreSQL 本就提供資料重力、備份與排序保證,一個 Go 殼層只會多加一個網路跳轉。

| 服務(`cmd/`) | 職責 |
|---|---|
| **orchestrator**(`boltrope-orchestratord`) | 大腦:agent 迴圈、回合、權限、hooks、context/token 預算、子 agent——以及內嵌的事件儲存(以 `pgx` 做 append/load/fork/subscribe)。 |
| **model-gateway**(`boltrope-modelgwd`) | 無狀態的供應商抽象:把內部的訊息/工具模型與各 LLM SDK 雙向正規化、串流 delta、計算 token、解析能力、集中化供應商重試與錯誤正規化。 |
| **tool-runtime**(`boltrope-toolruntimed`) | 模型可影響程式碼的信任邊界:工具註冊表(原生 + MCP)、JSON-Schema 驗證、每會話沙箱、MCP 客戶端,以及預設拒絕的網路出口 broker。 |
| **projectord**(`boltrope-projectord`) | 讀側 worker(在請求路徑之外):追蹤事件日誌,以 xmin 為界的安全推進游標執行 cost-rollup 與 OTel-export 的 projection。落後永不阻塞一個回合。 |

外加 `boltrope-migrate`(執行 DDL 後退出——一個發行閘門)與 `harnessctl`(客戶端 CLI/SDK)。服務之間以 gRPC + protobuf 搭配 mTLS 溝通;客戶端邊緣是 gRPC 加上 orchestrator HTTP 監聽埠上的一個極簡 [REST/SSE 外觀](#rest-api)與一個 [MCP 伺服器端點](#mcp-伺服器模式被呼叫端--callee)(三者都呼叫同一份伺服器方法、共用同一套認證;把每個 RPC 完整對映到 REST 是[路線圖](#路線圖與延後項目)項目)。

```
Client ──gRPC (resumable Run / Control)──> Orchestrator ──┬─ gRPC ─> Model Gateway ──> LLM APIs / self-hosted
                                            (agent loop +  │                            (Anthropic/Gemini/OpenAI)
                                             event store)  ├─ gRPC ─> Tool Runtime ──> Sandbox (per session)
                                                  │        │                            + egress broker
                                                  ▼        └──────────────────────────> External MCP servers
                                             PostgreSQL  <── projectord (cost-rollup, OTel export)
                                             (event log = single source of truth)
```

詳讀:

- [docs/architecture/00-architecture.md](docs/architecture/00-architecture.md) — 完整 v1 架構:服務拆解、PostgreSQL 事件儲存 schema、耐久性與 exactly-once 副作用、安全模型、並行/取消模型,以及跨四家族的供應商串流。
- [docs/decisions/](docs/decisions/) — 架構決策記錄(ADR 索引),記下每個重大選擇及其被否決的替代方案。
- [ARCHITECTURE.md](ARCHITECTURE.md) — 一張通往上述內容的簡短導覽圖。

---

## 安全性

- **服務對服務 mTLS**,透過 SPIFFE/SPIRE 工作負載身分,搭配預設拒絕的每 RPC 動詞閘門。一個僅限開發的靜態憑證後備以 `BOLTROPE_DEV_INSECURE=1` 做環境變數閘控,並記錄一則醒目警告——它存在於二進位檔中,但除非明確啟用否則惰性;發行映像以 `-tags spire` 建置以啟用 SPIRE 路徑。
- **客戶端邊緣認證**驗證 OIDC/bearer token(`iss`/`aud`/`exp`,拒絕 `alg=none`,JWKS 以 refresh-on-miss 輪替),並在每次呼叫驗證會話所有權。生產接線是兩個環境變數(`BOLTROPE_OIDC_ISSUER` / `_AUDIENCE`)——orchestrator 在啟動時執行 OIDC discovery,若沒有一個可達、issuer 相符的 IdP 就**拒絕啟動**;見[部署逐步說明](deploy/README.md#client-edge-auth-in-production-oidc)。
- **租戶隔離**在資料庫層,透過 PostgreSQL Row-Level Security(非擁有者角色、從驗證過的 token 設 `SET LOCAL` GUC、`FORCE ROW LEVEL SECURITY`)。
- **預設拒絕的網路出口** <a id="web-access-egress"></a> — 預設情況下,agent 的沙箱**完全沒有網際網路存取**,所以模型驅動的程式碼無法偷偷對網路發出連線;唯一的出口是 `webfetch`/`websearch` 工具,而即便是它們,也**只能連到操作者明確允許的主機**。細節如下:每會話沙箱以 `--network none` 執行,所以沙箱內的 `bash` 與 MCP-HTTP **沒有外部網路**。`webfetch`/`websearch` 工具透過**網路出口資料通道**([ADR-0021](docs/decisions/0021-egress-data-path.md))連到外界:一個位於 tool-runtime 信任邊界、強化過的行程內抓取器,**對每個請求與每個重導向跳轉**都由預設拒絕的 broker 中介(`BOLTROPE_TOOLRT_EGRESS_ALLOWLIST`;空 ⇒ 全部拒絕),搭配 DNS 釘選撥接與僅限公開位址的出口(SSRF 防禦)。`websearch` 查詢一個設定好的 SearXNG 相容 JSON 端點(`BOLTROPE_TOOLRT_SEARCH_URL`)。在操作者把主機加入白名單之前,什麼都連不到——即使加了,沙箱命名空間本身仍維持切斷。供應商原生 / 伺服器端工具在 v1 停用。
- **機密**只存在於 model-gateway 設定(環境變數)中,絕不在日誌或任何回應裡;持有機密的型別透過 `slog.LogValuer` 遮蔽。

**要部署到 Kubernetes?** 生產套件是位於 [`deploy/helm/boltrope/`](deploy/helm/boltrope/) 的 Helm chart——SPIRE 簽發的身分、OIDC 邊緣認證、migration 閘門 hook Job,以及沙箱化的 tool-runtime,**在 render 時即 fail-closed**(沒有 OIDC issuer / 沒有 SPIRE / `stub` 供應商 / 未明確確認的 dev-insecure 都會拒絕 render)。SPIRE 從零開始:[`deploy/k8s/spire/`](deploy/k8s/spire/)。

發現漏洞?請私下回報——見 [SECURITY.md](SECURITY.md)。完整圖像見 [ADR-0013 安全模型](docs/decisions/0013-security-model.md)。

---

## 路線圖與延後項目

v1 是一個刻意聚焦、不可再簡化的 harness。`Provider`、`Workspace`/`Runtime`、`EventLogPort` 與 MCP 抽象的形狀,使以下這些能**無需重新架構**地嵌入([ADR-0003](docs/decisions/0003-v1-scope.md)):

- **MCP 伺服器模式**([ADR-0022](docs/decisions/0022-mcp-server-mode.md))已於 v1 以 Streamable HTTP 交付([見上文](#mcp-伺服器模式被呼叫端--callee))。延後項目:stdio 傳輸、MCP `elicitation`、完整 OAuth Protected-Resource-Metadata discovery、`prompts`/`resources`/`sampling` capabilities,以及 **A2A 互通**。
- **microVM / gVisor / OS 原生沙箱後端**——v1 在 `Workspace`/`Runtime` 埠之後僅支援容器;因此互不信任程式碼的多租戶執行不在 v1 範圍內。
- **`boltrope-dev` 真實模型 + 本地執行**([ADR-0029](docs/decisions/0029-boltrope-dev-real-model-and-local-exec-opt-in.md),修訂 [ADR-0024](docs/decisions/0024-boltrope-dev-local-mode.md))——[本機開發模式](#本機開發模式-boltrope-dev)現已交付**選用的真實模型接線**(`--model-url`/`--model`,任何 OpenAI 相容端點)與一個**選用的 Docker local-exec 沙箱**(`--enable-local-exec`,重用生產 runtime 的每會話容器,具 `--network none` + cgroup/PID 限制),兩者皆預設關閉、置於醒目橫幅與生產訊號閘門之後。仍延後:SQLite/檔案持久化(`--store`),其旗標今日仍被拒絕以使延後明確,並可嵌入既有 `EventLogPort` 接縫。
- **沙箱內網路出口 proxy**——`webfetch`/`websearch` 今日已能透過[網路出口資料通道](#web-access-egress)連到白名單主機(一個由 broker 中介、強化過的行程內抓取器;[ADR-0021](docs/decisions/0021-egress-data-path.md))。沙箱本身維持 `--network none`,所以沙箱內的 `bash` 與 MCP-HTTP 仍無網路;那個能讓沙箱命名空間取得逐連線閘控路徑的 forward proxy 被延後(`EgressBroker` 埠與 `--network` 接縫的形狀已備好,可無需重新架構地嵌入)。
- **模型路由**與進階多 agent 拓撲。
- **每個 RPC 的完整 REST 對映**——v1 交付極簡外觀([REST API](#rest-api):CreateSession / GetSession / `Run` over SSE / Control / Fork);把完整 proto 表面做成由註解生成的對映被延後。
- **原生 Ollama NDJSON 轉接器**——改用 OpenAI 相容的 `/v1` 路徑。
- **語意化程式庫索引** / tree-sitter repo map;**以 LLM 為基礎的風險分類器**;非原生 function-calling 後備 / 受限解碼。
- **耐久工作區快照**(崩潰後一致的檔案系統續傳);虛擬檔案系統 context 掛載;互動式工作區存取。
- **SWE-bench / SWE-bench-Lite** 作為 CI 閘門——v1 閘門是確定性的客製評測套件;SWE-bench 是 v1 之後的外部目標。

---

## 文件

- [docs/README.md](docs/README.md) — 文件索引。
- [docs/spec/00-system-specification.md](docs/spec/00-system-specification.md) — v1 系統規格(功能 + 非功能需求、支援對照表、完成定義)。
- [docs/architecture/00-architecture.md](docs/architecture/00-architecture.md) — v1 架構。
- [docs/architecture/02-implementation-plan.md](docs/architecture/02-implementation-plan.md) — 測試優先的實作計畫。
- [docs/decisions/](docs/decisions/) — 架構決策記錄。
- [CONTRIBUTING.md](CONTRIBUTING.md) · [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) · [SECURITY.md](SECURITY.md)

---

## 參與貢獻

歡迎貢獻。Boltrope 以**規格優先、測試優先**打造——完整流程見 [CONTRIBUTING.md](CONTRIBUTING.md),CI 強制執行的慣例見 [docs/decisions/0006-engineering-conventions.md](docs/decisions/0006-engineering-conventions.md)。簡言之:

- **每個 commit 都要簽署**(`git commit -s`)——我們使用 [Developer Certificate of Origin](https://developercertificate.org/),而非 CLA;含未簽署 commit 的 PR 會在 DCO 檢查失敗。
- **[Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/)**——類型驅動語意化版本(`fix:` → patch、`feat:` → minor、`feat!:` / `BREAKING CHANGE:` → major)。
- **測試優先。** 保持 `go test ./...` 為綠;並行/DB 變更加上 `-race` 與 `-tags integration`;保持 `golangci-lint run` 乾淨——depguard/forbidigo 規則機械化地強制架構邊界。
- 從一個主題分支對 `main` 開 PR。所有 PR 需要綠色 CI(lint、unit `-race`、integration、build)與一位維護者核可;第三方 GitHub Actions 釘選到 commit SHA——更新時請保持釘選。

## 社群與支援

- **問題與點子** — 開一個 [GitHub Discussion](https://github.com/xd1lab/harness-ai/discussions)。
- **錯誤與功能請求** — 使用 [issue 範本](https://github.com/xd1lab/harness-ai/issues/new/choose)。
- **安全漏洞** — 請**不要**開公開 issue;依 [SECURITY.md](SECURITY.md) 私下回報。

### 徵求設計夥伴

Boltrope 還很年輕、由一個小團隊打造,所以它刻意圍繞一種使用者塑形:需要**自架**、需要**資料庫強制的租戶隔離**與**可稽核的每次執行紀錄**、且其 agent 會採取絕不能執行兩次的真實世界動作的團隊。如果那就是你——一個正在建立內部 agent 服務的平台或安全團隊——我們希望由你的需求驅動路線圖。帶著你的使用情境與限制,開一個[設計夥伴討論](https://github.com/xd1lab/harness-ai/discussions/categories/design-partners)。誠實看待適配:如果你在快速做原型、或想要一個 UI / 整合目錄,今日[其他 harness](docs/comparison.md) 會更適合你,我們也會這樣說。

---

## 開發

```bash
make tools          # 安裝釘選的開發工具(buf、protoc-gen-*、golangci-lint、migrate)
make lint           # golangci-lint run ./...
make test           # 快速單元測試(不需 Docker/網路)
make test-integration  # //go:build integration 測試(需要 Docker)
make gen            # 重新生成 gen/ 中的 protobuf stub(buf generate)
```

在 Windows 上,直接執行底層工具指令(每個 Make recipe 都是單一呼叫——見 [Makefile](Makefile) 標頭)。

---

## 授權

依 [Apache License 2.0](LICENSE) 授權。見 [NOTICE](NOTICE)。貢獻需要 Developer Certificate of Origin 簽署(`git commit -s`);見 [CONTRIBUTING.md](CONTRIBUTING.md) 與 [ADR-0002](docs/decisions/0002-license-apache-2.0.md)。
