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

> **狀態:** v1,純後端(尚無 UI)。今日已能運作的:agent 迴圈、四個模型家族的支援(Anthropic、Google、OpenAI,以及自架/OpenAI 相容)、耐久的事件日誌、每會話沙箱、權限與人工核可、一個 MCP 客戶端,以及內建的可觀測性。完整清單見[功能總覽](#功能總覽),目前刻意省略的內容見[路線圖](#路線圖與延後項目)。

---

## 目錄

- [快速開始](#快速開始) · [使用真實模型](#使用真實模型)
- [安裝 — 二進位檔與容器映像](#安裝)
- [REST API(SSE)](#rest-api) — 用 Python/curl 驅動,免 SDK
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

路由:`POST /v1/sessions` · `GET /v1/sessions/{id}` · `POST /v1/sessions/{id}/run`(SSE)· `POST /v1/sessions/{id}/control` · `POST /v1/sessions/{id}/fork`。

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

- **Agent 迴圈** — 單執行緒的 gather → act → verify(ReAct 風格)迴圈,具回合(turn)、`max_turns` / `max_budget_usd` 上限,以及具型別的終止子類型(`success`、`error_max_turns`、`error_max_budget_usd`、`error_during_execution`、`error_max_structured_output_retries`)。協作式取消、doom-loop(卡迴圈)偵測,以及限制深度的「子 agent 作為工具」。
- **多 LLM、可跨供應商移植** — model-gateway 之後是單一正規化的 `Provider` 介面(Generate / Stream / CountTokens / Capabilities),具備 **Anthropic Claude**、**Google Gemini**、**OpenAI**(以 Responses API 為主、Chat Completions 為子旗標)的轉接器,以及一個涵蓋**自架**端點(vLLM、Ollama、LM Studio、llama.cpp、TGI、LiteLLM)的 **OpenAI 相容**轉接器。能力旗標按每 `(端點, 模型)` 解析,而非按供應商家族。迴圈持有**零**供應商 SDK import——新增一個供應商只動到一個轉接器套件加上一筆能力表項目。
- **事件溯源的會話,含續傳與分叉** — 一份只追加的 PostgreSQL 日誌是唯一真實來源。追加是**樂觀的**(比對 `expected_seq`)、**有柵欄的**(lease epoch),且**冪等的**(重送的 `request_id` 是 no-op,不是衝突)。崩潰之後,一次執行會從耐久日誌中恰好停下的地方續跑,而不是從頭開始;它重播已記錄的步驟,但不會重做已完成的工作。分叉可在歷史中的任意一點從會話分支出去,不動到原始會話——用於時光回溯除錯,或把一次真實執行凍結成一個測試。_(因為已完成的回合不會重跑,續傳的執行也不會為它們重複計費——這只在長時間、高成本的執行上才有意義;對短執行而言差異可忽略。)_
- **沙箱化工具** — 核心原生工具(`read`、`edit`、`write`、`glob`、`grep`、`bash`、`webfetch`、`websearch`)在 `Workspace`/`Runtime` 埠之後的每會話容器內執行。工具輸入在執行前經 JSON-Schema 驗證;錯誤以 `Observation` 呈現,絕不 panic。取消時,行程群組在 cgroup/PID-namespace 邊界被擊殺。一份耐久的去重(dedup)帳本讓會變更狀態的工具在重啟後仍為至多一次(at-most-once)。
- **權限與 human-in-the-loop** — 分層的 `deny → mode → allow → tool` 策略管線,含 `default` / `acceptEdits` / `plan` / `bypass` 模式、針對 lethal-trifecta 風險的污點追蹤網路出口閘門,以及以事件持久化的核可決定(可在重播時重新檢查)。會話的常設模式在建立時設定:`harnessctl --permission-mode default|acceptEdits|plan`(環境變數 `BOLTROPE_CTL_PERMISSION_MODE`)在 CLI 建立會話時套用;`bypass` 僅限操作者,客戶端提供的 bypass 會在伺服器端被拒(ADR-0019)。
- **MCP(客戶端)** — 透過 **stdio 或 HTTP** 連接 Model Context Protocol 伺服器,具延遲 schema 載入;每個伺服器在自己的受限沙箱中執行;首次使用的註冊需要明確的人類核可,且 MCP 工具描述被當作不可信輸入。
- **Hooks / 中介層** — `PreToolUse`、`PostToolUse`、`Stop`、`PreCompact` hooks 以主機子行程在 `CommandRunner` 埠之後執行;一個 `PreToolUse` 阻擋會阻止派發。
- **Context 管理** — 即時 token 計量、在預算門檻前自動壓縮、只追加的工具結果清除(視窗中以 stub 呈現,完整內容保留在日誌/blob 儲存)、以及租戶範圍的 prompt 快取前綴。
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

外加 `boltrope-migrate`(執行 DDL 後退出——一個發行閘門)與 `harnessctl`(客戶端 CLI/SDK)。服務之間以 gRPC + protobuf 搭配 mTLS 溝通;客戶端邊緣是 gRPC 加上 orchestrator HTTP 監聽埠上的一個極簡 [REST/SSE 外觀](#rest-api)(把每個 RPC 完整對映到 REST 是[路線圖](#路線圖與延後項目)項目)。

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

- **MCP 伺服器模式**(以及 A2A 互通)——v1 只交付 MCP *客戶端*。
- **microVM / gVisor / OS 原生沙箱後端**——v1 在 `Workspace`/`Runtime` 埠之後僅支援容器;因此互不信任程式碼的多租戶執行不在 v1 範圍內。
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
