# SOP：在 IGX Thor 部署 VSS Warehouse MV3DT ＋ BEV Live 對照網站

本文件記錄如何從零重現以下成果（2026-11-10 實測於 NVIDIA IGX Thor Developer Kit）：

1. **VSS Warehouse industry profile — `mv3dt` 模式**：4 支攝影機的多視角 3D 追蹤（MV3DT）＋ BEV fusion ＋ behavior analytics ＋ ELK，Kafka 版、Extended 規模。
2. **BEV 軌跡影片**：用 `bev-visualizer.sh` 把 `mdx-bev` 融合軌跡畫在倉庫俯視圖上，存成 mp4。
3. **BEV Live 網站（port 8091）**：左邊即時 BEV 軌跡、右邊 4 路攝影機畫面，可互相對照。原始碼在 [`apps/bev-live/`](apps/bev-live/)。
4. **用 LLM 詢問碰撞事件**（2026-11-11 加入）：開啟「人型機器人 ↔ Person 靠近」事件，接上 Video Analytics MCP Server 與 VSS Agent（Azure OpenAI），在聊天介面問「過去 10 分鐘有哪些機器人跟人碰撞在一起？」（§7）。

```
NvStreamer(4 支 mp4) ─RTSP→ VST(VIOS) ─RTSP→ RT-CV MV3DT ─mdx-raw→ BEV fusion ─mdx-bev→ Kafka
                                │                                                 │
                                └──────── RTSP ──────┐            ┌────── mdx-bev ┘
                                                     ▼            ▼
                                             apps/bev-live/server.py (:8091)
                                               MJPEG ×4  ＋  SSE 軌跡 → 瀏覽器
```

> 文中 `<HOST_IP>` 指這台機器的區網 IP（實測為 `172.22.12.205`），`<REPO>` 指本 repo 的 clone 路徑。

---

## 0. 本 repo 相對於上游的改動

本 repo 從上游 [`NVIDIA-AI-Blueprints/video-search-and-summarization`](https://github.com/NVIDIA-AI-Blueprints/video-search-and-summarization) 的 **`v3.3.0` tag** 分支出來，額外加入：

| 路徑 | 內容 |
|---|---|
| [`apps/bev-live/`](apps/bev-live/) | BEV Live 對照網站（`server.py`、`index.html`、`map.png`、`transforms.yml`） |
| [`_builds/warehouse-mv3dt-kafka/`](_builds/warehouse-mv3dt-kafka/) | 這次部署的 build layer：`override.env`、`compose.yml`、`agent-patches.yml`、`reset.sh`（**不含**金鑰；`resolved.yml` / `configurator.env` / `secrets.env` 是部署時產生或本機私有、已被 `.gitignore` 排除） |
| `services/agent/packages/vss_agents/src/vss_agents/tools/` | Agent 修改：`multi_incident_formatter.py`、新增 `bev_camera_picker.py`（§7.4） |
| `warehouse-operations/vss-agent/configs/config.yml`、`warehouse-mv3dt-app/vss-behavior-analytics/configs/vss-behavior-analytics-config.json` | 開啟碰撞（靠近）事件、Agent 提示與攝影機挑選（§7.2） |
| `deploy/docker/industry-profiles/warehouse-operations/overrides.env` | 本機設定：`HOST_IP`、`VSS_APPS_DIR`、`VSS_DATA_DIR`、`HARDWARE_PROFILE=IGX-THOR`、`PROMETHEUS_HOST_PORT=19090` 等 |
| 其他 `deploy/docker/**/configs/` 下 json、txt、yml 的修改 | **blueprint-configurator 部署時自動改寫**的結果（不是手改的）。重新部署時會再被改寫，可以不必在意 |
| `deploy/docker/dns-override.yml` | 站點 DNS 不穩時，給 LLM NIM 用的 DNS / hosts 覆寫（本次 mv3dt 部署沒用到） |
| `.gitignore` | 排除 configurator 的 `*.backup_*`、TensorRT engines、資料目錄、`resolved.yml` 等 |
| `.lfsconfig` | Git LFS 物件改從上游下載，本 fork 不存 LFS 本體（見 §2.1） |

---

## 1. 前置條件

### 1.1 硬體與系統

| 項目 | 本次實測 |
|---|---|
| 主機 | NVIDIA IGX Thor Developer Kit（aarch64，JetPack / L4T R38.5） |
| 記憶體 | 122 GB unified memory |
| Docker | 29.1.3（需 `>= 28.3.3` 且 `< 29.5.0`），cgroup driver = `cgroupfs` |
| Docker Compose | v2.39.1 以上（實測 v5.5.0） |
| 其他工具 | `uv`、`jq`、`curl`、`ngc` CLI |

> `HARDWARE_PROFILE=IGX-THOR` 時 **不能**用 `bp_wh`（含 Agent / UI 的 2D 版本），只能用 `bp_wh_kafka` / `bp_wh_redis`。

### 1.2 主機設定（需要 sudo，一次性）

```bash
# Docker 版本與 cgroup driver（版本在範圍內時只會 apt-mark hold，不會重啟 dockerd）
bash <REPO>/deploy/docker/scripts/pin_docker_version.sh
docker info --format '{{.CgroupDriver}}'      # 必須是 cgroupfs

# Elasticsearch / Kafka 需要的 kernel 參數
sudo sysctl -w vm.max_map_count=262144
sudo sysctl -w net.core.rmem_max=5242880
sudo sysctl -w net.core.wmem_max=5242880

# Edge 平台必備：page cache 清理器（重開機後要重跑）
sudo tee /usr/local/bin/sys-cache-cleaner.sh << 'EOF'
#!/bin/bash
set -e
echo 0 | tee /proc/sys/vm/nr_hugepages
while true; do sync && echo 3 | tee /proc/sys/vm/drop_caches > /dev/null; sleep 3; done
EOF
sudo chmod +x /usr/local/bin/sys-cache-cleaner.sh
sudo -b /usr/local/bin/sys-cache-cleaner.sh
pgrep -f sys-cache-cleaner.sh && echo "cache cleaner OK"
```

> sudo 需要密碼時，請在**真正的終端機**（例如 VS Code 整合終端機）執行；Claude Code 輸入框的 `!` 指令無法輸入 sudo 密碼。

### 1.3 NGC 金鑰

```bash
ngc config set            # 把 NGC API key 存進 ~/.ngc/config
echo "$(sed -n 's/^\s*apikey\s*=\s*//p' ~/.ngc/config)" | docker login nvcr.io -u '$oauthtoken' --password-stdin
```

mv3dt 的 `reid-embed-init` 與 RT-CV 需要 `NGC_CLI_API_KEY` 下載模型。**不要**把金鑰寫進 `override.env`；部署時從 `~/.ngc/config` export 即可（見 §3.3）。

### 1.4 主機上的 port 衝突（這台機器特有）

| Port | 被誰佔用 | 處理 |
|---|---|---|
| 9092、`172.17.0.1:19092` | 主機自己跑的 Kafka（`~/kafka_2.13-4.2.0`） | VSS Kafka 對外改 `KAFKA_HOST_PORT=29192` |
| 1883、`172.17.0.1:11883` | 系統 mosquitto | VSS MQTT 對外改 `MQTT_HOST_PORT=21883` |
| 9090 | ROS rosbridge | `PROMETHEUS_HOST_PORT=19090` |
| 8090、8088 | 其他服務 | BEV Live 用 8091 |

換機器時，請先用下面的指令檢查（**要檢查所有位址**，不只 `0.0.0.0`）：

```bash
for p in 7777 9092 1883 9090 9200 5601 8081 30888 31000 35000 29192 21883 8091; do
  ss -ltnH "sport = :$p" | grep -q . && echo "BUSY $p"; done
```

---

## 2. 取得程式碼與資料

### 2.1 Clone

```bash
git clone https://github.com/fabian415/vss.git video-search-and-summarization
cd video-search-and-summarization
git remote add upstream https://github.com/NVIDIA-AI-Blueprints/video-search-and-summarization.git
git lfs pull        # LFS 檔案依 .lfsconfig 從 NVIDIA 公開上游下載
```

> 請用 `umask 022` 的 shell clone；`027` 會讓容器讀不到 bind-mount 的設定檔。

> **關於 Git LFS：** 上游 repo 用 Git LFS 存了約 1 GB 以上的預編譯 `.so`、wheel、範例影片。本 fork **不另存 LFS 物件**，`.lfsconfig` 把 LFS 來源指向上游，所以 clone 時會自動從上游下載。
> 推送到本 fork 時請用 `git push --no-verify ...`，跳過 git-lfs 的 pre-push 上傳；否則會把上游所有 LFS 物件重傳一份到你的 GitHub LFS 空間。

### 2.2 下載 warehouse app-data（約數 GB）

```bash
mkdir -p ~/Workspace/fabian/vss-data && cd ~/Workspace/fabian/vss-data
ngc registry resource download-version nvidia/vss-warehouse/vss-warehouse-app-data:3.3.0
# 解壓後，VSS_DATA_DIR 要指向「內層」目錄：
#   ~/Workspace/fabian/vss-data/vss-warehouse-app-data_v3.3.0/vss-warehouse-app-data
#   （裡面有 videos/ models/ playback/ data_log/）
ls ~/Workspace/fabian/vss-data/vss-warehouse-app-data_v3.3.0/vss-warehouse-app-data/videos/warehouse-4cams-20mx20m-synthetic
# Camera.mp4  Camera_01.mp4  Camera_02.mp4  Camera_03.mp4
```

calibration 已經在 repo 裡（`warehouse-mv3dt-app/calibration/sample-data/<dataset>/calibration.json`），不用另外跑。

---

## 3. 部署 Warehouse MV3DT

有兩種做法，結果相同。

### 3.A 用 Claude Code ＋ VSS Agent Skills（推薦）

```bash
# 安裝 skills（symlink，git pull 後自動更新）
ln -s "$PWD/skills/vss-build-vision-ai" ~/.claude/skills/
for d in skills/operations/*/; do ln -s "$PWD/$d" ~/.claude/skills/; done
# 重開 Claude Code session 後輸入：
/vss-build-vision-ai 部署 warehouse profile
```

問答依序選：`mv3dt` → `bp_wh_kafka` → `Extended` → `warehouse-4cams-20mx20m-synthetic`。
Skill 會產生 `_builds/<name>/`、驗證並部署。把 §1.4 的 port 覆寫告訴它，或部署後依 §6 疑難排解處理。

### 3.B 手動（照本 repo 的 build layer）

#### 3.1 調整 build layer

[`_builds/warehouse-mv3dt-kafka/override.env`](_builds/warehouse-mv3dt-kafka/override.env) 已經是這次實測可用的設定。換機器時只要改：

- `HOST_IP`、`EXTERNAL_IP`、`VSS_PUBLIC_HOST`、`TURN_PUBLIC_HOST`、`TURN_EXTERNAL_IP`（全部改成新的 `<HOST_IP>`）
- `VSS_APPS_DIR`、`VSS_DATA_DIR`，以及所有以 `VSS_APPS_DIR` 開頭的路徑（`SDR_CONTROLLER_CONFIG_PATH` 等）
- `BP_CONFIGURATOR_ENV_FILE`（指向新的 `_builds/warehouse-mv3dt-kafka/configurator.env` 絕對路徑）
- 若 port 沒衝突，可刪掉 `KAFKA_HOST_PORT` / `MQTT_HOST_PORT` / `PROMETHEUS_HOST_PORT`

> 為什麼要把 `EXTERNAL_IP` 等全部寫死？Compose 讀每一層 env 檔時就展開變數，後面的層改了 `HOST_IP` 不會回頭重算前面層衍生的值。

#### 3.2 產生 configurator.env 與 resolved.yml，並驗證

```bash
cd <REPO>
REPO=$PWD
BUILD_DIR=$REPO/_builds/warehouse-mv3dt-kafka
FOUNDATION_DIR=$REPO/deploy/docker/industry-profiles/warehouse-operations
SCRIPTS=$REPO/skills/vss-build-vision-ai/scripts

# NGC 金鑰只 export，不落地到 override.env
export NGC_CLI_API_KEY=$(sed -n 's/^\s*apikey\s*=\s*//p' ~/.ngc/config | head -1)
export NGC_API_KEY=$NGC_CLI_API_KEY

uv run $SCRIPTS/render_warehouse_configurator_env.py "$BUILD_DIR" --repo-root "$REPO"

docker compose \
  --env-file $REPO/deploy/docker/containers.env \
  --env-file $FOUNDATION_DIR/.env \
  --env-file $FOUNDATION_DIR/overrides.env \
  --env-file $BUILD_DIR/override.env \
  -f $BUILD_DIR/compose.yml \
  config --no-consistency > $BUILD_DIR/resolved.yml
chmod 600 $BUILD_DIR/resolved.yml          # 內含 NGC 金鑰

uv run $SCRIPTS/normalize_resolved_yml.py $BUILD_DIR/resolved.yml
uv run $SCRIPTS/validate_resolved_yml.py  $BUILD_DIR/resolved.yml --repo-root $REPO
uv run $SCRIPTS/validate_warehouse_env.py $BUILD_DIR --repo-root $REPO
# 可選：release 版驗證器
uv run --script deploy/docker/scripts/validate_release_compose.py \
  $BUILD_DIR/resolved.yml --repo-root . --expect-container-tag 3.3.0
```

預期：`Validated ...` 與 `warehouse constraints OK: MODE=mv3dt BP_PROFILE=bp_wh_kafka ...`，共 41 個 service。

#### 3.3 資料目錄權限

容器以不同 UID 寫入資料目錄，需要 world-writable：

```bash
D=~/Workspace/fabian/vss-data/vss-warehouse-app-data_v3.3.0/vss-warehouse-app-data
sudo chmod -R a+rwx $D/data_log $D/models
```

#### 3.4 部署

```bash
docker compose -f $BUILD_DIR/resolved.yml pull --ignore-buildable
docker compose -f $BUILD_DIR/resolved.yml up -d --build
```

> **只**用 `-f resolved.yml`，不要再加 `--env-file` 或 `--profile`。
> 第一次啟動 RT-CV 會下載模型並建 TensorRT engine，約 5–10 分鐘。

---

## 4. 驗證

```bash
H=<HOST_IP>
# (1) 容器：41 個都 running 或 Exited(0)
docker compose -f $BUILD_DIR/resolved.yml ps --all --format json |
  jq -r 'select((.State=="running" or (.State=="exited" and .ExitCode==0))|not) | "NOT READY \(.Name) \(.Status)"'

# (2) VST 的 4 個 sensor 都 online
curl -s http://$H:7777/vst/api/v1/sensor/list | jq -r '.[] | "\(.name) \(.state)"'

# (3) RT-CV 真的在處理（每路約 30 FPS、Active sources = 4）
docker logs --since 30s vss-rtvi-cv-mv3dt 2>&1 | grep -a stream_name | tail -4
docker logs --since 60s vss-rtvi-cv-mv3dt 2>&1 | grep -a "Active sources" | tail -1

# (4) 下游有資料
curl -s "http://$H:9200/_cat/indices/mdx-*?h=index,docs.count&s=index"
curl -s http://$H:8081/livez; curl -s -o /dev/null -w '%{http_code}\n' http://$H:5601/kibana/api/status
```

| 服務 | URL |
|---|---|
| VST | `http://<HOST_IP>:7777/vst/` |
| Kibana | `http://<HOST_IP>:7777/kibana/` |
| Video Analytics API | `http://<HOST_IP>:7777/video-analytics-api/` |
| NvStreamer | `http://<HOST_IP>:31000` |
| Grafana | `http://<HOST_IP>:35000` |
| Kafka（對外） | `<HOST_IP>:29192` |

> 「容器全部 healthy」不代表成功；一定要看 (3) 的 FPS > 0 與 `Active sources : 4`。

---

## 5. BEV 軌跡

### 5.1 準備 BEV 素材（map.png ＋ transforms.yml）

`apps/bev-live/` 已附好。換資料集時重新產生：

```bash
APP=<REPO>/services/rtvi/rt-cv-3d/rt-cv-mv3dt
CAL=<REPO>/deploy/docker/industry-profiles/warehouse-operations/warehouse-mv3dt-app/calibration/sample-data/warehouse-4cams-20mx20m-synthetic
mkdir -p ~/bev-dataset && cp $CAL/images/Top.png ~/bev-dataset/map.png
$APP/scripts/generate-transforms.sh $CAL/calibration.json ~/bev-dataset/map.png
# 預期：sanity check: 24/24 calibration ground points land inside the map image
```

### 5.2 建立 Python venv（BEV 影片與 BEV Live 共用）

```bash
cd $APP && source scripts/ensure-venv.sh && ensure_venv
# 會在 services/rtvi/rt-cv-3d/rt-cv-mv3dt/utils/venv 裝 opencv、confluent-kafka、protobuf、PyYAML
```

### 5.3 錄一段 BEV 影片（mp4）

```bash
cd $APP
env -u DISPLAY BEV_SOURCE=fused BEV_KAFKA_BROKER=<HOST_IP>:29192 \
  BEV_DATASET_PATH=~/bev-dataset BEV_OUTPUT=~/bev-output BEV_SAVE_VIDEO=1 \
  timeout -s TERM 120 ./scripts/bev-visualizer.sh
ls ~/bev-output/fused_trajectory_video_*.mp4
```

收到 SIGTERM 時會正常收尾寫出 mp4。要在 Thor 本機螢幕即時看：拿掉 `env -u DISPLAY`、`BEV_SAVE_VIDEO=1`、`timeout`，設 `DISPLAY=:0`，按 `q` 關閉。

---

## 6. BEV Live 對照網站（port 8091）

### 6.1 功能

- 左：倉庫俯視圖上的即時融合軌跡（舊段漸淡），軌跡頭標 ID；滑鼠移上去顯示世界座標（公尺）。
- 右：4 路 VST 即時畫面（2×2），點一下放大。
- 滑過某個攝影機畫面時，BEV 上對應的攝影機標記會亮綠閃爍。
- 狀態列：連線狀態、BEV FPS、追蹤人數、延遲；控制：軌跡長度 5–120 s、ID／攝影機位置開關、暫停、清除。

### 6.2 架構

| 元件 | 說明 |
|---|---|
| `server.py` | Python 標準庫 HTTP server。Kafka `mdx-bev`（protobuf，用 rt-cv-mv3dt 的 `schema_pb2`）→ `/events`（SSE，約 30 FPS）；VST RTSP → `/cam/<name>`（MJPEG，640 寬、15 FPS，沒人看 10 秒後自動停止解碼）；`/config.json` 提供地圖尺寸、`T_ov2px` 與 calibration 裡的攝影機座標 |
| `index.html` | 單檔前端，canvas 畫軌跡，`<img>` 播 MJPEG，無外部相依 |
| `map.png` / `transforms.yml` | §5.1 產生的底圖與世界座標→像素矩陣 |

不需要另外安裝套件，使用 §5.2 的 venv。

### 6.3 啟動

```bash
cd <REPO>
services/rtvi/rt-cv-3d/rt-cv-mv3dt/utils/venv/bin/python apps/bev-live/server.py \
  --kafka <HOST_IP>:29192 --vst http://<HOST_IP>:7777 --port 8091
# 瀏覽器打開 http://<HOST_IP>:8091
```

參數：`--topic`（預設 `mdx-bev`）、`--calibration`（預設 repo 內 4cams-20mx20m-synthetic）、`--cam-width`、`--cam-fps`。

背景常駐（可選）：

```bash
nohup services/rtvi/rt-cv-3d/rt-cv-mv3dt/utils/venv/bin/python apps/bev-live/server.py \
  > apps/bev-live/server.log 2> apps/bev-live/server.err &
```

### 6.4 檢查

```bash
curl -s http://127.0.0.1:8091/config.json | jq -c '[.map, (.cameras|length)]'   # [{"w":1920,"h":1080},4]
curl -sN -m 2 http://127.0.0.1:8091/events | head -c 200                        # data: {"t":...,"o":[["16",6.68,-9.11],...]}
```

### 6.5 注意

- warehouse stack 重新部署後 VST 的 sensor ID 會變；**重啟 server** 即可（啟動時會重新向 VST 查詢）。
- 攝影機畫面（VST live）與 BEV（RT-CV → bev-fusion 處理後）之間可能差零點幾秒。
- 攝影機標記位置取自 `calibration.json` 的 `coordinates` 欄位；目前看起來是攝影機在地面上的位置，但官方文件未明確定義。
- 目前軌跡包含 Person 與兩種人形機器人（Agility Digit、Fourier GR1）。Transporter / Pallet 在 `ds-pgie-config.yml` 的 `filter-out-class-ids: 4;6` 被過濾掉。

---

## 7. 用 LLM 詢問碰撞事件（VSS Agent ＋ Video Analytics MCP）

2026-11-11 加入：在既有的 mv3dt 部署上開啟「人型機器人 ↔ Person 靠近」事件，並接上 Video Analytics MCP Server 與 VSS Agent，讓你在聊天介面問「過去 10 分鐘有哪些機器人跟人碰撞在一起？」。

```
RT-CV MV3DT ─mdx-raw→ BEV fusion ─mdx-bev→ Behavior Analytics（靠近偵測，人型機器人 ↔ Person < 2 m 持續 1 秒）
                                                   │
                                                   ▼  Elasticsearch: mdx-incidents（Proximity Violation）
                                      vss-va-mcp :9901（get_incidents / get_incident / …）
                                                   │
                                      vss-agent（LLM＝Azure OpenAI gpt-5.4）─→ Phoenix 追蹤記錄
                                                   │
                                      Agent UI（http://<HOST_IP>:7777/）
```

### 7.1 為什麼事件都在 `bev-sensor-1` 上

`bev-sensor-1` 是上游設定好的「4 支攝影機融合俯視圖」sensor（見 calibration.json）。靠近事件要在同一個以公尺為單位的座標系裡才算得出距離、同一個人才不會被重複計算、兩個物件也不必在同一支攝影機裡，所以 behavior analytics 是在 `bev-sensor-1` 上產生事件。它**沒有自己的影片**，因此：

- 問 Agent 時要寫 `bev-sensor-1`，或不寫 sensor（提示已設定預設用它）；問 `Camera`、`Camera_01` 會是 0 筆。
- Agent 回答時會自動挑「最看得清楚機器人與人」的攝影機，附上快照與影片（見 §7.4）。
- Alerts 分頁的縮圖仍是空白：UI 用 VST 的 sensor 清單找影片，裡面沒有 `bev-sensor-1`（尚未處理）。

### 7.2 設定（已在本 repo 裡）

| 檔案 | 內容 |
|---|---|
| `warehouse-mv3dt-app/vss-behavior-analytics/configs/vss-behavior-analytics-config.json` | `app` 加上 `proximityViolationIncidentEnable=true`、`proximityViolationIncidentThreshold=1.0`（預設關閉，所以原本 `mdx-incidents` 一直是 0 筆；靠近偵測本身早就在算） |
| `_builds/warehouse-mv3dt-kafka/override.env` | `COMPOSE_PROFILES` 加 `vss-va-mcp,vss-agent,vss-ui,phoenix`；`LLM_NAME=gpt-5.4`、`LLM_BASE_URL=https://<資源名>.cognitiveservices.azure.com/openai`、`LLM_MODEL_TYPE=openai`、`LLM_NAME_SLUG=none`；`VSS_AGENT_HOST_PORT=18000` |
| `_builds/warehouse-mv3dt-kafka/agent-patches.yml` | 把修改過的 Agent 程式碼 bind-mount 進 `vss-agent`（不用重建 image），已加進 build 的 `compose.yml` |
| `warehouse-operations/vss-agent/configs/config.yml` | 開啟攝影機挑選（`bev_camera_es_url`）；路由提示加上 `bev-sensor-1` 與「碰撞＝Proximity Violation、只查一次、不要分頁」等規則 |

注意：

- **`LLM_MODE` 要維持 `none`**。IGX-THOR ＋ mv3dt 的 configurator 只接受 `none`，設成 `remote` 會讓 `vss-configurator-mv3dt` Exited(1)、整個部署卡住。Agent 實際只讀 `LLM_BASE_URL` / `LLM_MODEL_TYPE` / `LLM_NAME`，照樣會呼叫遠端 LLM。
- **Azure 要填部署名稱（deployment name）**到 `LLM_NAME`。Agent 會呼叫 `${LLM_BASE_URL}/v1`，也就是 Azure 的 `/openai/v1` 端點。
- 主機的 8000 被 `file_share_node` 佔用，所以 Agent 對外改用 18000；UI 走 haproxy :7777，不受影響。

### 7.3 API 金鑰（不進 git）

金鑰放在 build 目錄旁的 `secrets.env`（已被 `.gitignore` 排除），`reset.sh up` 會自動載入；`LLM_MODEL_TYPE=openai` 卻沒有金鑰時會直接停下並說明原因。

```bash
cd <REPO>
read -rsp "Azure OpenAI key: " K; echo
printf 'OPENAI_API_KEY=%s\n' "$K" > _builds/warehouse-mv3dt-kafka/secrets.env
chmod 600 _builds/warehouse-mv3dt-kafka/secrets.env; unset K

# 確認端點與部署名稱正確（預期 HTTP 200；金鑰不會印出）
set -a; . _builds/warehouse-mv3dt-kafka/secrets.env; set +a
curl -s -o /dev/null -w 'HTTP %{http_code}\n' \
  https://<資源名>.cognitiveservices.azure.com/openai/v1/chat/completions \
  -H "Authorization: Bearer $OPENAI_API_KEY" -H 'content-type: application/json' \
  -d '{"model":"gpt-5.4","messages":[{"role":"user","content":"OK"}],"max_completion_tokens":20}'
```

設定好之後照 §8.1 跑 `reset.sh`（完整重置）即可，Agent、MCP、UI、Phoenix 會一起起來。

### 7.4 Agent 程式碼修改

都在 `services/agent/packages/vss_agents/src/vss_agents/tools/`，由 `agent-patches.yml` 掛進容器：

| 修改 | 原因 |
|---|---|
| `multi_incident_formatter.py`：抓不到影片時只把快照、影片標成 `Unavailable`，事件類型、起訖時間、物件 ID、地點照樣輸出 | 原本整筆變成 `Error`，LLM 什麼都看不到 |
| 同上：向 MCP 要欄位時 `object_ids` → `objectIds`，並加 `place` | 上游 bug：MCP 把欄位名直接拿去查 ES，`object_ids` 查不到，物件 ID 從來沒回來過 |
| 新增 `bev_camera_picker.py`：用 `mdx-raw` 替每支攝影機評分，分數＝事件期間平均的 min（機器人的 bbox 面積×可見度，最清楚的那個人的 bbox 面積×可見度），選最高者抓快照與影片 | `bev-sensor-1` 沒有影片。只數「看到幾次」不行（MV3DT 會把所有追蹤物件發到每一路），只看 `visibility` 也不行（畫面外的物件數值也可能很高） |
| 備援抓影片限制最多 3 個同時進行 | VST 要畫偵測框再剪片，20 段同時請求會逾時 |
| 每筆事件附上物件類型（`Objects`：`Agility_Digit_Humanoid#66`、`Person#2`…），從 `mdx-behavior` 查 | 回答「哪一台機器人、哪一個人」要有根據 |
| 事件超過 20 筆時附上 `all_incidents_summary`（每筆一行） | 只給 20 筆時，LLM 會以為資料不完整而切小時間窗重查，撞上 15 步上限（`Recursion limit of 15 reached`） |
| 合併同一人的多個 Person ID（`person_id_merges`）：事件期間中位距離 < 0.5 m 就是分身；新 ID 出生前 1 秒、0.8 m 內有較舊的 Person ID 則視為接力 | 追蹤會把同一人拆成多個 ID（見 §9 #15） |

改了 Agent 程式碼或 `config.yml` 之後，只要 `docker restart vss-agent`（Agent 不保存狀態，不影響串流）。若 `agent-patches.yml` 新增掛載，要重新產生 `resolved.yml` 後執行 `docker compose -f resolved.yml up -d --no-deps vss-agent`（理由見 §9 #14）。

### 7.5 怎麼問

打開 **http://\<HOST_IP\>:7777/**，選 Warehouse Agent，**一次問完整**，例如：

- 「過去 10 分鐘有哪些機器人跟人碰撞在一起？」
- 「列出 bev-sensor-1 過去 30 分鐘，是哪一個機器人跟哪一個人碰撞？」

回答約 40–90 秒，會依「機器人 ↔ 人」組合統計次數，並附上前 20 筆的快照、影片連結與統計圖。最上面那行 `Report Agent: Failed to run VLM analysis` 可以忽略：那是想用 VLM 寫事故報告，這套部署沒有 VLM（GPU 已被 RT-CV 用滿）。

也可以直接呼叫 API（注意：這個端點只讀最後一則訊息，條件要寫在同一句）：

```bash
curl -s -m900 http://<HOST_IP>:18000/v1/chat/completions -H 'content-type: application/json' \
  -d '{"model":"agent","messages":[{"role":"user","content":"過去 10 分鐘有哪些機器人跟人碰撞在一起?"}]}'
```

### 7.6 Phoenix：看 Agent 每一步做了什麼

**http://\<HOST_IP\>:7777/phoenix** 會記錄每次提問的 LLM 呼叫、prompt、工具與參數、耗時與錯誤，排查「Agent 為什麼這樣回答」時先看這裡。Phoenix 直連的 6006 預設只綁 127.0.0.1（它的接收端沒有驗證），從其他電腦請走 7777 的 `/phoenix`。

---

## 8. 停止 / 重新部署

### 8.1 一鍵清空並重新部署（建議）

[`_builds/warehouse-mv3dt-kafka/reset.sh`](_builds/warehouse-mv3dt-kafka/reset.sh) 會清掉所有執行期資料（容器、volume、`data_log/` 的 Kafka / ES / Redis / VST 錄影等），保留 `models/`、`videos/`、image 與 repo 設定，然後重新部署並等 4 路串流接上：

```bash
bash _builds/warehouse-mv3dt-kafka/reset.sh           # down → clean（會問 sudo 密碼）→ up
bash _builds/warehouse-mv3dt-kafka/reset.sh down      # 只停止並刪除容器與 volume
bash _builds/warehouse-mv3dt-kafka/reset.sh clean     # 只清 data_log（需要 sudo）
bash _builds/warehouse-mv3dt-kafka/reset.sh up        # 重新產生 resolved.yml 並部署
bash _builds/warehouse-mv3dt-kafka/reset.sh status    # 看容器 / VST sensor / RT-CV 狀態
```

全新部署約需 10–15 分鐘才會接上 4 路串流。BEV Live 網站要另外啟動（`up` 結束時會印出指令）。`up` 會讀取 `secrets.env` 裡的 `OPENAI_API_KEY`（§7.3）；`up` 若因健康檢查逾時失敗兩次，等負載下降後再執行 `docker compose -f _builds/warehouse-mv3dt-kafka/resolved.yml up -d` 即可。

> **不要用 `docker compose stop` / `start` 暫停再接續。** 實測（2026-11-11）：`stop` 後再 `start`，VST 資料庫裡
> 前一次的 sensor 全部變成 offline，configurator 重新註冊又被 `found existing sensor with same name` 擋掉，
> RT-CV 4 路都是 0 FPS。要停就 `reset.sh down`，要再用就 `reset.sh`（完整重置）。

> **改了 RT-CV 設定（偵測、追蹤器參數）後，不要只 `docker restart vss-rtvi-cv-mv3dt`。**
> 實測（2026-11-10）：只重啟 RT-CV 數次後，ReID（Milvus，資料在容器內）、bev-fusion、Redis 等仍留著舊 ID 的狀態，
> 造成同一個人被配成 2–3 個 ID（跨攝影機配對失敗率 29%、97% 的幀有重疊）。用 `reset.sh` 全部清空重來後，
> 失敗率降到 0.8%、重疊 1%。比較參數效果時，每一組都要從乾淨狀態開始。

### 8.2 手動停止

```bash
# 停止 BEV Live：Ctrl-C，或 pkill -f apps/bev-live/server.py
docker compose -f $BUILD_DIR/resolved.yml down --remove-orphans
```

**重新部署前一定要清掉舊狀態**，否則 VST / SDR controller 會沿用舊的 sensor 與串流分配，RT-CV 會一直沒有串流（見 §9 #5、#6）：

```bash
D=~/Workspace/fabian/vss-data/vss-warehouse-app-data_v3.3.0/vss-warehouse-app-data
docker volume rm vss_vios_pg_data vss_logstash-libs 2>/dev/null
rm -f $D/data_log/redis/data/dump.rdb
# 若 vss_kafka-data 指向不存在的路徑，也一併刪除：
docker volume inspect -f '{{index .Options "device"}}' vss_kafka-data 2>/dev/null
```

---

## 9. 疑難排解（這次實際遇到的問題）

| # | 症狀 | 原因 | 處理 |
|---|---|---|---|
| 1 | 資料目錄檢查失敗：`data_log/vst/clip_storage` 不可寫 | 前一次部署的容器以 root 建立了 `data_log/vst/` | `sudo chmod -R a+rwx $D/data_log $D/models`（§3.3） |
| 2 | `up` 失敗：`mount .../vss-apps-data/data_log/kafka ... no such file or directory` | 舊的 `vss_kafka-data` volume 指向已不存在的路徑 | `docker rm -f kafka && docker volume rm vss_kafka-data` 後重新 `up` |
| 3 | `sdrc-wdm-env-from-config` Exited(2)：`DNS: transient error`、`gettext (no such package)` | 站點 DNS `172.22.2.99` 間歇性丟封包 | 直接重跑 `up -d`。若經常發生，在 `/etc/docker/daemon.json` 加 `"dns": ["8.8.8.8","1.1.1.1"]`（需重啟 dockerd，會中斷其他容器） |
| 4 | kafka / mosquitto 卡在 `Created`，dockerd log：`failed to bind host port 0.0.0.0:19092: address already in use`；kafka healthcheck `UnknownHostException: kafka` | 主機 Kafka / mosquitto 也綁了 `172.17.0.1:19092` / `:11883`；容器啟動失敗後網路連線被收回 | 改用真正空閒的 port（29192 / 21883），重新產生 `resolved.yml` 後 `up -d`（§1.4） |
| 5 | 容器全 healthy，但 RT-CV `stream_name` FPS 全是 0、`Stream Not Found (404)`；VST sensor 全 offline；sensor-ms log `found existing sensor with same name` | 舊的 `vss_vios_pg_data` 留著前次部署的 sensor，新 sensor 註冊不進去 | `down` → `docker volume rm vss_vios_pg_data` → `up -d` |
| 6 | VST sensor online，但 RT-CV `Active sources : 0`；sdr-controller log `Max streams reached. New stream will not be provisioned.`、envoy `cds: add 5 cluster(s), remove 4` 洗版 | Redis（`data_log/redis/data/dump.rdb`）留著舊的串流分配表，SDRC 以為 RT-CV 已滿 | `down` → `rm dump.rdb` ＋ `docker volume rm vss_vios_pg_data` → `up -d`（兩者要一起清） |
| 7 | `validate_resolved_yml.py`：`NGC_CLI_API_KEY resolved empty` | 產生 `resolved.yml` 時沒 export NGC 金鑰 | 照 §3.2 先 export 再 `docker compose config` |
| 8 | sudo 指令沒生效 | 在 Claude Code 輸入框用 `!` 執行，無法輸入密碼 | 改在一般終端機執行 |
| 9 | 同一個人在 BEV 上有 2–3 個 ID，人數變多 | 反覆只重啟 RT-CV，ReID / bev-fusion / Redis 留著舊 ID 狀態 | `reset.sh` 全部清空重新部署（§8.1） |
| 10 | `up` 失敗：`dependency failed to start: container vss-reid-etcd-mv3dt is unhealthy`；minio log 有 `Rename across devices not allowed` | 全新部署時負載很高，健康檢查（3 秒逾時）誤判；minio 的 rename 訊息是 overlayfs 的無害警告 | 等負載下降再 `docker compose -f resolved.yml up -d` 一次（`reset.sh` 已自動重試一次） |
| 11 | 打開推車（Transporter）後延遲從 <1 秒變 10 秒 | 追蹤物件變多，RT-CV 掉到 24–28 FPS，畫面在 RTSP 緩衝區累積到 `latency=10000` 上限 | 關閉推車；或降低負擔後再打開。緩衝上限由 `blueprint_config.yml` 的 `ds_config_latency` 決定（Kafka 版 10000 ms） |
| 12 | `vss-configurator-mv3dt` Exited(1)：`Variable 'LLM_MODE' has invalid value 'remote'. Allowed values: ['none']`；`sensor-bp-wait-bp-configurator` timeout | IGX-THOR ＋ mv3dt 的 configurator 只接受 `LLM_MODE=none` | `LLM_MODE=none`，遠端 LLM 只靠 `LLM_BASE_URL` / `LLM_MODEL_TYPE` / `LLM_NAME`（§7.2） |
| 13 | `mdx-incidents` 一直是 0 筆，但 `mdx-frames` 的 `socialDistancing.proximityDetections` 有值 | behavior analytics 的 `proximityViolationIncidentEnable` 預設關閉：有偵測、沒寫成事件 | 打開它（§7.2），再完整重置 |
| 14 | 只想加一個服務（例如 phoenix），`up -d --dry-run` 卻顯示會重建 `vss-configurator-mv3dt`、`sdrc-render-config` | 這兩個容器的環境變數裡帶著整串 `COMPOSE_PROFILES`，profile 一變就被視為設定改變 | 用 `docker compose -f resolved.yml up -d --no-deps <服務名>`，只動那一個 |
| 15 | Agent 說某台機器人和很多不同的 Person ID 碰撞，但現場只有一個人 | 4 支影片都是 300 秒循環播放，每輪接縫人會瞬間跳回起點，追蹤器配新 ID；部署跑越久，假 ID 越不會消失，成為貼在真人旁邊（< 0.3 m）的分身 | Agent 已合併同時存在的分身與接力換號（§7.4）。循環後才重新出現、時間上沒重疊的 ID 無法靠位置合併（需要 ReID），根本解法是改用不循環的影片 / 真實攝影機，或定期 `reset.sh` |
| 16 | Agent 回 `Sorry, I wasn't able to complete your request`，Phoenix / 中間步驟顯示 `Recursion limit of 15 reached` | LLM 反覆重查事件：想用 VLM 驗證是否「真的碰撞」，或看到「20 / N 筆」想分頁補齊 | 已在 `config.yml` 提示與 formatter 的 `all_incidents_summary` 修正（§7.4）。若再發生，先到 Phoenix 看它在重查什麼 |
| 17 | Agent 說 `bev-sensor-1` 不存在，要你改選 Camera，結果 0 筆 | Agent 拿 VST 的攝影機清單去「驗證」sensor，裡面沒有 `bev-sensor-1` | 已在提示中註明 `bev-sensor-1` 是合法的事件來源（§7.1）；問題裡直接寫出 `bev-sensor-1` |

### 寫入 repo 的副作用

部署時 `bp-configurator-<mode>` 會改寫多個納入版控的設定檔，並在旁邊留下 `*.backup_<timestamp>.*`。備份檔已被 `.gitignore` 排除；被改寫的原檔若不想保留，可用 `git checkout -- <file>` 還原（下次部署會再改寫）。

---

## 10. 附錄：Port 一覽

| 用途 | Port |
|---|---|
| HAProxy ingress（VST / Kibana / VA API） | 7777 |
| VST ingress | 30888 |
| NvStreamer | 31000 |
| Streamprocessing RTSP（VST live） | 30554–30564 |
| Elasticsearch / Kibana / VA API | 9200 / 5601 / 8081 |
| Kafka（對外，本機改過） | 29192 |
| MQTT（對外，本機改過） | 21883 |
| Grafana / Prometheus | 35000 / 19090 |
| SDR controller | 10000、5003、8011、9902 |
| **BEV Live** | **8091** |
| Agent UI（直連；建議走 7777） | 3000 |
| VSS Agent API（主機 8000 被佔用，本機改過） | 18000 |
| Video Analytics MCP Server | 9901 |
| Phoenix（只綁 127.0.0.1；外部走 7777/phoenix） | 6006 |
