# SOP：在 IGX Thor 部署 VSS Warehouse MV3DT ＋ BEV Live 對照網站

本文件記錄如何從零重現以下成果（2026-11-10 實測於 NVIDIA IGX Thor Developer Kit）：

1. **VSS Warehouse industry profile — `mv3dt` 模式**：4 支攝影機的多視角 3D 追蹤（MV3DT）＋ BEV fusion ＋ behavior analytics ＋ ELK，Kafka 版、Extended 規模。
2. **BEV 軌跡影片**：用 `bev-visualizer.sh` 把 `mdx-bev` 融合軌跡畫在倉庫俯視圖上，存成 mp4。
3. **BEV Live 網站（port 8091）**：左邊即時 BEV 軌跡、右邊 4 路攝影機畫面，可互相對照。原始碼在 [`apps/bev-live/`](apps/bev-live/)。

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
| [`_builds/warehouse-mv3dt-kafka/`](_builds/warehouse-mv3dt-kafka/) | 這次部署的 build layer：`override.env`、`compose.yml`（**不含**金鑰；`resolved.yml` / `configurator.env` 是部署時產生、已被 `.gitignore` 排除） |
| `deploy/docker/industry-profiles/warehouse-operations/overrides.env` | 本機設定：`HOST_IP`、`VSS_APPS_DIR`、`VSS_DATA_DIR`、`HARDWARE_PROFILE=IGX-THOR`、`PROMETHEUS_HOST_PORT=19090` 等 |
| 其他 `deploy/docker/**/configs/` 下 json、txt、yml 的修改 | **blueprint-configurator 部署時自動改寫**的結果（不是手改的）。重新部署時會再被改寫，可以不必在意 |
| `deploy/docker/dns-override.yml` | 站點 DNS 不穩時，給 LLM NIM 用的 DNS / hosts 覆寫（本次 mv3dt 部署沒用到） |
| `.gitignore` | 排除 configurator 的 `*.backup_*`、TensorRT engines、資料目錄、`resolved.yml` 等 |

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
```

> 請用 `umask 022` 的 shell clone；`027` 會讓容器讀不到 bind-mount 的設定檔。

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

## 7. 停止 / 重新部署

```bash
# 停止 BEV Live：Ctrl-C，或 pkill -f apps/bev-live/server.py
docker compose -f $BUILD_DIR/resolved.yml down --remove-orphans
```

**重新部署前一定要清掉舊狀態**，否則 VST / SDR controller 會沿用舊的 sensor 與串流分配，RT-CV 會一直沒有串流（見 §8 #5、#6）：

```bash
D=~/Workspace/fabian/vss-data/vss-warehouse-app-data_v3.3.0/vss-warehouse-app-data
docker volume rm vss_vios_pg_data vss_logstash-libs 2>/dev/null
rm -f $D/data_log/redis/data/dump.rdb
# 若 vss_kafka-data 指向不存在的路徑，也一併刪除：
docker volume inspect -f '{{index .Options "device"}}' vss_kafka-data 2>/dev/null
```

---

## 8. 疑難排解（這次實際遇到的問題）

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

### 寫入 repo 的副作用

部署時 `bp-configurator-<mode>` 會改寫多個納入版控的設定檔，並在旁邊留下 `*.backup_<timestamp>.*`。備份檔已被 `.gitignore` 排除；被改寫的原檔若不想保留，可用 `git checkout -- <file>` 還原（下次部署會再改寫）。

---

## 9. 附錄：Port 一覽

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
