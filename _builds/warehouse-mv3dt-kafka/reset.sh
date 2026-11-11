#!/usr/bin/env bash
# 清空 warehouse mv3dt 部署的所有執行期資料，並重新部署。
#
# 會清掉：所有 VSS 容器（含 ReID 的 Milvus/etcd/minio，資料在容器內）、
#         Docker volume（VST 資料庫等）、data_log/ 下的 Kafka / Elasticsearch /
#         Redis / VST 錄影 / VA API 狀態 / SDRC 產生的設定 / configurator 備份檔。
# 會保留：models/（模型與 TensorRT engine）、videos/、playback/、image、repo 設定檔。
#
# 用法（在 repo 根目錄或任何地方）：
#   bash _builds/warehouse-mv3dt-kafka/reset.sh          # 全部：down → clean → up
#   bash _builds/warehouse-mv3dt-kafka/reset.sh down     # 只停掉並刪除容器與 volume
#   bash _builds/warehouse-mv3dt-kafka/reset.sh clean    # 只清 data_log（需要 sudo 密碼）
#   bash _builds/warehouse-mv3dt-kafka/reset.sh up       # 只重新產生 resolved.yml 並部署
#   bash _builds/warehouse-mv3dt-kafka/reset.sh status   # 看目前狀態
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BUILD_DIR="$REPO/_builds/warehouse-mv3dt-kafka"
FOUNDATION_DIR="$REPO/deploy/docker/industry-profiles/warehouse-operations"
SCRIPTS="$REPO/skills/vss-build-vision-ai/scripts"
RESOLVED="$BUILD_DIR/resolved.yml"
val() { sed -n "s/^$1=//p" "$BUILD_DIR/override.env" | tail -1 | tr -d "\"'"; }
DATA="$(val VSS_DATA_DIR)"
APPS="$(val VSS_APPS_DIR)"
HOST_IP="$(val HOST_IP)"
KAFKA_PORT="$(val KAFKA_HOST_PORT)"

step() { printf '\n\033[1;32m==> %s\033[0m\n' "$*"; }

do_down() {
  step "停止 BEV Live 網站（若有在跑）"
  pkill -f 'apps/bev-live/server.py' 2>/dev/null && echo "stopped" || echo "not running"
  step "docker compose down -v（刪除所有容器、網路、volume）"
  if [ -f "$RESOLVED" ]; then
    docker compose -f "$RESOLVED" down -v --remove-orphans
  else
    echo "找不到 $RESOLVED，改用 project 名稱 vss"
    docker compose -p vss down -v --remove-orphans
  fi
  left="$(docker ps -a --filter label=com.docker.compose.project=vss -q | wc -l)"
  echo "剩餘 vss 容器數：$left"
}

do_clean() {
  step "清除 data_log（需要 sudo）：Kafka / ES / Redis / VST 錄影 / VA API / SDRC / 備份檔"
  sudo env VSS_DATA_DIR="$DATA" VSS_APPS_DIR="$APPS" \
    bash "$REPO/deploy/docker/scripts/cleanup_all_datalog.sh" \
    --env-file "$FOUNDATION_DIR/.env" --skip-revert-from-oldest-backup
  step "重建資料目錄並開放權限（容器用不同 UID 寫入）"
  for d in data_log/analytics_cache data_log/calibration_toolkit data_log/elastic/data \
           data_log/elastic/logs data_log/kafka data_log/redis/data data_log/redis/log \
           data_log/vss_video_analytics_api data_log/vst/clip_storage \
           data_log/nvstreamer/vst_data agent_eval/dataset agent_eval/results models; do
    sudo mkdir -p "$DATA/$d"
  done
  sudo chmod -R a+rwx "$DATA/data_log" "$DATA/agent_eval" "$DATA/models"
  du -sh "$DATA/data_log" "$DATA/models"
}

do_up() {
  step "產生 configurator.env 與 resolved.yml，並驗證"
  export NGC_CLI_API_KEY; NGC_CLI_API_KEY="$(sed -n 's/^\s*apikey\s*=\s*//p' ~/.ngc/config | head -1)"
  export NGC_API_KEY="$NGC_CLI_API_KEY"
  [ -n "$NGC_CLI_API_KEY" ] || { echo "~/.ngc/config 裡沒有 apikey" >&2; exit 1; }
  # 遠端 LLM 的 key（OPENAI_API_KEY）放在 gitignored 的 secrets.env，不寫進 override.env
  if [ -f "$BUILD_DIR/secrets.env" ]; then set -a; . "$BUILD_DIR/secrets.env"; set +a; fi
  if [ "$(val LLM_MODEL_TYPE)" = openai ] && [ -z "${OPENAI_API_KEY:-}" ]; then
    echo "LLM_MODEL_TYPE=openai 但沒有 OPENAI_API_KEY：請建立 $BUILD_DIR/secrets.env" >&2; exit 1
  fi
  uv run -q "$SCRIPTS/render_warehouse_configurator_env.py" "$BUILD_DIR" --repo-root "$REPO"
  docker compose \
    --env-file "$REPO/deploy/docker/containers.env" \
    --env-file "$FOUNDATION_DIR/.env" \
    --env-file "$FOUNDATION_DIR/overrides.env" \
    --env-file "$BUILD_DIR/override.env" \
    -f "$BUILD_DIR/compose.yml" config --no-consistency > "$RESOLVED" 2> "$BUILD_DIR/.config.err"
  chmod 600 "$RESOLVED"
  uv run -q "$SCRIPTS/normalize_resolved_yml.py" "$RESOLVED"
  uv run -q "$SCRIPTS/validate_resolved_yml.py" "$RESOLVED" --repo-root "$REPO"
  uv run -q "$SCRIPTS/validate_warehouse_env.py" "$BUILD_DIR" --repo-root "$REPO"

  step "docker compose up -d"
  # 全新部署時 40 個容器同時啟動，負載很高，minio / etcd 的健康檢查（3 秒逾時）
  # 可能誤判一次 unhealthy，Compose 就會中止、留下一批 Created 的服務。
  # 等負載降下來再跑一次 up -d（已在跑的容器不會被重建）。
  if ! docker compose -f "$RESOLVED" up -d; then
    echo "⚠ 第一次 up 失敗（多半是開機負載造成健康檢查逾時），60 秒後重試一次"
    sleep 60
    docker compose -f "$RESOLVED" up -d
  fi

  step "等待 RT-CV 接上 4 路串流（最多 15 分鐘）"
  end=$((SECONDS + 900)); ok=0
  while [ $SECONDS -lt $end ]; do
    a="$(docker logs --since 15s vss-rtvi-cv-mv3dt 2>&1 | grep -a 'Active sources' | tail -1 | grep -oE '[0-9]+$' || true)"
    if [ "${a:-0}" -ge 4 ]; then ok=1; break; fi
    sleep 15
  done
  [ $ok = 1 ] && echo "RT-CV Active sources = $a（花了 ${SECONDS}s）" || echo "⚠ 15 分鐘內沒等到 4 路串流，請用 status 檢查"
  do_status
  cat <<EOF

BEV Live 網站要另外啟動：
  cd $REPO
  services/rtvi/rt-cv-3d/rt-cv-mv3dt/utils/venv/bin/python apps/bev-live/server.py \\
    --kafka $HOST_IP:${KAFKA_PORT:-9092} --vst http://$HOST_IP:7777
EOF
}

do_status() {
  step "狀態"
  if [ -f "$RESOLVED" ]; then
    docker compose -f "$RESOLVED" ps --all --format json 2>/dev/null |
      jq -r 'select((.State=="running" or (.State=="exited" and .ExitCode==0))|not) | "NOT READY: \(.Name) \(.Status)"' || true
    echo "容器數：$(docker compose -f "$RESOLVED" ps --all -q 2>/dev/null | wc -l) / $(docker compose -f "$RESOLVED" config --services 2>/dev/null | wc -l)"
  fi
  echo "VST sensors:"; curl -s -m 10 "http://$HOST_IP:7777/vst/api/v1/sensor/list" | jq -r '.[] | "  \(.name) \(.state)"' 2>/dev/null || echo "  (VST 尚未就緒)"
  echo "RT-CV:"; docker logs --since 15s vss-rtvi-cv-mv3dt 2>&1 | grep -aE 'stream_name|Active sources' | tail -5 | sed 's/^/  /' || true
}

case "${1:-all}" in
  all)    do_down; do_clean; do_up ;;
  down)   do_down ;;
  clean)  do_clean ;;
  up)     do_up ;;
  status) do_status ;;
  *) echo "用法：$0 [all|down|clean|up|status]" >&2; exit 2 ;;
esac
