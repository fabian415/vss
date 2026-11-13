#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""BEV Live — 即時對照 4 路攝影機畫面與 mdx-bev 融合軌跡。

執行（用 rt-cv-mv3dt 的 utils/venv，已含 opencv / confluent-kafka / protobuf / PyYAML）：
    services/rtvi/rt-cv-3d/rt-cv-mv3dt/utils/venv/bin/python apps/bev-live/server.py \
        --kafka <HOST_IP>:<KAFKA_HOST_PORT> --vst http://<HOST_IP>:7777

- /events      SSE：mdx-bev 每一幀 {t, o:[[id,x,y,type],...]}；
               碰撞（Proximity Violation）事件以 `event: incident` 推送 {start, ids, types, category}
- /cam/<name>  MJPEG：從 VST 拉 RTSP，降解析度後轉送（沒人看時自動停）
- /config.json 地圖尺寸、T_ov2px、攝影機位置與內外參
- /bev3d.js    3D 視圖（three.js，從 jsDelivr 載入）
"""
import argparse, json, os, queue, sys, threading, time, urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

# VST 轉出的 H.264 每幀都帶一段長度欄位多算 1 byte 的 SEI，FFmpeg 會以 ERROR 等級逐幀抱怨
# （"SEI type 5 size 1040 truncated at 1039"），畫面其實解碼正常；只留 FATAL 以免 log 爆量。
os.environ.setdefault("OPENCV_FFMPEG_LOGLEVEL", "8")
import cv2, yaml  # noqa: E402
from confluent_kafka import Consumer

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
UTILS = REPO / "services/rtvi/rt-cv-3d/rt-cv-mv3dt/utils"
sys.path.insert(0, str(UTILS))
from schema_pb2 import Frame  # noqa: E402

p = argparse.ArgumentParser()
p.add_argument("--kafka", default="172.22.12.205:29192")
p.add_argument("--topic", default="mdx-bev")
p.add_argument("--incident-topic", default="mdx-incidents")
p.add_argument("--vst", default="http://172.22.12.205:7777")
p.add_argument("--calibration", default=str(REPO / "deploy/docker/industry-profiles/warehouse-operations/warehouse-mv3dt-app/calibration/sample-data/warehouse-4cams-20mx20m-synthetic/calibration.json"))
p.add_argument("--port", type=int, default=8091)
p.add_argument("--cam-width", type=int, default=640)
p.add_argument("--cam-fps", type=float, default=15)
args = p.parse_args()

# ---------- static config ----------
T = yaml.safe_load((HERE / "transforms.yml").read_text())["T_ov2px"]
mh, mw = cv2.imread(str(HERE / "map.png")).shape[:2]
cams_cal = {}
for s in json.loads(Path(args.calibration).read_text())["sensors"]:
    if s.get("type") != "camera":
        continue
    d = next((a["value"] for a in s.get("attributes", []) if a["name"] == "direction"), None)
    attr = {a["name"]: a["value"] for a in s.get("attributes", [])}
    cams_cal[s["id"]] = {"x": s["coordinates"]["x"], "y": s["coordinates"]["y"],
                         "dir": float(d) if d is not None else None,
                         # 3D 視圖用：外參 [R|t]、內參 K、影像尺寸 -> 攝影機位置與視錐
                         "ext": s.get("extrinsicMatrix"), "K": s.get("intrinsicMatrix"),
                         "wh": [int(attr.get("frameWidth") or 1920), int(attr.get("frameHeight") or 1080)]}


def vst_get(path):
    with urllib.request.urlopen(args.vst + "/vst/api/v1/" + path, timeout=10) as r:
        return json.load(r)


def resolve_rtsp():
    """name -> rtsp url（把 VST 內部主機名換成 VST 對外的 host）。"""
    host = args.vst.split("://", 1)[1].split(":")[0].split("/")[0]
    names = {s["sensorId"]: s["name"] for s in vst_get("sensor/list")}
    out = {}
    for item in vst_get("live/streams"):
        for sid, streams in item.items():
            if sid in names and streams:
                url = streams[0]["url"]
                out[names[sid]] = url.replace(url.split("//", 1)[1].split(":")[0], host, 1)
    return dict(sorted(out.items()))


# ---------- Kafka -> SSE ----------
clients, clients_lock = set(), threading.Lock()
stats = {"frames": 0, "last_t": 0}
last_type = {}  # object id -> 最近一次在 mdx-bev 看到的類別（給碰撞通知用）


def broadcast(message):
    """把一則完整的 SSE 訊息丟給所有連線；佇列滿時丟掉最舊的。"""
    with clients_lock:
        for q in clients:
            if q.full():
                try:
                    q.get_nowait()
                except queue.Empty:
                    pass
            q.put_nowait(message)


def kafka_loop():
    c = Consumer({"bootstrap.servers": args.kafka, "group.id": f"bev-live-{os.getpid()}",
                  "auto.offset.reset": "latest", "enable.auto.commit": False})
    c.subscribe([args.topic])
    last_bucket = None
    while True:
        m = c.poll(0.5)
        if m is None or m.error():
            continue
        f = Frame()
        try:
            f.ParseFromString(m.value())
            bucket = int(f.id)
        except Exception:
            continue
        if last_bucket is not None and bucket <= last_bucket:
            continue  # 亂序/重複
        last_bucket = bucket
        t = f.timestamp.seconds * 1000 + f.timestamp.nanos // 1_000_000
        # [id, x, y, type(, w, l, h, yaw)]；type 用來分類計數與換形狀，
        # 尺寸與朝向（bbox3d 9-DoF: x y z w l h pitch roll yaw）給 3D 視圖
        objs = []
        for o in f.objects:
            bb = o.bbox3d.coordinates  # 別用 c：那是外層的 Kafka consumer
            if len(bb) < 2:
                continue
            row = [o.id, round(bb[0], 3), round(bb[1], 3), o.type]
            if len(bb) >= 9:
                row += [round(bb[3], 2), round(bb[4], 2), round(bb[5], 2), round(bb[8], 3)]
            objs.append(row)
        payload = json.dumps({"t": t, "o": objs}, separators=(",", ":"))
        stats["frames"] += 1
        stats["last_t"] = t
        for o in f.objects:
            if o.type:
                last_type[o.id] = o.type
        broadcast(f"data: {payload}\n\n")


# ---------- mdx-incidents（nv.Incident protobuf）-> SSE ----------
def _varint(buf, i):
    shift = val = 0
    while True:
        b = buf[i]
        i += 1
        val |= (b & 0x7F) << shift
        if b < 0x80:
            return val, i
        shift += 7


def _fields(buf):
    """極簡 protobuf 解碼：逐一產生 (field_number, wire_type, value)。"""
    i = 0
    while i < len(buf):
        key, i = _varint(buf, i)
        num, wt = key >> 3, key & 7
        if wt == 0:
            val, i = _varint(buf, i)
        elif wt == 2:
            n, i = _varint(buf, i)
            val, i = buf[i:i + n], i + n
        elif wt == 1:
            val, i = buf[i:i + 8], i + 8
        elif wt == 5:
            val, i = buf[i:i + 4], i + 4
        else:
            raise ValueError(f"wire type {wt}")
        yield num, wt, val


def _timestamp_ms(buf):
    sec = nanos = 0
    for num, _, val in _fields(buf):
        if num == 1:
            sec = val
        elif num == 2:
            nanos = val
    return sec * 1000 + nanos // 1_000_000


def parse_incident(buf):
    """nv.Incident 只取需要的欄位：1 sensorId、2 timestamp、3 end、4 objectIds、8 category、11 info。"""
    inc = {"sensor": "", "start": 0, "end": 0, "ids": [], "category": "", "info": {}}
    for num, wt, val in _fields(buf):
        if wt != 2:
            continue
        if num == 1:
            inc["sensor"] = val.decode()
        elif num == 2:
            inc["start"] = _timestamp_ms(val)
        elif num == 3:
            inc["end"] = _timestamp_ms(val)
        elif num == 4:
            inc["ids"].append(val.decode())
        elif num == 8:
            inc["category"] = val.decode()
        elif num == 11:
            entry = {n: v.decode() for n, w, v in _fields(val) if w == 2}
            inc["info"][entry.get(1, "")] = entry.get(2, "")
    return inc


def incident_loop():
    """每個事件只通知一次：行為分析在事件持續期間會重複送出更新，用（開始時間, 主要物件）去重。"""
    c = Consumer({"bootstrap.servers": args.kafka, "group.id": f"bev-live-inc-{os.getpid()}",
                  "auto.offset.reset": "latest", "enable.auto.commit": False})
    c.subscribe([args.incident_topic])
    seen = {}
    while True:
        m = c.poll(0.5)
        if m is None or m.error():
            continue
        try:
            inc = parse_incident(m.value())
        except Exception:
            continue
        if not inc["ids"]:
            continue
        key = (inc["sensor"], inc["start"], inc["ids"][0])
        now = time.time()
        if key in seen:
            continue
        seen[key] = now
        for k in [k for k, t in seen.items() if now - t > 600]:
            del seen[k]
        stats["incidents"] = stats.get("incidents", 0) + 1
        event = {"start": inc["start"], "ids": inc["ids"], "category": inc["category"],
                 "types": {i: last_type.get(i, "") for i in inc["ids"]}}
        print(f"incident {inc['category']} {event['types']}", flush=True)
        broadcast(f"event: incident\ndata: {json.dumps(event, separators=(',', ':'))}\n\n")


# ---------- RTSP -> MJPEG ----------
class Camera:
    def __init__(self, name, url):
        self.name, self.url = name, url
        self.jpeg, self.seq = None, 0
        self.cond = threading.Condition()
        self.viewers, self.last_view = 0, 0.0
        threading.Thread(target=self.loop, daemon=True).start()

    def loop(self):
        period = 1.0 / args.cam_fps
        while True:
            if self.viewers == 0 and time.time() - self.last_view > 10:
                time.sleep(0.5)
                continue
            cap = cv2.VideoCapture(self.url, cv2.CAP_FFMPEG)
            if not cap.isOpened():
                time.sleep(2)
                continue
            next_t = 0.0
            while self.viewers > 0 or time.time() - self.last_view <= 10:
                ok = cap.grab()
                if not ok:
                    break
                now = time.time()
                if now < next_t:
                    continue  # 只 grab 不 decode，維持即時又省 CPU
                next_t = now + period
                ok, frame = cap.retrieve()
                if not ok:
                    break
                h, w = frame.shape[:2]
                frame = cv2.resize(frame, (args.cam_width, int(h * args.cam_width / w)),
                                   interpolation=cv2.INTER_AREA)
                ok, buf = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, 75])
                if ok:
                    with self.cond:
                        self.jpeg, self.seq = buf.tobytes(), self.seq + 1
                        self.cond.notify_all()
            cap.release()


cameras = {}
cameras_lock = threading.Lock()


def refresh_cameras():
    """重新向 VST 查攝影機。reset 後 sensorId/RTSP 網址會變，VST 剛起來時清單也可能是空的。"""
    try:
        found = resolve_rtsp()
    except Exception as e:
        print(f"VST 查詢失敗：{e}", flush=True)
        return
    with cameras_lock:
        for name, url in found.items():
            if name not in cameras or cameras[name].url != url:
                cameras[name] = Camera(name, url)
                print(f"camera {name}: {url}", flush=True)


# ---------- HTTP ----------
class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def send_bytes(self, body, ctype, cache="no-cache"):
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", cache)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = self.path.split("?", 1)[0]
        if path in ("/", "/index.html"):
            return self.send_bytes((HERE / "index.html").read_bytes(), "text/html; charset=utf-8")
        if path == "/bev3d.js":
            return self.send_bytes((HERE / "bev3d.js").read_bytes(), "text/javascript; charset=utf-8")
        if path == "/map.png":
            return self.send_bytes((HERE / "map.png").read_bytes(), "image/png", "max-age=3600")
        if path == "/config.json":
            refresh_cameras()
            cfg = {"map": {"w": mw, "h": mh}, "T": T,
                   "cameras": [{"name": n, **cams_cal.get(n, {})} for n in cameras]}
            return self.send_bytes(json.dumps(cfg).encode(), "application/json")
        if path == "/events":
            return self.sse()
        if path.startswith("/cam/") and path[5:] in cameras:
            return self.mjpeg(cameras[path[5:]])
        self.send_error(404)

    def sse(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.end_headers()
        q = queue.Queue(maxsize=120)
        with clients_lock:
            clients.add(q)
        try:
            while True:
                try:
                    self.wfile.write(q.get(timeout=10).encode())
                except queue.Empty:
                    self.wfile.write(b": ping\n\n")
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            with clients_lock:
                clients.discard(q)

    def mjpeg(self, cam):
        self.send_response(200)
        self.send_header("Content-Type", "multipart/x-mixed-replace; boundary=frame")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        cam.viewers += 1
        seen = -1
        try:
            while True:
                with cam.cond:
                    cam.cond.wait_for(lambda: cam.seq != seen, timeout=10)
                    jpeg, seen = cam.jpeg, cam.seq
                if jpeg is None:
                    continue
                self.wfile.write(b"--frame\r\nContent-Type: image/jpeg\r\nContent-Length: "
                                 + str(len(jpeg)).encode() + b"\r\n\r\n" + jpeg + b"\r\n")
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            cam.viewers -= 1
            cam.last_view = time.time()


if __name__ == "__main__":
    refresh_cameras()
    threading.Thread(target=kafka_loop, daemon=True).start()
    threading.Thread(target=incident_loop, daemon=True).start()
    print(f"kafka {args.kafka} topics {args.topic}, {args.incident_topic}", flush=True)
    srv = ThreadingHTTPServer(("0.0.0.0", args.port), Handler)
    srv.daemon_threads = True
    print(f"BEV Live: http://0.0.0.0:{args.port}/", flush=True)
    srv.serve_forever()
