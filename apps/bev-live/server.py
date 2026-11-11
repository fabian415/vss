#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""BEV Live — 即時對照 4 路攝影機畫面與 mdx-bev 融合軌跡。

執行（用 rt-cv-mv3dt 的 utils/venv，已含 opencv / confluent-kafka / protobuf / PyYAML）：
    services/rtvi/rt-cv-3d/rt-cv-mv3dt/utils/venv/bin/python apps/bev-live/server.py \
        --kafka <HOST_IP>:<KAFKA_HOST_PORT> --vst http://<HOST_IP>:7777

- /events      SSE：mdx-bev 每一幀 {t, o:[[id,x,y],...]}
- /cam/<name>  MJPEG：從 VST 拉 RTSP，降解析度後轉送（沒人看時自動停）
- /config.json 地圖尺寸、T_ov2px、攝影機位置
"""
import argparse, json, os, queue, sys, threading, time, urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import cv2, yaml
from confluent_kafka import Consumer

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
UTILS = REPO / "services/rtvi/rt-cv-3d/rt-cv-mv3dt/utils"
sys.path.insert(0, str(UTILS))
from schema_pb2 import Frame  # noqa: E402

p = argparse.ArgumentParser()
p.add_argument("--kafka", default="172.22.12.205:29192")
p.add_argument("--topic", default="mdx-bev")
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
    cams_cal[s["id"]] = {"x": s["coordinates"]["x"], "y": s["coordinates"]["y"],
                         "dir": float(d) if d is not None else None}


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
        # [id, x, y, type]；type 用來在前端分類計數與換形狀
        payload = json.dumps({"t": t, "o": [[o.id, round(o.bbox3d.coordinates[0], 3),
                                             round(o.bbox3d.coordinates[1], 3), o.type]
                                            for o in f.objects if len(o.bbox3d.coordinates) >= 2]},
                             separators=(",", ":"))
        stats["frames"] += 1
        stats["last_t"] = t
        with clients_lock:
            for q in clients:
                if q.full():
                    try:
                        q.get_nowait()
                    except queue.Empty:
                        pass
                q.put_nowait(payload)


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
        if path == "/map.png":
            return self.send_bytes((HERE / "map.png").read_bytes(), "image/png", "max-age=3600")
        if path == "/config.json":
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
                    self.wfile.write(f"data: {q.get(timeout=10)}\n\n".encode())
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
    for name, url in resolve_rtsp().items():
        cameras[name] = Camera(name, url)
        print(f"camera {name}: {url}", flush=True)
    threading.Thread(target=kafka_loop, daemon=True).start()
    print(f"kafka {args.kafka} topic {args.topic}", flush=True)
    srv = ThreadingHTTPServer(("0.0.0.0", args.port), Handler)
    srv.daemon_threads = True
    print(f"BEV Live: http://0.0.0.0:{args.port}/", flush=True)
    srv.serve_forever()
