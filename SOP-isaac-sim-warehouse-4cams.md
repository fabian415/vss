# SOP：在 Isaac Sim 重建 nv-warehouse-4cams 場景並生成影片

目標：在 Isaac Sim 建一個倉庫場景，裡面 4 支攝影機的位置、朝向、焦距跟
`nv-warehouse-4cams/calibration.json` 一模一樣。用 Actor SDG（Isaac Sim Replicator Agent，IRA）讓人、AMR、人型機器人在裡面走動，
輸出 4 支 1920×1080 / 30 fps 的 mp4。這些影片可以直接餵給既有的 warehouse mv3dt 部署（見
[SOP-warehouse-mv3dt-bev-live.md](SOP-warehouse-mv3dt-bev-live.md)）。

這是後續「改善方案 → 模擬驗證」閉環的第一步：先有一個跟 VSS 部署**同一套座標、同一套攝影機**的數位孿生。

> **先講清楚限制**
> - 本 repo **沒有** NVIDIA 生成 nv-warehouse-4cams 用的原始 USD 場景和 IRA 設定檔，只有
>   `calibration.json`、`camInfo/`、`Top.png` 和 mp4。所以這裡是「相同攝影機、相近場景」的重建，
>   畫面裡的貨架、人物、路徑不會跟原始影片逐格相同。
> - 場景用 Isaac Sim 內附的 `Simple_Warehouse/full_warehouse.usd`。**它是不是原始場景、原點是否一致，還沒確認**，
>   這要靠 §4 的對照步驟驗證。對不上的話，處理方式見 §4.3。
> - IRA 設定檔的格式在 Isaac Sim 5.1（IRA 0.x）和 6.x 之後（IRA 1.x）**不相容**。本文以 6.x 為準，
>   `version` 欄位與欄位名稱一律以你安裝版本內附的 `sample_configs/` 為準。

---

## 0. 從 calibration.json 讀出來的攝影機參數（參考值）

座標系：Z 軸朝上、單位公尺，跟 Isaac Sim 倉庫場景慣例相同。用 `C = -Rᵀt` 從 `extrinsicMatrix` 算出：

| 攝影機 | 位置 (x, y, z) m | 朝向（世界座標） | 俯角 | fx / fy (px) |
|---|---|---|---|---|
| `Camera`    | ( 0.00,  0.00, 3.60) | (-0.83, -0.16, -0.53) | 31.9° | 999.6 / 988.7 |
| `Camera_01` | (-13.69, -2.63, 2.95) | ( 0.81,  0.37, -0.46) | 27.4° | 935.2 / 928.4 |
| `Camera_02` | (-7.00,  4.88, 3.41) | ( 0.08, -0.89, -0.46) | 27.2° | 933.9 / 926.4 |
| `Camera_03` | (-6.52, -9.10, 3.07) | (-0.03,  0.91, -0.41) | 24.3° | 939.9 / 934.7 |

解析度都是 1920×1080，主點 (960, 540)，沒有畸變參數。

偵測模型的類別（[ds-detector-labels.txt](deploy/docker/industry-profiles/warehouse-operations/warehouse-mv3dt-app/deepstream/configs/ds-detector-labels.txt)）：
`Person, Agility_Digit_Humanoid, Fourier_GR1_T2_Humanoid, Nova_Carter, Transporter, Forklift, Pallet`。
場景裡放的角色要在這個清單內，偵測模型才認得出來。

---

## 1. 環境

| 項目 | 需求 |
|---|---|
| 機器 | **另一台 x86_64 + NVIDIA RTX GPU 的工作站**（建議 RTX 4080/5080 以上，VRAM ≥ 16 GB）。Isaac Sim 需要 RTX 光追；我沒查到 IGX / Jetson Thor 的官方支援，不要在 Thor 上裝 |
| Isaac Sim | 6.x（本文寫法）。5.1 也可以，但 IRA 設定檔格式不同，見 §3 |
| 擴充套件 | `isaacsim.replicator.agent.core`、`isaacsim.replicator.agent.ui`、`isaacsim.anim.robot.core`、`omni.metropolis.pipeline`；要重新產生校正檔時另需 `isaacsim.sensors.rtx.placement`（5.x）或 `isaacsim.sensors.rtx.calibration`（6.x） |
| 其他 | `ffmpeg`、Python 3.10（跑 [tools/sdg-postprocessing/](tools/sdg-postprocessing/README.md)） |

從 Thor 把校正資料和原始影片複製到工作站：

```bash
# 在工作站上執行
REPO_ON_THOR=<THOR_USER>@<HOST_IP>:~/Workspace/fabian/video-search-and-summarization
CAL_SUB=deploy/docker/industry-profiles/warehouse-operations/warehouse-mv3dt-app/calibration/sample-data/nv-warehouse-4cams
mkdir -p ~/sim4cams/ref && cd ~/sim4cams/ref
scp -r "$REPO_ON_THOR/$CAL_SUB" ./calib
scp -r <THOR_USER>@<HOST_IP>:~/Workspace/fabian/vss-data/vss-warehouse-app-data_v3.3.0/vss-warehouse-app-data/videos/nv-warehouse-4cams ./videos
```

---

## 2. 建立場景與 4 支攝影機（Script Editor）

### 2.1 載入倉庫

1. 啟動 Isaac Sim，在 **Window > Extensions** 啟用 §1 列的擴充套件，並勾 autoload。
2. **File > Open**，在 Isaac Sim Assets 開
   `Isaac/Environments/Simple_Warehouse/full_warehouse.usd`。
3. 確認 stage 的 Up Axis = Z、Meters Per Unit = 1.0（**Layer** 面板 > root layer 屬性）。

### 2.2 依 calibration.json 放攝影機

**Window > Script Editor**，貼上下面的腳本，把 `CAL` 改成你的路徑後執行：

```python
# 依 VSS calibration.json 在 /World/Cameras 下建立攝影機（名稱與 sensor id 相同）
import json
import numpy as np
import omni.usd
from pxr import Gf, UsdGeom

CAL = "/home/<you>/sim4cams/ref/calib/calibration.json"
H_APERTURE_MM = 20.955          # 水平感光尺寸，任意固定值即可，焦距會依它換算

stage = omni.usd.get_context().get_stage()
UsdGeom.Xform.Define(stage, "/World/Cameras")

# OpenCV 相機座標 (x 右, y 下, z 前) → USD 相機座標 (x 右, y 上, 看向 -z)
CV_TO_USD = np.diag([1.0, -1.0, -1.0])

for s in json.load(open(CAL))["sensors"]:
    if s.get("type") != "camera":
        continue
    attrs = {a["name"]: a["value"] for a in s["attributes"]}
    W, H = int(attrs["frameWidth"]), int(attrs["frameHeight"])
    K = np.array(s["intrinsicMatrix"], dtype=float)
    E = np.array(s["extrinsicMatrix"], dtype=float)   # world -> camera, 3x4
    R, t = E[:, :3], E[:, 3]

    cam_to_world = np.eye(4)
    cam_to_world[:3, :3] = R.T @ CV_TO_USD
    cam_to_world[:3, 3] = -R.T @ t

    fx, fy = K[0, 0], K[1, 1]
    focal_mm = fx * H_APERTURE_MM / W
    v_aperture_mm = focal_mm * H / fy                # 讓 fy 也對上

    path = f"/World/Cameras/{s['id']}"
    cam = UsdGeom.Camera.Define(stage, path)
    cam.CreateFocalLengthAttr(focal_mm)
    cam.CreateHorizontalApertureAttr(H_APERTURE_MM)
    cam.CreateVerticalApertureAttr(v_aperture_mm)
    cam.CreateClippingRangeAttr(Gf.Vec2f(0.05, 500.0))

    xf = UsdGeom.Xformable(cam.GetPrim())
    xf.ClearXformOpOrder()
    # Gf.Matrix4d 是 row-vector 慣例，要轉置
    xf.AddTransformOp().Set(Gf.Matrix4d(cam_to_world.T.tolist()))
    print(f"{path}: pos={cam_to_world[:3, 3].round(3)} focal={focal_mm:.3f}mm")
```

預期輸出的位置要跟 §0 的表一致。

### 2.3 存成新場景

**File > Save As…** → `~/sim4cams/warehouse_4cams.usd`。之後的 IRA 設定檔都以這個檔案為基底，
這樣攝影機就不會被 IRA 自動擺放的攝影機取代。

---

## 3. 用 IRA（Actor SDG）讓角色動起來並錄影

### 3.1 取得範本

IRA 內附的範本在：

```
<Isaac Sim 安裝目錄>/extscache/isaacsim.replicator.agent.core-<版本>/data/sample_configs/
```

複製 `warehouse.yaml`（或 `minimal.yaml`）到 `~/sim4cams/ira_4cams.yaml`，然後照下面改。
**`version` 一律保留範本原本的值**（6.0 用 1.6.x、6.1 之後會拒絕舊的版本號）。

### 3.2 設定檔重點（IRA 1.x，Isaac Sim 6.x）

```yaml
isaacsim.replicator.agent:
  version: <保留範本的值>
  seed: 20261116                     # 固定 seed → 可重現；做多輪隨機測試時再換
  simulation_duration: 120.0         # 秒；IRA 以 30 Hz 推進 → 3600 幀
  environment:
    base_stage_asset_path: /home/<you>/sim4cams/warehouse_4cams.usd
  character:
    groups:
      workers:
        num: 6
        routines:
          - wander: {}               # 先用隨機走動；之後要製造人車交會再改 patrol + path_points
  robot:
    groups:
      carters:
        num: 2
        config_file_path: nova_carter.yaml     # 名稱以範本 / 安裝內附的 robot config 為準
        write_data: false
      transporters:
        num: 1
        config_file_path: transporter.yaml
        write_data: false
  # sensor: 不設 sensor groups，沿用 stage 裡 /World/Cameras 下 §2 建的 4 支攝影機
  replicator:
    writers:
      IRABasicWriter:
        output_dir: /home/<you>/sim4cams/ira_output
        start_frame: 30              # 前 1 秒角色還在落地，略過
        rgb: true
        camera_params: true
        bounding_box_2d_tight: true  # 之後評估偵測率用的 GT
        sensor_prim_list:
          - /World/Cameras/Camera
          - /World/Cameras/Camera_01
          - /World/Cameras/Camera_02
          - /World/Cameras/Camera_03
```

需要依你的版本確認的地方：

| 項目 | 說明 |
|---|---|
| 不設 `sensor` 是否合法 | 文件說 writer 不給 `sensor_prim_list` 時會用 `sensor.root_prim_path`（預設 `/World/Cameras`）下全部攝影機。如果你的版本要求一定要有 sensor group，就把 `num` 設 0；不接受 0 的話，改在 UI 按完 **Set Up Simulation** 後刪掉自動產生的攝影機再錄 |
| 輸出解析度 | 必須是 1920×1080，不然內參就跟 `calibration.json` 對不上。在 Configuration File Guide 找 writer / render product 的解析度欄位 |
| 人型機器人（Digit / GR1） | 透過 `isaacsim.anim.robot.core` 加入，欄位名稱以內附範本為準。第一輪可以先不加 |
| Isaac Sim 5.1（IRA 0.x） | 格式不同：`scene.asset_path`、`sensor.camera_num`、`global.seed` 等。直接用 5.1 內附的範本改 |

### 3.3 執行

**有 GUI（第一次建議這樣做，可以邊看邊調）：**
**Tools > Action and Event Data Generation > Actor SDG** → 載入 `ira_4cams.yaml` → **Set Up Simulation** →
確認 4 支攝影機的畫面（Viewport 切換攝影機）→ **Start Data Generation**。

**無頭模式（批次、多輪隨機測試用）：**

```bash
cd <Isaac Sim 安裝目錄>
./python.sh tools/actor_sdg/actor_sdg.py -c ~/sim4cams/ira_4cams.yaml
# Isaac Sim 7.0 之後要加 --allow-trusted-scripts（只對你信任的設定檔使用）
```

輸出：`~/sim4cams/ira_output/<每支攝影機一個資料夾>/rgb/*.png`、`camera_params/`、`object_detection.json`。

---

## 4. 驗證：重建的攝影機跟原版對不對得上

這一步最重要，攝影機對不上的話，後面的 MV3DT / BEV 全部會錯。

### 4.1 用 camera_params 驗證內外參

IRA 每一幀都會輸出 `camera_params/*.json`。拿第一幀的投影矩陣跟 `calibration.json` 的 `cameraMatrix` 比，
差異應該只有浮點誤差。也可以直接跑 repo 內的檢查工具：

```bash
cd <REPO>/tools/sdg-postprocessing
pip install -r requirements.txt
python data_sanity_check/dataset_sanity_check_calibration.py \
  --base_dir ~/sim4cams/dataset --calibration ~/sim4cams/ref/calib/calibration.json \
  --output_dir ~/sim4cams/calib_check
```

（`--base_dir` 要先整理成 §5.1 的資料夾結構。）

### 4.2 畫面對照

把原始影片第一幀和重建的第一幀疊在一起看：

```bash
cd ~/sim4cams
for c in Camera Camera_01 Camera_02 Camera_03; do
  ffmpeg -y -loglevel error -i ref/videos/$c.mp4 -frames:v 1 ref_$c.png
  ffmpeg -y -loglevel error -i ref_$c.png -i dataset/_World_Cameras_$c/rgb/rgb_00000.jpg \
    -filter_complex "[0][1]blend=all_mode=difference" diff_$c.png
done
```

地板線、貼地標線、柱子、貨架底部要對齊（人和機器人位置本來就不同，不用管）。

### 4.3 對不上怎麼辦

| 現象 | 原因 | 處理 |
|---|---|---|
| 視角正確但整個場景平移或旋轉 | `full_warehouse.usd` 的原點跟原始場景不同 | 把倉庫 prim 包在一個 Xform 下，調整它的 translate/rotate，讓地標對齊；**不要動攝影機** |
| 場景根本是不同倉庫 | 原始場景不是 `full_warehouse.usd` | 兩個選擇：(a) 找原始場景（PhysicalAI-SmartSpaces 資料集頁面或 NVIDIA 窗口），(b) 接受用新場景，照 §6 重新產生自己的 `calibration.json` 和 `Top.png` |
| 畫面放大或縮小 | 輸出解析度不是 1920×1080，或者 aperture 被 IRA 改掉 | 檢查 writer 解析度與 `/World/Cameras/*` 的 focalLength |
| 上下顛倒或看向反方向 | 腳本裡的座標轉換被改過 | 確認 `CV_TO_USD = diag(1,-1,-1)` 和 `.T` 轉置 |

---

## 5. 轉成 VSS 可用的資料集

### 5.1 整理成 sdg-postprocessing 的資料夾結構

repo 的工具預期這種結構（攝影機資料夾名稱 = prim path 把 `/` 換成 `_`）：

```
~/sim4cams/dataset/
  _World_Cameras_Camera/rgb/rgb_00000.jpg ...
  _World_Cameras_Camera_01/rgb/...
  _World_Cameras_Camera_02/rgb/...
  _World_Cameras_Camera_03/rgb/...
```

IRA 輸出的資料夾名稱和副檔名（PNG）可能不同，先轉一次：

```bash
cd ~/sim4cams
for c in Camera Camera_01 Camera_02 Camera_03; do
  src=$(ls -d ira_output/*${c} | grep -E "(^|_)${c}$" | head -1)   # 依實際輸出名稱調整
  dst=dataset/_World_Cameras_$c/rgb; mkdir -p $dst
  i=0; for f in $(ls $src/rgb/*.png | sort); do
    ffmpeg -loglevel error -y -i $f -q:v 2 $(printf "$dst/rgb_%05d.jpg" $i); i=$((i+1))
  done
done
```

### 5.2 檢查與轉檔

```bash
cd <REPO>/tools/sdg-postprocessing
N=$(ls ~/sim4cams/dataset/_World_Cameras_Camera/rgb | wc -l)
python data_sanity_check/dataset_sanity_check_rgb.py --base_dir ~/sim4cams/dataset --total_frames $N
bash data_conversion/convert_images_to_videos_no_bframes.sh ~/sim4cams/dataset   # 30 fps、無 B-frame
bash data_sanity_check/dataset_sanity_check_videos.sh ~/sim4cams/dataset
```

4 支影片的幀數必須相同，MV3DT 才能用同步的 timestamp 融合。

### 5.3 放回 Thor 的 VSS 部署

用新名稱 `sim-warehouse-4cams`，不要蓋掉原版：

```bash
# 影片
DATA=~/Workspace/fabian/vss-data/vss-warehouse-app-data_v3.3.0/vss-warehouse-app-data/videos
mkdir -p $DATA/sim-warehouse-4cams
scp <workstation>:~/sim4cams/dataset/_World_Cameras_Camera/*.mp4    $DATA/sim-warehouse-4cams/Camera.mp4
scp <workstation>:~/sim4cams/dataset/_World_Cameras_Camera_01/*.mp4 $DATA/sim-warehouse-4cams/Camera_01.mp4
scp <workstation>:~/sim4cams/dataset/_World_Cameras_Camera_02/*.mp4 $DATA/sim-warehouse-4cams/Camera_02.mp4
scp <workstation>:~/sim4cams/dataset/_World_Cameras_Camera_03/*.mp4 $DATA/sim-warehouse-4cams/Camera_03.mp4

# 校正：§4 對得上 → 原封不動沿用原版
CAL=<REPO>/deploy/docker/industry-profiles/warehouse-operations/warehouse-mv3dt-app/calibration/sample-data
cp -r $CAL/nv-warehouse-4cams $CAL/sim-warehouse-4cams
rm -f $CAL/sim-warehouse-4cams/*.backup_*
```

部署時選 `SAMPLE_VIDEO_DATASET=sim-warehouse-4cams`、`NUM_STREAMS=4`。注意
[blueprint_config.yml](deploy/docker/industry-profiles/warehouse-operations/blueprint-configurator/blueprint_config.yml)
對 `nv-warehouse-4cams` 有特別處理（例如 `pre_cluster_threshold: 0.6`），新名稱會用預設值 0.5。
要一致的話，在那裡把 `sim-warehouse-4cams` 加進同一個條件。

接著照 [SOP-warehouse-mv3dt-bev-live.md](SOP-warehouse-mv3dt-bev-live.md) 部署，用 BEV Live 確認軌跡合理。

---

## 6.（選用）場景跟原版不同時：重新產生 calibration.json 與 Top.png

只有在 §4.3 決定改用新場景時才需要。

1. 啟用 `isaacsim.sensors.rtx.calibration`（6.x）或 `isaacsim.sensors.rtx.placement`（5.x）。
2. 建一支**垂直向下**的俯視攝影機，視野要涵蓋 `/World/Cameras` 下全部攝影機和地面工作區。
3. **Tools > Sensors > Camera Calibration**：設定 top-view camera、輸出資料夾等屬性。
4. **Create Dot Prims**：在 `/World/Calibration_Dots/<攝影機名稱>/` 產生校正點；切到每支攝影機確認點都看得到。
5. 產生 `calibration.json`；勾 **Create FOV Polygon Images** 可以順便檢查 FOV。
6. 產生俯視圖和 `imageMetadata.json`，改名放到 `sample-data/<dataset>/images/Top.png`。
7. 用 [tools/rtvi-cv-mv3dt-utils/](tools/rtvi-cv-mv3dt-utils/README.md) 從新的 `calibration.json` 產生 `camInfo/*.yml`。
8. 重新產生 BEV Live 的 `transforms.yml`（見 BEV Live SOP），否則 3D 視圖的攝影機位置會錯。

---

## 7. 下一步（閉環驗證會用到的）

- **製造可重現的事件**：把 `wander` 改成 `patrol` + `path_points`，讓工人路線跟 AMR 路線交叉，
  固定 seed，產生「基準情境」影片，確認 VSS 會報出 proximity incident。
- **套用改善方案**：改 AMR 限速或路徑、加柵欄 prim、移動 danger zone，跑相同 seed 和 N 組不同 seed。
- **同一把尺量**：每一輪都走 §5 → VSS mv3dt → 統計 `mdx-incidents` 事件數，比較改善前後。

## 參考

- Isaac Sim：[Actor Simulation and Synthetic Data Generation](https://docs.isaacsim.omniverse.nvidia.com/latest/action_and_event_data_generation/tutorial_replicator_agent.html)
- Isaac Sim：[IRA Configuration File Guide](https://docs.isaacsim.omniverse.nvidia.com/latest/action_and_event_data_generation/ext_replicator-agent/ext_isaacsim_replicator_agent_configuration.html)、[Sample Configs](https://docs.isaacsim.omniverse.nvidia.com/latest/action_and_event_data_generation/ext_replicator-agent/ext_isaacsim_replicator_agent_sample_configs.html)
- Isaac Sim：[IRA 0.x → 1.x 遷移指南](https://docs.isaacsim.omniverse.nvidia.com/6.2.0/migration_guides/isaac_sim_6_0/ext_isaacsim_replicator_agent_migration_guide.html)
- Isaac Sim：[Camera Calibration](https://docs.isaacsim.omniverse.nvidia.com/6.0.1/action_and_event_data_generation/ext_sensors_rtx_placement/camera_calibration.html)
- VSS：[Simulation-Based Calibration](https://docs.nvidia.com/vss/3.2.1/sdg-calibration.html)
- repo：[tools/sdg-postprocessing/](tools/sdg-postprocessing/README.md)
