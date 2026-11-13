// SPDX-License-Identifier: Apache-2.0
// BEV Live 3D 視圖：把 mdx-bev 世界座標（公尺，Z 朝上）畫成可旋轉的 3D 場景。
// 由 index.html 在第一次切到 3D 時動態載入；共用頁面上的 tracks / cfg / alerts 等全域狀態。
import * as THREE from "three";
import { OrbitControls } from "three/addons/controls/OrbitControls.js";
import { CSS2DRenderer, CSS2DObject } from "three/addons/renderers/CSS2DRenderer.js";
import { mergeGeometries } from "three/addons/utils/BufferGeometryUtils.js";

// 各類別在 bbox3d 沒給尺寸時的預設大小 [寬, 長, 高]（公尺）
const DEFAULT_DIM = {
  Person: [0.5, 0.5, 1.75], Transporter: [1.2, 0.8, 1.0], Forklift: [2.2, 1.2, 2.1],
  Nova_Carter: [0.7, 0.5, 0.45], Agility_Digit_Humanoid: [0.6, 0.5, 1.6], Fourier_GR1_T2_Humanoid: [0.6, 0.5, 1.65],
};
const TRAIL_Z = 0.04;

// T_ov2px 的反矩陣：地圖像素 -> 世界座標
function inv3(m) {
  const [[a, b, c], [d, e, f], [g, h, i]] = m;
  const A = e * i - f * h, B = -(d * i - f * g), C = d * h - e * g, det = a * A + b * B + c * C;
  return [[A / det, -(b * i - c * h) / det, (b * f - c * e) / det],
          [B / det, (a * i - c * g) / det, -(a * f - c * d) / det],
          [C / det, -(a * h - b * g) / det, (a * e - b * d) / det]];
}
const apply = (M, u, v) => { const w = M[2][0] * u + M[2][1] * v + M[2][2];
  return [(M[0][0] * u + M[0][1] * v + M[0][2]) / w, (M[1][0] * u + M[1][1] * v + M[1][2]) / w]; };

export function create(wrap, { cfg, crop, mapUrl, colorOf, groupOf }) {
  const renderer = new THREE.WebGLRenderer({ antialias: true });
  renderer.setPixelRatio(devicePixelRatio || 1);
  renderer.domElement.className = "view3d";
  renderer.domElement.style.removeProperty("display");   // three.js 預設行內 display:block，會蓋掉 2D/3D 切換的 CSS
  wrap.appendChild(renderer.domElement);
  const labels = new CSS2DRenderer();
  labels.domElement.className = "view3d labels3d";
  wrap.appendChild(labels.domElement);

  const scene = new THREE.Scene();
  scene.background = new THREE.Color(0x0e1116);
  scene.add(new THREE.HemisphereLight(0xffffff, 0x30363d, 1.6));
  const sun = new THREE.DirectionalLight(0xffffff, 1.4); sun.position.set(-8, -6, 20); scene.add(sun);

  // ---- 地面：map.png（只取去灰邊後的範圍）依 T 的反矩陣貼到世界座標 ----
  const Tinv = inv3(cfg.T), W = cfg.map.w, H = cfg.map.h;
  const c = crop || { x: 0, y: 0, w: W, h: H };
  const px4 = [[c.x, c.y + c.h], [c.x + c.w, c.y + c.h], [c.x + c.w, c.y], [c.x, c.y]];
  const corners = px4.map(([u, v]) => apply(Tinv, u, v));
  const g = new THREE.BufferGeometry();
  g.setAttribute("position", new THREE.Float32BufferAttribute(corners.flatMap(([x, y]) => [x, y, 0]), 3));
  g.setAttribute("uv", new THREE.Float32BufferAttribute(px4.flatMap(([u, v]) => [u / W, 1 - v / H]), 2));
  g.setIndex([0, 1, 2, 0, 2, 3]); g.computeVertexNormals();
  const tex = new THREE.TextureLoader().load(mapUrl);
  tex.colorSpace = THREE.SRGBColorSpace; tex.anisotropy = renderer.capabilities.getMaxAnisotropy();
  scene.add(new THREE.Mesh(g, new THREE.MeshLambertMaterial({ map: tex, side: THREE.DoubleSide })));

  // 地圖實際內容（去灰邊）中心與大小，當作預設視角的目標
  const [cx, cy] = apply(Tinv, c.x + c.w / 2, c.y + c.h / 2);
  const span = Math.max(Math.abs(c.w / cfg.T[0][0]), Math.abs(c.h / cfg.T[1][1]));

  const camera = new THREE.PerspectiveCamera(45, 1, 0.1, 500);
  camera.up.set(0, 0, 1);
  const controls = new OrbitControls(camera, renderer.domElement);
  controls.enableDamping = true; controls.dampingFactor = 0.12;
  controls.maxPolarAngle = Math.PI / 2 - 0.03; controls.minDistance = 2; controls.maxDistance = span * 3;
  controls.screenSpacePanning = false;   // 平移沿地面
  let viewingFrom = null;   // 從哪台攝影機看：那台自己的桿子與視錐不畫
  const home = () => {
    viewingFrom = null;
    controls.target.set(cx, cy, 0);
    camera.fov = 45; camera.updateProjectionMatrix();
    camera.position.set(cx, cy - span * 0.85, span * 0.75);
    controls.update();
  };
  home();
  renderer.domElement.addEventListener("dblclick", home);

  // ---- 攝影機：用外參算出真實位置與視錐 ----
  const cams = new Map();
  for (const cam of cfg.cameras) {
    if (!cam.ext) continue;
    const R = cam.ext.map(r => r.slice(0, 3)), t = cam.ext.map(r => r[3]);
    const C = [0, 1, 2].map(j => -(R[0][j] * t[0] + R[1][j] * t[1] + R[2][j] * t[2]));
    const K = cam.K, [fw, fh] = cam.wh || [1920, 1080], D = 1.6;
    const ray = (u, v) => { const xc = (u - K[0][2]) / K[0][0], yc = (v - K[1][2]) / K[1][1];
      return [0, 1, 2].map(j => C[j] + D * (R[0][j] * xc + R[1][j] * yc + R[2][j])); };
    const q = [ray(0, 0), ray(fw, 0), ray(fw, fh), ray(0, fh)];
    const pts = [];
    for (let i = 0; i < 4; i++) pts.push(...C, ...q[i], ...q[i], ...q[(i + 1) % 4]);
    const lg = new THREE.BufferGeometry(); lg.setAttribute("position", new THREE.Float32BufferAttribute(pts, 3));
    const mat = new THREE.LineBasicMaterial({ color: 0xffffff, transparent: true, opacity: 0.85 });
    const frustum = new THREE.LineSegments(lg, mat);
    const body = new THREE.Mesh(new THREE.BoxGeometry(0.35, 0.35, 0.35), new THREE.MeshLambertMaterial({ color: 0xe6edf3 }));
    body.position.set(...C);
    const pole = new THREE.Mesh(new THREE.CylinderGeometry(0.03, 0.03, C[2]), new THREE.MeshLambertMaterial({ color: 0x8b949e }));
    pole.rotation.x = Math.PI / 2; pole.position.set(C[0], C[1], C[2] / 2);
    const el = document.createElement("div"); el.className = "lbl cam3d"; el.textContent = cam.name;
    const label = new CSS2DObject(el); label.position.set(C[0], C[1], C[2] + 0.45);
    const grp = new THREE.Group(); grp.add(frustum, body, pole, label); scene.add(grp);
    const fwd = [R[2][0], R[2][1], R[2][2]];
    cams.set(cam.name, { grp, mat, body, el, C, fwd, vfov: 2 * Math.atan(fh / 2 / K[1][1]) * 180 / Math.PI });
  }

  // 切到某台攝影機的視角（與右側畫面同一個角度）
  function lookFrom(name) {
    const k = cams.get(name);
    viewingFrom = k ? name : null;
    if (!k) return home();
    camera.position.set(...k.C);
    controls.target.set(k.C[0] + k.fwd[0] * 6, k.C[1] + k.fwd[1] * 6, k.C[2] + k.fwd[2] * 6);
    camera.fov = k.vfov; camera.updateProjectionMatrix(); controls.update();
  }

  // ---- 物件 ----
  const objs = new Map();   // id -> {mesh, trail, label, el, key}
  const unit = { box: new THREE.BoxGeometry(1, 1, 1), cyl: new THREE.CylinderGeometry(0.5, 0.5, 1, 20) };
  unit.cyl.rotateX(Math.PI / 2);   // 圓柱軸改成 Z
  // 推車：下方 35% 高的平台 + 一側直立把手（單位立方體內，之後依 w/l/h 縮放）
  unit.cart = mergeGeometries([
    new THREE.BoxGeometry(1, 1, 0.35).translate(0, 0, -0.325),
    new THREE.BoxGeometry(0.08, 0.9, 0.65).translate(0.46, 0, 0.175),
  ]);
  // 人 → 圓柱；推車 → 平台＋把手；機器人與其他 → 長方體
  const shapeOf = type => ({ person: unit.cyl, cart: unit.cart }[groupOf(type)] || unit.box);
  function makeObj(id, type) {
    // three.js 只認逗號分隔的 hsl()，頁面用的是 CSS4 空白寫法
    const col = new THREE.Color(colorOf(id).replace(/hsl\((\S+) (\S+) (\S+)\)/, "hsl($1, $2, $3)"));
    const geo = shapeOf(type);
    const mesh = new THREE.Mesh(geo, new THREE.MeshLambertMaterial({ color: col }));
    const edges = new THREE.LineSegments(new THREE.EdgesGeometry(geo, 30),
      new THREE.LineBasicMaterial({ color: 0x0e1116 }));
    mesh.add(edges);
    const trail = new THREE.Line(new THREE.BufferGeometry(), new THREE.LineBasicMaterial({ vertexColors: true }));
    trail.frustumCulled = false;
    const el = document.createElement("div"); el.className = "lbl"; el.style.color = colorOf(id); el.textContent = id;
    const label = new CSS2DObject(el);
    scene.add(mesh, trail, label);
    return { mesh, trail, label, el, col, key: type };
  }
  function drop(id) {
    const o = objs.get(id);
    scene.remove(o.mesh, o.trail, o.label); o.trail.geometry.dispose(); o.mesh.material.dispose();
    o.mesh.children[0].geometry.dispose(); objs.delete(id);
  }

  // ---- 碰撞高亮：紅色脈動光圈 + 連線 ----
  const ringGeo = new THREE.RingGeometry(0.55, 0.75, 40);
  const ringMat = new THREE.MeshBasicMaterial({ color: 0xf85149, transparent: true, side: THREE.DoubleSide, depthWrite: false });
  const rings = [];
  const alertLines = new THREE.LineSegments(new THREE.BufferGeometry(),
    new THREE.LineDashedMaterial({ color: 0xf85149, dashSize: 0.3, gapSize: 0.2 }));
  alertLines.frustumCulled = false; scene.add(alertLines);

  const tmp = new THREE.Vector3(), tmpC = new THREE.Color(), bg = new THREE.Color(0x0e1116);
  function resize() {
    const r = wrap.getBoundingClientRect();
    renderer.setSize(r.width, r.height); labels.setSize(r.width, r.height);
    camera.aspect = r.width / Math.max(1, r.height); camera.updateProjectionMatrix();
  }
  new ResizeObserver(resize).observe(wrap); resize();

  // 每幀由 index.html 的 draw() 呼叫；回傳 heads（含螢幕座標，給滑鼠提示用）
  function render({ tracks, heads, cutoff, trailMs, alerts, hoverId, hoverCam, showIds, showCams }) {
    const live = new Set(heads.map(h => h.id));
    for (const id of objs.keys()) if (!tracks.has(id)) drop(id);
    for (const [id, tr] of tracks) {
      let o = objs.get(id);
      if (!o || o.key !== tr.type) { if (o) drop(id); objs.set(id, o = makeObj(id, tr.type)); }
      const [, x, y] = tr.pts[tr.pts.length - 1];
      // mdx-bev 目前對所有類別都填固定 0.6×0.6×1.6、yaw 0（非實測），已知類別一律用預設尺寸
      const d = tr.dim && tr.dim[0] > 0.05 ? tr.dim : null;
      const [w, l, h] = DEFAULT_DIM[tr.type] || (d ? d : [0.6, 0.6, 1.0]);
      const s = hoverId === id ? 1.2 : 1;
      o.mesh.visible = o.label.visible = live.has(id);
      o.label.visible &&= showIds;
      o.mesh.scale.set(w * s, l * s, h * s);   // 寬沿物件 x、長沿物件 y（NVSchema w,l,h）
      o.mesh.position.set(x, y, h * s / 2);
      o.mesh.rotation.z = d ? d[3] : 0;
      o.label.position.set(x, y, h * s + 0.3);
      // 軌跡：舊→新 由暗到亮
      const n = tr.pts.length, pos = new Float32Array(n * 3), colr = new Float32Array(n * 3);
      for (let i = 0; i < n; i++) {
        const [t, px, py] = tr.pts[i], k = 0.15 + 0.85 * Math.max(0, (t - cutoff) / trailMs);
        pos.set([px, py, TRAIL_Z], i * 3);
        tmpC.copy(bg).lerp(o.col, k); colr.set([tmpC.r, tmpC.g, tmpC.b], i * 3);
      }
      const tg = o.trail.geometry;
      tg.setAttribute("position", new THREE.BufferAttribute(pos, 3));
      tg.setAttribute("color", new THREE.BufferAttribute(colr, 3));
      tg.setDrawRange(0, n);
    }

    for (const [name, k] of cams) {
      k.grp.visible = showCams && viewingFrom !== name;
      const hl = hoverCam === name;
      k.mat.color.set(hl ? 0x76b900 : 0xffffff); k.body.material.color.set(hl ? 0x76b900 : 0xe6edf3);
      k.el.classList.toggle("hl", hl);
    }

    // 碰撞
    const now = performance.now(), pulse = 0.5 + 0.5 * Math.sin(now / 120);
    const pos = new Map(heads.map(h => [h.id, h])), segs = [];
    let ri = 0;
    for (const a of alerts) {
      const p0 = pos.get(a.primary);
      for (const id of a.ids) {
        const p = pos.get(id);
        if (!p) continue;
        if (p0 && id !== a.primary) segs.push(p0.x, p0.y, 0.1, p.x, p.y, 0.1);
        if (!rings[ri]) { rings[ri] = new THREE.Mesh(ringGeo, ringMat); scene.add(rings[ri]); }
        const r = rings[ri++]; r.visible = true; r.position.set(p.x, p.y, 0.06);
        r.scale.setScalar(1 + 0.3 * pulse);
      }
    }
    ringMat.opacity = 0.5 + 0.4 * pulse;
    for (; ri < rings.length; ri++) rings[ri].visible = false;
    alertLines.geometry.setAttribute("position", new THREE.Float32BufferAttribute(segs, 3));
    alertLines.computeLineDistances();

    controls.update();
    renderer.render(scene, camera); labels.render(scene, camera);

    const r = wrap.getBoundingClientRect();
    return heads.map(hd => {
      const o = objs.get(hd.id);
      tmp.set(hd.x, hd.y, o ? o.mesh.scale.z / 2 : 0).project(camera);
      return { ...hd, px: (tmp.x + 1) / 2 * r.width, py: (1 - tmp.y) / 2 * r.height, behind: tmp.z > 1 };
    }).filter(hd => !hd.behind);
  }

  return { render, home, lookFrom, cameras: [...cams.keys()] };
}
