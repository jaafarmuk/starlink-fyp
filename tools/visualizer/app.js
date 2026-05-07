// ===========================================================================
// Starlink MHR Visualizer — app.js v3
// ===========================================================================
// Routing modes:
//   Dijkstra = unconstrained shortest-delay path on fixed ISL topology
//              (matches real-world shortest-path routing, e.g. OSPF)
//
//   BPP Paper = greedy Wang-style hop-by-hop forwarding (arXiv:2303.02286)
//               • Scans ALL nodes within d_th km at each hop (not ISL-only)
//               • Applies c1 (direction ≤ θ_r), c2 (dome ≥ θ_s), c3 (dist ≤ d_th, LoS)
//               • Picks candidate with smallest dome angle to destination
//               • No backtracking — if stuck: packet interrupted
//               • EXACTLY matches analyze_mhr_reliability.py simulate_route()
//
// Delay model:
//   Propagation  = distance_km / 299792.458 km/s
//   Serialization = packet_bytes × 8 / link_rate_bps
//   Queuing (M/D/1) = ρ / (2(1−ρ)) × T_serialization
//   where ρ = offered_load_bps / link_rate_bps
//
// Congestion:
//   Each traversed edge increments state.linkLoad[edgeIdx].
//   Edges are recolored after each packet: blue→cyan→amber→red by load count.
//   Matches "empirical interruption probability" concept from the research.
//
// Multi-epoch:
//   10 topology snapshots (snap_optA, t1–t9) each 9 minutes apart.
//   Switching epoch reloads satellite positions + ISL graph.
//
// ===========================================================================

import * as THREE from 'three';
import { OrbitControls } from 'three/addons/controls/OrbitControls.js';

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const EARTH_RADIUS_KM  = 6378.137;
const C_KM_S           = 299792.458;
const ECEF_SCENE_SCALE = 1.0 / 1000.0;   // 1 scene unit = 1000 km
const SCENE_TO_KM      = 1000.0;
const EARTH_SCENE_R    = EARTH_RADIUS_KM * ECEF_SCENE_SCALE;
const HOP_ANIM_BASE_MS = 1500;            // wall-clock ms per hop at speed 1×
const MAX_GREEDY_HOPS  = 60;              // safety cap for greedy BPP loop

const EARTH_TEXTURE_URL =
  'https://threejs.org/examples/textures/land_ocean_ice_cloud_2048.jpg';

// Epoch file sets: base (t0) + t1…t9, each 9 minutes apart
// (Mar 21 2026 21:30 UTC → 22:51 UTC)
const EPOCH_NODES_URLS = [
  '../../results/snap_optA_nodes.csv',
  ...Array.from({ length: 9 }, (_, i) => `../../results/snap_optA_nodes.t${i + 1}.csv`),
];
const EPOCH_EDGES_URLS = [
  '../../results/snap_optA_edges.csv',
  ...Array.from({ length: 9 }, (_, i) => `../../results/snap_optA_edges.t${i + 1}.csv`),
];
const META_URL = '../../results/snap_optA_meta.json';

const EPOCH_LABELS = [
  'Epoch 0 — 21:30 UTC  (base)', 'Epoch 1 — 21:39 UTC  (+9 min)',
  'Epoch 2 — 21:48 UTC  (+18 min)', 'Epoch 3 — 21:57 UTC  (+27 min)',
  'Epoch 4 — 22:06 UTC  (+36 min)', 'Epoch 5 — 22:15 UTC  (+45 min)',
  'Epoch 6 — 22:24 UTC  (+54 min)', 'Epoch 7 — 22:33 UTC  (+63 min)',
  'Epoch 8 — 22:42 UTC  (+72 min)', 'Epoch 9 — 22:51 UTC  (+81 min)',
];

// ---------------------------------------------------------------------------
// Application state
// ---------------------------------------------------------------------------

const state = {
  // Graph
  nodes: [],        // parsed node objects
  edges: [],        // parsed edge objects
  adj:   [],        // adjacency list (ISL+access edges)

  // Scene objects
  renderEdges: [],  // THREE.Line per edge (background ISL/access)
  nodeMeshes:  [],  // THREE.Mesh per node

  // Selection
  source: null,
  dest:   null,
  pickMode: null,   // 'src' | 'dst' | null

  // Animation
  timeScale: 1,
  epochIdx: 0,

  // Packet animation
  activePacket: null,
  trailEdges: [],

  // BPP parameters (radians / km)
  bppParams: {
    thetaR: 45 * Math.PI / 180,   // c1: direction limit
    thetaS: 15 * Math.PI / 180,   // c2: dome min angle
    dth:    5000,                  // c3: max hop distance (km)
    strategy: 'density',
  },

  routingMode: 'dijkstra',  // 'dijkstra' | 'bpp'

  // Congestion heatmap: edgeIndex → cumulative packet count
  linkLoad: new Map(),

  // Session stats
  packetsSent:      0,
  packetsDelivered: 0,
  packetsLost:      0,
  totalLatencyMs:   0.0,
  bppSent:          0,
  bppDelivered:     0,
  dijkstraSent:     0,
  dijkstraDelivered: 0,

  // Current path
  currentPath:   null,
  currentCosts:  null,
  epoch:         '',

  // Camera fly-to tween
  cameraTween: null,

  // Comparison orchestration
  compareQueue: null,
};

// Precomputed flat position arrays for fast BPP scanning
// (updated every time loadData is called)
let NPX = new Float64Array(0);  // scene X per node
let NPY = new Float64Array(0);  // scene Y per node
let NPZ = new Float64Array(0);  // scene Z per node
let NODE_KIND = [];             // 'gateway' | 'satellite' per node

// ---------------------------------------------------------------------------
// UI element references
// ---------------------------------------------------------------------------

const ui = {
  src:            document.getElementById('src-select'),
  dst:            document.getElementById('dst-select'),
  pickSrc:        document.getElementById('pick-src'),
  pickDst:        document.getElementById('pick-dst'),
  locateSrc:      document.getElementById('locate-src'),
  locateDst:      document.getElementById('locate-dst'),
  randomPair:     document.getElementById('random-pair'),
  compare:        document.getElementById('compare'),
  advHeader:      document.getElementById('adv-header'),
  advToggle:      document.getElementById('adv-toggle'),
  advBody:        document.getElementById('adv-body'),
  packetSize:     document.getElementById('packet-size'),
  linkRate:       document.getElementById('link-rate'),
  offeredLoad:    document.getElementById('offered-load'),
  lossProb:       document.getElementById('loss-probability'),
  launch:         document.getElementById('launch'),
  launchBurst:    document.getElementById('launch-burst'),
  stop:           document.getElementById('stop'),
  timeScale:      document.getElementById('time-scale'),
  timeScaleLabel: document.getElementById('time-scale-label'),
  metaInfo:       document.getElementById('meta-info'),
  pathInfo:       document.getElementById('path-info'),
  metrics:        document.getElementById('metrics'),
  status:         document.getElementById('status'),
  tooltip:        document.getElementById('tooltip'),
  pathOverlay:    document.getElementById('path-overlay'),
  scene:          document.getElementById('scene'),
  fileInput:      document.getElementById('file-input'),
  epochSelect:    document.getElementById('epoch-select'),
  satCountSelect: document.getElementById('sat-count-select'),
  compareSection: document.getElementById('compare-section'),
  compareResults: document.getElementById('compare-results'),
  resetHeatmap:   document.getElementById('reset-heatmap'),
  // BPP
  bppThetaR:      document.getElementById('bpp-theta-r'),
  bppThetaRVal:   document.getElementById('bpp-theta-r-val'),
  bppThetaS:      document.getElementById('bpp-theta-s'),
  bppThetaSVal:   document.getElementById('bpp-theta-s-val'),
  bppDth:         document.getElementById('bpp-dth'),
  bppDthVal:      document.getElementById('bpp-dth-val'),
  bppStrategy:    document.getElementById('bpp-strategy'),
  routeDijkstra:  document.getElementById('route-dijkstra'),
  routeBpp:       document.getElementById('route-bpp'),
};

// ---------------------------------------------------------------------------
// CSV parsing
// ---------------------------------------------------------------------------

function splitCsvLine(line) {
  if (line.indexOf('"') < 0) return line.split(',');
  const out = []; let cur = ''; let inQ = false;
  for (let i = 0; i < line.length; ++i) {
    const c = line[i];
    if (inQ) {
      if (c === '"') { if (i + 1 < line.length && line[i+1] === '"') { cur += '"'; ++i; } else inQ = false; }
      else cur += c;
    } else {
      if (c === ',') { out.push(cur); cur = ''; }
      else if (c === '"' && cur === '') inQ = true;
      else cur += c;
    }
  }
  out.push(cur); return out;
}

function parseCsv(text) {
  const lines = text.split(/\r?\n/).filter(l => l.length > 0);
  if (!lines.length) return [];
  let start = 0;
  if (lines[0].startsWith('schema_version=')) start = 1;
  const header = splitCsvLine(lines[start]);
  const out = [];
  for (let i = start + 1; i < lines.length; ++i) {
    const cells = splitCsvLine(lines[i]);
    const row = {};
    for (let j = 0; j < header.length; ++j) row[header[j]] = cells[j];
    out.push(row);
  }
  return out;
}

async function fetchCsv(url) {
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`failed to load ${url}: ${resp.status}`);
  return parseCsv(await resp.text());
}

async function fetchJson(url) {
  try { const r = await fetch(url); if (!r.ok) return null; return r.json(); }
  catch (_) { return null; }
}

// ---------------------------------------------------------------------------
// Three.js scene
// ---------------------------------------------------------------------------

let scene, camera, renderer, controls;
let earth, earthWire, starfield;
let sunLight, ambientLight;
let srcMarker = null, dstMarker = null;
const raycaster = new THREE.Raycaster();
const pointer   = new THREE.Vector2();

function initScene() {
  scene = new THREE.Scene();
  scene.background = new THREE.Color(0x04070b);
  const w = ui.scene.clientWidth, h = ui.scene.clientHeight;
  camera = new THREE.PerspectiveCamera(45, w / h, 0.1, 5000);
  camera.position.set(0, 0, 25);
  renderer = new THREE.WebGLRenderer({ antialias: true });
  renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2));
  renderer.setSize(w, h);
  ui.scene.appendChild(renderer.domElement);
  controls = new OrbitControls(camera, renderer.domElement);
  controls.enableDamping = true; controls.dampingFactor = 0.08;
  controls.minDistance = 8; controls.maxDistance = 200;
  ambientLight = new THREE.AmbientLight(0x404060, 1.5); scene.add(ambientLight);
  sunLight = new THREE.DirectionalLight(0xffffff, 1.0);
  sunLight.position.set(50, 20, 30); scene.add(sunLight);
  const R = EARTH_SCENE_R;
  buildEarth(R);
  earthWire = new THREE.LineSegments(
    new THREE.WireframeGeometry(new THREE.SphereGeometry(R, 24, 16)),
    new THREE.LineBasicMaterial({ color: 0x1b3650, transparent: true, opacity: 0.25 })
  );
  scene.add(earthWire);
  // Equator ring
  const eq = [];
  for (let i = 0; i <= 128; ++i) {
    const t = (i / 128) * Math.PI * 2;
    eq.push(new THREE.Vector3(R * Math.cos(t), 0, R * Math.sin(t)));
  }
  scene.add(new THREE.Line(
    new THREE.BufferGeometry().setFromPoints(eq),
    new THREE.LineBasicMaterial({ color: 0x2d5c86 })
  ));
  // Starfield
  const sPos = [];
  for (let i = 0; i < 1800; ++i) {
    const r = 600, t = Math.random() * Math.PI * 2, p = Math.acos(2 * Math.random() - 1);
    sPos.push(r * Math.sin(p) * Math.cos(t), r * Math.sin(p) * Math.sin(t), r * Math.cos(p));
  }
  const sg = new THREE.BufferGeometry();
  sg.setAttribute('position', new THREE.Float32BufferAttribute(sPos, 3));
  starfield = new THREE.Points(sg, new THREE.PointsMaterial({ color: 0x446688, size: 1.5, sizeAttenuation: false }));
  scene.add(starfield);
  window.addEventListener('resize', onResize);
  renderer.domElement.addEventListener('pointermove', onPointerMove);
  renderer.domElement.addEventListener('click', onClick);
}

function buildEarth(R) {
  const geo = new THREE.SphereGeometry(R, 64, 48);
  earth = new THREE.Mesh(geo, new THREE.MeshBasicMaterial({ color: 0x0a1a2f, transparent: true, opacity: 0.88 }));
  scene.add(earth);
  new THREE.TextureLoader().load(EARTH_TEXTURE_URL, (tex) => {
    if (earth) { scene.remove(earth); earth.geometry.dispose(); earth.material.dispose(); }
    earth = new THREE.Mesh(geo, new THREE.MeshPhongMaterial({ map: tex, specular: 0x111122, shininess: 8 }));
    scene.add(earth);
    if (earthWire) earthWire.material.opacity = 0.15;
  });
}

function onResize() {
  const w = ui.scene.clientWidth, h = ui.scene.clientHeight;
  camera.aspect = w / h; camera.updateProjectionMatrix(); renderer.setSize(w, h);
}

// ---------------------------------------------------------------------------
// Node position helpers
// ---------------------------------------------------------------------------

// ECEF → scene: x stays, z↔y axis swap, y negated
// (ECEF: +Z=north pole; Three.js sphere texture: +Y=north pole)
function vec3OfNode(n) {
  return new THREE.Vector3(
     n.ecef[0] * ECEF_SCENE_SCALE,
     n.ecef[2] * ECEF_SCENE_SCALE,
    -n.ecef[1] * ECEF_SCENE_SCALE
  );
}

// Precompute flat position arrays for fast O(N) BPP scans
function precomputeNodePositions() {
  const N = state.nodes.length;
  NPX = new Float64Array(N);
  NPY = new Float64Array(N);
  NPZ = new Float64Array(N);
  NODE_KIND = new Array(N);
  for (let i = 0; i < N; i++) {
    const n = state.nodes[i];
    NPX[i] =  n.ecef[0] * ECEF_SCENE_SCALE;
    NPY[i] =  n.ecef[2] * ECEF_SCENE_SCALE;
    NPZ[i] = -n.ecef[1] * ECEF_SCENE_SCALE;
    NODE_KIND[i] = n.kind;
  }
}

// ---------------------------------------------------------------------------
// Geometry utilities (flat arrays, scene units)
// ---------------------------------------------------------------------------

function dist2Flat(ax, ay, az, bx, by, bz) {
  const dx = bx-ax, dy = by-ay, dz = bz-az;
  return dx*dx + dy*dy + dz*dz;
}

// Line-of-sight check: does segment AB avoid passing through Earth?
// Uses scene coordinates where EARTH_SCENE_R is Earth's radius.
function hasLoSFlat(ax, ay, az, bx, by, bz) {
  const dx = bx-ax, dy = by-ay, dz = bz-az;
  const d2 = dx*dx + dy*dy + dz*dz;
  if (d2 < 1e-12) return true;
  const t = Math.max(0, Math.min(1, -(ax*dx + ay*dy + az*dz) / d2));
  const cx = ax + dx*t, cy = ay + dy*t, cz = az + dz*t;
  // Add small tolerance to avoid false blockage on grazing paths
  return (cx*cx + cy*cy + cz*cz) >= (EARTH_SCENE_R * EARTH_SCENE_R * 0.9998);
}

// ---------------------------------------------------------------------------
// Build ISL adjacency list (for Dijkstra)
// ---------------------------------------------------------------------------

function buildGraph() {
  state.adj = Array.from({ length: state.nodes.length }, () => []);
  for (let i = 0; i < state.edges.length; ++i) {
    const e = state.edges[i];
    state.adj[e.u].push({ v: e.v, delayMs: e.delayMs, edgeIndex: i, kind: e.kind });
    state.adj[e.v].push({ v: e.u, delayMs: e.delayMs, edgeIndex: i, kind: e.kind });
  }
}

// ---------------------------------------------------------------------------
// Scene objects (nodes + edges)
// ---------------------------------------------------------------------------

const EDGE_BASE_COLORS = {
  intra_plane: 0x9ad8ff,
  inter_plane: 0xffe08a,
  access:      0xff7171,
  unknown:     0x888888,
};

function edgeColorByLoad(edgeIdx) {
  const load = state.linkLoad.get(edgeIdx) || 0;
  if (load === 0) return null;          // use base color
  if (load <= 3)  return 0x4ad6ff;     // cyan — light use
  if (load <= 15) return 0xffb347;     // amber — moderate
  return 0xff6a6a;                      // red — heavy
}

function addSceneObjects() {
  for (let i = 0; i < state.edges.length; ++i) {
    const e = state.edges[i];
    const a = vec3OfNode(state.nodes[e.u]);
    const b = vec3OfNode(state.nodes[e.v]);
    const geo = new THREE.BufferGeometry().setFromPoints([a, b]);
    const baseCol = EDGE_BASE_COLORS[e.kind] || EDGE_BASE_COLORS.unknown;
    const mat = new THREE.LineBasicMaterial({ color: baseCol, transparent: true, opacity: 0.35 });
    const line = new THREE.Line(geo, mat);
    line.userData.edgeIndex = i;
    line.userData.baseColor = baseCol;
    state.renderEdges.push(line);
    scene.add(line);
  }
  for (const n of state.nodes) {
    const isGw = n.kind === 'gateway';
    const geo  = new THREE.SphereGeometry(isGw ? 0.12 : 0.065, 12, 12);
    const mat  = new THREE.MeshBasicMaterial({ color: isGw ? 0xff4763 : 0x8affc1 });
    const m    = new THREE.Mesh(geo, mat);
    m.position.copy(vec3OfNode(n));
    m.userData.nodeId = n.id;
    state.nodeMeshes.push(m);
    scene.add(m);
  }
}

// Recolor all ISL edges based on accumulated link load
function updateLinkVisuals() {
  for (const line of state.renderEdges) {
    const idx  = line.userData.edgeIndex;
    const load = state.linkLoad.get(idx) || 0;
    const base = line.userData.baseColor;
    if (load === 0) {
      line.material.color.setHex(base);
      line.material.opacity = 0.35;
    } else if (load <= 3) {
      line.material.color.setHex(0x4ad6ff);
      line.material.opacity = 0.55;
    } else if (load <= 15) {
      line.material.color.setHex(0xffb347);
      line.material.opacity = 0.70;
    } else {
      line.material.color.setHex(0xff6a6a);
      line.material.opacity = 0.85;
    }
  }
}

// ---------------------------------------------------------------------------
// Dropdowns
// ---------------------------------------------------------------------------

function populateSelects() {
  const gws  = state.nodes.filter(n => n.kind === 'gateway');
  const sats = state.nodes.filter(n => n.kind !== 'gateway');
  const all  = [...gws, ...sats];
  for (const sel of [ui.src, ui.dst]) {
    sel.innerHTML = '';
    for (const n of all) {
      const opt = document.createElement('option');
      opt.value = String(n.id);
      opt.textContent = `${n.id} — ${n.name} [${n.kind}]`;
      sel.appendChild(opt);
    }
  }
  if (all.length >= 2) {
    setSource(all[0].id);
    setDest(all[all.length - 1].id);
  }
}

// ---------------------------------------------------------------------------
// Source / destination markers
// ---------------------------------------------------------------------------

function makeRingMarker(color) {
  const g = new THREE.Group();
  g.add(new THREE.Mesh(
    new THREE.SphereGeometry(0.18, 16, 16),
    new THREE.MeshBasicMaterial({ color, transparent: true, opacity: 0.9 })
  ));
  const pts = [];
  for (let i = 0; i <= 64; ++i) {
    const t = (i / 64) * Math.PI * 2;
    pts.push(new THREE.Vector3(0.35 * Math.cos(t), 0.35 * Math.sin(t), 0));
  }
  g.add(new THREE.Line(
    new THREE.BufferGeometry().setFromPoints(pts),
    new THREE.LineBasicMaterial({ color, transparent: true, opacity: 0.8 })
  ));
  return g;
}

function setSource(id) {
  state.source = id; ui.src.value = String(id);
  placeMarker('src', id); refreshPath();
}
function setDest(id) {
  state.dest = id; ui.dst.value = String(id);
  placeMarker('dst', id); refreshPath();
}

function placeMarker(role, id) {
  const n = state.nodes[id]; if (!n) return;
  const pos = vec3OfNode(n);
  if (role === 'src') {
    if (!srcMarker) { srcMarker = makeRingMarker(0xffffff); scene.add(srcMarker); }
    srcMarker.position.copy(pos);
  } else {
    if (!dstMarker) { dstMarker = makeRingMarker(0xffd966); scene.add(dstMarker); }
    dstMarker.position.copy(pos);
  }
}

function locateNode(idx, pullback = 9) {
  if (idx == null || !state.nodes[idx]) return;
  const target = vec3OfNode(state.nodes[idx]);
  const radial = target.clone().normalize();
  const camPos = target.clone().add(radial.multiplyScalar(pullback));
  state.cameraTween = {
    fromPos: camera.position.clone(), toPos: camPos,
    fromTarget: controls.target.clone(), toTarget: target.clone(),
    elapsed: 0, duration: 1.1,
  };
}

// ---------------------------------------------------------------------------
// Trail edges (path visualization, per-hop colored lines)
// ---------------------------------------------------------------------------

function clearTrailEdges() {
  for (const l of state.trailEdges) {
    scene.remove(l); l.geometry.dispose(); l.material.dispose();
  }
  state.trailEdges = [];
}

// Build dim trail lines for each hop. edgeIdxPath[i] = -1 means synthetic BPP link.
function buildTrailEdges(path, edgeIdxPath) {
  clearTrailEdges();
  for (let i = 0; i < path.length - 1; ++i) {
    const a = vec3OfNode(state.nodes[path[i]]);
    const b = vec3OfNode(state.nodes[path[i + 1]]);
    const geo = new THREE.BufferGeometry().setFromPoints([a, b]);
    const isSynthetic = edgeIdxPath && edgeIdxPath[i] === -1;
    const mat = new THREE.LineBasicMaterial({
      color: isSynthetic ? 0x7744cc : 0x334466,
      transparent: true,
      opacity: isSynthetic ? 0.55 : 0.70,
      linewidth: 1,
    });
    const line = new THREE.Line(geo, mat);
    line.renderOrder = 1;
    line.userData.synthetic = isSynthetic;
    state.trailEdges.push(line);
    scene.add(line);
  }
}

function markHopCompleted(i) {
  if (i < 0 || i >= state.trailEdges.length) return;
  state.trailEdges[i].material.color.setHex(0x44ff88);
  state.trailEdges[i].material.opacity = 1.0;
}
function markHopFailed(i) {
  if (i < 0 || i >= state.trailEdges.length) return;
  state.trailEdges[i].material.color.setHex(0xff3333);
  state.trailEdges[i].material.opacity = 1.0;
}

// ---------------------------------------------------------------------------
// Glowing packet sphere
// ---------------------------------------------------------------------------

function makePacketGroup(color) {
  const g = new THREE.Group();
  g.add(new THREE.Mesh(new THREE.SphereGeometry(0.10, 14, 14),
    new THREE.MeshBasicMaterial({ color })));
  const glow = new THREE.Mesh(new THREE.SphereGeometry(0.22, 14, 14),
    new THREE.MeshBasicMaterial({ color, transparent: true, opacity: 0.22,
      blending: THREE.AdditiveBlending, depthWrite: false }));
  glow.userData.isGlow = true;
  g.add(glow);
  g.userData.color = color;
  return g;
}

function setPacketColor(g, color) {
  for (const c of g.children) c.material.color.setHex(color);
  g.userData.color = color;
}

function removePacket() {
  if (!state.activePacket) return;
  scene.remove(state.activePacket);
  for (const c of state.activePacket.children) { c.geometry.dispose(); c.material.dispose(); }
  state.activePacket = null;
}

// ---------------------------------------------------------------------------
// Dijkstra (unconstrained shortest-delay on ISL graph)
// ---------------------------------------------------------------------------

function dijkstra(src, dst) {
  const N = state.nodes.length;
  const dist     = new Float64Array(N); dist.fill(Infinity);
  const prev     = new Int32Array(N);   prev.fill(-1);
  const prevEdge = new Int32Array(N);   prevEdge.fill(-1);
  dist[src] = 0;

  const heap = [];
  const push = (d, u) => {
    heap.push([d, u]);
    let i = heap.length - 1;
    while (i > 0) {
      const p = (i - 1) >> 1;
      if (heap[p][0] <= heap[i][0]) break;
      [heap[p], heap[i]] = [heap[i], heap[p]]; i = p;
    }
  };
  const pop = () => {
    const top = heap[0], last = heap.pop();
    if (heap.length > 0) {
      heap[0] = last; let i = 0;
      for (;;) {
        const l = 2*i+1, r = 2*i+2; let m = i;
        if (l < heap.length && heap[l][0] < heap[m][0]) m = l;
        if (r < heap.length && heap[r][0] < heap[m][0]) m = r;
        if (m === i) break;
        [heap[m], heap[i]] = [heap[i], heap[m]]; i = m;
      }
    }
    return top;
  };

  push(0, src);
  while (heap.length) {
    const [d, u] = pop();
    if (d > dist[u]) continue;
    if (u === dst) break;
    for (const { v, delayMs, edgeIndex } of state.adj[u]) {
      const nd = d + delayMs;
      if (nd < dist[v]) { dist[v] = nd; prev[v] = u; prevEdge[v] = edgeIndex; push(nd, v); }
    }
  }
  if (!isFinite(dist[dst])) return null;

  const path = [], edgeIdxPath = [];
  let u = dst;
  while (u !== -1) {
    path.unshift(u);
    if (prevEdge[u] !== -1) edgeIdxPath.unshift(prevEdge[u]);
    u = prev[u];
  }
  return { path, edgeIdxPath, totalPropMs: dist[dst] };
}

// ---------------------------------------------------------------------------
// BPP Paper routing — greedy Wang-style (matches analyze_mhr_reliability.py)
// ---------------------------------------------------------------------------
//
// At each hop from node `cur`:
//   1. Check if destination is directly reachable (dist ≤ d_th AND line-of-sight).
//      If yes: direct hop, done.
//   2. Scan ALL nodes (not just ISL neighbours) within d_th km.
//      Apply c3: dist ≤ d_th AND LoS.
//      Apply c1: direction angle ≤ θ_r  (skip for gateway hops).
//      Apply c2: dome angle ≥ θ_s       (skip for gateway hops).
//   3. Among valid candidates: pick the one with smallest dome angle to destination
//      (= most geometric progress). This is the paper's "density" strategy selection.
//   4. If no valid candidate: packet INTERRUPTED.
//
// Returns edgeIdxPath[i] = -1 for "synthetic" BPP links that are not pre-built ISLs.
// These are drawn as purple lines and cost = distance / c.

function bppGreedyRoute(srcIdx, dstIdx) {
  const bp  = state.bppParams;
  const dth = bp.dth * ECEF_SCENE_SCALE;

  const path        = [srcIdx];
  const edgeIdxPath = [];
  const hopStats    = [];   // per-hop explanation data
  const visited     = new Set([srcIdx]);
  let totalPropMs   = 0;

  let cur = srcIdx;

  for (let hop = 1; hop <= MAX_GREEDY_HOPS; hop++) {
    const ax = NPX[cur], ay = NPY[cur], az = NPZ[cur];
    const dx = NPX[dstIdx], dy = NPY[dstIdx], dz = NPZ[dstIdx];

    // 1. Direct hop to destination?
    const ddx = dx-ax, ddy = dy-ay, ddz = dz-az;
    const distToDst = Math.sqrt(ddx*ddx + ddy*ddy + ddz*ddz);

    if (distToDst <= dth && hasLoSFlat(ax, ay, az, dx, dy, dz)) {
      path.push(dstIdx);
      const distKm = distToDst * SCENE_TO_KM;
      totalPropMs += distKm / C_KM_S * 1000;
      const eIdx = findEdgeIndex(cur, dstIdx);
      edgeIdxPath.push(eIdx);
      hopStats.push({ hop, directReach: true, distKm,
        chosen: { nodeIdx: dstIdx, name: state.nodes[dstIdx]?.name || 'destination',
                  distKm, domeToDestDeg: 0 },
        nSatInRange: 0, nFailLoS: 0, nFailC1: 0, nFailC2: 0, interrupted: false });
      return { path, edgeIdxPath, hopStats, reached: true, totalPropMs, interruptedHop: -1 };
    }

    // 2. Scan all nodes — find best candidate and collect rejection stats.
    let bestV = -1, bestDome = Infinity, bestDist = 0;
    let nSatInRange = 0, nFailLoS = 0, nFailC1 = 0, nFailC2 = 0;

    const toDestLen = Math.sqrt(ddx*ddx + ddy*ddy + ddz*ddz);
    const tdx = ddx / toDestLen, tdy = ddy / toDestLen, tdz = ddz / toDestLen;
    const curNorm = Math.sqrt(ax*ax + ay*ay + az*az);
    const curIsGw = NODE_KIND[cur] === 'gateway';

    for (let v = 0; v < state.nodes.length; v++) {
      if (v === cur || visited.has(v)) continue;

      const bx = NPX[v], by = NPY[v], bz = NPZ[v];
      const vdx = bx-ax, vdy = by-ay, vdz = bz-az;
      const dist = Math.sqrt(vdx*vdx + vdy*vdy + vdz*vdz);
      if (dist > dth) continue;

      const vIsGw = NODE_KIND[v] === 'gateway';
      const relax = curIsGw || vIsGw;

      if (!relax) nSatInRange++;   // count satellite-to-satellite candidates in range

      // c3: LoS
      if (!hasLoSFlat(ax, ay, az, bx, by, bz)) { if (!relax) nFailLoS++; continue; }

      if (!relax) {
        // c1: direction angle ≤ θ_r
        const vdxN = vdx/dist, vdyN = vdy/dist, vdzN = vdz/dist;
        const dotC1 = Math.max(-1, Math.min(1, tdx*vdxN + tdy*vdyN + tdz*vdzN));
        if (Math.acos(dotC1) > bp.thetaR) { nFailC1++; continue; }

        // c2: geocentric dome angle ≥ θ_s
        const vNorm = Math.sqrt(bx*bx + by*by + bz*bz);
        if (vNorm < 1e-9 || curNorm < 1e-9) { nFailC2++; continue; }
        const dotC2 = Math.max(-1, Math.min(1, (ax*bx + ay*by + az*bz) / (curNorm * vNorm)));
        if (Math.acos(dotC2) < bp.thetaS) { nFailC2++; continue; }
      }

      // Selection metric: geocentric angle between candidate and destination
      const vNorm2 = Math.sqrt(bx*bx + by*by + bz*bz);
      const dstNorm = Math.sqrt(dx*dx + dy*dy + dz*dz);
      let dome = Math.PI / 2;
      if (vNorm2 > 1e-9 && dstNorm > 1e-9)
        dome = Math.acos(Math.max(-1, Math.min(1, (bx*dx + by*dy + bz*dz) / (vNorm2 * dstNorm))));

      if (dome < bestDome) { bestDome = dome; bestV = v; bestDist = dist; }
    }

    if (bestV === -1) {
      hopStats.push({ hop, nSatInRange, nFailLoS, nFailC1, nFailC2,
                      chosen: null, interrupted: true });
      return { path, edgeIdxPath, hopStats, reached: false, totalPropMs,
               interruptedHop: hop, reason: 'no_forward_candidate' };
    }

    hopStats.push({ hop, nSatInRange, nFailLoS, nFailC1, nFailC2, interrupted: false,
      chosen: { nodeIdx: bestV, name: state.nodes[bestV]?.name || '?',
                distKm: bestDist * SCENE_TO_KM,
                domeToDestDeg: +(bestDome * 180 / Math.PI).toFixed(1) } });

    path.push(bestV);
    const distKm = bestDist * SCENE_TO_KM;
    totalPropMs += distKm / C_KM_S * 1000;
    edgeIdxPath.push(findEdgeIndex(cur, bestV));
    visited.add(bestV);
    cur = bestV;
  }

  return {
    path, edgeIdxPath, hopStats, reached: false, totalPropMs,
    interruptedHop: MAX_GREEDY_HOPS, reason: 'max_hops',
  };
}

// Find the ISL edge index between two nodes in the adjacency list (-1 if none)
function findEdgeIndex(u, v) {
  for (const nb of state.adj[u]) if (nb.v === v) return nb.edgeIndex;
  return -1;
}

// ---------------------------------------------------------------------------
// Traffic / delay model
// ---------------------------------------------------------------------------

function readParams() {
  return {
    packetBytes: Math.max(1, Number(ui.packetSize.value)),
    islMbps:     Math.max(0.001, Number(ui.linkRate.value)),
    lossProb:    Math.max(0, Math.min(1, Number(ui.lossProb.value))),
    offeredMbps: Math.max(0, Number(ui.offeredLoad.value) || 0),
  };
}

// M/D/1 queuing: W_q = ρ / (2(1−ρ)) × T_s
function md1QueueMs(rho, serMs) {
  if (rho <= 0)    return 0.0;
  if (rho >= 0.99) return serMs * 50;  // heavily saturated cap
  return (rho / (2 * (1 - rho))) * serMs;
}

// Compute per-hop cost breakdown. Handles edgeIdx = -1 (synthetic BPP link).
function hopCostsForPath(path, edgeIdxPath, params) {
  const rows = [];
  let totMs = 0, totProp = 0, totSer = 0, totQueue = 0;
  const rateBps    = params.islMbps * 1e6;
  const offeredBps = params.offeredMbps * 1e6;
  const rho        = Math.min(0.999, offeredBps > 0 ? offeredBps / rateBps : 0);

  for (let i = 0; i < edgeIdxPath.length; ++i) {
    const eIdx = edgeIdxPath[i];
    let distanceKm, propMs, edgeKind;

    if (eIdx !== -1 && eIdx < state.edges.length) {
      // Known ISL/access edge
      const e  = state.edges[eIdx];
      distanceKm = e.distanceKm;
      propMs     = e.delayMs;
      edgeKind   = e.kind;
    } else {
      // Synthetic BPP link (not a pre-built ISL)
      const ax = NPX[path[i]],   ay = NPY[path[i]],   az = NPZ[path[i]];
      const bx = NPX[path[i+1]], by = NPY[path[i+1]], bz = NPZ[path[i+1]];
      const d  = Math.sqrt((bx-ax)*(bx-ax) + (by-ay)*(by-ay) + (bz-az)*(bz-az));
      distanceKm = d * SCENE_TO_KM;
      propMs     = distanceKm / C_KM_S * 1000;
      edgeKind   = 'bpp_synthetic';
    }

    const serMs   = (params.packetBytes * 8.0) / rateBps * 1000.0;
    const queueMs = md1QueueMs(rho, serMs);
    const hopMs   = propMs + serMs + queueMs;

    totMs    += hopMs;
    totProp  += propMs;
    totSer   += serMs;
    totQueue += queueMs;

    rows.push({
      hop: i + 1,
      u: path[i], v: path[i + 1],
      edgeKind, distanceKm, propMs, serMs, queueMs, hopMs, rho, eIdx,
    });
  }

  const rhoMax = rho;
  return { rows, totalMs: totMs, totalPropMs: totProp, totalSerMs: totSer,
           totalQueueMs: totQueue, rho, rhoMax, offeredBps, rateBps };
}

// ---------------------------------------------------------------------------
// Path info rendering
// ---------------------------------------------------------------------------

function kv(label, value, cls = '') {
  return `<div class="kv${cls ? ' ' + cls : ''}"><span>${label}</span><span>${value}</span></div>`;
}

function tagHtml(kind) {
  if (kind === 'intra_plane' || kind === 'inter_plane')
    return `<span class="tag tag-isl">ISL</span>`;
  if (kind === 'access')
    return `<span class="tag tag-acc">GW</span>`;
  if (kind === 'bpp_synthetic')
    return `<span class="tag tag-syn">BPP</span>`;
  return '';
}

function buildRouteExplanation(result, meta) {
  const { mode, reached } = meta;
  const hopStats = result.hopStats || [];

  let out = `<div style="margin-top:10px;border-top:1px solid var(--border);padding-top:8px">
    <div style="font-size:10px;text-transform:uppercase;letter-spacing:1px;
                color:var(--ink-dim);margin-bottom:6px;font-weight:600">Why this route?</div>`;

  if (mode !== 'bpp') {
    // Dijkstra explanation
    out += `<div class="muted" style="font-size:10.5px;line-height:1.5">
      <b style="color:var(--ink)">Algorithm:</b> Dijkstra shortest-path on the prebuilt
      inter-satellite link graph. At each hop the next node is whichever neighbour leads
      to the minimum total propagation + serialization + queuing delay to the destination.
      Unlike BPP greedy routing, Dijkstra has full global knowledge of the graph and never
      gets stuck — it always finds a path if one exists.
    </div>`;
    out += `</div>`;
    return out;
  }

  // BPP explanation
  const thetaRDeg = (state.bppParams.thetaR * 180 / Math.PI).toFixed(0);
  const thetaSDeg = (state.bppParams.thetaS * 180 / Math.PI).toFixed(0);
  const dthKm     = state.bppParams.dth;

  out += `<div class="muted" style="font-size:10.5px;line-height:1.5;margin-bottom:6px">
    <b style="color:var(--ink)">Algorithm:</b> Wang-style greedy routing. At every hop,
    all nodes within ${dthKm} km are scanned. Three constraints filter candidates:
    <b style="color:var(--accent)">c1</b> direction within ${thetaRDeg}°,
    <b style="color:var(--accent)">c2</b> geocentric dome angle ≥ ${thetaSDeg}°,
    <b style="color:var(--accent)">c3</b> line-of-sight (Earth not blocking).
    The candidate with the smallest geocentric angle to the destination is chosen
    (most geometric progress). No backtracking — if zero candidates survive: interrupted.
  </div>`;

  for (const s of hopStats) {
    const nodeLabel = s.chosen ? s.chosen.name : '—';
    const shortLabel = nodeLabel.length > 20 ? nodeLabel.slice(0,18)+'…' : nodeLabel;
    const nPassed = s.nSatInRange - s.nFailLoS - s.nFailC1 - s.nFailC2;

    if (s.directReach) {
      out += `<div style="font-size:10px;padding:4px 0;border-bottom:1px dashed var(--border)">
        <span style="color:var(--accent-2)">Hop ${s.hop}</span>
        <span style="color:var(--ink)"> → ${shortLabel}</span>
        <span style="color:var(--ink-dim)"> (${s.chosen.distKm.toFixed(0)} km)</span>
        <div class="muted" style="font-size:9.5px;margin-top:2px">
          Destination within ${dthKm} km with clear line of sight — direct reach.
        </div>
      </div>`;
      continue;
    }

    if (s.interrupted) {
      // Determine which constraint was the binding one
      const afterLoS = s.nSatInRange - s.nFailLoS;
      const afterC1  = afterLoS - s.nFailC1;
      let bindingMsg;
      if (s.nSatInRange === 0) {
        bindingMsg = `<span style="color:var(--err)">No satellite candidates within ${dthKm} km at this position.</span>`;
      } else if (afterLoS === 0) {
        bindingMsg = `<span style="color:var(--err)">All ${s.nFailLoS} candidates blocked by Earth (c3 line-of-sight). </span>
          <span class="muted">Earth's curvature blocks every visible satellite from this angle.</span>`;
      } else if (afterC1 === 0) {
        bindingMsg = `<span style="color:var(--err)">All ${s.nFailC1} remaining candidates outside the ${thetaRDeg}° direction cone (c1). </span>
          <span class="muted">This is a Walker-delta orbital plane gap — no satellite lies in the required direction.</span>`;
      } else {
        bindingMsg = `<span style="color:var(--err)">${s.nFailC2} candidates failed dome-angle threshold ${thetaSDeg}° (c2). </span>
          <span class="muted">Candidates are too geometrically close to make meaningful progress.</span>`;
      }
      out += `<div style="font-size:10px;padding:4px 0;border-bottom:1px dashed var(--border)">
        <span style="color:var(--err)">✗ Hop ${s.hop} — STUCK</span>
        <div style="font-size:9.5px;margin-top:3px;line-height:1.5">
          Scanned <b>${s.nSatInRange}</b> satellite candidates in range.<br>
          ${s.nFailLoS > 0 ? `<span style="color:var(--warn)">${s.nFailLoS} blocked by Earth (c3).</span> ${afterLoS} remained.<br>` : ''}
          ${s.nFailC1 > 0  ? `<span style="color:var(--warn)">${s.nFailC1} outside direction cone (c1).</span> ${afterC1} remained.<br>` : ''}
          ${s.nFailC2 > 0  ? `<span style="color:var(--warn)">${s.nFailC2} failed dome angle (c2).</span> 0 remained.<br>` : ''}
          ${bindingMsg}
        </div>
      </div>`;
    } else {
      out += `<div style="font-size:10px;padding:4px 0;border-bottom:1px dashed var(--border)">
        <span style="color:var(--accent-2)">Hop ${s.hop}</span>
        <span style="color:var(--ink)"> → ${shortLabel}</span>
        <span style="color:var(--ink-dim)"> (${s.chosen.distKm.toFixed(0)} km)</span>
        <div style="font-size:9.5px;color:var(--ink-dim);margin-top:2px;line-height:1.5">
          Scanned <b style="color:var(--ink)">${s.nSatInRange}</b> sat. candidates.
          ${s.nFailLoS ? `<span style="color:var(--warn)">${s.nFailLoS} blocked LoS.</span>` : ''}
          ${s.nFailC1  ? `<span style="color:var(--warn)">${s.nFailC1} failed c1.</span>` : ''}
          ${s.nFailC2  ? `<span style="color:var(--warn)">${s.nFailC2} failed c2.</span>` : ''}
          <b style="color:var(--ink)">${nPassed > 0 ? nPassed : '?'}</b> valid candidates.
          Chose this node — geocentric angle to destination
          <b style="color:var(--accent)">${s.chosen.domeToDestDeg}°</b>
          (smallest = most progress).
        </div>
      </div>`;
    }
  }

  out += `</div>`;
  return out;
}

function renderPathInfo(result, costs, meta) {
  // meta = { mode, reached, interruptedHop, reason }
  const { mode, reached, interruptedHop, reason } = meta;
  const hops = result.path.length - 1;

  const modeLabel = mode === 'bpp' ? 'BPP Paper routing' : 'Dijkstra routing';
  const statusHtml = reached
    ? `<span style="color:var(--accent-2)">&#10003; Delivered</span>`
    : `<span style="color:var(--err)">&#10007; Interrupted at hop ${interruptedHop}</span>`;

  let html = `
    <div style="font-weight:600;margin-bottom:6px">${modeLabel} &mdash; ${statusHtml}</div>
  `;

  if (reached) {
    const congClass = costs.rho > 0.8 ? 'err-row' : costs.rho > 0.4 ? 'warn-row' : '';
    html += `
      ${kv('Hops', hops)}
      ${kv('Propagation (one-way)', costs.totalPropMs.toFixed(2) + ' ms')}
      ${kv('Serialization', costs.totalSerMs.toFixed(3) + ' ms')}
      ${kv('Queuing (M/D/1)', costs.totalQueueMs.toFixed(3) + ' ms', congClass)}
      ${kv('Link utilization ρ', costs.rho.toFixed(3), congClass)}
      ${kv('E2E estimate', '<b>' + costs.totalMs.toFixed(2) + ' ms</b>')}
    `;
    if (costs.rho > 0.8) {
      html += `<div class="banner warn" style="margin-top:4px;margin-bottom:0">
        Congested: ρ = ${costs.rho.toFixed(2)} — queuing delay is high</div>`;
    }
  } else {
    html += `
      ${kv('Hops traversed', hops)}
      ${kv('Partial propagation', costs ? costs.totalPropMs.toFixed(2) + ' ms' : '—')}
      ${kv('Interruption reason', reason || 'unknown')}
      ${kv('θ<sub>r</sub> (c1)', (state.bppParams.thetaR * 180 / Math.PI).toFixed(0) + '°')}
      ${kv('θ<sub>s</sub> (c2)', (state.bppParams.thetaS * 180 / Math.PI).toFixed(0) + '°')}
      ${kv('d<sub>th</sub> (c3)', state.bppParams.dth + ' km')}
    `;
  }

  // Per-hop table
  if (costs && costs.rows.length > 0) {
    html += `<div style="margin-top:7px;font-size:10px;color:var(--ink-dim)">Per-hop breakdown</div>
    <div class="hop-table">
      <div class="hop-row hdr">
        <span>#</span><span>Node</span><span class="h-km">km</span>
        <span class="h-ms">ms</span><span class="h-rho">&rho;</span>
      </div>`;

    for (const r of costs.rows) {
      const nodeName = state.nodes[r.v] ? state.nodes[r.v].name : '?';
      const shortName = nodeName.length > 18 ? nodeName.slice(0, 16) + '…' : nodeName;
      const congCls = r.rho > 0.8 ? 'hicong' : r.rho > 0.4 ? 'cong' : '';
      const propTotal = (r.propMs + r.serMs + r.queueMs).toFixed(1);
      html += `
        <div class="hop-row ${congCls}" title="${nodeName}">
          <span class="h-idx">${r.hop}</span>
          <span class="h-name" style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap">
            ${shortName}${tagHtml(r.edgeKind)}
          </span>
          <span class="h-km">${r.distanceKm.toFixed(0)}</span>
          <span class="h-ms">${propTotal}</span>
          <span class="h-rho">${r.rho > 0 ? r.rho.toFixed(2) : '—'}</span>
        </div>`;
    }
    html += '</div>';
  }

  // ── Why this route? explanation ───────────────────────────────────
  html += buildRouteExplanation(result, meta);

  ui.pathInfo.innerHTML = html;
}

function refreshPath() {
  if (state.source == null || state.dest == null) return;
  let r, meta;

  if (state.routingMode === 'bpp') {
    r = bppGreedyRoute(state.source, state.dest);
    meta = { mode: 'bpp', reached: r.reached,
             interruptedHop: r.interruptedHop, reason: r.reason };
  } else {
    r = dijkstra(state.source, state.dest);
    if (!r) {
      ui.pathInfo.innerHTML = '<span style="color:var(--err)">No path between source and destination</span>';
      clearTrailEdges(); return;
    }
    meta = { mode: 'dijkstra', reached: true, interruptedHop: -1 };
  }

  state.currentPath  = r;
  const params = readParams();
  state.currentCosts = hopCostsForPath(r.path, r.edgeIdxPath, params);
  buildTrailEdges(r.path, r.edgeIdxPath);
  renderPathInfo(r, state.currentCosts, meta);
}

// ---------------------------------------------------------------------------
// Packet animation state
// ---------------------------------------------------------------------------

let activePacketAnim = null;

function clearActivePacket() {
  removePacket(); activePacketAnim = null;
  ui.pathOverlay.textContent = '';
}

// ---------------------------------------------------------------------------
// Launch packet
// ---------------------------------------------------------------------------

function launchPacket() {
  if (state.source == null || state.dest == null) {
    ui.status.textContent = 'pick source and destination first'; return;
  }

  let routePath, routeEdgeIdx, routeMeta, costs;

  if (state.routingMode === 'bpp') {
    // BPP paper greedy routing (matches research script exactly)
    const bpp = bppGreedyRoute(state.source, state.dest);
    routePath    = bpp.path;
    routeEdgeIdx = bpp.edgeIdxPath;
    routeMeta    = { mode: 'bpp', reached: bpp.reached,
                     interruptedHop: bpp.interruptedHop, reason: bpp.reason };

    const params = readParams();
    costs = hopCostsForPath(bpp.path, bpp.edgeIdxPath, params);

    if (!bpp.reached) {
      // Interrupted before reaching destination
      buildTrailEdges(bpp.path, bpp.edgeIdxPath);
      renderPathInfo(bpp, costs, routeMeta);
      ui.status.textContent = `BPP interrupted at hop ${bpp.interruptedHop}: ${bpp.reason}`;
      ui.pathOverlay.innerHTML =
        `<span style="color:#ff3333">&#10007; BPP interrupted at hop ${bpp.interruptedHop} — ${bpp.reason}</span>`;
      state.packetsSent  += 1; state.packetsLost += 1;
      state.bppSent      += 1;
      updateAggregate();
      setTimeout(() => { ui.pathOverlay.textContent = ''; }, 4000);
      onPacketEnd({ delivered: false, mode: 'bpp', hops: bpp.path.length - 1,
                    propMs: bpp.totalPropMs, serMs: 0, queueMs: 0, totalMs: 0,
                    rho: 0, reason: bpp.reason, interruptedHop: bpp.interruptedHop, costs });
      return;
    }
  } else {
    // Dijkstra
    const r = dijkstra(state.source, state.dest);
    if (!r) {
      ui.status.textContent = 'no path found';
      ui.pathOverlay.innerHTML = `<span style="color:#ff3333">&#10007; No ISL connectivity</span>`;
      state.packetsSent += 1; state.packetsLost += 1;
      state.dijkstraSent += 1;
      updateAggregate();
      setTimeout(() => { ui.pathOverlay.textContent = ''; }, 3000);
      onPacketEnd({ delivered: false, mode: 'dijkstra', hops: 0, propMs: 0,
                    serMs: 0, queueMs: 0, totalMs: 0, rho: 0, reason: 'no_connectivity' });
      return;
    }
    routePath    = r.path;
    routeEdgeIdx = r.edgeIdxPath;
    routeMeta    = { mode: 'dijkstra', reached: true, interruptedHop: -1 };
    const params = readParams();
    costs = hopCostsForPath(r.path, r.edgeIdxPath, params);
  }

  // Build scene objects
  clearActivePacket();
  buildTrailEdges(routePath, routeEdgeIdx);
  renderPathInfo({ path: routePath, edgeIdxPath: routeEdgeIdx }, costs, routeMeta);

  const color = state.routingMode === 'bpp' ? 0xff77dd : 0x4ad6ff;
  const pktGrp = makePacketGroup(color);
  pktGrp.position.copy(vec3OfNode(state.nodes[routePath[0]]));
  scene.add(pktGrp);
  state.activePacket = pktGrp;

  // Per-link Bernoulli loss (Dijkstra mode only — BPP uses geometric blocking)
  const params = readParams();
  let dropIndex = -1;
  if (state.routingMode !== 'bpp') {
    for (let i = 0; i < routePath.length - 1; ++i) {
      if (Math.random() < params.lossProb) { dropIndex = i; break; }
    }
  }

  activePacketAnim = {
    path: routePath, edgeIdxPath: routeEdgeIdx, costs,
    meta: routeMeta, hopIndex: 0, hopProgress: 0.0,
    dropIndex, startTime: performance.now(), totalMs: costs.totalMs,
    allCosts: costs,
  };

  state.packetsSent += 1;
  if (state.routingMode === 'bpp') state.bppSent += 1;
  else                             state.dijkstraSent += 1;
  updateAggregate();
}

// ---------------------------------------------------------------------------
// Burst mode
// ---------------------------------------------------------------------------

function launchBurst(n) {
  n = n || 20;
  if (state.source == null || state.dest == null) return;
  const r = dijkstra(state.source, state.dest);
  if (!r) return;
  buildTrailEdges(r.path, r.edgeIdxPath);
  const params = readParams();
  const costs  = hopCostsForPath(r.path, r.edgeIdxPath, params);
  const hops   = r.path.length - 1;

  for (let k = 0; k < n; ++k) {
    let drop = -1;
    for (let i = 0; i < hops; ++i)
      if (Math.random() < params.lossProb) { drop = i; break; }
    const delivered = drop === -1;
    state.packetsSent    += 1;
    state.dijkstraSent   += 1;
    if (delivered) {
      state.packetsDelivered   += 1;
      state.dijkstraDelivered  += 1;
      state.totalLatencyMs     += costs.totalMs;
      // Track link load
      for (const eIdx of r.edgeIdxPath) {
        if (eIdx >= 0) state.linkLoad.set(eIdx, (state.linkLoad.get(eIdx) || 0) + 1);
      }
    } else {
      state.packetsLost += 1;
    }
    // Flash hop edges
    for (let i = 0; i < hops; ++i) {
      const col = (!delivered && i >= drop) ? 0xff3333 : 0x44ff88;
      setTimeout(() => {
        if (i < state.trailEdges.length) {
          state.trailEdges[i].material.color.setHex(col);
          state.trailEdges[i].material.opacity = 1.0;
          setTimeout(() => {
            if (i < state.trailEdges.length) {
              state.trailEdges[i].material.color.setHex(0x334466);
              state.trailEdges[i].material.opacity = 0.7;
            }
          }, 400);
        }
      }, i * 60 + k * 80);
    }
  }
  updateAggregate();
  updateLinkVisuals();
}

// ---------------------------------------------------------------------------
// Packet animation step (called every frame)
// ---------------------------------------------------------------------------

function stepActivePacket(dtMs) {
  if (!activePacketAnim) return;
  const p = activePacketAnim;
  if (!state.activePacket) { activePacketAnim = null; return; }

  const totalHops = p.path.length - 1;
  const hop = p.costs.rows[p.hopIndex];

  // ── All hops done ──────────────────────────────────────────────────────────
  if (!hop) {
    markHopCompleted(p.hopIndex - 1);
    setPacketColor(state.activePacket, 0x44ff88);
    const mode = p.meta.mode === 'bpp' ? ' · BPP' : ' · Dijkstra';
    ui.pathOverlay.innerHTML =
      `<span style="color:#44ff88">&#10003; Delivered &middot; ${p.costs.totalPropMs.toFixed(1)} ms prop` +
      ` &middot; ${p.costs.totalMs.toFixed(1)} ms E2E &middot; ${totalHops} hops${mode}</span>`;

    // Record link load
    for (const eIdx of p.edgeIdxPath)
      if (eIdx >= 0) state.linkLoad.set(eIdx, (state.linkLoad.get(eIdx) || 0) + 1);
    updateLinkVisuals();

    state.packetsDelivered += 1;
    if (p.meta.mode === 'bpp') state.bppDelivered   += 1;
    else                       state.dijkstraDelivered += 1;
    state.totalLatencyMs += p.totalMs;
    activePacketAnim = null;
    updateAggregate();
    setTimeout(() => clearActivePacket(), 700);

    onPacketEnd({
      delivered: true, mode: p.meta.mode, hops: totalHops,
      propMs: p.costs.totalPropMs, serMs: p.costs.totalSerMs,
      queueMs: p.costs.totalQueueMs, totalMs: p.costs.totalMs,
      rho: p.costs.rho, reason: 'delivered', interruptedHop: -1, costs: p.costs,
    });
    return;
  }

  // ── Advance progress ───────────────────────────────────────────────────────
  const hopAnimMs = Math.max(1, HOP_ANIM_BASE_MS / state.timeScale);
  p.hopProgress += dtMs / hopAnimMs;

  const from = vec3OfNode(state.nodes[p.path[p.hopIndex]]);
  const to   = vec3OfNode(state.nodes[p.path[p.hopIndex + 1]]);
  state.activePacket.position.copy(from.clone().lerp(to, Math.min(p.hopProgress, 1.0)));

  // Pulse glow
  for (const child of state.activePacket.children) {
    if (child.userData.isGlow)
      child.scale.setScalar(1.0 + 0.25 * Math.sin(performance.now() * 0.006));
  }

  // In-flight overlay
  const modeTag = p.meta.mode === 'bpp' ? ' · BPP' : ' · Dijkstra';
  const rhoStr  = hop.rho > 0 ? ` · ρ=${hop.rho.toFixed(2)}` : '';
  const totHopMs = (hop.propMs + hop.serMs + hop.queueMs).toFixed(1);
  ui.pathOverlay.innerHTML =
    `Hop ${p.hopIndex + 1}/${totalHops} &middot; ${hop.distanceKm.toFixed(0)} km` +
    ` &middot; ${totHopMs} ms${rhoStr}${modeTag}`;

  // ── Hop boundary ───────────────────────────────────────────────────────────
  if (p.hopProgress >= 1.0) {
    p.hopProgress = 0.0;
    const done = p.hopIndex;

    // Bernoulli loss (Dijkstra only)
    if (p.dropIndex !== -1 && done >= p.dropIndex) {
      markHopFailed(done);
      setPacketColor(state.activePacket, 0xff3333);
      ui.pathOverlay.innerHTML =
        `<span style="color:#ff3333">&#10007; Link loss at hop ${done + 1}/${totalHops}</span>`;
      setTimeout(() => clearActivePacket(), 900);
      state.packetsLost += 1; activePacketAnim = null; updateAggregate();
      onPacketEnd({ delivered: false, mode: p.meta.mode, hops: done + 1,
                    propMs: p.costs.totalPropMs, serMs: 0, queueMs: 0, totalMs: 0,
                    rho: p.costs.rho, reason: 'link_loss', interruptedHop: done + 1, costs: null });
      return;
    }

    markHopCompleted(done);
    p.hopIndex += 1;
  }
}

// ---------------------------------------------------------------------------
// Stop all
// ---------------------------------------------------------------------------

function stopAll() {
  clearActivePacket(); clearTrailEdges(); state.compareQueue = null;
  if (state.currentPath) buildTrailEdges(state.currentPath.path, state.currentPath.edgeIdxPath);
  ui.pathOverlay.textContent = ''; ui.status.textContent = 'stopped';
}

// ---------------------------------------------------------------------------
// Comparison orchestration (Dijkstra → BPP, sequential)
// ---------------------------------------------------------------------------

function runComparison() {
  if (state.source == null || state.dest == null) {
    ui.status.textContent = 'pick source and destination first'; return;
  }
  state.compareQueue = { stage: 'dijkstra', dijkstraOutcome: null, bppOutcome: null };
  state.routingMode = 'dijkstra';
  if (ui.routeDijkstra) ui.routeDijkstra.checked = true;
  ui.pathOverlay.innerHTML =
    `<span style="color:#4ad6ff"><b>Compare 1/2:</b> Dijkstra (shortest delay)</span>`;
  ui.compareSection.style.display = 'none';
  setTimeout(() => launchPacket(), 600);
}

function onPacketEnd(outcome) {
  if (!state.compareQueue) return;
  const cq = state.compareQueue;

  if (cq.stage === 'dijkstra') {
    cq.dijkstraOutcome = outcome;
    cq.stage = 'bpp';
    setTimeout(() => {
      state.routingMode = 'bpp';
      if (ui.routeBpp) ui.routeBpp.checked = true;
      ui.pathOverlay.innerHTML =
        `<span style="color:#ff77dd"><b>Compare 2/2:</b> BPP Paper routing</span>`;
      setTimeout(() => launchPacket(), 1200);
    }, 1600);
    return;
  }

  if (cq.stage === 'bpp') {
    cq.bppOutcome = outcome;
    cq.stage = 'done';
    state.compareQueue = null;
    showCompareResults(cq.dijkstraOutcome, cq.bppOutcome);
  }
}

// ---------------------------------------------------------------------------
// Comparison results panel
// ---------------------------------------------------------------------------

function showCompareResults(d, b) {
  if (!d || !b) return;

  function colSection(outcome, cls, label) {
    const delivered = outcome.delivered;
    const statusHtml = delivered
      ? `<span style="color:var(--accent-2)">&#10003; Delivered</span>`
      : `<span style="color:var(--err)">&#10007; Interrupted</span>`;

    let rows = `${kv('Status', statusHtml)}${kv('Hops', outcome.hops)}`;

    if (delivered) {
      const rhoClass = outcome.rho > 0.8 ? 'err-row' : outcome.rho > 0.4 ? 'warn-row' : '';
      rows += `
        ${kv('Prop (ms)', outcome.propMs.toFixed(2))}
        ${kv('Ser (ms)',  outcome.serMs.toFixed(3))}
        ${kv('Queue (ms)', outcome.queueMs.toFixed(3), rhoClass)}
        ${kv('<b>E2E (ms)</b>', '<b>' + outcome.totalMs.toFixed(2) + '</b>')}
        ${kv('ρ utilization', outcome.rho > 0 ? outcome.rho.toFixed(3) : '0')}
      `;
    } else {
      rows += `
        ${kv('At hop', outcome.interruptedHop >= 0 ? outcome.interruptedHop : '—')}
        ${kv('Reason', outcome.reason || '—')}
        ${kv('Partial prop (ms)', outcome.propMs.toFixed(2))}
      `;
    }

    return `<div>
      <div class="cmp-hdr ${cls}">${label}</div>
      ${rows}
    </div>`;
  }

  const dHtml = colSection(d, 'dijk', '&#10024; Dijkstra');
  const bHtml = colSection(b, 'bpp', '&#9679; BPP Paper');

  // Verdict
  let verdictClass = 'ok', verdictText = '';
  if (d.delivered && b.delivered) {
    const dHops = d.hops, bHops = b.hops;
    const dMs   = d.totalMs, bMs = b.totalMs;
    const extraHops = bHops - dHops;
    const extraMs   = bMs   - dMs;
    verdictClass = extraMs > 10 ? 'warn' : 'ok';
    verdictText = `Both delivered. BPP needed ${extraHops >= 0 ? '+' : ''}${extraHops} hops ` +
      `and ${extraMs >= 0 ? '+' : ''}${extraMs.toFixed(1)} ms vs Dijkstra. ` +
      `BPP constraints added ${((extraMs / dMs) * 100).toFixed(1)}% overhead.`;
  } else if (d.delivered && !b.delivered) {
    verdictClass = 'bad';
    verdictText = `BPP interrupted — greedy routing got stuck (${b.reason}). ` +
      `Dijkstra delivered in ${d.hops} hops (${d.totalMs.toFixed(1)} ms E2E). ` +
      `This pair contributes to the empirical interruption probability. ` +
      `At full Starlink density (4 080 sats), empirical interruption ~78.6%; BPP predicts only ~1.4% -- a 77pp gap.`;
  } else if (!d.delivered && b.delivered) {
    verdictClass = 'warn';
    verdictText = `Dijkstra had link loss; BPP delivered (loss prob = ${
      (Number(ui.lossProb.value) * 100).toFixed(1)}%).`;
  } else {
    verdictClass = 'bad';
    verdictText = `Both failed. ${!d.delivered ? 'Dijkstra: ' + d.reason + '. ' : ''}` +
                  `${!b.delivered ? 'BPP: ' + b.reason + '.' : ''}`;
  }

  ui.compareResults.innerHTML = `
    <div class="cmp-grid">${dHtml}${bHtml}</div>
    <div class="verdict ${verdictClass}"><b>Verdict:</b> ${verdictText}</div>
  `;
  ui.compareSection.style.display = '';

  // Scroll compare section into view
  ui.compareSection.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

// ---------------------------------------------------------------------------
// Session stats panel
// ---------------------------------------------------------------------------

function updateAggregate() {
  const sent      = state.packetsSent;
  const delivered = state.packetsDelivered;
  const lost      = state.packetsLost;
  const pdr       = sent ? (delivered / sent * 100) : 0;
  const meanMs    = delivered ? (state.totalLatencyMs / delivered) : 0;
  const bppIR     = state.bppSent ? ((state.bppSent - state.bppDelivered) / state.bppSent * 100) : 0;

  ui.metrics.innerHTML = `
    ${kv('Packets sent', sent)}
    ${kv('Delivered', delivered)}
    ${kv('Lost / interrupted', lost)}
    ${kv('Delivery ratio', pdr.toFixed(1) + ' %')}
    ${kv('Mean E2E (delivered)', meanMs.toFixed(1) + ' ms')}
    <div style="margin-top:5px;border-top:1px solid var(--border);padding-top:5px">
    ${kv('Dijkstra sent', state.dijkstraSent)}
    ${kv('Dijkstra delivered', state.dijkstraDelivered)}
    ${kv('BPP sent', state.bppSent)}
    ${kv('BPP interruption rate', bppIR.toFixed(1) + ' %')}
    </div>
  `;
}

// ---------------------------------------------------------------------------
// Hover tooltip + click picking
// ---------------------------------------------------------------------------

function onPointerMove(ev) {
  const rect = renderer.domElement.getBoundingClientRect();
  pointer.x =  ((ev.clientX - rect.left) / rect.width)  * 2 - 1;
  pointer.y = -((ev.clientY - rect.top)  / rect.height) * 2 + 1;
  raycaster.setFromCamera(pointer, camera);
  const hits = raycaster.intersectObjects(state.nodeMeshes, false);
  if (hits.length) {
    const nid = hits[0].object.userData.nodeId;
    const n   = state.nodes[nid];
    const load = [...state.linkLoad.entries()]
      .filter(([ei]) => state.edges[ei] && (state.edges[ei].u === nid || state.edges[ei].v === nid))
      .reduce((s, [, c]) => s + c, 0);
    ui.tooltip.style.display = 'block';
    ui.tooltip.style.left    = (ev.clientX - rect.left + 14) + 'px';
    ui.tooltip.style.top     = (ev.clientY - rect.top  + 14) + 'px';
    ui.tooltip.innerHTML =
      `<b>${n.name}</b><br/>id ${n.id} &middot; ${n.kind}<br/>` +
      `shell ${n.shellId} &middot; plane ${n.planeId}<br/>` +
      `lat ${n.lat.toFixed(2)}° lon ${n.lon.toFixed(2)}°<br/>` +
      `alt ${n.altKm.toFixed(0)} km` +
      (load > 0 ? `<br/>link load: ${load} pkts` : '');
  } else {
    ui.tooltip.style.display = 'none';
  }
}

function onClick(ev) {
  if (!state.pickMode) return;
  const rect = renderer.domElement.getBoundingClientRect();
  pointer.x =  ((ev.clientX - rect.left) / rect.width)  * 2 - 1;
  pointer.y = -((ev.clientY - rect.top)  / rect.height) * 2 + 1;
  raycaster.setFromCamera(pointer, camera);
  const hits = raycaster.intersectObjects(state.nodeMeshes, false);
  if (!hits.length) return;
  const nid = hits[0].object.userData.nodeId;
  if (state.pickMode === 'src') { setSource(nid); ui.pickSrc.classList.remove('active'); }
  else                          { setDest(nid);   ui.pickDst.classList.remove('active'); }
  state.pickMode = null; ui.status.textContent = 'ready';
}

// ---------------------------------------------------------------------------
// Local file loading
// ---------------------------------------------------------------------------

function handleFileInput(files) {
  let nodesFile = null, edgesFile = null, metaFile = null;
  for (const f of files) {
    const name = f.name.toLowerCase();
    if      (name.includes('node') || name.includes('nodes')) nodesFile = f;
    else if (name.includes('edge') || name.includes('edges')) edgesFile = f;
    else if (name.includes('meta') || name.endsWith('.json')) metaFile  = f;
  }
  if (!nodesFile || !edgesFile) {
    ui.metaInfo.innerHTML = '<div class="banner err">Need a nodes CSV and edges CSV (filenames must contain "nodes"/"edges").</div>';
    return;
  }
  ui.metaInfo.innerHTML = '<div class="muted">loading files…</div>';
  const read = f => new Promise((res, rej) => {
    const r = new FileReader(); r.onload = e => res(e.target.result); r.onerror = rej; r.readAsText(f);
  });
  const ps = [read(nodesFile), read(edgesFile)];
  if (metaFile) ps.push(read(metaFile));
  Promise.all(ps).then(([nt, et, mt]) => {
    const meta = mt ? (() => { try { return JSON.parse(mt); } catch(_) { return null; } })() : null;
    loadData(parseCsv(nt), parseCsv(et), meta);
  }).catch(err => {
    ui.metaInfo.innerHTML = `<div class="banner err">File read error: ${err.message}</div>`;
  });
}

// ---------------------------------------------------------------------------
// Data parsing
// ---------------------------------------------------------------------------

function parseNodes(rows) {
  return rows.map(r => {
    const hasEcef = r.ecef_x_km != null;
    let x = 0, y = 0, z = 0;
    if (hasEcef) {
      x = Number(r.ecef_x_km ?? 0); y = Number(r.ecef_y_km ?? 0); z = Number(r.ecef_z_km ?? 0);
    } else if (r.eci_x_km != null) {
      x = Number(r.eci_x_km ?? 0); y = Number(r.eci_y_km ?? 0); z = Number(r.eci_z_km ?? 0);
    } else {
      x = Number(r.x_km ?? 0); y = Number(r.y_km ?? 0); z = Number(r.z_km ?? 0);
    }
    return {
      id: Number(r.id), name: r.name, kind: r.kind || 'satellite',
      shellId: Number(r.shell_id ?? -1), planeId: Number(r.plane_id ?? -1),
      ecef: [x, y, z], lat: Number(r.lat_deg ?? 0),
      lon: Number(r.lon_deg ?? 0), altKm: Number(r.altitude_km ?? 0),
    };
  });
}

function parseEdges(rows) {
  return rows.map(r => ({
    u: Number(r.u), v: Number(r.v),
    distanceKm: Number(r.distance_km),
    delayMs:    Number(r.prop_delay_ms ?? r.delay_ms),
    kind:       r.kind || 'unknown',
    shellId:    Number(r.shell_id ?? -1),
  }));
}

// ---------------------------------------------------------------------------
// Clear scene data
// ---------------------------------------------------------------------------

function clearSceneData() {
  for (const l of state.renderEdges) { scene.remove(l); l.geometry.dispose(); l.material.dispose(); }
  state.renderEdges = [];
  for (const m of state.nodeMeshes)  { scene.remove(m); m.geometry.dispose(); m.material.dispose(); }
  state.nodeMeshes = [];
  clearTrailEdges();
  if (srcMarker) { scene.remove(srcMarker); srcMarker = null; }
  if (dstMarker) { scene.remove(dstMarker); dstMarker = null; }
  clearActivePacket();
}

// ---------------------------------------------------------------------------
// Satellite subsampling (for performance)
// ---------------------------------------------------------------------------

function subsampleRows(nodesRows, edgesRows) {
  const n = parseInt((ui.satCountSelect && ui.satCountSelect.value) || '0', 10);
  if (!n) return { nodesRows, edgesRows }; // 0 = show all

  const gateways = nodesRows.filter(r => r.kind === 'gateway');
  let sats = nodesRows.filter(r => r.kind !== 'gateway');

  if (sats.length > n) {
    // Fisher-Yates shuffle then take first n — reproducible feel
    for (let i = sats.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      const tmp = sats[i]; sats[i] = sats[j]; sats[j] = tmp;
    }
    sats = sats.slice(0, n);
  }

  const kept = [...gateways, ...sats];
  const keptIds = new Set(kept.map(r => r.id));
  const keptEdges = edgesRows.filter(r => keptIds.has(r.u) && keptIds.has(r.v));
  return { nodesRows: kept, edgesRows: keptEdges };
}

// ---------------------------------------------------------------------------
// Load data (called on epoch switch or file upload)
// ---------------------------------------------------------------------------

function loadData(nodesRows, edgesRows, meta, keepSelection = false) {
  const prevSrc = state.source, prevDst = state.dest;
  clearSceneData();

  const sub = subsampleRows(nodesRows, edgesRows);
  nodesRows = sub.nodesRows;
  edgesRows = sub.edgesRows;

  state.nodes = parseNodes(nodesRows);
  state.edges = parseEdges(edgesRows);
  state.source = null; state.dest = null;
  state.currentPath = null; state.currentCosts = null;

  buildGraph();
  precomputeNodePositions();
  addSceneObjects();
  updateLinkVisuals();
  populateSelects();

  // Try to restore selection after epoch switch
  if (keepSelection && prevSrc != null && state.nodes[prevSrc]) setSource(prevSrc);
  if (keepSelection && prevDst != null && state.nodes[prevDst]) setDest(prevDst);

  state.epoch = meta ? (meta.base_epoch_utc || '') : '';

  const sats = state.nodes.filter(n => n.kind !== 'gateway').length;
  const gws  = state.nodes.filter(n => n.kind === 'gateway').length;
  ui.metaInfo.innerHTML = meta
    ? `<div>
         <span class="pill">schema ${meta.schema_version || '?'}</span>
         <span class="pill">${meta.base_epoch_utc || ''}</span>
       </div>
       <div style="margin-top:5px">
         ${sats} satellites &middot; ${gws} gateways &middot; ${state.edges.length} links
         ${meta.isl_policy?.max_km ? ' &middot; ' + meta.isl_policy.max_km + ' km max ISL' : ''}
       </div>`
    : `${state.nodes.length} nodes &middot; ${state.edges.length} edges`;

  updateStatusOverlay();
}

// ---------------------------------------------------------------------------
// Epoch management
// ---------------------------------------------------------------------------

function populateEpochSelect() {
  ui.epochSelect.innerHTML = '';
  for (let i = 0; i < EPOCH_LABELS.length; i++) {
    const opt = document.createElement('option');
    opt.value = String(i);
    opt.textContent = EPOCH_LABELS[i];
    ui.epochSelect.appendChild(opt);
  }
  ui.epochSelect.value = '0';
}

async function switchEpoch(idx) {
  ui.status.textContent = `loading epoch ${idx}…`;
  try {
    const [nodesRows, edgesRows] = await Promise.all([
      fetchCsv(EPOCH_NODES_URLS[idx]),
      fetchCsv(EPOCH_EDGES_URLS[idx]),
    ]);
    state.epochIdx = idx;
    // Cache raw rows so satellite-count changes can reuse them without re-fetching
    state.rawNodesRows = nodesRows;
    state.rawEdgesRows = edgesRows;
    // Reload meta only for epoch 0
    let meta = null;
    if (idx === 0) meta = await fetchJson(META_URL);
    state.rawMeta = meta || state.rawMeta;
    loadData(nodesRows, edgesRows, meta, true);
    ui.status.textContent = `epoch ${idx} loaded — ${EPOCH_LABELS[idx].split('—')[1].trim()}`;
  } catch (err) {
    ui.status.textContent = `epoch ${idx} load failed: ${err.message}`;
    console.warn('Epoch switch failed:', err);
  }
}

// ---------------------------------------------------------------------------
// Status overlay
// ---------------------------------------------------------------------------

function updateStatusOverlay() {
  const mode  = state.routingMode === 'bpp' ? 'BPP paper routing' : 'Dijkstra';
  const epoch = `epoch ${state.epochIdx}`;
  ui.status.innerHTML =
    `${state.nodes.length} nodes &middot; ${state.edges.length} links &middot; ${mode} &middot; ${epoch}`;
}

// ---------------------------------------------------------------------------
// UI wiring
// ---------------------------------------------------------------------------

function wireUi() {
  ui.src.addEventListener('change', () => setSource(Number(ui.src.value)));
  ui.dst.addEventListener('change', () => setDest(Number(ui.dst.value)));

  ui.pickSrc.addEventListener('click', () => {
    state.pickMode = 'src'; ui.pickSrc.classList.add('active');
    ui.pickDst.classList.remove('active'); ui.status.textContent = 'click a node to set source…';
  });
  ui.pickDst.addEventListener('click', () => {
    state.pickMode = 'dst'; ui.pickDst.classList.add('active');
    ui.pickSrc.classList.remove('active'); ui.status.textContent = 'click a node to set destination…';
  });

  ui.randomPair.addEventListener('click', () => {
    const n = state.nodes.length; if (n < 2) return;
    let a = Math.floor(Math.random() * n);
    let b = Math.floor(Math.random() * (n - 1)); if (b >= a) b += 1;
    setSource(a); setDest(b);
  });

  if (ui.locateSrc) ui.locateSrc.addEventListener('click', () => locateNode(state.source));
  if (ui.locateDst) ui.locateDst.addEventListener('click', () => locateNode(state.dest));

  ui.routeDijkstra.addEventListener('change', () => {
    state.routingMode = 'dijkstra'; refreshPath(); updateStatusOverlay();
  });
  ui.routeBpp.addEventListener('change', () => {
    state.routingMode = 'bpp'; refreshPath(); updateStatusOverlay();
  });

  if (ui.advHeader) {
    ui.advHeader.addEventListener('click', () => {
      const open = ui.advBody.style.display !== 'none';
      ui.advBody.style.display = open ? 'none' : 'block';
      ui.advToggle.innerHTML   = open ? '&#9654; show' : '&#9660; hide';
    });
  }

  ui.bppThetaR.addEventListener('input', () => {
    const v = Number(ui.bppThetaR.value);
    state.bppParams.thetaR = v * Math.PI / 180;
    ui.bppThetaRVal.textContent = v + '°';
    if (state.routingMode === 'bpp') refreshPath();
  });
  ui.bppThetaS.addEventListener('input', () => {
    const v = Number(ui.bppThetaS.value);
    state.bppParams.thetaS = v * Math.PI / 180;
    ui.bppThetaSVal.textContent = v + '°';
    if (state.routingMode === 'bpp') refreshPath();
  });
  ui.bppDth.addEventListener('input', () => {
    const v = Number(ui.bppDth.value);
    state.bppParams.dth = v;
    ui.bppDthVal.textContent = v + ' km';
    if (state.routingMode === 'bpp') refreshPath();
  });
  ui.bppStrategy.addEventListener('change', () => {
    state.bppParams.strategy = ui.bppStrategy.value;
    if (state.routingMode === 'bpp') refreshPath();
  });

  ui.timeScale.addEventListener('input', () => {
    state.timeScale = Number(ui.timeScale.value);
    ui.timeScaleLabel.textContent = state.timeScale.toFixed(1) + '×';
  });

  for (const el of [ui.packetSize, ui.linkRate, ui.offeredLoad, ui.lossProb]) {
    if (!el) continue;
    el.addEventListener('change', refreshPath);
    el.addEventListener('input',  refreshPath);
  }

  ui.launch.addEventListener('click', launchPacket);
  ui.launchBurst.addEventListener('click', () => launchBurst(20));
  ui.stop.addEventListener('click', stopAll);
  if (ui.compare) ui.compare.addEventListener('click', runComparison);

  ui.fileInput.addEventListener('change', ev => {
    handleFileInput(ev.target.files); ev.target.value = '';
  });

  // Epoch selector
  if (ui.epochSelect) {
    ui.epochSelect.addEventListener('change', () => {
      const idx = Number(ui.epochSelect.value);
      if (idx !== state.epochIdx) switchEpoch(idx);
    });
  }

  if (ui.satCountSelect) {
    ui.satCountSelect.addEventListener('change', () => {
      if (state.rawNodesRows && state.rawEdgesRows) {
        const n = parseInt(ui.satCountSelect.value, 10);
        const label = n ? `${n} satellites` : 'all 4080 satellites';
        ui.status.textContent = `reloading with ${label}…`;
        loadData(state.rawNodesRows, state.rawEdgesRows, state.rawMeta || null, false);
        ui.status.textContent = `loaded — ${label}`;
      } else {
        switchEpoch(state.epochIdx ?? 0);
      }
    });
  }

  // Reset heatmap
  if (ui.resetHeatmap) {
    ui.resetHeatmap.addEventListener('click', () => {
      state.linkLoad.clear();
      state.packetsSent = 0; state.packetsDelivered = 0; state.packetsLost = 0;
      state.totalLatencyMs = 0; state.bppSent = 0; state.bppDelivered = 0;
      state.dijkstraSent = 0; state.dijkstraDelivered = 0;
      updateLinkVisuals(); updateAggregate();
      ui.pathInfo.innerHTML = '<span class="muted">stats reset</span>';
      ui.compareSection.style.display = 'none';
    });
  }
}

// ---------------------------------------------------------------------------
// Animation loop
// ---------------------------------------------------------------------------

let lastTs = performance.now();

function animate() {
  const now = performance.now();
  const dt  = Math.min(now - lastTs, 100);
  lastTs = now;

  stepActivePacket(dt);

  // Camera fly-to tween
  if (state.cameraTween) {
    const tw = state.cameraTween;
    tw.elapsed += dt / 1000;
    let t = Math.min(tw.elapsed / tw.duration, 1.0);
    t = t < 0.5 ? 4*t*t*t : 1 - Math.pow(-2*t+2, 3)/2;  // ease in-out cubic
    camera.position.lerpVectors(tw.fromPos, tw.toPos, t);
    controls.target.lerpVectors(tw.fromTarget, tw.toTarget, t);
    if (tw.elapsed >= tw.duration) state.cameraTween = null;
  }

  if (srcMarker) srcMarker.lookAt(camera.position);
  if (dstMarker) dstMarker.lookAt(camera.position);

  controls.update();
  renderer.render(scene, camera);
  requestAnimationFrame(animate);
}

// ---------------------------------------------------------------------------
// Bootstrap
// ---------------------------------------------------------------------------

async function main() {
  initScene();
  populateEpochSelect();
  wireUi();
  updateAggregate();
  ui.status.textContent = 'loading snapshot…';

  try {
    const [nodesRows, edgesRows, meta] = await Promise.all([
      fetchCsv(EPOCH_NODES_URLS[0]),
      fetchCsv(EPOCH_EDGES_URLS[0]),
      fetchJson(META_URL),
    ]);
    state.rawNodesRows = nodesRows;
    state.rawEdgesRows = edgesRows;
    state.rawMeta = meta;
    loadData(nodesRows, edgesRows, meta);
    ui.status.textContent = 'ready';
  } catch (err) {
    console.warn('Default snapshot load failed:', err.message);
    ui.metaInfo.innerHTML = `<div class="banner warn">
      Could not load snapshot (<code>${EPOCH_NODES_URLS[0]}</code>).<br/>
      Run: <code>cd /path/to/starlink-fyp &amp;&amp; python3 -m http.server 8080</code><br/>
      Then open: <code>http://localhost:8080/tools/visualizer/</code><br/>
      Error: ${err.message}
    </div>`;
    ui.status.textContent = 'no snapshot — use "Load local CSVs"';
  }

  animate();
}

main();
