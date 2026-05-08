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
// Snapshot:
//   The visualizer uses one frozen-time topology by default. Additional
//   epoch files are kept as data sources, but the UI no longer exposes
//   epoch switching so the simulation remains fixed and comparable.
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

function formatSnapshotTime(iso) {
  if (!iso) return 'unknown snapshot time';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return new Intl.DateTimeFormat('en-US', {
    weekday: 'long',
    year: 'numeric',
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    timeZone: 'UTC',
    timeZoneName: 'short',
  }).format(d);
}

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
  dijkstraTotalMs:     0.0,
  dijkstraTotalPropMs: 0.0,
  dijkstraTotalQueueMs:0.0,
  dijkstraTotalHops:   0,
  bppTotalMs:          0.0,
  bppTotalPropMs:      0.0,
  bppTotalQueueMs:     0.0,
  bppTotalHops:        0,

  // Current path
  currentPath:   null,
  currentCosts:  null,
  epoch:         '',

  // Camera fly-to tween
  cameraTween: null,

  // Comparison orchestration
  compareQueue: null,

  // Background traffic engine
  traffic: {
    on:        false,
    intensity: 'medium',           // 'low' | 'medium' | 'high' | 'stress' | 'custom'
    customRate: 35,                // pkt/s if custom
    demandMultiplier: 600,         // amplifies rendered traffic to simulated offered load
    flows:     [],                 // active rendered flows
    spawnAcc:  0,
    nextFlowId: 1,
    sentTotal:      0,
    deliveredTotal: 0,
    lostTotal:      0,
    activeCap:      180,           // max simultaneous in-flight rendered flows
    // Sliding-window history of recently delivered flows for accurate metrics
    history:        [],            // [{ e2eMs, propMs, queueMs, serMs, hops, packetBits, deliveredAt }]
    historyMax:     400,
    // Aggregate "real demand" stats — count includes demand multiplier
    simSent:        0,
    simDelivered:   0,
    simLost:        0,
    simBitsDelivered: 0,
  },

  // Per-edge live state
  edgeLoad:        null,     // Float32Array per edge (0..1) — derived from edgeRecentBits
  edgeRecentBits:  null,     // Float32Array — EMA accumulator of offered bits (last τ seconds)
};

// Precomputed flat position arrays for fast BPP scanning
// (updated every time loadData is called)
let NPX = new Float64Array(0);  // scene X per node
let NPY = new Float64Array(0);  // scene Y per node
let NPZ = new Float64Array(0);  // scene Z per node
let NODE_KIND = [];             // 'gateway' | 'satellite' per node

// ---------------------------------------------------------------------------
// Location / reverse-geocoding (Nominatim)
// ---------------------------------------------------------------------------

const locationCache = {};

async function getNodeLocation(node) {
  if (!node) return '';
  const key = `${node.lat.toFixed(1)},${node.lon.toFixed(1)}`;
  if (locationCache[key] !== undefined) return locationCache[key];
  locationCache[key] = '';  // prevent duplicate requests
  try {
    const url = `https://nominatim.openstreetmap.org/reverse?lat=${node.lat}&lon=${node.lon}&format=json&zoom=3&accept-language=en`;
    const r = await fetch(url, { headers: { 'User-Agent': 'StarlinkMHRVisualizer/1.0' } });
    if (!r.ok) return '';
    const data = await r.json();
    const country = data.address?.country || '';
    locationCache[key] = country;
    return country;
  } catch (_) { return ''; }
}

// Try to extract a country name from gateway names like "GW-Aerzen - Germany"
// or "Aerzen, Germany" or trailing parenthesized country.
function inferCountryFromName(name) {
  if (!name) return '';
  // Pattern: "... - Country" or "..., Country" or "... (Country)"
  const m = name.match(/[-–—]\s*([A-Za-z][A-Za-z\s]+)$/) ||
            name.match(/,\s*([A-Za-z][A-Za-z\s]+)$/) ||
            name.match(/\(([A-Za-z][A-Za-z\s]+)\)\s*$/);
  if (m) return m[1].trim();
  return '';
}

function showNodeLocation(node, el) {
  if (!node || !el) return;
  if (node.kind === 'gateway') {
    // First try to infer from name (free, instant)
    const inferred = inferCountryFromName(node.name);
    if (inferred) {
      el.textContent = `📍 ${inferred}`;
      return;
    }
    // Fall back to Nominatim reverse geocoding
    el.textContent = '⟳ locating…';
    getNodeLocation(node).then(c => {
      if (el) el.textContent = c ? `📍 ${c}` : `${node.lat.toFixed(1)}°, ${node.lon.toFixed(1)}°`;
    });
  } else {
    el.textContent = `🛰  shell ${node.shellId} · plane ${node.planeId} · ${node.lat.toFixed(1)}°,${node.lon.toFixed(1)}°`;
  }
}

// ---------------------------------------------------------------------------
// Gateway link cap (~5 closest access links per gateway)
// ---------------------------------------------------------------------------

function capGatewayLinks(edgesRows, nodesRows, maxLinks) {
  const kindMap = {};
  for (const r of nodesRows) kindMap[r.id] = r.kind;

  const gwEdgeMap = {};
  const otherEdges = [];

  for (const e of edgesRows) {
    const uGw = kindMap[e.u] === 'gateway';
    const vGw = kindMap[e.v] === 'gateway';
    if (uGw || vGw) {
      const gwId = uGw ? e.u : e.v;
      if (!gwEdgeMap[gwId]) gwEdgeMap[gwId] = [];
      gwEdgeMap[gwId].push(e);
    } else {
      otherEdges.push(e);
    }
  }

  const cappedGw = [];
  for (const edges of Object.values(gwEdgeMap)) {
    const sorted = edges.slice().sort((a, b) => Number(a.distance_km) - Number(b.distance_km));
    cappedGw.push(...sorted.slice(0, maxLinks));
  }
  return [...otherEdges, ...cappedGw];
}

// ---------------------------------------------------------------------------
// UI element references
// ---------------------------------------------------------------------------

const ui = {
  src:            document.getElementById('src-select'),
  dst:            document.getElementById('dst-select'),
  srcLocation:    document.getElementById('src-location'),
  dstLocation:    document.getElementById('dst-location'),
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
  launch:         document.getElementById('launch'),
  launchBurst:    document.getElementById('launch-burst'),
  stop:           document.getElementById('stop'),
  timeScale:      document.getElementById('time-scale'),
  timeScaleLabel: document.getElementById('time-scale-label'),
  metaInfo:       document.getElementById('meta-info'),
  pathInfo:       document.getElementById('path-info'),
  metrics:        document.getElementById('metrics'),
  liveTraffic:    document.getElementById('live-traffic'),
  trafficOn:      document.getElementById('traffic-on'),
  trafficIntensity: document.getElementById('traffic-intensity'),
  trafficCustomRow: document.getElementById('traffic-custom-row'),
  trafficRate:    document.getElementById('traffic-rate'),
  trafficRateVal: document.getElementById('traffic-rate-val'),
  status:         document.getElementById('status'),
  tooltip:        document.getElementById('tooltip'),
  pathOverlay:    document.getElementById('path-overlay'),
  scene:          document.getElementById('scene'),
  fileInput:      document.getElementById('file-input'),
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
  // Allow nearly-free vertical rotation — past the poles
  controls.minPolarAngle = -Infinity;
  controls.maxPolarAngle =  Infinity;
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
    let px =  n.ecef[0] * ECEF_SCENE_SCALE;
    let py =  n.ecef[2] * ECEF_SCENE_SCALE;
    let pz = -n.ecef[1] * ECEF_SCENE_SCALE;

    // Ground stations are stored in ECEF with the true ellipsoidal Earth
    // (polar radius ≈ 6357 km), but our collision sphere uses the equatorial
    // radius (6378.137 km).  High-latitude gateways therefore fall slightly
    // *inside* the sphere, causing hasLoSFlat() to report every LoS blocked.
    // Fix: clamp gateway positions radially to the sphere surface so the LoS
    // geometry is consistent.  Satellite positions are unaffected (they are
    // always 480–570 km above the surface).
    if (n.kind === 'gateway') {
      const r = Math.sqrt(px*px + py*py + pz*pz);
      if (r > 1e-9 && r < EARTH_SCENE_R) {
        const s = EARTH_SCENE_R / r;
        px *= s; py *= s; pz *= s;
      }
    }

    NPX[i] = px;
    NPY[i] = py;
    NPZ[i] = pz;
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

// ── Batched edge rendering ────────────────────────────────────────────────
// Single LineSegments draw call for ALL edges. Vertex-color updates drive
// congestion heat. Massive perf win vs per-edge THREE.Line.
let edgeBatch = null;        // THREE.LineSegments
let edgeBatchColors = null;  // Float32Array — vertex colors (rgb per vertex)
let edgeBatchBaseRGB = null; // Float32Array — base color per edge

// ── Instanced satellite rendering ────────────────────────────────────────
let satInstanced = null;     // THREE.InstancedMesh — all satellites
let satInstanceToNodeId = []; // instanceIndex → nodeId
let nodeIdToSatInstance = []; // nodeId → instanceIndex (-1 if gateway)

const _tmpMat = new THREE.Matrix4();
const _tmpColor = new THREE.Color();

function addSceneObjects() {
  // ── Batched edges ───────────────────────────────────────────────────
  const E = state.edges.length;
  const positions = new Float32Array(E * 6);    // 2 verts × 3
  edgeBatchColors  = new Float32Array(E * 6);   // 2 verts × 3 (rgb)
  edgeBatchBaseRGB = new Float32Array(E * 3);   // 1 rgb per edge
  for (let i = 0; i < E; ++i) {
    const e = state.edges[i];
    const a = vec3OfNode(state.nodes[e.u]);
    const b = vec3OfNode(state.nodes[e.v]);
    positions[i * 6 + 0] = a.x; positions[i * 6 + 1] = a.y; positions[i * 6 + 2] = a.z;
    positions[i * 6 + 3] = b.x; positions[i * 6 + 4] = b.y; positions[i * 6 + 5] = b.z;
    const baseHex = EDGE_BASE_COLORS[e.kind] || EDGE_BASE_COLORS.unknown;
    _tmpColor.setHex(baseHex);
    edgeBatchBaseRGB[i * 3 + 0] = _tmpColor.r;
    edgeBatchBaseRGB[i * 3 + 1] = _tmpColor.g;
    edgeBatchBaseRGB[i * 3 + 2] = _tmpColor.b;
    // start at base color
    for (let k = 0; k < 2; ++k) {
      edgeBatchColors[i * 6 + k * 3 + 0] = _tmpColor.r;
      edgeBatchColors[i * 6 + k * 3 + 1] = _tmpColor.g;
      edgeBatchColors[i * 6 + k * 3 + 2] = _tmpColor.b;
    }
  }
  const eg = new THREE.BufferGeometry();
  eg.setAttribute('position', new THREE.BufferAttribute(positions, 3));
  eg.setAttribute('color',    new THREE.BufferAttribute(edgeBatchColors, 3));
  edgeBatch = new THREE.LineSegments(eg, new THREE.LineBasicMaterial({
    vertexColors: true, transparent: true, opacity: 0.45,
  }));
  scene.add(edgeBatch);

  state.edgeLoad       = new Float32Array(E);
  state.edgeRecentBits = new Float32Array(E);

  // ── Nodes: gateways individual, satellites instanced ───────────────
  satInstanceToNodeId = [];
  nodeIdToSatInstance = new Array(state.nodes.length).fill(-1);

  const sats = [];
  for (const n of state.nodes) {
    if (n.kind === 'gateway') {
      const geo = new THREE.SphereGeometry(0.12, 14, 14);
      const mat = new THREE.MeshBasicMaterial({ color: 0xff4763 });
      const m   = new THREE.Mesh(geo, mat);
      m.position.copy(vec3OfNode(n));
      m.userData.nodeId = n.id;
      m.userData.kind   = 'gateway';
      state.nodeMeshes.push(m);
      scene.add(m);
    } else {
      sats.push(n);
    }
  }

  if (sats.length) {
    const geo = new THREE.SphereGeometry(0.065, 8, 8);
    const mat = new THREE.MeshBasicMaterial({ color: 0xffffff });
    satInstanced = new THREE.InstancedMesh(geo, mat, sats.length);
    satInstanced.instanceMatrix.setUsage(THREE.DynamicDrawUsage);
    const baseColor = new THREE.Color(0x8affc1);
    for (let i = 0; i < sats.length; ++i) {
      const n = sats[i];
      _tmpMat.identity();
      _tmpMat.setPosition(vec3OfNode(n));
      satInstanced.setMatrixAt(i, _tmpMat);
      satInstanced.setColorAt(i, baseColor);
      satInstanceToNodeId.push(n.id);
      nodeIdToSatInstance[n.id] = i;
    }
    satInstanced.instanceMatrix.needsUpdate = true;
    if (satInstanced.instanceColor) satInstanced.instanceColor.needsUpdate = true;
    satInstanced.userData.kind = 'satellite';
    scene.add(satInstanced);
  }
}

// ── Heat color map for live edge load (0..1) ─────────────────────────
function applyHeatRGB(load, baseR, baseG, baseB, out, off) {
  if (load <= 0.001) {
    out[off]     = baseR; out[off + 1] = baseG; out[off + 2] = baseB;
    out[off + 3] = baseR; out[off + 4] = baseG; out[off + 5] = baseB;
    return;
  }
  // 0..1 → cyan (low) → amber (mid) → red (high)
  let r, g, b;
  if (load < 0.4) {
    const t = load / 0.4;
    r = 0.29 + (1.00 - 0.29) * t;
    g = 0.84 + (0.70 - 0.84) * t;
    b = 1.00 + (0.28 - 1.00) * t;
  } else if (load < 0.8) {
    const t = (load - 0.4) / 0.4;
    r = 1.00; g = 0.70 - 0.28 * t; b = 0.28 - 0.16 * t;
  } else {
    const t = Math.min(1, (load - 0.8) / 0.2);
    r = 1.00; g = 0.42 - 0.21 * t; b = 0.12;
  }
  out[off]     = r; out[off + 1] = g; out[off + 2] = b;
  out[off + 3] = r; out[off + 4] = g; out[off + 5] = b;
}

// Update batched edge vertex colors from current load
function updateLinkVisuals() {
  if (!edgeBatch || !edgeBatchColors) return;
  const E = state.edges.length;
  for (let i = 0; i < E; ++i) {
    const live = state.edgeLoad ? state.edgeLoad[i] : 0;
    const cumulative = state.linkLoad.get(i) || 0;
    const cumNorm = cumulative > 16 ? 1.0 : cumulative / 16;
    // Combine live (decays) + cumulative (heatmap memory)
    const load = Math.max(live, cumNorm * 0.6);
    applyHeatRGB(
      load,
      edgeBatchBaseRGB[i * 3 + 0],
      edgeBatchBaseRGB[i * 3 + 1],
      edgeBatchBaseRGB[i * 3 + 2],
      edgeBatchColors, i * 6
    );
  }
  edgeBatch.geometry.attributes.color.needsUpdate = true;
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
  showNodeLocation(state.nodes[id], ui.srcLocation);
}
function setDest(id) {
  state.dest = id; ui.dst.value = String(id);
  placeMarker('dst', id); refreshPath();
  showNodeLocation(state.nodes[id], ui.dstLocation);
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
  const nodePos = vec3OfNode(state.nodes[idx]);
  const radial  = nodePos.clone().normalize();
  // Camera flies to a point just outside the node, looking inward toward Earth.
  // Keep controls.target at the origin so user can still freely orbit Earth.
  const camPos = nodePos.clone().add(radial.multiplyScalar(pullback));
  state.cameraTween = {
    fromPos: camera.position.clone(), toPos: camPos,
    fromTarget: controls.target.clone(), toTarget: new THREE.Vector3(0, 0, 0),
    elapsed: 0, duration: 1.1,
  };
}

// ---------------------------------------------------------------------------
// Background traffic engine — continuous random gateway↔gateway flows
// ---------------------------------------------------------------------------
//
// Each flow has a precomputed Dijkstra route. We advance its hop progress
// each frame at a fixed visual speed. When it completes a hop we mark the
// edge as "busy" (live load) and increment the cumulative linkLoad. When
// the flow reaches its destination we record latency / delivery.
//
// The simplified network model:
//   • each visible flow represents demandMultiplier real packets
//   • per-edge recent offered bits are tracked with an EMA window
//   • per-edge ρ = recent_offered_bps / edge_capacity_bps
//   • queuing delay is estimated per hop via M/D/1 in hopCostsForPath()
//   • packet loss rises smoothly at high ρ to approximate buffer pressure

// Traffic intensity presets:
//   rate:   rendered packet flows per second (visible sprites)
//   demand: multiplier so each rendered packet represents many real packets
//           (lets us produce visible per-edge ρ without rendering thousands)
//   cap:    max in-flight rendered flows
// Tuned so that with 100 Mbps links + Dijkstra routing concentration:
//   low   ≈ ρ_hot ~0.03   (essentially uncongested)
//   medium≈ ρ_hot ~0.4-0.5 (mild visible queueing on hot access/trunk links)
//   high  ≈ ρ_hot ~0.75   (visible queueing + occasional loss)
//   stress: saturated (clear drops on hot links)
const TRAFFIC_PRESETS = {
  low:    { rate:  8,  demand:  400,  cap:  80  },
  medium: { rate: 35,  demand: 2500,  cap: 220  },
  high:   { rate: 80,  demand: 1700,  cap: 320  },
  stress: { rate: 160, demand: 2800,  cap: 500  },
};
const TRAFFIC_TAU_S      = 1.5;     // EMA window for live traffic load (seconds)
const TRAFFIC_HOP_VIS_MS = 280;     // visual ms per hop for background flows
const TRAFFIC_MAX_RENDER = 120;     // cap on visible flow sprites
const THROUGHPUT_WINDOW_MS = 5000;  // window for goodput calc (5 s)
const HISTORY_RECENT_FOR_STATS = 200;  // how many recent flows feed the stats

// Probabilistic per-link loss as a function of edge ρ.
//   ρ < 0.65 : zero
//   0.65–0.85: linear 0% → 2%
//   0.85–1.0 : linear 2% → 8%
function lossProbFromRho(rho) {
  if (rho < 0.65) return 0;
  if (rho < 0.85) return 0.02 * (rho - 0.65) / 0.20;
  return 0.02 + 0.06 * Math.min(1, (rho - 0.85) / 0.15);
}

function edgeCapacityBps(eIdx, params) {
  // Gateway access links use a separate (lower) capacity to model the
  // last-mile bottleneck between ground station and the satellite network.
  if (eIdx >= 0 && state.edgeIsAccess && state.edgeIsAccess[eIdx]) {
    return (params.gatewayMbps ?? params.islMbps) * 1e6;
  }
  return params.islMbps * 1e6;
}

function liveRhoForEdge(eIdx, params) {
  if (eIdx < 0 || !state.edgeRecentBits || eIdx >= state.edgeRecentBits.length) return 0;
  return Math.min(0.999, state.edgeRecentBits[eIdx] / TRAFFIC_TAU_S / edgeCapacityBps(eIdx, params));
}

function addRecentEdgeDemand(eIdx, bits) {
  if (eIdx >= 0 && state.edgeRecentBits && eIdx < state.edgeRecentBits.length) {
    state.edgeRecentBits[eIdx] += bits;
  }
}

let trafficInstanced = null;        // THREE.InstancedMesh of small dim spheres
let trafficInstanceCap = 0;

function ensureTrafficInstancedMesh() {
  if (trafficInstanced) return;
  const geo = new THREE.SphereGeometry(0.045, 6, 6);
  const mat = new THREE.MeshBasicMaterial({
    color: 0x4ad6ff, transparent: true, opacity: 0.85,
    blending: THREE.AdditiveBlending, depthWrite: false,
  });
  trafficInstanced = new THREE.InstancedMesh(geo, mat, TRAFFIC_MAX_RENDER);
  trafficInstanced.instanceMatrix.setUsage(THREE.DynamicDrawUsage);
  // Hide all initially (zero scale)
  for (let i = 0; i < TRAFFIC_MAX_RENDER; i++) {
    _tmpMat.makeScale(0, 0, 0);
    trafficInstanced.setMatrixAt(i, _tmpMat);
  }
  trafficInstanced.instanceMatrix.needsUpdate = true;
  trafficInstanced.count = TRAFFIC_MAX_RENDER;
  scene.add(trafficInstanced);
  trafficInstanceCap = TRAFFIC_MAX_RENDER;
}

function clearTrafficVisuals() {
  if (trafficInstanced) {
    scene.remove(trafficInstanced);
    trafficInstanced.geometry.dispose();
    trafficInstanced.material.dispose();
    trafficInstanced = null;
  }
}

function trafficSpawnRate() {
  const t = state.traffic;
  if (t.intensity === 'custom') return t.customRate;
  const p = TRAFFIC_PRESETS[t.intensity];
  return p ? p.rate : 25;
}

function trafficDemand() {
  const t = state.traffic;
  if (t.intensity === 'custom') return Math.max(50, t.customRate * 25);
  const p = TRAFFIC_PRESETS[t.intensity];
  return p ? p.demand : 600;
}

function pickGateway() {
  // Find a random gateway. Falls back to any node if none.
  const gws = [];
  for (let i = 0; i < state.nodes.length; i++)
    if (NODE_KIND[i] === 'gateway') gws.push(i);
  if (gws.length === 0) return Math.floor(Math.random() * state.nodes.length);
  return gws[Math.floor(Math.random() * gws.length)];
}

function pickRandomNode() {
  return Math.floor(Math.random() * state.nodes.length);
}

function spawnTrafficFlow() {
  const t = state.traffic;
  if (t.flows.length >= t.activeCap) return;
  if (state.nodes.length < 2) return;

  // ~70% gateway↔gateway, ~30% sat↔sat for visual variety + global spread
  const useSatPair = Math.random() < 0.30;
  const pickFn = useSatPair ? pickRandomNode : pickGateway;

  let src = pickFn();
  let dst = pickFn();
  for (let tries = 0; tries < 10 && dst === src; tries++) dst = pickFn();
  if (dst === src) return;

  const r = dijkstra(src, dst);
  if (!r || r.path.length < 2) {
    t.lostTotal += 1; t.sentTotal += 1;
    return;
  }
  const params = readParams();
  const costs  = hopCostsForPath(r.path, r.edgeIdxPath, params);

  t.flows.push({
    id: t.nextFlowId++,
    path: r.path,
    edgeIdxPath: r.edgeIdxPath,
    costs,
    hopIndex: 0,
    hopProgress: 0.0,
    startedAt: performance.now(),
    delivered: false,
  });
  t.sentTotal += 1;
  t.simSent   += t.demandMultiplier;
}

function stepTrafficFlows(dtMs) {
  const t = state.traffic;
  if (!t.on) return;
  if (!state.edgeRecentBits || !state.edgeLoad) return;
  if (!state.nodes.length || !state.edges.length) return;

  // Spawn new flows
  const rate = trafficSpawnRate();
  t.spawnAcc += (dtMs / 1000) * rate;
  while (t.spawnAcc >= 1) { spawnTrafficFlow(); t.spawnAcc -= 1; }

  const params = readParams();
  const packetBits  = params.packetBytes * 8;

  // ── Sliding-window EMA decay ────────────────────────────────────────
  // Each edge accumulates "offered bits" — we decay each frame and divide
  // by τ to get an instantaneous offered-bps. ρ is offered / capacity.
  const decay    = Math.exp(-dtMs / 1000 / TRAFFIC_TAU_S);
  const rb       = state.edgeRecentBits;
  const E        = rb.length;
  for (let i = 0; i < E; i++) {
    rb[i] *= decay;
    state.edgeLoad[i] = liveRhoForEdge(i, params);
  }

  // ── Step each flow ──────────────────────────────────────────────────
  const now       = performance.now();
  const remaining = [];
  for (const f of t.flows) {
    const totalHops = f.path.length - 1;
    f.hopProgress += dtMs / TRAFFIC_HOP_VIS_MS;
    let dropped = false;

    while (!f.delivered && !dropped && f.hopProgress >= 1.0) {
      f.hopProgress -= 1.0;
      const eIdx = f.edgeIdxPath[f.hopIndex];

      if (eIdx >= 0) {
        // Add this packet's offered bits (× demand multiplier) to the edge.
        addRecentEdgeDemand(eIdx, packetBits * t.demandMultiplier);
        // Cumulative heat-load (for the existing heatmap memory)
        state.linkLoad.set(eIdx, (state.linkLoad.get(eIdx) || 0) + 1);
        // Probabilistic loss based on current ρ
        const liveRho = liveRhoForEdge(eIdx, params);
        const lossP   = lossProbFromRho(liveRho);
        if (lossP > 0 && Math.random() < lossP) {
          t.lostTotal += 1;
          t.simLost   += t.demandMultiplier;
          dropped = true;
          break;
        }
      }

      f.hopIndex += 1;
      if (f.hopIndex >= totalHops) {
        // Re-cost using current live ρ so the recorded delay reflects congestion
        const finalCosts = hopCostsForPath(f.path, f.edgeIdxPath, params);
        t.deliveredTotal  += 1;
        t.simDelivered    += t.demandMultiplier;
        t.simBitsDelivered += packetBits * t.demandMultiplier;
        t.history.push({
          e2eMs:       finalCosts.totalMs,
          propMs:      finalCosts.totalPropMs,
          queueMs:     finalCosts.totalQueueMs,
          serMs:       finalCosts.totalSerMs,
          hops:        totalHops,
          packetBits:  packetBits * t.demandMultiplier,
          deliveredAt: now,
        });
        if (t.history.length > t.historyMax) t.history.shift();
        f.delivered = true;
        break;
      }
    }
    if (!f.delivered && !dropped) remaining.push(f);
  }
  t.flows = remaining;

  // Render visible packets via instanced sprites
  ensureTrafficInstancedMesh();
  const N = Math.min(t.flows.length, TRAFFIC_MAX_RENDER);
  for (let i = 0; i < N; i++) {
    const f = t.flows[i];
    const u = f.path[f.hopIndex], v = f.path[f.hopIndex + 1];
    const ax = NPX[u], ay = NPY[u], az = NPZ[u];
    const bx = NPX[v], by = NPY[v], bz = NPZ[v];
    const tt = f.hopProgress;
    _tmpMat.makeScale(1, 1, 1);
    _tmpMat.setPosition(ax + (bx-ax)*tt, ay + (by-ay)*tt, az + (bz-az)*tt);
    trafficInstanced.setMatrixAt(i, _tmpMat);
  }
  for (let i = N; i < TRAFFIC_MAX_RENDER; i++) {
    _tmpMat.makeScale(0, 0, 0);
    trafficInstanced.setMatrixAt(i, _tmpMat);
  }
  trafficInstanced.instanceMatrix.needsUpdate = true;

  // Refresh edge colors (live congestion)
  if (t.on) updateLinkVisuals();
}

function p95(arr) {
  if (!arr.length) return 0;
  const s = arr.slice().sort((a, b) => a - b);
  return s[Math.floor(s.length * 0.95)] || s[s.length - 1];
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
    // Brighter route highlight so it stands out over background traffic
    const mat = new THREE.LineBasicMaterial({
      color: isSynthetic ? 0xbb88ff : 0xffd700,
      transparent: true,
      opacity: isSynthetic ? 0.85 : 0.85,
      linewidth: 2,
      blending: THREE.AdditiveBlending,
      depthWrite: false,
    });
    const line = new THREE.Line(geo, mat);
    line.renderOrder = 2;
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
  // Bright core
  g.add(new THREE.Mesh(new THREE.SphereGeometry(0.18, 18, 18),
    new THREE.MeshBasicMaterial({ color: 0xffffff })));
  // Inner halo
  const halo1 = new THREE.Mesh(new THREE.SphereGeometry(0.32, 16, 16),
    new THREE.MeshBasicMaterial({ color, transparent: true, opacity: 0.55,
      blending: THREE.AdditiveBlending, depthWrite: false }));
  halo1.userData.isGlow = true;
  g.add(halo1);
  // Outer corona
  const halo2 = new THREE.Mesh(new THREE.SphereGeometry(0.55, 16, 16),
    new THREE.MeshBasicMaterial({ color, transparent: true, opacity: 0.22,
      blending: THREE.AdditiveBlending, depthWrite: false }));
  halo2.userData.isGlow = true;
  halo2.userData.coronaScale = 1.0;
  g.add(halo2);
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
      // Gateways are endpoints only — never relay through one mid-route
      if (NODE_KIND[v] === 'gateway' && v !== dst) continue;
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
    packetBytes:  Math.max(1, Number(ui.packetSize?.value || 1000)),
    islMbps:      Math.max(0.001, Number(ui.linkRate?.value || 100)),
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
  // Live ρ comes from the background-traffic congestion engine when ON
  const useLive    = state.traffic && state.traffic.on && state.edgeLoad;

  let rhoMax = 0;

  for (let i = 0; i < edgeIdxPath.length; ++i) {
    const eIdx = edgeIdxPath[i];
    let distanceKm, propMs, edgeKind;

    if (eIdx !== -1 && eIdx < state.edges.length) {
      const e  = state.edges[eIdx];
      distanceKm = e.distanceKm;
      propMs     = e.delayMs;
      edgeKind   = e.kind;
    } else {
      const ax = NPX[path[i]],   ay = NPY[path[i]],   az = NPZ[path[i]];
      const bx = NPX[path[i+1]], by = NPY[path[i+1]], bz = NPZ[path[i+1]];
      const d  = Math.sqrt((bx-ax)*(bx-ax) + (by-ay)*(by-ay) + (bz-az)*(bz-az));
      distanceKm = d * SCENE_TO_KM;
      propMs     = distanceKm / C_KM_S * 1000;
      edgeKind   = 'bpp_synthetic';
    }

    const capacityBps = edgeCapacityBps(eIdx, params);

    // Per-hop ρ comes from live background traffic on this edge.
    let edgeRho = 0;
    if (useLive && eIdx !== -1 && eIdx < state.edgeLoad.length) {
      edgeRho = Math.min(0.999, state.edgeLoad[eIdx]);
    }
    if (edgeRho > rhoMax) rhoMax = edgeRho;

    const serMs   = (params.packetBytes * 8.0) / capacityBps * 1000.0;
    const queueMs = md1QueueMs(edgeRho, serMs);
    const hopMs   = propMs + serMs + queueMs;

    totMs    += hopMs;
    totProp  += propMs;
    totSer   += serMs;
    totQueue += queueMs;

    rows.push({
      hop: i + 1,
      u: path[i], v: path[i + 1],
      edgeKind, distanceKm, propMs, serMs, queueMs, hopMs, rho: edgeRho, eIdx,
    });
  }

  const rho = rhoMax;
  return { rows, totalMs: totMs, totalPropMs: totProp, totalSerMs: totSer,
           totalQueueMs: totQueue, rho, rhoMax };
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

  // Per-link loss comes from the live ρ-based congestion model.
  // Applied to BOTH Dijkstra and BPP packets when traffic is ON.
  const params  = readParams();
  const useLive = state.traffic && state.traffic.on && state.edgeLoad;
  let dropIndex = -1;
  for (let i = 0; i < routePath.length - 1; ++i) {
    let p = 0;
    if (useLive) {
      const eIdx = routeEdgeIdx[i];
      if (eIdx >= 0 && eIdx < state.edgeLoad.length) {
        p = lossProbFromRho(state.edgeLoad[eIdx]);
      }
    }
    if (p > 0 && Math.random() < p) { dropIndex = i; break; }
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
  const packetBits = params.packetBytes * 8;

  const useLive = state.traffic && state.traffic.on && state.edgeLoad;
  for (let k = 0; k < n; ++k) {
    let drop = -1;
    for (let i = 0; i < hops; ++i) {
      let p = 0;
      if (useLive) {
        const eIdx = r.edgeIdxPath[i];
        if (eIdx >= 0 && eIdx < state.edgeLoad.length) {
          p = lossProbFromRho(state.edgeLoad[eIdx]);
        }
      }
      if (p > 0 && Math.random() < p) { drop = i; break; }
    }
    const delivered = drop === -1;
    state.packetsSent    += 1;
    state.dijkstraSent   += 1;
    if (delivered) {
      state.packetsDelivered   += 1;
      state.dijkstraDelivered  += 1;
      state.totalLatencyMs        += costs.totalMs;
      state.dijkstraTotalMs       += costs.totalMs;
      state.dijkstraTotalPropMs   += costs.totalPropMs;
      state.dijkstraTotalQueueMs  += costs.totalQueueMs;
      state.dijkstraTotalHops     += hops;
      // Track link load
      for (const eIdx of r.edgeIdxPath) {
        if (eIdx >= 0) {
          state.linkLoad.set(eIdx, (state.linkLoad.get(eIdx) || 0) + 1);
          addRecentEdgeDemand(eIdx, packetBits);
        }
      }
    } else {
      state.packetsLost += 1;
      for (let i = 0; i <= drop && i < r.edgeIdxPath.length; i++) {
        addRecentEdgeDemand(r.edgeIdxPath[i], packetBits);
      }
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
    if (p.meta.mode === 'bpp') {
      state.bppDelivered       += 1;
      state.bppTotalMs         += p.costs.totalMs;
      state.bppTotalPropMs     += p.costs.totalPropMs;
      state.bppTotalQueueMs    += p.costs.totalQueueMs;
      state.bppTotalHops       += totalHops;
    } else {
      state.dijkstraDelivered     += 1;
      state.dijkstraTotalMs       += p.costs.totalMs;
      state.dijkstraTotalPropMs   += p.costs.totalPropMs;
      state.dijkstraTotalQueueMs  += p.costs.totalQueueMs;
      state.dijkstraTotalHops     += totalHops;
    }
    state.totalLatencyMs += p.costs.totalMs;
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
    const eIdx = p.edgeIdxPath[done];
    if (eIdx >= 0) {
      addRecentEdgeDemand(eIdx, readParams().packetBytes * 8);
    }

    // Bernoulli loss from live rho-based congestion.
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
    verdictText = `Dijkstra had congestion loss; BPP delivered.`;
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

// Live traffic dashboard — refreshed at ~5Hz from animate()
function updateLiveDashboard() {
  if (!ui.liveTraffic) return;
  const t = state.traffic;
  if (!t.on && t.sentTotal === 0) {
    ui.liveTraffic.innerHTML = `<div class="muted" style="font-size:10px">
      Background traffic OFF — toggle ON above to start continuous flows.
    </div>`;
    return;
  }

  // Use simulation-amplified counts (each rendered packet represents N real packets)
  const simSent = t.simSent, simDel = t.simDelivered, simLost = t.simLost;
  const lossRate = simSent ? (simLost / simSent * 100) : 0;

  // Sliding-window stats from recent delivered-flow history
  const hist = t.history;
  const N    = Math.min(hist.length, HISTORY_RECENT_FOR_STATS);
  const recent = N > 0 ? hist.slice(hist.length - N) : [];

  let mean = 0, meanProp = 0, meanQueue = 0, meanSer = 0, jitter = 0, p95v = 0;
  if (recent.length > 0) {
    let sE = 0, sP = 0, sQ = 0, sSer = 0;
    for (const r of recent) {
      sE += r.e2eMs; sP += r.propMs; sQ += r.queueMs; sSer += r.serMs;
    }
    mean      = sE   / recent.length;
    meanProp  = sP   / recent.length;
    meanQueue = sQ   / recent.length;
    meanSer   = sSer / recent.length;

    // Jitter — mean absolute successive E2E difference (RTP-style)
    if (recent.length > 1) {
      let s = 0;
      for (let i = 1; i < recent.length; i++) {
        s += Math.abs(recent[i].e2eMs - recent[i - 1].e2eMs);
      }
      jitter = s / (recent.length - 1);
    }
    // p95
    const sorted = recent.map(r => r.e2eMs).sort((a, b) => a - b);
    p95v = sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * 0.95))];
  }

  // Goodput — recent delivered bits over the throughput window
  const now = performance.now();
  let bitsRecent = 0;
  for (let i = hist.length - 1; i >= 0; i--) {
    if (now - hist[i].deliveredAt > THROUGHPUT_WINDOW_MS) break;
    bitsRecent += hist[i].packetBits;
  }
  const goodputMbps = bitsRecent / (THROUGHPUT_WINDOW_MS / 1000) / 1e6;

  // Busiest edge ρ
  let maxLoad = 0;
  if (state.edgeLoad) {
    for (let i = 0; i < state.edgeLoad.length; i++) {
      if (state.edgeLoad[i] > maxLoad) maxLoad = state.edgeLoad[i];
    }
  }
  const congestionLabel = maxLoad < 0.30 ? 'low' :
                          maxLoad < 0.60 ? 'moderate' :
                          maxLoad < 0.85 ? 'high' : 'saturated';
  const congestionClass = maxLoad < 0.30 ? 'ok-row' :
                          maxLoad < 0.60 ? '' :
                          maxLoad < 0.85 ? 'warn-row' : 'err-row';

  const fmt = (v, d=1, unit='ms') => v > 0 ? v.toFixed(d) + ' ' + unit : '—';

  ui.liveTraffic.innerHTML = `
    <div style="font-size:10px;font-weight:700;color:var(--accent-2);margin-bottom:4px">
      Background traffic ${t.on ? '<span style="color:var(--accent-2)">● LIVE</span>' : '<span style="color:var(--ink-dim)">paused</span>'}
    </div>
    ${kv('Active flows', t.flows.length + ' / ' + t.activeCap)}
    ${kv('Packets sent (sim)',      simSent.toLocaleString())}
    ${kv('Delivered',               simDel.toLocaleString())}
    ${kv('Lost',                    simLost.toLocaleString())}
    ${kv('Packet loss',             simSent ? lossRate.toFixed(2) + ' %' : '—',
         lossRate > 4 ? 'err-row' : lossRate > 1 ? 'warn-row' : simSent ? 'ok-row' : '')}
    ${kv('Mean E2E delay',          mean > 0 ? '<b>' + mean.toFixed(1) + ' ms</b>' : '—')}
    ${kv('  Propagation',           fmt(meanProp))}
    ${kv('  Queuing',               fmt(meanQueue, 2))}
    ${kv('  Serialization',         fmt(meanSer, 2))}
    ${kv('p95 delay',               fmt(p95v))}
    ${kv('Jitter',                  fmt(jitter, 2))}
    ${kv('Goodput',                 goodputMbps > 0 ? goodputMbps.toFixed(1) + ' Mbps' : '—')}
    ${kv('Busiest link ρ',          maxLoad.toFixed(2) + ' (' + congestionLabel + ')', congestionClass)}
  `;
}

function updateAggregate() {
  const sent = state.packetsSent;
  const lost = state.packetsLost;
  const lossRate = sent ? (lost / sent * 100) : 0;

  // Dijkstra metrics
  const dSent = state.dijkstraSent;
  const dDel  = state.dijkstraDelivered;
  const dLoss = dSent - dDel;
  const dLossPct  = dSent ? (dLoss / dSent * 100) : 0;
  const dMeanHops = dDel  ? (state.dijkstraTotalHops / dDel) : 0;
  const dMeanE2E  = dDel  ? (state.dijkstraTotalMs   / dDel) : 0;
  const dMeanProp = dDel  ? (state.dijkstraTotalPropMs / dDel) : 0;
  const dMeanQ    = dDel  ? (state.dijkstraTotalQueueMs / dDel) : 0;

  // BPP metrics
  const bSent = state.bppSent;
  const bDel  = state.bppDelivered;
  const bIR   = bSent ? ((bSent - bDel) / bSent * 100) : 0;
  const bMeanHops = bDel ? (state.bppTotalHops / bDel) : 0;
  const bMeanE2E  = bDel ? (state.bppTotalMs   / bDel) : 0;
  const bMeanProp = bDel ? (state.bppTotalPropMs  / bDel) : 0;
  const bMeanQ    = bDel ? (state.bppTotalQueueMs / bDel) : 0;

  const na = '—';
  const ms = (v, d=1) => v > 0 ? v.toFixed(d) + ' ms' : na;

  const dSerEst = (dMeanE2E > 0 && dMeanProp > 0) ? dMeanE2E - dMeanProp - dMeanQ : 0;
  const bSerEst = (bMeanE2E > 0 && bMeanProp > 0) ? bMeanE2E - bMeanProp - bMeanQ : 0;

  ui.metrics.innerHTML = `
    <div style="margin-top:8px;margin-bottom:4px;font-size:10px;font-weight:700;color:var(--accent)">
      Manual · Dijkstra — ${dSent} packets
    </div>
    ${kv('Packet loss', dSent ? dLossPct.toFixed(1) + ' %' : na,
         dLossPct > 20 ? 'err-row' : dLossPct > 5 ? 'warn-row' : dSent ? 'ok-row' : '')}
    ${kv('Mean hops', dMeanHops > 0 ? dMeanHops.toFixed(1) : na)}
    ${kv('Total delay (mean)', dMeanE2E > 0 ? '<b>' + dMeanE2E.toFixed(1) + ' ms</b>' : na)}
    ${kv('  Propagation', ms(dMeanProp))}
    ${kv('  Queuing', ms(dMeanQ, 2))}
    ${kv('  Serialization', dSerEst > 0 ? dSerEst.toFixed(2) + ' ms' : na)}

    <div style="margin-top:8px;margin-bottom:4px;font-size:10px;font-weight:700;color:#ff77dd">
      Manual · BPP Paper — ${bSent} packets
    </div>
    ${kv('Interruption rate', bSent ? bIR.toFixed(1) + ' %' : na,
         bIR > 50 ? 'err-row' : bIR > 20 ? 'warn-row' : bSent ? 'ok-row' : '')}
    ${kv('Mean hops', bMeanHops > 0 ? bMeanHops.toFixed(1) : na)}
    ${kv('Total delay (mean)', bMeanE2E > 0 ? '<b>' + bMeanE2E.toFixed(1) + ' ms</b>' : na)}
    ${kv('  Propagation', ms(bMeanProp))}
    ${kv('  Queuing', ms(bMeanQ, 2))}
    ${kv('  Serialization', bSerEst > 0 ? bSerEst.toFixed(2) + ' ms' : na)}
  `;
}

// ---------------------------------------------------------------------------
// Hover tooltip + click picking
// ---------------------------------------------------------------------------

function _pickNodeAt(ev) {
  const rect = renderer.domElement.getBoundingClientRect();
  pointer.x =  ((ev.clientX - rect.left) / rect.width)  * 2 - 1;
  pointer.y = -((ev.clientY - rect.top)  / rect.height) * 2 + 1;
  raycaster.setFromCamera(pointer, camera);
  // Gateways (individual meshes) take priority since they're larger
  const targets = [...state.nodeMeshes];
  if (satInstanced) targets.push(satInstanced);
  const hits = raycaster.intersectObjects(targets, false);
  if (!hits.length) return { nid: null, rect };
  const h = hits[0];
  if (h.object === satInstanced) {
    const nid = satInstanceToNodeId[h.instanceId];
    return { nid, rect };
  }
  return { nid: h.object.userData.nodeId, rect };
}

function onPointerMove(ev) {
  const { nid, rect } = _pickNodeAt(ev);
  if (nid != null && state.nodes[nid]) {
    const n = state.nodes[nid];
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
  const { nid } = _pickNodeAt(ev);
  if (nid == null) return;
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
  // Legacy per-edge lines (in case any leftover)
  for (const l of state.renderEdges) { scene.remove(l); l.geometry.dispose(); l.material.dispose(); }
  state.renderEdges = [];
  // Batched edges
  if (edgeBatch) {
    scene.remove(edgeBatch); edgeBatch.geometry.dispose(); edgeBatch.material.dispose();
    edgeBatch = null; edgeBatchColors = null; edgeBatchBaseRGB = null;
  }
  // Gateway meshes
  for (const m of state.nodeMeshes)  { scene.remove(m); m.geometry.dispose(); m.material.dispose(); }
  state.nodeMeshes = [];
  // Instanced satellites
  if (satInstanced) {
    scene.remove(satInstanced); satInstanced.geometry.dispose(); satInstanced.material.dispose();
    satInstanced = null; satInstanceToNodeId = []; nodeIdToSatInstance = [];
  }
  // Background traffic visuals
  clearTrafficVisuals();
  // Manual route trail
  clearTrailEdges();
  if (srcMarker) { scene.remove(srcMarker); srcMarker = null; }
  if (dstMarker) { scene.remove(dstMarker); dstMarker = null; }
  clearActivePacket();
  // Reset live engine state
  if (state.traffic) {
    state.traffic.flows = [];
    state.traffic.spawnAcc = 0;
  }
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
  // Cap each gateway to its closest satellite access links. Cap scales
  // with satellite density so subsampling doesn't disconnect the graph.
  const satCount = sub.nodesRows.filter(r => r.kind !== 'gateway').length;
  const gwCap = satCount >= 3000 ? 12 :
                satCount >= 1500 ? 18 :
                satCount >= 800  ? 28 : 50;
  edgesRows = capGatewayLinks(sub.edgesRows, sub.nodesRows, gwCap);

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
  const snapTime = meta ? formatSnapshotTime(meta.base_epoch_utc) : formatSnapshotTime(state.epoch);
  ui.metaInfo.innerHTML = meta
    ? `<div>
         <span class="pill">schema ${meta.schema_version || '?'}</span>
         <span class="pill">fixed epoch</span>
       </div>
       <div style="margin-top:5px;color:var(--accent-2)">
         ${snapTime}
       </div>
       <div style="margin-top:5px">
         ${sats} satellites &middot; ${gws} gateways &middot; ${state.edges.length} links
         ${meta.isl_policy?.max_km ? ' &middot; ' + meta.isl_policy.max_km + ' km max ISL' : ''}
       </div>`
    : `${state.nodes.length} nodes &middot; ${state.edges.length} edges`;

  updateStatusOverlay();
}

// ---------------------------------------------------------------------------
// Status overlay
// ---------------------------------------------------------------------------

function updateStatusOverlay() {
  const mode  = state.routingMode === 'bpp' ? 'BPP paper routing' : 'Dijkstra';
  const snapshotTime = state.epoch ? formatSnapshotTime(state.epoch) : `epoch ${state.epochIdx}`;
  ui.status.innerHTML =
    `${state.nodes.length} nodes &middot; ${state.edges.length} links &middot; ${mode} &middot; ${snapshotTime}`;
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
    // Prefer gateway-to-gateway pairs (more realistic flows). Retry until
    // we find a pair Dijkstra can actually route — avoids "No ISL connectivity"
    // errors when the graph is partially disconnected.
    const gws = [];
    for (let i = 0; i < n; i++) if (NODE_KIND[i] === 'gateway') gws.push(i);
    const pool = gws.length >= 2 ? gws : Array.from({length: n}, (_, i) => i);

    let a = -1, b = -1;
    for (let tries = 0; tries < 30; tries++) {
      a = pool[Math.floor(Math.random() * pool.length)];
      b = pool[Math.floor(Math.random() * pool.length)];
      if (a === b) continue;
      if (dijkstra(a, b)) { setSource(a); setDest(b); return; }
    }
    // Last resort — just use any pair, even if no path
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

  for (const el of [ui.packetSize, ui.linkRate, ui.gatewayRate]) {
    if (!el) continue;
    el.addEventListener('change', refreshPath);
    el.addEventListener('input',  refreshPath);
  }

  ui.launch.addEventListener('click', launchPacket);
  ui.launchBurst.addEventListener('click', () => launchBurst(20));
  ui.stop.addEventListener('click', stopAll);
  if (ui.compare) ui.compare.addEventListener('click', runComparison);

  if (ui.fileInput) {
    ui.fileInput.addEventListener('change', ev => {
      handleFileInput(ev.target.files); ev.target.value = '';
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
        ui.status.textContent = 'snapshot data not loaded yet';
      }
    });
  }

  // Background traffic
  if (ui.trafficOn) {
    ui.trafficOn.addEventListener('change', () => {
      state.traffic.on = !!ui.trafficOn.checked;
      if (!state.traffic.on) {
        // Pause: hide flows but keep stats
        state.traffic.flows = [];
        if (state.edgeRecentBits) state.edgeRecentBits.fill(0);
        if (state.edgeLoad) state.edgeLoad.fill(0);
        if (trafficInstanced) {
          for (let i = 0; i < TRAFFIC_MAX_RENDER; i++) {
            _tmpMat.makeScale(0, 0, 0);
            trafficInstanced.setMatrixAt(i, _tmpMat);
          }
          trafficInstanced.instanceMatrix.needsUpdate = true;
        }
      }
      updateAggregate();
    });
  }
  function applyTrafficPreset() {
    const t = state.traffic;
    const preset = TRAFFIC_PRESETS[t.intensity];
    if (preset) {
      t.demandMultiplier = preset.demand;
      t.activeCap        = preset.cap;
    } else {
      t.demandMultiplier = trafficDemand();
      t.activeCap        = Math.min(600, Math.max(80, Math.round(t.customRate * 6)));
    }
  }
  if (ui.trafficIntensity) {
    ui.trafficIntensity.addEventListener('change', () => {
      state.traffic.intensity = ui.trafficIntensity.value;
      ui.trafficCustomRow.style.display =
        state.traffic.intensity === 'custom' ? 'block' : 'none';
      applyTrafficPreset();
    });
  }
  if (ui.trafficRate) {
    ui.trafficRate.addEventListener('input', () => {
      state.traffic.customRate = Number(ui.trafficRate.value);
      ui.trafficRateVal.textContent = state.traffic.customRate + '/s';
      if (state.traffic.intensity === 'custom') applyTrafficPreset();
    });
  }
  // Initial preset application
  applyTrafficPreset();

  // Reset heatmap
  if (ui.resetHeatmap) {
    ui.resetHeatmap.addEventListener('click', () => {
      state.linkLoad.clear();
      state.packetsSent = 0; state.packetsDelivered = 0; state.packetsLost = 0;
      state.totalLatencyMs = 0; state.bppSent = 0; state.bppDelivered = 0;
      state.dijkstraSent = 0; state.dijkstraDelivered = 0;
      state.dijkstraTotalMs = 0; state.dijkstraTotalPropMs = 0;
      state.dijkstraTotalQueueMs = 0; state.dijkstraTotalHops = 0;
      state.bppTotalMs = 0; state.bppTotalPropMs = 0;
      state.bppTotalQueueMs = 0; state.bppTotalHops = 0;
      state.traffic.sentTotal = 0;
      state.traffic.deliveredTotal = 0;
      state.traffic.lostTotal = 0;
      state.traffic.history = [];
      state.traffic.simSent = 0;
      state.traffic.simDelivered = 0;
      state.traffic.simLost = 0;
      state.traffic.simBitsDelivered = 0;
      if (state.edgeLoad) state.edgeLoad.fill(0);
      if (state.edgeRecentBits) state.edgeRecentBits.fill(0);
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

let liveDashAcc = 0;

function animate() {
  const now = performance.now();
  const dt  = Math.min(now - lastTs, 100);
  lastTs = now;

  stepActivePacket(dt);
  stepTrafficFlows(dt);

  // Throttle live dashboard updates to ~5 Hz
  liveDashAcc += dt;
  if (liveDashAcc > 200) {
    liveDashAcc = 0;
    updateLiveDashboard();
  }

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
  wireUi();
  updateAggregate();
  updateLiveDashboard();
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
    ui.status.textContent = 'snapshot unavailable';
  }

  animate();
}

main();
