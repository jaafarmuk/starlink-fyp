// Interactive Starlink snapshot visualizer.
//
// Loads the snapshot_nodes.csv / snapshot_edges.csv produced by
// tools/tle_to_snapshot.py, renders a 3D Earth with satellites and ISL /
// access links, and lets the user inject traffic.
//
// Routing uses Dijkstra on propagation delay (matches the ns-3 scenario's
// weighted shortest-path oracle). Per-hop serialization and queueing are
// estimated analytically from the UI inputs — this tool is for teaching /
// what-if exploration, not a replacement for the FlowMonitor run.
//
// Controls:
//   - source / dest: pick any node (satellite or gateway)
//   - packet size + link rate + queue size + offered load -> per-hop cost
//   - "Send packet" animates a single packet hop-by-hop
//   - "Steady load" keeps injecting at the configured packets-per-second
//
// A single per-link loss model is applied (configurable). Packets that
// are "dropped" do not reach the destination — they are shown fading red.

import * as THREE from 'three';
import { OrbitControls } from 'three/addons/controls/OrbitControls.js';

const EARTH_RADIUS_KM = 6378.137;
const C_KM_S = 299792.458;
const ECEF_SCENE_SCALE = 1.0 / 1000.0;   // 1 unit = 1000 km
const C_SCENE_S = C_KM_S * ECEF_SCENE_SCALE; // scene units / real second

const NODES_URL = '../../results/snapshot_nodes.csv';
const EDGES_URL = '../../results/snapshot_edges.csv';
const META_URL  = '../../results/snapshot_meta.json';

const state = {
  nodes: [],          // {id, name, kind, shellId, planeId, ecef:[x,y,z], lat, lon}
  edges: [],          // {u, v, distanceKm, delayMs, kind, shellId}
  adj: [],            // [{v, delayMs, kind, edgeIndex}]
  renderEdges: [],    // THREE.Line objects
  nodeMeshes: [],     // THREE.Mesh satellite/gateway dots
  nodeLabels: [],     // optional text (hidden by default)
  source: null,
  dest: null,
  pickMode: null,     // 'src' | 'dst' | null
  timeScale: 200,     // screen speed-up
  steadyTimer: null,
  packetsSent: 0,
  packetsDelivered: 0,
  packetsLost: 0,
  totalLatencyMs: 0.0,
};

const ui = {
  src: document.getElementById('src-select'),
  dst: document.getElementById('dst-select'),
  pickSrc: document.getElementById('pick-src'),
  pickDst: document.getElementById('pick-dst'),
  packetSize: document.getElementById('packet-size'),
  linkRate: document.getElementById('link-rate'),
  accessRate: document.getElementById('access-rate'),
  queueSize: document.getElementById('queue-size'),
  lossProb: document.getElementById('loss-probability'),
  launch: document.getElementById('launch'),
  launchBurst: document.getElementById('launch-burst'),
  steady: document.getElementById('steady'),
  stop: document.getElementById('stop'),
  showIsl: document.getElementById('show-isl'),
  showAccess: document.getElementById('show-access'),
  autoRotate: document.getElementById('auto-rotate'),
  timeScale: document.getElementById('time-scale'),
  timeScaleLabel: document.getElementById('time-scale-label'),
  metaInfo: document.getElementById('meta-info'),
  flowSummary: document.getElementById('flow-summary'),
  hoplist: document.getElementById('hoplist'),
  metrics: document.getElementById('metrics'),
  status: document.getElementById('status'),
  tooltip: document.getElementById('tooltip'),
  scene: document.getElementById('scene'),
};

// ---------------------------------------------------------------------------
// CSV parsing (tolerant to optional schema_version preamble)
// ---------------------------------------------------------------------------

function parseCsv(text) {
  const lines = text.split(/\r?\n/).filter(l => l.length > 0);
  if (!lines.length) return [];
  let start = 0;
  if (lines[0].startsWith('schema_version=')) start = 1;
  const header = lines[start].split(',');
  const out = [];
  for (let i = start + 1; i < lines.length; ++i) {
    const cells = lines[i].split(',');
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
  const resp = await fetch(url);
  if (!resp.ok) return null;
  return resp.json();
}

// ---------------------------------------------------------------------------
// Three.js scene
// ---------------------------------------------------------------------------

let scene, camera, renderer, controls;
let earth, starfield;
const raycaster = new THREE.Raycaster();
const pointer = new THREE.Vector2();

function initScene() {
  scene = new THREE.Scene();
  scene.background = new THREE.Color(0x04070b);

  const w = ui.scene.clientWidth, h = ui.scene.clientHeight;
  camera = new THREE.PerspectiveCamera(45, w / h, 0.1, 5000);
  camera.position.set(0, 0, 25);

  renderer = new THREE.WebGLRenderer({ antialias: true });
  renderer.setPixelRatio(Math.min(devicePixelRatio || 1, 2));
  renderer.setSize(w, h);
  ui.scene.appendChild(renderer.domElement);

  controls = new OrbitControls(camera, renderer.domElement);
  controls.enableDamping = true;
  controls.dampingFactor = 0.08;
  controls.minDistance = 8;
  controls.maxDistance = 200;

  // Earth: wireframe sphere + subtle fill so we read depth
  const R = EARTH_RADIUS_KM * ECEF_SCENE_SCALE;
  const earthGeo = new THREE.SphereGeometry(R, 48, 36);
  const earthMat = new THREE.MeshBasicMaterial({
    color: 0x0a1a2f, transparent: true, opacity: 0.85
  });
  earth = new THREE.Mesh(earthGeo, earthMat);
  scene.add(earth);

  const wire = new THREE.LineSegments(
    new THREE.WireframeGeometry(new THREE.SphereGeometry(R, 24, 16)),
    new THREE.LineBasicMaterial({ color: 0x1b3650, transparent: true, opacity: 0.35 })
  );
  scene.add(wire);

  // Equator + prime meridian emphasis
  const eqPts = [];
  for (let i = 0; i <= 128; ++i) {
    const t = (i / 128) * Math.PI * 2;
    eqPts.push(new THREE.Vector3(R * Math.cos(t), R * Math.sin(t), 0));
  }
  const equator = new THREE.Line(
    new THREE.BufferGeometry().setFromPoints(eqPts),
    new THREE.LineBasicMaterial({ color: 0x2d5c86 })
  );
  scene.add(equator);

  // Starfield
  const starGeo = new THREE.BufferGeometry();
  const positions = [];
  for (let i = 0; i < 1800; ++i) {
    const r = 600;
    const theta = Math.random() * Math.PI * 2;
    const phi = Math.acos(2 * Math.random() - 1);
    positions.push(
      r * Math.sin(phi) * Math.cos(theta),
      r * Math.sin(phi) * Math.sin(theta),
      r * Math.cos(phi)
    );
  }
  starGeo.setAttribute('position', new THREE.Float32BufferAttribute(positions, 3));
  starfield = new THREE.Points(
    starGeo,
    new THREE.PointsMaterial({ color: 0x446688, size: 1.5, sizeAttenuation: false })
  );
  scene.add(starfield);

  window.addEventListener('resize', onResize);
  renderer.domElement.addEventListener('pointermove', onPointerMove);
  renderer.domElement.addEventListener('click', onClick);
}

function onResize() {
  const w = ui.scene.clientWidth, h = ui.scene.clientHeight;
  camera.aspect = w / h;
  camera.updateProjectionMatrix();
  renderer.setSize(w, h);
}

function vec3OfNode(n) {
  return new THREE.Vector3(
    n.ecef[0] * ECEF_SCENE_SCALE,
    n.ecef[1] * ECEF_SCENE_SCALE,
    n.ecef[2] * ECEF_SCENE_SCALE
  );
}

// ---------------------------------------------------------------------------
// Build scene from CSVs
// ---------------------------------------------------------------------------

function buildGraph() {
  state.adj = Array.from({ length: state.nodes.length }, () => []);
  for (let i = 0; i < state.edges.length; ++i) {
    const e = state.edges[i];
    state.adj[e.u].push({ v: e.v, delayMs: e.delayMs, kind: e.kind, edgeIndex: i });
    state.adj[e.v].push({ v: e.u, delayMs: e.delayMs, kind: e.kind, edgeIndex: i });
  }
}

const EDGE_COLORS = {
  intra_plane: 0x9ad8ff,
  inter_plane: 0xffe08a,
  access: 0xff7171,
  unknown: 0x888888,
};

function addSceneObjects() {
  // Edges
  for (let i = 0; i < state.edges.length; ++i) {
    const e = state.edges[i];
    const a = vec3OfNode(state.nodes[e.u]);
    const b = vec3OfNode(state.nodes[e.v]);
    const geo = new THREE.BufferGeometry().setFromPoints([a, b]);
    const color = EDGE_COLORS[e.kind] || EDGE_COLORS.unknown;
    const mat = new THREE.LineBasicMaterial({ color, transparent: true, opacity: 0.5 });
    const line = new THREE.Line(geo, mat);
    line.userData.edgeIndex = i;
    line.userData.kind = e.kind;
    state.renderEdges.push(line);
    scene.add(line);
  }

  // Nodes
  for (const n of state.nodes) {
    const isGw = n.kind === 'gateway';
    const geo = new THREE.SphereGeometry(isGw ? 0.12 : 0.07, 12, 12);
    const mat = new THREE.MeshBasicMaterial({
      color: isGw ? 0xff4763 : 0x8affc1
    });
    const m = new THREE.Mesh(geo, mat);
    m.position.copy(vec3OfNode(n));
    m.userData.nodeId = n.id;
    state.nodeMeshes.push(m);
    scene.add(m);
  }
}

function populateSelects() {
  const sats = state.nodes.filter(n => n.kind !== 'gateway');
  const gws = state.nodes.filter(n => n.kind === 'gateway');
  const combined = [...gws, ...sats];
  for (const sel of [ui.src, ui.dst]) {
    sel.innerHTML = '';
    for (const n of combined) {
      const opt = document.createElement('option');
      opt.value = String(n.id);
      opt.textContent = `${n.id} — ${n.name} [${n.kind}]`;
      sel.appendChild(opt);
    }
  }
  if (combined.length >= 2) {
    ui.src.value = String(combined[0].id);
    ui.dst.value = String(combined[combined.length - 1].id);
    setSource(combined[0].id);
    setDest(combined[combined.length - 1].id);
  }
}

// ---------------------------------------------------------------------------
// Selection highlighting
// ---------------------------------------------------------------------------

let srcMarker, dstMarker;
function setSource(id) {
  state.source = id;
  ui.src.value = String(id);
  updateMarker('src', id);
  refreshPath();
}
function setDest(id) {
  state.dest = id;
  ui.dst.value = String(id);
  updateMarker('dst', id);
  refreshPath();
}

function updateMarker(role, id) {
  const n = state.nodes[id];
  if (!n) return;
  const pos = vec3OfNode(n).multiplyScalar(1.0);
  if (role === 'src') {
    if (!srcMarker) {
      const g = new THREE.SphereGeometry(0.2, 16, 16);
      const m = new THREE.MeshBasicMaterial({ color: 0xffffff, transparent: true, opacity: 0.9 });
      srcMarker = new THREE.Mesh(g, m);
      scene.add(srcMarker);
    }
    srcMarker.position.copy(pos);
  } else {
    if (!dstMarker) {
      const g = new THREE.SphereGeometry(0.2, 16, 16);
      const m = new THREE.MeshBasicMaterial({ color: 0xffffff, transparent: true, opacity: 0.9 });
      dstMarker = new THREE.Mesh(g, m);
      scene.add(dstMarker);
    }
    dstMarker.position.copy(pos);
  }
}

let pathLine = null;
function refreshPath() {
  if (state.source == null || state.dest == null) return;
  const r = dijkstra(state.source, state.dest);
  if (!r) {
    ui.flowSummary.textContent = 'no path between source and destination';
    ui.hoplist.innerHTML = '';
    if (pathLine) { scene.remove(pathLine); pathLine = null; }
    return;
  }
  const pts = r.path.map(id => vec3OfNode(state.nodes[id]));
  if (pathLine) scene.remove(pathLine);
  pathLine = new THREE.Line(
    new THREE.BufferGeometry().setFromPoints(pts),
    new THREE.LineBasicMaterial({ color: 0xffffff, transparent: true, opacity: 0.9 })
  );
  scene.add(pathLine);
  renderFlowSummary(r);
}

// ---------------------------------------------------------------------------
// Dijkstra on propagation delay (ms)
// ---------------------------------------------------------------------------

function dijkstra(src, dst) {
  const N = state.nodes.length;
  const dist = new Float64Array(N); dist.fill(Infinity);
  const prev = new Int32Array(N); prev.fill(-1);
  const prevEdge = new Int32Array(N); prevEdge.fill(-1);
  dist[src] = 0;
  // Simple binary heap
  const heap = [];
  const push = (d, u) => { heap.push([d, u]); heap.sort((a, b) => a[0] - b[0]); };
  push(0, src);
  while (heap.length) {
    const [d, u] = heap.shift();
    if (d > dist[u]) continue;
    if (u === dst) break;
    for (const { v, delayMs, edgeIndex } of state.adj[u]) {
      const nd = d + delayMs;
      if (nd < dist[v]) {
        dist[v] = nd; prev[v] = u; prevEdge[v] = edgeIndex;
        push(nd, v);
      }
    }
  }
  if (!isFinite(dist[dst])) return null;
  const path = [];
  const edgeIdxPath = [];
  let u = dst;
  while (u !== -1) {
    path.unshift(u);
    if (prevEdge[u] !== -1) edgeIdxPath.unshift(prevEdge[u]);
    u = prev[u];
  }
  return { path, edgeIdxPath, totalPropMs: dist[dst] };
}

// ---------------------------------------------------------------------------
// Hop-by-hop metrics using UI inputs
// ---------------------------------------------------------------------------

function readParams() {
  return {
    packetBytes: Math.max(1, Number(ui.packetSize.value)),
    islMbps: Math.max(0.001, Number(ui.linkRate.value)),
    accessMbps: Math.max(0.001, Number(ui.accessRate.value)),
    queuePkts: Math.max(1, Number(ui.queueSize.value)),
    lossProb: Math.max(0, Math.min(1, Number(ui.lossProb.value))),
  };
}

// Approximate mean queuing delay using the M/M/1 formula rho/(1-rho) * service,
// bounded above by queuePkts * servicePacketMs so that the reported delay never
// exceeds what a finite buffer of `queuePkts` can hold. This is intentionally
// NOT a full M/M/1/K solution: overflow loss is not computed here, and the
// per-hop loss input (params.lossProb) is applied independently by the caller.
// For a proper blocking probability use estimateBlockingProb() below.
function estimateQueuingMs(rho, servicePacketMs, queuePkts) {
  if (rho <= 0) return 0.0;
  const maxQueueMs = queuePkts * servicePacketMs;
  if (rho >= 1.0) return maxQueueMs;
  const meanQueueMs = (rho / (1.0 - rho)) * servicePacketMs;
  return Math.min(meanQueueMs, maxQueueMs);
}

// M/M/1/K blocking probability (packet loss due to finite buffer K).
// K is the total system capacity (queue slots + server). Returns PB in [0,1].
function estimateBlockingProb(rho, K) {
  if (K <= 0) return 1.0;
  if (rho <= 0) return 0.0;
  if (Math.abs(rho - 1.0) < 1e-9) return 1.0 / (K + 1);
  const num = (1.0 - rho) * Math.pow(rho, K);
  const den = 1.0 - Math.pow(rho, K + 1);
  return den > 0 ? num / den : 1.0;
}

function hopCostsForPath(path, edgeIdxPath, params) {
  const rows = [];
  let totalMs = 0.0;
  let totalPropMs = 0.0;
  let totalSerMs = 0.0;
  let totalQueueMs = 0.0;

  // TCP BulkSend saturates the path: the achievable rate is bounded by the
  // slowest link along the path (usually the access link, if any).
  let bottleneckBps = Infinity;
  for (let i = 0; i < edgeIdxPath.length; ++i) {
    const e = state.edges[edgeIdxPath[i]];
    const rate = (e.kind === 'access' ? params.accessMbps : params.islMbps) * 1e6;
    if (rate < bottleneckBps) bottleneckBps = rate;
  }
  if (!isFinite(bottleneckBps)) bottleneckBps = params.islMbps * 1e6;

  const offeredBps = bottleneckBps;

  for (let i = 0; i < edgeIdxPath.length; ++i) {
    const e = state.edges[edgeIdxPath[i]];
    const u = path[i], v = path[i + 1];
    const isAccess = e.kind === 'access';
    const rateMbps = isAccess ? params.accessMbps : params.islMbps;
    const rateBps = rateMbps * 1e6;
    const serMs = (params.packetBytes * 8.0) / rateBps * 1000.0;
    // Every hop on a TCP saturating flow sees the same bottleneck offered
    // rate, so upstream links below the bottleneck have rho < 1 (correctly).
    const rho = Math.min(0.999, offeredBps / rateBps);
    const queueMs = estimateQueuingMs(rho, serMs, params.queuePkts);
    const propMs = e.delayMs;
    const hopMs = propMs + serMs + queueMs;

    totalMs += hopMs;
    totalPropMs += propMs;
    totalSerMs += serMs;
    totalQueueMs += queueMs;

    rows.push({
      hop: i + 1,
      u, v,
      edgeKind: e.kind,
      distanceKm: e.distanceKm,
      propMs, serMs, queueMs, hopMs,
      rho,
      rateMbps,
    });
  }
  return { rows, totalMs, totalPropMs, totalSerMs, totalQueueMs };
}

function renderFlowSummary(dijkstraResult) {
  const params = readParams();
  const costs = hopCostsForPath(dijkstraResult.path, dijkstraResult.edgeIdxPath, params);
  const hops = dijkstraResult.path.length - 1;
  // Bottleneck along the path: min(rate) over edges. TCP BulkSend saturates
  // this; we report it as the effective offered rate.
  let bottleneckMbps = Infinity;
  for (const ei of dijkstraResult.edgeIdxPath) {
    const e = state.edges[ei];
    const r = e.kind === 'access' ? params.accessMbps : params.islMbps;
    if (r < bottleneckMbps) bottleneckMbps = r;
  }
  if (!isFinite(bottleneckMbps)) bottleneckMbps = params.islMbps;
  const offeredMbps = bottleneckMbps;

  ui.flowSummary.innerHTML = `
    <div class="kv"><span>Transport</span><span>TCP</span></div>
    <div class="kv"><span>Hops</span><span>${hops}</span></div>
    <div class="kv"><span>Propagation</span><span>${costs.totalPropMs.toFixed(2)} ms</span></div>
    <div class="kv"><span>Serialization</span><span>${costs.totalSerMs.toFixed(3)} ms</span></div>
    <div class="kv"><span>Queuing (est)</span><span>${costs.totalQueueMs.toFixed(3)} ms</span></div>
    <div class="kv"><span>Total one-way</span><span><b>${costs.totalMs.toFixed(2)} ms</b></span></div>
    <div class="kv"><span>Offered rate</span><span>${offeredMbps.toFixed(3)} Mbps (TCP saturating)</span></div>
  `;

  ui.hoplist.innerHTML = costs.rows.map(r => `
    <div class="hop">
      <b>hop ${r.hop}</b> node ${r.u} → node ${r.v} &middot; ${r.edgeKind}<br/>
      ${r.distanceKm.toFixed(1)} km &middot;
      prop ${r.propMs.toFixed(2)}ms &middot;
      ser ${r.serMs.toFixed(3)}ms &middot;
      queue ${r.queueMs.toFixed(3)}ms<br/>
      load ρ=${(r.rho * 100).toFixed(1)}% @ ${r.rateMbps.toFixed(1)} Mbps
    </div>
  `).join('');
}

// ---------------------------------------------------------------------------
// Packet animation
// ---------------------------------------------------------------------------

const activePackets = [];

function launchPacket() {
  if (state.source == null || state.dest == null) return;
  const result = dijkstra(state.source, state.dest);
  if (!result) return;

  const params = readParams();
  const costs = hopCostsForPath(result.path, result.edgeIdxPath, params);
  const hops = result.path.length - 1;

  // Evaluate per-link loss
  let dropIndex = -1;
  for (let i = 0; i < hops; ++i) {
    if (Math.random() < params.lossProb) { dropIndex = i; break; }
  }

  const color = dropIndex === -1 ? 0x4ad6ff : 0xff6a6a;
  const mat = new THREE.MeshBasicMaterial({ color });
  const geo = new THREE.SphereGeometry(0.09, 10, 10);
  const mesh = new THREE.Mesh(geo, mat);
  mesh.position.copy(vec3OfNode(state.nodes[result.path[0]]));
  scene.add(mesh);

  activePackets.push({
    mesh,
    path: result.path,
    edgeIdxPath: result.edgeIdxPath,
    costs,
    hopIndex: 0,
    hopProgress: 0.0,
    dropIndex,
    startedAt: performance.now(),
    realTotalMs: costs.totalMs,
    color,
  });

  state.packetsSent += 1;
  updateAggregate();
}

function launchBurst(n = 50) {
  for (let i = 0; i < n; ++i) setTimeout(launchPacket, i * 40);
}

function startSteadyLoad() {
  stopSteadyLoad();
  // Steady-load demo: fixed animation rate. This drives only the on-screen
  // packet animation pace — the actual TCP bottleneck math is in readParams().
  const STEADY_PPS = 50;
  const STEADY_INTERVAL_MS = 1000.0 / STEADY_PPS;
  state.steadyTimer = setInterval(() => { launchPacket(); },
                                  STEADY_INTERVAL_MS);
  ui.status.textContent = `steady load: ${STEADY_PPS} pps (animation)`;
}
function stopSteadyLoad() {
  if (state.steadyTimer) clearInterval(state.steadyTimer);
  state.steadyTimer = null;
  ui.status.textContent = `steady load: stopped`;
}

function stepPackets(dtMs) {
  const realDtMs = dtMs * state.timeScale;
  for (let i = activePackets.length - 1; i >= 0; --i) {
    const p = activePackets[i];
    const hop = p.costs.rows[p.hopIndex];
    if (!hop) { cleanupPacket(p, i, 'delivered'); continue; }

    const hopMs = hop.hopMs;
    p.hopProgress += realDtMs / Math.max(0.001, hopMs);
    if (p.hopProgress >= 1.0) {
      p.hopProgress = 0.0;
      p.hopIndex += 1;
      if (p.dropIndex !== -1 && p.hopIndex > p.dropIndex) {
        p.mesh.material.color.setHex(0xff6a6a);
        p.mesh.material.transparent = true;
        p.mesh.material.opacity = 0.5;
        cleanupPacket(p, i, 'lost');
        continue;
      }
      if (p.hopIndex >= p.path.length - 1) {
        cleanupPacket(p, i, 'delivered');
        continue;
      }
    }

    // position interpolation
    const from = vec3OfNode(state.nodes[p.path[p.hopIndex]]);
    const to = vec3OfNode(state.nodes[p.path[p.hopIndex + 1]]);
    p.mesh.position.copy(from.clone().lerp(to, p.hopProgress));
  }
}

function cleanupPacket(p, idx, outcome) {
  scene.remove(p.mesh);
  p.mesh.geometry.dispose();
  p.mesh.material.dispose();
  activePackets.splice(idx, 1);
  if (outcome === 'delivered') {
    state.packetsDelivered += 1;
    state.totalLatencyMs += p.realTotalMs;
  } else {
    state.packetsLost += 1;
  }
  updateAggregate();
}

function updateAggregate() {
  const delivered = state.packetsDelivered;
  const lost = state.packetsLost;
  const sent = state.packetsSent;
  const deliveryPct = sent ? (delivered / sent * 100) : 0;
  const meanMs = delivered ? (state.totalLatencyMs / delivered) : 0;
  ui.metrics.innerHTML = `
    <div class="kv"><span>Sent</span><span>${sent}</span></div>
    <div class="kv"><span>Delivered</span><span>${delivered}</span></div>
    <div class="kv"><span>Lost</span><span>${lost}</span></div>
    <div class="kv"><span>Delivery</span><span>${deliveryPct.toFixed(2)} %</span></div>
    <div class="kv"><span>Mean E2E</span><span>${meanMs.toFixed(2)} ms</span></div>
  `;
}

// ---------------------------------------------------------------------------
// Pointer / picking
// ---------------------------------------------------------------------------

function onPointerMove(ev) {
  const rect = renderer.domElement.getBoundingClientRect();
  pointer.x = ((ev.clientX - rect.left) / rect.width) * 2 - 1;
  pointer.y = -((ev.clientY - rect.top) / rect.height) * 2 + 1;

  raycaster.setFromCamera(pointer, camera);
  const hits = raycaster.intersectObjects(state.nodeMeshes, false);
  if (hits.length) {
    const nid = hits[0].object.userData.nodeId;
    const n = state.nodes[nid];
    ui.tooltip.style.display = 'block';
    ui.tooltip.style.left = (ev.clientX - rect.left + 12) + 'px';
    ui.tooltip.style.top = (ev.clientY - rect.top + 12) + 'px';
    ui.tooltip.innerHTML =
      `<b>${n.name}</b><br/>id ${n.id} &middot; ${n.kind}<br/>` +
      `shell ${n.shellId} &middot; plane ${n.planeId}<br/>` +
      `lat ${n.lat.toFixed(2)}° lon ${n.lon.toFixed(2)}°`;
  } else {
    ui.tooltip.style.display = 'none';
  }
}

function onClick(ev) {
  if (!state.pickMode) return;
  const rect = renderer.domElement.getBoundingClientRect();
  pointer.x = ((ev.clientX - rect.left) / rect.width) * 2 - 1;
  pointer.y = -((ev.clientY - rect.top) / rect.height) * 2 + 1;
  raycaster.setFromCamera(pointer, camera);
  const hits = raycaster.intersectObjects(state.nodeMeshes, false);
  if (!hits.length) return;
  const nid = hits[0].object.userData.nodeId;
  if (state.pickMode === 'src') setSource(nid);
  else setDest(nid);
  state.pickMode = null;
  ui.pickSrc.classList.remove('secondary-active');
  ui.pickDst.classList.remove('secondary-active');
}

// ---------------------------------------------------------------------------
// UI wiring
// ---------------------------------------------------------------------------

function wireUi() {
  ui.src.addEventListener('change', () => setSource(Number(ui.src.value)));
  ui.dst.addEventListener('change', () => setDest(Number(ui.dst.value)));
  ui.pickSrc.addEventListener('click', () => { state.pickMode = 'src'; ui.status.textContent = 'click a node to pick source'; });
  ui.pickDst.addEventListener('click', () => { state.pickMode = 'dst'; ui.status.textContent = 'click a node to pick destination'; });
  ui.launch.addEventListener('click', launchPacket);
  ui.launchBurst.addEventListener('click', () => launchBurst(50));
  ui.steady.addEventListener('click', startSteadyLoad);
  ui.stop.addEventListener('click', stopSteadyLoad);

  ui.showIsl.addEventListener('change', refreshEdgeVisibility);
  ui.showAccess.addEventListener('change', refreshEdgeVisibility);
  ui.autoRotate.addEventListener('change', () => {
    controls.autoRotate = ui.autoRotate.checked;
    controls.autoRotateSpeed = 0.6;
  });

  ui.timeScale.addEventListener('input', () => {
    state.timeScale = Number(ui.timeScale.value);
    ui.timeScaleLabel.textContent = `${state.timeScale}x`;
  });

  for (const el of [ui.packetSize, ui.linkRate, ui.accessRate,
                    ui.queueSize, ui.lossProb]) {
    el.addEventListener('change', refreshPath);
    el.addEventListener('input', refreshPath);
  }
}

function refreshEdgeVisibility() {
  for (const line of state.renderEdges) {
    const k = line.userData.kind;
    const isAccess = k === 'access';
    line.visible = isAccess ? ui.showAccess.checked : ui.showIsl.checked;
  }
}

// ---------------------------------------------------------------------------
// Animation loop
// ---------------------------------------------------------------------------

let lastTs = performance.now();
function animate() {
  const now = performance.now();
  const dt = now - lastTs;
  lastTs = now;
  stepPackets(dt);
  controls.update();
  renderer.render(scene, camera);
  requestAnimationFrame(animate);
}

// ---------------------------------------------------------------------------
// Bootstrap
// ---------------------------------------------------------------------------

async function main() {
  try {
    ui.status.textContent = 'loading snapshot CSVs…';
    const [nodesRows, edgesRows, meta] = await Promise.all([
      fetchCsv(NODES_URL),
      fetchCsv(EDGES_URL),
      fetchJson(META_URL),
    ]);

    state.nodes = nodesRows.map(r => {
      const hasEcef = r.ecef_x_km != null
                   || r.ecef_y_km != null
                   || r.ecef_z_km != null;
      const hasEci = r.eci_x_km != null
                  || r.eci_y_km != null
                  || r.eci_z_km != null;
      let x = 0, y = 0, z = 0;
      if (hasEcef) {
        x = Number(r.ecef_x_km ?? 0);
        y = Number(r.ecef_y_km ?? 0);
        z = Number(r.ecef_z_km ?? 0);
      } else if (hasEci) {
        // v1 schema only wrote eci_*; approximate layout using ECI as-if-ECEF.
        x = Number(r.eci_x_km ?? 0);
        y = Number(r.eci_y_km ?? 0);
        z = Number(r.eci_z_km ?? 0);
      } else {
        x = Number(r.x_km ?? 0);
        y = Number(r.y_km ?? 0);
        z = Number(r.z_km ?? 0);
      }
      return {
        id: Number(r.id),
        name: r.name,
        kind: r.kind || 'satellite',
        shellId: Number(r.shell_id ?? -1),
        planeId: Number(r.plane_id ?? -1),
        ecef: [x, y, z],
        ecefIsActuallyEci: !hasEcef && hasEci,
        lat: Number(r.lat_deg ?? 0),
        lon: Number(r.lon_deg ?? 0),
        altKm: Number(r.altitude_km ?? 0),
      };
    });
    // Warn if the snapshot lacked ECEF and we had to fall back.
    if (state.nodes.length > 0 && state.nodes.every(n => n.ecefIsActuallyEci)) {
      const msg = document.createElement('div');
      msg.className = 'banner warn';
      msg.textContent = 'snapshot uses v1 schema (no ECEF). Earth layout is ' +
        'approximated from ECI coordinates. Regenerate the snapshot with the ' +
        'new generator for accurate geodetic placement.';
      ui.metaInfo.parentNode.insertBefore(msg, ui.metaInfo);
    }
    state.edges = edgesRows.map(r => ({
      u: Number(r.u),
      v: Number(r.v),
      distanceKm: Number(r.distance_km),
      delayMs: Number(r.delay_ms),
      kind: r.kind || 'unknown',
      shellId: Number(r.shell_id ?? -1),
    }));

    buildGraph();
    initScene();
    addSceneObjects();
    populateSelects();
    wireUi();
    refreshPath();

    ui.metaInfo.innerHTML = meta
      ? `<div class="pill">schema ${meta.schema_version || '?'}</div>
         <div class="pill">${meta.base_epoch_utc || ''}</div>
         <div class="pill">${meta.epoch_policy || ''}</div>
         <div style="margin-top:6px">
           ${state.nodes.length} nodes &middot; ${state.edges.length} edges
           &middot; ${meta.isl_policy?.max_km ?? '?'} km max ISL
         </div>`
      : `${state.nodes.length} nodes &middot; ${state.edges.length} edges`;

    ui.status.textContent = 'ready';
    animate();
  } catch (err) {
    console.error(err);
    ui.status.textContent = 'load error: ' + err.message;
    ui.metaInfo.innerHTML = `<div class="banner warn">
      Could not load snapshot files. Make sure you served this from the repo
      root (e.g. <code>python -m http.server</code> in the repo root) and
      that <code>results/snapshot_nodes.csv</code> exists. Error: ${err.message}
    </div>`;
  }
}

main();
