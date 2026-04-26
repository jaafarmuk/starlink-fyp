// Starlink snapshot scenario for ns-3.
//
// Design notes:
//
//   * Each P2P link's per-interface metric is set proportional to its
//     propagation delay (ms*100, clamped to [1, 65535]) so global routing
//     actually minimizes DELAY, not hop count. For LEO this matters: a
//     2x0.3 ms intra-plane pair beats a 16 ms cross-shell single hop.
//   * Flow endpoints use each node's first non-loopback interface address
//     as a stable "primary". (An earlier attempt put a 7.x.x.x /8 on the
//     loopback, but ns-3's GlobalRouter skips loopback when emitting LSAs,
//     so those addresses had no route and silently black-holed traffic.)
//   * hop_count_unweighted is counted along the SAME min-delay path that
//     ns-3 routes over, so `hops` and `shortest_delay_ms` describe the
//     same route.
//   * Transport is TCP-only (BulkSend). Starlink user traffic is TCP/QUIC
//     in practice; modelling loss/jitter under realistic congestion control
//     is more meaningful than a raw UDP CBR probe.
//   * Throughput, goodput, and utilization are distinct. There is no
//     explicit "offered load" for TCP because BulkSend saturates the path.
//   * IPv4 allocation uses an explicit /30 subnet allocator that fails
//     loudly when exhausted instead of silently wrapping.
//   * NetAnim positions come from the node CSV (ECEF equirectangular) so
//     the animation matches the real constellation geometry.
//   * Traffic model is configurable: random, longest-path, nearest, or
//     gateway-satellite-gateway patterns.
//   * Snapshot metadata (results/snapshot_meta.json) is checked so the
//     scenario fails if its schema version does not match.
//   * This is explicitly a frozen-time snapshot — no topology evolution
//     during a run — and that contract is printed on startup.

#include "ns3/core-module.h"
#include "ns3/network-module.h"
#include "ns3/internet-module.h"
#include "ns3/point-to-point-module.h"
#include "ns3/applications-module.h"
#include "ns3/flow-monitor-helper.h"
#include "ns3/ipv4-flow-classifier.h"
#include "ns3/ipv4-static-routing-helper.h"
#include "ns3/ipv4-list-routing-helper.h"
#include "ns3/ipv4-global-routing-helper.h"
#include "ns3/netanim-module.h"
#include "ns3/queue-size.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <fstream>
#include <limits>
#include <map>
#include <memory>
#include <queue>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

using namespace ns3;

static constexpr const char* SCHEMA_VERSION = "2.1.0";
// Older snapshot schemas that this scenario still accepts. Keep the list
// small and explicit — accepting everything silently masks real schema
// drift.
static const std::vector<std::string> ACCEPTED_SCHEMAS = {"2.0.0", "2.1.0"};

struct Edge
{
  uint32_t u;
  uint32_t v;
  double distanceKm;
  double delayMs;
  std::string kind;      // intra_plane, inter_plane, access
  int32_t shellId;
};

struct NodeInfo
{
  uint32_t id;
  std::string name;
  std::string kind;      // satellite / gateway
  int32_t shellId;
  int32_t planeId;
  double latDeg;
  double lonDeg;
  double altKm;
  double ecefX;
  double ecefY;
  double ecefZ;
};

struct AppFlow
{
  uint32_t flowIndex;
  uint32_t srcNode;
  uint32_t dstNode;
  uint16_t port;
};

struct PerFlowRow
{
  uint32_t flowIndex;
  uint32_t srcNode;
  uint32_t dstNode;
  uint16_t port;
  double goodputMbps;
  double deliveryRatioPercent;
  double tcpRetransOverheadPercent;
  double meanDelayMs;
  double meanJitterMs;
  uint32_t hopCountUnweighted;
  double shortestDelayMs;         // propagation-weighted shortest path
  uint64_t txPackets;
  uint64_t rxPackets;
  uint64_t lostPackets;
  uint64_t txBytes;
  uint64_t rxBytes;
};

// ---------------------------------------------------------------------------
// Per-device PhyTxEnd accumulator. Used to measure true on-wire byte counts
// per net device so we can report real per-link utilization (not just the
// path-summed upper bound). Declared as a free function so ns-3's
// MakeBoundCallback can bind the counter pointer and index cleanly.
// ---------------------------------------------------------------------------
static void
AccumDevTxBytes(std::vector<uint64_t>* counters, uint32_t deviceIndex,
                Ptr<const Packet> p)
{
  if (counters && deviceIndex < counters->size())
  {
    (*counters)[deviceIndex] += p->GetSize();
  }
}

// ---------------------------------------------------------------------------
// CSV helpers (tolerant of column ordering; header-driven).
// ---------------------------------------------------------------------------

// RFC-4180-lite splitter: handles double-quoted fields containing commas and
// escaped "" inside quotes. Falls back to a plain comma split when no quotes
// are present. The generator currently guarantees no commas in fields, but
// the parser is defensive so a user-provided CSV cannot silently corrupt the
// topology if a field ever needs quoting.
static std::vector<std::string> SplitCsvLine(const std::string& line)
{
  std::vector<std::string> cells;
  if (line.find('"') == std::string::npos)
  {
    std::stringstream ss(line);
    std::string cell;
    while (std::getline(ss, cell, ',')) cells.push_back(cell);
    return cells;
  }
  std::string cur;
  bool inQuote = false;
  for (size_t i = 0; i < line.size(); ++i)
  {
    char c = line[i];
    if (inQuote)
    {
      if (c == '"')
      {
        if (i + 1 < line.size() && line[i + 1] == '"') { cur.push_back('"'); ++i; }
        else inQuote = false;
      }
      else cur.push_back(c);
    }
    else
    {
      if (c == ',') { cells.push_back(cur); cur.clear(); }
      else if (c == '"' && cur.empty()) inQuote = true;
      else cur.push_back(c);
    }
  }
  cells.push_back(cur);
  return cells;
}

static std::map<std::string, size_t>
IndexHeader(const std::vector<std::string>& header)
{
  std::map<std::string, size_t> idx;
  for (size_t i = 0; i < header.size(); ++i)
  {
    idx[header[i]] = i;
  }
  return idx;
}

static std::string
GetOr(const std::vector<std::string>& row,
      const std::map<std::string, size_t>& idx,
      const std::string& col,
      const std::string& def = "")
{
  auto it = idx.find(col);
  if (it == idx.end() || it->second >= row.size()) return def;
  return row[it->second];
}

static double
StodOr(const std::string& s, double def)
{
  if (s.empty()) return def;
  try { return std::stod(s); } catch (...) { return def; }
}

static int32_t
StoiOr(const std::string& s, int32_t def)
{
  if (s.empty()) return def;
  try { return static_cast<int32_t>(std::stol(s)); } catch (...) { return def; }
}

static std::vector<Edge>
ReadEdgesCsv(const std::string& path, uint32_t& outMaxId)
{
  std::ifstream f(path);
  if (!f.is_open())
  {
    NS_FATAL_ERROR("Could not open edges CSV: " << path);
  }

  std::string line;
  if (!std::getline(f, line))
  {
    NS_FATAL_ERROR("Edges CSV is empty: " << path);
  }

  auto header = SplitCsvLine(line);
  auto idx = IndexHeader(header);

  std::vector<Edge> edges;
  uint32_t maxId = 0;
  bool anyEdge = false;

  while (std::getline(f, line))
  {
    if (line.empty()) continue;
    auto cells = SplitCsvLine(line);
    Edge e{};
    e.u = static_cast<uint32_t>(std::stoul(GetOr(cells, idx, "u")));
    e.v = static_cast<uint32_t>(std::stoul(GetOr(cells, idx, "v")));
    e.distanceKm = StodOr(GetOr(cells, idx, "distance_km"), 0.0);
    // Schema 2.1.0 canonical name is prop_delay_ms (one-way vacuum
    // propagation). Schema 2.0.0 wrote this as delay_ms, which is kept as
    // a deprecated alias. Prefer the new name, fall back to the old.
    double propDelay = StodOr(GetOr(cells, idx, "prop_delay_ms"), 0.0);
    if (propDelay <= 0.0)
    {
      propDelay = StodOr(GetOr(cells, idx, "delay_ms"), 0.0);
    }
    e.delayMs = propDelay;
    e.kind = GetOr(cells, idx, "kind", "unknown");
    e.shellId = StoiOr(GetOr(cells, idx, "shell_id"), -1);
    if (e.delayMs <= 0.0)
    {
      // Fall back to geometry if both column names are missing / empty.
      e.delayMs = (e.distanceKm / 299792.458) * 1000.0;
    }
    maxId = std::max(maxId, std::max(e.u, e.v));
    anyEdge = true;
    edges.push_back(e);
  }
  outMaxId = anyEdge ? maxId : 0;
  return edges;
}

static std::vector<NodeInfo>
ReadNodesCsv(const std::string& path)
{
  std::vector<NodeInfo> nodes;
  std::ifstream f(path);
  if (!f.is_open())
  {
    return nodes;
  }

  std::string line;
  if (!std::getline(f, line))
  {
    return nodes;
  }

  auto header = SplitCsvLine(line);
  auto idx = IndexHeader(header);

  while (std::getline(f, line))
  {
    if (line.empty()) continue;
    auto cells = SplitCsvLine(line);
    NodeInfo n{};
    try
    {
      n.id = static_cast<uint32_t>(std::stoul(GetOr(cells, idx, "id")));
    }
    catch (...)
    {
      continue;
    }
    n.name = GetOr(cells, idx, "name", "node-" + std::to_string(n.id));
    n.kind = GetOr(cells, idx, "kind", "satellite");
    n.shellId = StoiOr(GetOr(cells, idx, "shell_id"), -1);
    n.planeId = StoiOr(GetOr(cells, idx, "plane_id"), -1);
    n.latDeg = StodOr(GetOr(cells, idx, "lat_deg"), 0.0);
    n.lonDeg = StodOr(GetOr(cells, idx, "lon_deg"), 0.0);
    n.altKm = StodOr(GetOr(cells, idx, "altitude_km"), 0.0);
    n.ecefX = StodOr(GetOr(cells, idx, "ecef_x_km"), 0.0);
    n.ecefY = StodOr(GetOr(cells, idx, "ecef_y_km"), 0.0);
    n.ecefZ = StodOr(GetOr(cells, idx, "ecef_z_km"), 0.0);
    nodes.push_back(n);
  }
  return nodes;
}

static bool
CheckMetadataSchema(const std::string& path)
{
  std::ifstream f(path);
  if (!f.is_open())
  {
    std::cerr << "INFO: no snapshot_meta.json at " << path
              << " (skipping schema check)\n";
    return true;
  }
  std::stringstream ss;
  ss << f.rdbuf();
  std::string blob = ss.str();
  // Find "schema_version" then scan past the colon/whitespace to the value
  // string's opening quote, then take text up to the closing quote.
  auto keyPos = blob.find("\"schema_version\"");
  if (keyPos == std::string::npos)
  {
    std::cerr << "WARNING: snapshot_meta.json has no schema_version field.\n";
    return true;
  }
  // Skip past the key's closing quote to find the colon, then the value's
  // opening quote.  "schema_version" is 16 chars so keyPos+16 lands after it.
  auto openQuote = blob.find('"', keyPos + 16);
  if (openQuote == std::string::npos) return true;
  auto closeQuote = blob.find('"', openQuote + 1);
  if (closeQuote == std::string::npos) return true;
  std::string ver = blob.substr(openQuote + 1, closeQuote - openQuote - 1);
  bool accepted = false;
  for (const auto& s : ACCEPTED_SCHEMAS)
  {
    if (ver == s) { accepted = true; break; }
  }
  if (!accepted)
  {
    NS_FATAL_ERROR("Snapshot schema " << ver
                   << " is not in the scenario's accepted list "
                   << "(regenerate snapshot with the matching tools/).");
  }
  if (ver != SCHEMA_VERSION)
  {
    std::cerr << "INFO: snapshot schema " << ver
              << " is older than scenario schema " << SCHEMA_VERSION
              << "; reading in backwards-compatible mode.\n";
  }
  return true;
}

// ---------------------------------------------------------------------------
// Graph algorithms
// ---------------------------------------------------------------------------

static std::vector<std::vector<std::pair<uint32_t, double>>>
BuildWeightedAdjacency(uint32_t nodeCount, const std::vector<Edge>& edges)
{
  std::vector<std::vector<std::pair<uint32_t, double>>> adj(nodeCount);
  for (const auto& e : edges)
  {
    adj.at(e.u).push_back({e.v, e.delayMs});
    adj.at(e.v).push_back({e.u, e.delayMs});
  }
  return adj;
}

static std::vector<std::vector<uint32_t>>
WeightedToUnweighted(
    const std::vector<std::vector<std::pair<uint32_t, double>>>& adj)
{
  std::vector<std::vector<uint32_t>> out(adj.size());
  for (size_t i = 0; i < adj.size(); ++i)
  {
    for (const auto& p : adj[i]) out[i].push_back(p.first);
  }
  return out;
}

static uint32_t
BfsUnweightedHops(const std::vector<std::vector<uint32_t>>& adj,
                  uint32_t src, uint32_t dst)
{
  if (src == dst) return 0;
  const uint32_t INF = std::numeric_limits<uint32_t>::max();
  std::vector<uint32_t> dist(adj.size(), INF);
  std::queue<uint32_t> q;
  dist[src] = 0;
  q.push(src);
  while (!q.empty())
  {
    uint32_t u = q.front(); q.pop();
    for (uint32_t v : adj[u])
    {
      if (dist[v] == INF)
      {
        dist[v] = dist[u] + 1;
        if (v == dst) return dist[v];
        q.push(v);
      }
    }
  }
  return INF;
}

struct DijkstraResult
{
  double delayMs;   // -1.0 if no path
  uint32_t hops;    // 0 if no path
};

static DijkstraResult
DijkstraDelay(const std::vector<std::vector<std::pair<uint32_t, double>>>& adj,
              uint32_t src, uint32_t dst)
{
  if (src == dst) return {0.0, 0};
  const double INF = std::numeric_limits<double>::infinity();
  std::vector<double> dist(adj.size(), INF);
  std::vector<int64_t> prev(adj.size(), -1);
  using Item = std::pair<double, uint32_t>;
  std::priority_queue<Item, std::vector<Item>, std::greater<Item>> pq;
  dist[src] = 0.0;
  pq.push({0.0, src});
  while (!pq.empty())
  {
    auto [d, u] = pq.top(); pq.pop();
    if (d > dist[u]) continue;
    if (u == dst) break;
    for (const auto& [v, w] : adj[u])
    {
      double nd = d + w;
      if (nd < dist[v])
      {
        dist[v] = nd;
        prev[v] = static_cast<int64_t>(u);
        pq.push({nd, v});
      }
    }
  }
  if (std::isinf(dist[dst])) return {-1.0, 0};
  uint32_t hops = 0;
  for (int64_t u = static_cast<int64_t>(dst); u != -1 && u != static_cast<int64_t>(src); u = prev[u])
  {
    ++hops;
  }
  return {dist[dst], hops};
}

static std::vector<std::vector<uint32_t>>
ConnectedComponents(const std::vector<std::vector<uint32_t>>& adj)
{
  std::vector<std::vector<uint32_t>> comps;
  std::vector<bool> seen(adj.size(), false);
  for (uint32_t s = 0; s < adj.size(); ++s)
  {
    if (seen[s]) continue;
    std::vector<uint32_t> comp;
    std::queue<uint32_t> q;
    q.push(s); seen[s] = true;
    while (!q.empty())
    {
      uint32_t u = q.front(); q.pop();
      comp.push_back(u);
      for (uint32_t v : adj[u])
      {
        if (!seen[v]) { seen[v] = true; q.push(v); }
      }
    }
    std::sort(comp.begin(), comp.end());
    comps.push_back(std::move(comp));
  }
  std::sort(comps.begin(), comps.end(),
            [](const auto& a, const auto& b) {
              return a.size() > b.size();
            });
  return comps;
}

// ---------------------------------------------------------------------------
// Scalable /30 subnet allocator. Walks 10.0.0.0/8 in /30 increments, which
// is 2^22 = 4,194,304 subnets before exhaustion. Uses 172.16.0.0/12 and
// 192.168.0.0/16 as spill space.
// ---------------------------------------------------------------------------

class SubnetAllocator
{
public:
  SubnetAllocator() : m_block(0), m_idx(0) {}

  std::pair<Ipv4Address, Ipv4Mask> Next()
  {
    const uint32_t kMask = 0xFFFFFFFCu; // /30
    for (int guard = 0; guard < 3; ++guard)
    {
      uint32_t baseHost = 0;
      uint32_t baseTop = 0;
      uint32_t count = 0;
      switch (m_block)
      {
        case 0: baseHost = (10u << 24);                   count = (1u << 22); break;
        case 1: baseHost = (172u << 24) | (16u << 16);    count = (1u << 18); break;
        case 2: baseHost = (192u << 24) | (168u << 16);   count = (1u << 14); break;
        default: NS_FATAL_ERROR("SubnetAllocator: /30 space exhausted. "
                                "Use a smaller topology or custom allocator.");
      }
      baseTop = baseHost + count * 4;
      if (m_idx < count)
      {
        uint32_t net = baseHost + m_idx * 4;
        m_idx++;
        return { Ipv4Address(net), Ipv4Mask(kMask) };
      }
      m_block++;
      m_idx = 0;
      (void)baseTop;
    }
    NS_FATAL_ERROR("SubnetAllocator: exhausted all private ranges.");
  }

private:
  int m_block;
  uint32_t m_idx;
};

// ---------------------------------------------------------------------------
// Flow pair builders (review item 17)
// ---------------------------------------------------------------------------

enum class FlowPattern { Random, LongestPath, Nearest, GatewayPair };

static FlowPattern
ParseFlowPattern(const std::string& s)
{
  if (s == "random") return FlowPattern::Random;
  if (s == "longest") return FlowPattern::LongestPath;
  if (s == "nearest") return FlowPattern::Nearest;
  if (s == "gateway") return FlowPattern::GatewayPair;
  NS_FATAL_ERROR("Unknown flowPattern: " << s
                 << " (expected: random|longest|nearest|gateway)");
  return FlowPattern::Random;
}

struct FlowPairBuilderCtx
{
  const std::vector<uint32_t>& candidateNodes;
  const std::vector<uint32_t>& gatewayNodes;
  uint32_t numFlows;
  uint32_t seed;
  const std::vector<std::vector<uint32_t>>& adjUnweighted;
  const std::vector<std::vector<std::pair<uint32_t, double>>>& adjWeighted;
};

static std::vector<std::pair<uint32_t, uint32_t>>
BuildRandomPairs(const FlowPairBuilderCtx& ctx)
{
  std::mt19937 rng(ctx.seed);
  std::vector<uint32_t> pool = ctx.candidateNodes;
  std::vector<std::pair<uint32_t, uint32_t>> pairs;
  // Scale attempt cap with requested flow count so large asks aren't silently
  // truncated, and reject duplicate (src,dst) pairs.
  std::set<std::pair<uint32_t, uint32_t>> seen;
  const int attemptCap = std::max<int>(500,
                                       static_cast<int>(ctx.numFlows) * 50);
  int attempts = 0;
  while (pairs.size() < ctx.numFlows && attempts < attemptCap)
  {
    if (pool.size() < 2) break;
    std::uniform_int_distribution<size_t> di(0, pool.size() - 1);
    uint32_t src = pool[di(rng)];
    uint32_t dst = pool[di(rng)];
    attempts++;
    if (src == dst) continue;
    auto key = std::make_pair(std::min(src, dst), std::max(src, dst));
    if (seen.count(key)) continue;
    if (BfsUnweightedHops(ctx.adjUnweighted, src, dst)
        == std::numeric_limits<uint32_t>::max()) continue;
    seen.insert(key);
    pairs.push_back({src, dst});
  }
  if (pairs.size() < ctx.numFlows)
  {
    std::cerr << "WARNING: BuildRandomPairs generated only "
              << pairs.size() << " of " << ctx.numFlows
              << " requested flows after " << attempts << " attempts.\n";
  }
  return pairs;
}

static std::vector<std::pair<uint32_t, uint32_t>>
BuildLongestPairs(const FlowPairBuilderCtx& ctx)
{
  const uint32_t INF = std::numeric_limits<uint32_t>::max();
  std::vector<std::tuple<uint32_t, uint32_t, uint32_t>> pd;
  // For very large topologies this would be heavy; this branch is opt-in.
  for (size_t i = 0; i < ctx.candidateNodes.size(); ++i)
  {
    for (size_t j = i + 1; j < ctx.candidateNodes.size(); ++j)
    {
      uint32_t src = ctx.candidateNodes[i];
      uint32_t dst = ctx.candidateNodes[j];
      uint32_t h = BfsUnweightedHops(ctx.adjUnweighted, src, dst);
      if (h != INF && h > 0) pd.push_back({src, dst, h});
    }
  }
  std::sort(pd.begin(), pd.end(),
            [](auto& a, auto& b) { return std::get<2>(a) > std::get<2>(b); });
  std::vector<std::pair<uint32_t, uint32_t>> pairs;
  for (auto& e : pd)
  {
    if (pairs.size() >= ctx.numFlows) break;
    pairs.push_back({std::get<0>(e), std::get<1>(e)});
  }
  return pairs;
}

static std::vector<std::pair<uint32_t, uint32_t>>
BuildNearestPairs(const FlowPairBuilderCtx& ctx)
{
  std::vector<std::pair<uint32_t, uint32_t>> pairs;
  for (size_t i = 0; i < ctx.candidateNodes.size()
       && pairs.size() < ctx.numFlows; ++i)
  {
    uint32_t src = ctx.candidateNodes[i];
    double best = std::numeric_limits<double>::infinity();
    uint32_t dst = src;
    for (uint32_t nb : ctx.adjUnweighted[src])
    {
      for (auto& pw : ctx.adjWeighted[src])
      {
        if (pw.first == nb && pw.second < best) { best = pw.second; dst = nb; }
      }
    }
    if (dst != src) pairs.push_back({src, dst});
  }
  return pairs;
}

static std::vector<std::pair<uint32_t, uint32_t>>
BuildGatewayPairs(const FlowPairBuilderCtx& ctx)
{
  std::mt19937 rng(ctx.seed);
  std::vector<std::pair<uint32_t, uint32_t>> pairs;
  if (ctx.gatewayNodes.size() < 2)
  {
    std::cerr << "WARNING: flowPattern=gateway requested but only "
              << ctx.gatewayNodes.size() << " gateways available; "
              << "falling back to random.\n";
    return BuildRandomPairs(ctx);
  }
  std::uniform_int_distribution<size_t> di(0, ctx.gatewayNodes.size() - 1);
  std::set<std::pair<uint32_t, uint32_t>> seen;
  const int attemptCap = std::max<int>(1000,
                                       static_cast<int>(ctx.numFlows) * 50);
  int attempts = 0;
  while (pairs.size() < ctx.numFlows && attempts < attemptCap)
  {
    attempts++;
    uint32_t src = ctx.gatewayNodes[di(rng)];
    uint32_t dst = ctx.gatewayNodes[di(rng)];
    if (src == dst) continue;
    auto key = std::make_pair(std::min(src, dst), std::max(src, dst));
    if (seen.count(key)) continue;
    if (BfsUnweightedHops(ctx.adjUnweighted, src, dst)
        == std::numeric_limits<uint32_t>::max()) continue;
    seen.insert(key);
    pairs.push_back({src, dst});
  }
  if (pairs.size() < ctx.numFlows)
  {
    std::cerr << "WARNING: BuildGatewayPairs generated only "
              << pairs.size() << " of " << ctx.numFlows
              << " requested gateway flows after " << attempts
              << " attempts (only " << ctx.gatewayNodes.size()
              << " gateways).\n";
  }
  return pairs;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

int main(int argc, char* argv[])
{
  std::string edgesPath = "results/snapshot_edges.csv";
  std::string nodesPath = "results/snapshot_nodes.csv";
  std::string metaPath = "results/snapshot_meta.json";
  std::string perFlowOut = "results/per_flow_metrics.csv";
  std::string runMetaOut = "results/run_meta.json";

  double simTime = 10.0;
  double appStart = 1.0;
  uint32_t numFlows = 4;
  // 1400 B is closer to real Ethernet / IPv4 MTU (1500 B) minus TCP/IP
  // headers than the old 1000 B placeholder.
  uint32_t packetSize = 1400;
  // Defaults picked to be physically plausible (not toy) while still
  // running on a laptop within a few seconds:
  //   * ISL 1 Gbps: order-of-magnitude below reported Starlink optical
  //     ISLs (~100 Gbps) but high enough that routing, not serialisation,
  //     dominates latency for typical packets.
  //   * Access 1 Gbps: sized to avoid being the default bottleneck; real
  //     Starlink downlinks are typically 100-500 Mbps per user beam.
  //   * Queue 1000p: larger than the old 100p default so TCP at Gbps
  //     rates has enough BDP headroom to avoid pathological tail-drops.
  // Override via --rate / --accessRate / --queueSize.
  std::string rate = "1Gbps";
  std::string accessRate = "1Gbps";
  std::string queueSize = "1000p";
  bool enableAnim = true;
  // Default to gateway<->gateway pairs. Random sat<->sat user flows don't
  // correspond to how Starlink carries user traffic (bent-pipe to gateway,
  // or ISL to a gateway footprint) and produce misleading end-to-end paths.
  std::string flowPatternStr = "gateway";
  uint32_t seed = 1;
  double fragmentationFailFrac = 0.0;
  bool printFlows = false;
  bool verbose = false;

  CommandLine cmd;
  cmd.AddValue("edges", "CSV edge file", edgesPath);
  cmd.AddValue("nodes", "CSV node file", nodesPath);
  cmd.AddValue("meta", "Snapshot metadata JSON (for schema check)", metaPath);
  cmd.AddValue("simTime", "Simulation time (s)", simTime);
  cmd.AddValue("appStart", "Traffic start time (s)", appStart);
  cmd.AddValue("rate", "ISL link rate", rate);
  cmd.AddValue("accessRate", "Ground access link rate", accessRate);
  cmd.AddValue("numFlows", "Number of flows", numFlows);
  cmd.AddValue("packetSize", "TCP segment size (bytes)", packetSize);
  cmd.AddValue("queueSize", "Per-device queue size (e.g. '100p' or '64KB')", queueSize);
  cmd.AddValue("enableAnim", "Enable NetAnim XML output", enableAnim);
  cmd.AddValue("flowPattern",
               "random|longest|nearest|gateway traffic model",
               flowPatternStr);
  cmd.AddValue("seed", "RNG seed for traffic generation", seed);
  cmd.AddValue("perFlowOut", "CSV path for per-flow metrics", perFlowOut);
  cmd.AddValue("runMetaOut", "JSON path for run metadata", runMetaOut);
  cmd.AddValue("fragmentationFailFrac",
               "Fail if largest_cc/num_nodes < this (0 to disable)",
               fragmentationFailFrac);
  cmd.AddValue("printFlows",
               "Print per-flow setup and per-flow result lines on stdout. "
               "Off by default; the per-flow CSV is always written.",
               printFlows);
  cmd.AddValue("verbose",
               "Alias for --printFlows; also leaves all info messages.",
               verbose);
  cmd.Parse(argc, argv);
  if (verbose) printFlows = true;

  if (appStart >= simTime)
  {
    NS_FATAL_ERROR("appStart must be < simTime");
  }
  const double activeDuration = simTime - appStart;
  FlowPattern flowPattern = ParseFlowPattern(flowPatternStr);
  RngSeedManager::SetSeed(seed);

  CheckMetadataSchema(metaPath);

  uint32_t maxEdgeId = 0;
  auto edges = ReadEdgesCsv(edgesPath, maxEdgeId);
  auto nodeInfos = ReadNodesCsv(nodesPath);

  uint32_t N = 0;
  std::vector<NodeInfo> nodeInfoById;
  if (!nodeInfos.empty())
  {
    uint32_t nodesMax = 0;
    for (const auto& n : nodeInfos) nodesMax = std::max(nodesMax, n.id);
    N = std::max<uint32_t>(nodesMax + 1, maxEdgeId + 1);
    nodeInfoById.resize(N);
    for (const auto& n : nodeInfos) nodeInfoById[n.id] = n;
    std::cout << "Node count source: nodes CSV (N=" << N << ")\n";
  }
  else
  {
    N = edges.empty() ? 0 : maxEdgeId + 1;
    nodeInfoById.resize(N);
    std::cout << "Node count source: edges fallback (N=" << N << ")\n";
  }

  if (N == 0) NS_FATAL_ERROR("No nodes available");
  if (!edges.empty() && maxEdgeId >= N)
  {
    NS_FATAL_ERROR("edges reference id " << maxEdgeId
                   << " but N=" << N << " (nodes/edges CSV mismatch)");
  }

  auto adjW = BuildWeightedAdjacency(N, edges);
  auto adjU = WeightedToUnweighted(adjW);
  auto comps = ConnectedComponents(adjU);
  const auto& largestComp = comps.front();
  double largestFrac = static_cast<double>(largestComp.size()) / N;

  uint32_t active = 0;
  for (const auto& a : adjU) if (!a.empty()) ++active;

  std::cout << "Schema version: " << SCHEMA_VERSION << "\n";
  std::cout << "Frozen-time snapshot: true (no topology evolution "
               "during the run)\n";
  std::cout << "NOTE: link rates / queues are scenario parameters; they do "
               "NOT reflect real Starlink hardware. Built-in gateways from "
               "the generator are DEMO placeholders unless --gateways_csv "
               "was used. Edge delay_ms is one-way vacuum propagation; "
               "user-perceived latency also includes queueing, scheduling, "
               "gateway/PoP hops, and internet transit.\n";
  std::cout << "Nodes: " << N << " (active=" << active << ")\n";
  std::cout << "Edges: " << edges.size() << "\n";
  std::cout << "Components: " << comps.size()
            << ", largest=" << largestComp.size()
            << " (" << 100.0 * largestFrac << "%)\n";

  if (fragmentationFailFrac > 0.0 && largestFrac < fragmentationFailFrac)
  {
    NS_FATAL_ERROR("Largest CC is only " << largestFrac
                   << " of nodes; below --fragmentationFailFrac="
                   << fragmentationFailFrac);
  }
  if (comps.size() > 1)
  {
    std::cerr << "WARNING: topology is fragmented into " << comps.size()
              << " components; flows will only be drawn from the largest.\n";
    uint32_t excluded = 0;
    for (size_t i = 1; i < comps.size(); ++i) excluded += comps[i].size();
    std::cerr << "         " << excluded << " nodes excluded from traffic.\n";
  }

  NodeContainer nodes;
  nodes.Create(N);

  InternetStackHelper internet;
  internet.Install(nodes);

  // primaryAddr[i] is filled in AFTER p2p links are installed (see below).
  // Earlier versions put a 7.0.0.i/8 on loopback, but ns-3's GlobalRouter
  // skips loopback when emitting LSAs — those addresses had no remote route
  // and silently sank traffic on the source node.
  std::vector<Ipv4Address> primaryAddr(N, Ipv4Address::GetZero());

  std::unique_ptr<AnimationInterface> anim;
  if (enableAnim)
  {
    anim = std::make_unique<AnimationInterface>(
        "results/starlink-animation.xml");
    anim->EnablePacketMetadata(true);

    // Real constellation layout, equirectangular projection from ECEF-derived
    // lat/lon (review item 15). Units chosen so NetAnim shows the whole map.
    const double W = 2000.0, H = 1000.0;
    for (uint32_t i = 0; i < N; ++i)
    {
      double lat = 0.0, lon = 0.0;
      std::string desc = "Node-" + std::to_string(i);
      if (i < nodeInfoById.size())
      {
        lat = nodeInfoById[i].latDeg;
        lon = nodeInfoById[i].lonDeg;
        desc = nodeInfoById[i].name.empty()
               ? desc : nodeInfoById[i].name;
      }
      double x = (lon + 180.0) / 360.0 * W;
      double y = (90.0 - lat) / 180.0 * H;
      anim->SetConstantPosition(nodes.Get(i), x, y);
      anim->UpdateNodeDescription(nodes.Get(i), desc);
      if (i < nodeInfoById.size() && nodeInfoById[i].kind == "gateway")
      {
        anim->UpdateNodeColor(nodes.Get(i), 220, 60, 60);
      }
    }
  }

  // Install ISL / access links with a scalable allocator (review item 14).
  // We also set Ipv4::SetMetric on each end of every P2P link proportional to
  // the propagation delay in ms, so ns-3's global routing picks min-DELAY
  // paths instead of the default min-HOP paths. This matches what a real
  // LEO network does (you'd never pay 16 ms for a single cross-shell hop
  // when two 0.3 ms intra-plane hops get you there) and makes the reported
  // `shortest_delay_ms` the actual path the simulator uses.
  SubnetAllocator alloc;
  uint32_t linksAccess = 0, linksISL = 0;
  auto delayToMetric = [](double delayMs) -> uint16_t {
    // 100 * ms gives two-decimal-ms resolution; clamp to [1, 65535].
    double m = std::round(delayMs * 100.0);
    if (m < 1.0) m = 1.0;
    if (m > 65535.0) m = 65535.0;
    return static_cast<uint16_t>(m);
  };

  // Per-device byte counters, indexed by install order. Each edge installs
  // exactly two devices (one per endpoint), so edge i owns indices 2i and
  // 2i+1. We attach a PhyTxEnd trace that accumulates the on-wire byte
  // count; this is the authoritative measurement of link load (it includes
  // TCP retransmissions and any control traffic GlobalRouter emits).
  // Shared state lives in heap-allocated vectors so the trace lambdas can
  // capture by pointer without lifetime concerns.
  auto devTxBytes = std::make_shared<std::vector<uint64_t>>();
  auto devEdgeIdx = std::make_shared<std::vector<uint32_t>>();
  auto devSide    = std::make_shared<std::vector<uint8_t>>();

  for (size_t ei = 0; ei < edges.size(); ++ei)
  {
    const auto& e = edges[ei];
    PointToPointHelper p2p;
    std::string thisRate = (e.kind == "access") ? accessRate : rate;
    p2p.SetDeviceAttribute("DataRate", StringValue(thisRate));
    p2p.SetChannelAttribute("Delay", TimeValue(MilliSeconds(e.delayMs)));
    p2p.SetQueue("ns3::DropTailQueue<Packet>",
                 "MaxSize", QueueSizeValue(QueueSize(queueSize)));
    NodeContainer pair(nodes.Get(e.u), nodes.Get(e.v));
    NetDeviceContainer devs = p2p.Install(pair);
    auto [net, mask] = alloc.Next();

    Ipv4AddressHelper ip;
    ip.SetBase(net, mask);
    Ipv4InterfaceContainer ifs = ip.Assign(devs);

    // Tag both interfaces with the delay-proportional metric so SPF uses it.
    uint16_t metric = delayToMetric(e.delayMs);
    for (uint32_t side = 0; side < 2; ++side)
    {
      Ptr<Ipv4> ipv4 = ifs.Get(side).first;
      uint32_t iface = ifs.Get(side).second;
      if (ipv4) ipv4->SetMetric(iface, metric);
    }

    // Attach the PhyTxEnd trace on each device. Callback updates the
    // corresponding counter by the on-wire packet size.
    for (uint32_t side = 0; side < 2; ++side)
    {
      uint32_t devIndex = static_cast<uint32_t>(devTxBytes->size());
      devTxBytes->push_back(0);
      devEdgeIdx->push_back(static_cast<uint32_t>(ei));
      devSide->push_back(static_cast<uint8_t>(side));
      Ptr<NetDevice> nd = devs.Get(side);
      // PointToPointNetDevice exposes a "PhyTxEnd" trace source that fires
      // when the last bit of a packet has left the device — the right
      // moment to count on-wire bytes.
      nd->TraceConnectWithoutContext(
          "PhyTxEnd",
          MakeBoundCallback(&AccumDevTxBytes, devTxBytes.get(), devIndex));
    }

    if (e.kind == "access") ++linksAccess; else ++linksISL;
  }
  std::cout << "ISL links installed: " << linksISL
            << ", access links installed: " << linksAccess << "\n";

  // Now that every P2P link has been installed and addressed, pick a stable
  // "primary" address per node: the IP on its first non-loopback interface.
  // Edges are sorted by (u, v) in the generator, so this mapping is
  // deterministic across runs. Global routing has routes for these because
  // they sit on real /30 subnets.
  for (uint32_t i = 0; i < N; ++i)
  {
    Ptr<Ipv4> ipv4 = nodes.Get(i)->GetObject<Ipv4>();
    if (!ipv4) continue;
    for (uint32_t j = 1; j < ipv4->GetNInterfaces(); ++j)
    {
      if (ipv4->GetNAddresses(j) == 0) continue;
      primaryAddr[i] = ipv4->GetAddress(j, 0).GetLocal();
      break;
    }
  }

  Ipv4GlobalRoutingHelper::PopulateRoutingTables();

  // Pick candidate / gateway node lists strictly from the largest CC.
  std::vector<uint32_t> candidateNodes;
  std::vector<uint32_t> gatewayNodes;
  for (uint32_t nid : largestComp)
  {
    candidateNodes.push_back(nid);
    if (nid < nodeInfoById.size() && nodeInfoById[nid].kind == "gateway")
    {
      gatewayNodes.push_back(nid);
    }
  }
  if (candidateNodes.size() < 2)
  {
    NS_FATAL_ERROR("Largest component has <2 nodes; nothing to simulate.");
  }

  FlowPairBuilderCtx ctx{
    candidateNodes, gatewayNodes, numFlows, seed, adjU, adjW};
  std::vector<std::pair<uint32_t, uint32_t>> pairs;
  switch (flowPattern)
  {
    case FlowPattern::Random:      pairs = BuildRandomPairs(ctx);   break;
    case FlowPattern::LongestPath: pairs = BuildLongestPairs(ctx);  break;
    case FlowPattern::Nearest:     pairs = BuildNearestPairs(ctx);  break;
    case FlowPattern::GatewayPair: pairs = BuildGatewayPairs(ctx);  break;
  }

  if (pairs.empty())
  {
    NS_FATAL_ERROR("Traffic model produced zero flow pairs "
                   "(candidate pool size " << candidateNodes.size() << ").");
  }
  numFlows = pairs.size();
  std::cout << "Flow pattern: " << flowPatternStr
            << ", generated flows: " << pairs.size() << "\n";
  std::cout << "Transport: TCP (BulkSend)\n";

  const uint32_t basePort = 9000;
  const uint32_t maxPort = 65535;
  if (basePort + pairs.size() > maxPort + 1)
  {
    NS_FATAL_ERROR("Too many flows (" << pairs.size()
                   << "): basePort " << basePort
                   << " + numFlows would overflow uint16_t ("
                   << maxPort << "). Reduce numFlows to at most "
                   << (maxPort + 1 - basePort) << ".");
  }
  std::vector<AppFlow> appFlows;

  for (uint32_t i = 0; i < pairs.size(); ++i)
  {
    uint32_t src = pairs[i].first;
    uint32_t dst = pairs[i].second;
    uint16_t port = static_cast<uint16_t>(basePort + i);

    PacketSinkHelper sink("ns3::TcpSocketFactory",
                          InetSocketAddress(Ipv4Address::GetAny(), port));
    auto sinkApp = sink.Install(nodes.Get(dst));
    sinkApp.Start(Seconds(0.0));
    sinkApp.Stop(Seconds(simTime));

    BulkSendHelper source("ns3::TcpSocketFactory",
                          InetSocketAddress(primaryAddr[dst], port));
    source.SetAttribute("MaxBytes", UintegerValue(0));
    source.SetAttribute("SendSize", UintegerValue(packetSize));
    auto sourceApp = source.Install(nodes.Get(src));
    sourceApp.Start(Seconds(appStart));
    sourceApp.Stop(Seconds(simTime));

    appFlows.push_back({i, src, dst, port});
    if (printFlows)
    {
      std::cout << "Flow " << i << ": " << src << " -> " << dst
                << " port " << port << "\n";
    }
  }

  FlowMonitorHelper fm;
  Ptr<FlowMonitor> monitor = fm.InstallAll();

  Simulator::Stop(Seconds(simTime));
  Simulator::Run();

  monitor->CheckForLostPackets();
  auto stats = monitor->GetFlowStats();
  Ptr<Ipv4FlowClassifier> classifier =
      DynamicCast<Ipv4FlowClassifier>(fm.GetClassifier());

  std::map<uint16_t, AppFlow> flowByPort;
  for (const auto& f : appFlows) flowByPort[f.port] = f;

  std::set<uint32_t> reportedFlowIndices;

  std::vector<PerFlowRow> perFlowRows;

  double totalRxBytes = 0.0, totalTxBytes = 0.0;
  double sumDelaySec = 0.0;
  uint64_t sumRxPkts = 0;

  if (printFlows) std::cout << "\n=== PER-FLOW RESULTS ===\n";
  for (const auto& kv : stats)
  {
    if (!classifier) continue;
    auto tuple = classifier->FindFlow(kv.first);
    auto it = flowByPort.find(tuple.destinationPort);
    if (it == flowByPort.end()) continue;
    const AppFlow& flow = it->second;
    const auto& s = kv.second;

    double goodputMbps = (s.rxBytes * 8.0) / activeDuration / 1e6;
    double deliveryRatio = s.txBytes
        ? 100.0 * static_cast<double>(s.rxBytes) / s.txBytes : 0.0;
    // For TCP, FlowMonitor's lostPackets count includes retransmissions at
    // the IP layer. Report this as a retransmission overhead ratio
    // (retx bytes / delivered bytes), NOT as application-level loss.
    double tcpRetransOverhead = s.rxBytes
        ? 100.0 * static_cast<double>(
            (s.txBytes > s.rxBytes ? s.txBytes - s.rxBytes : 0))
            / s.rxBytes
        : 0.0;

    double meanDelayMs = s.rxPackets
        ? (s.delaySum.GetSeconds() / s.rxPackets) * 1000.0 : 0.0;
    double meanJitterMs = s.rxPackets > 1
        ? (s.jitterSum.GetSeconds() / (s.rxPackets - 1)) * 1000.0 : 0.0;

    // hop count counted along the same min-delay path that ns-3 actually
    // routes over (since we set per-interface metrics = delay*100 above).
    DijkstraResult dj = DijkstraDelay(adjW, flow.srcNode, flow.dstNode);
    uint32_t hops = dj.hops;
    double shortestDelayMs = dj.delayMs;

    reportedFlowIndices.insert(flow.flowIndex);

    totalRxBytes += s.rxBytes;
    totalTxBytes += s.txBytes;
    sumDelaySec += s.delaySum.GetSeconds();
    sumRxPkts += s.rxPackets;

    if (printFlows)
    {
      std::cout << "Flow " << flow.flowIndex
                << " " << flow.srcNode << "->" << flow.dstNode
                << " (" << (hops == std::numeric_limits<uint32_t>::max() ? 0u : hops)
                << " hops, weighted " << shortestDelayMs << " ms): "
                << "goodput=" << goodputMbps << " Mbps, "
                << "delivery=" << deliveryRatio << "%, "
                << "meanDelay=" << meanDelayMs << " ms, "
                << "jitter=" << meanJitterMs << " ms, "
                << "tcpRetransOverhead=" << tcpRetransOverhead << "%\n";
    }

    perFlowRows.push_back({
      flow.flowIndex, flow.srcNode, flow.dstNode, flow.port,
      goodputMbps, deliveryRatio, tcpRetransOverhead,
      meanDelayMs, meanJitterMs,
      (hops == std::numeric_limits<uint32_t>::max() ? 0u : hops),
      shortestDelayMs,
      s.txPackets, s.rxPackets, s.lostPackets,
      s.txBytes, s.rxBytes
    });
  }

  // Surface any configured flows that did not appear in FlowMonitor stats:
  // those would otherwise be silently omitted from the CSV / aggregates.
  std::vector<uint32_t> missingFlows;
  for (const auto& f : appFlows)
  {
    if (!reportedFlowIndices.count(f.flowIndex))
      missingFlows.push_back(f.flowIndex);
  }
  if (!missingFlows.empty())
  {
    std::cerr << "WARNING: " << missingFlows.size() << " of "
              << appFlows.size() << " configured flow(s) had no FlowMonitor "
              << "statistics and are excluded from aggregates: ";
    for (size_t i = 0; i < missingFlows.size(); ++i)
    {
      std::cerr << missingFlows[i] << (i + 1 < missingFlows.size() ? "," : "");
    }
    std::cerr << "\n";
    for (uint32_t idx : missingFlows)
    {
      const AppFlow& f = appFlows[idx];
      perFlowRows.push_back({
        f.flowIndex, f.srcNode, f.dstNode, f.port,
        0.0, 0.0,
        std::numeric_limits<double>::quiet_NaN(),
        0.0, 0.0, 0u, 0.0,
        0ull, 0ull, 0ull, 0ull, 0ull
      });
    }
  }

  double aggGoodputMbps = (totalRxBytes * 8.0) / activeDuration / 1e6;
  double aggTxLoadMbps = (totalTxBytes * 8.0) / activeDuration / 1e6;
  double meanDelayMs = sumRxPkts ? (sumDelaySec / sumRxPkts) * 1000.0 : 0.0;

  // Aggregate "installed-capacity utilization" is aggTxLoad / totalCapacity.
  // It is deliberately NOT called "utilization" because it cannot reveal
  // bottlenecks on individual hops — a saturated link can sit next to many
  // idle links and still produce a low aggregate number. Tx load is the
  // right numerator here because TCP BulkSend has no "offered rate".
  auto parseRateMbps = [](const std::string& r) -> double {
    double v = 0; std::string u; std::istringstream ss(r);
    ss >> v >> u;
    if (u.find("Gbps") != std::string::npos) return v * 1000.0;
    if (u.find("Mbps") != std::string::npos) return v;
    if (u.find("Kbps") != std::string::npos) return v / 1000.0;
    if (u.find("bps") != std::string::npos) return v / 1e6;
    return v;
  };
  double islMbps = parseRateMbps(rate);
  double accessMbps = parseRateMbps(accessRate);
  double totalCapacityMbps = linksISL * islMbps + linksAccess * accessMbps;
  double aggCapUtil = totalCapacityMbps > 0.0
      ? aggTxLoadMbps / totalCapacityMbps * 100.0 : 0.0;

  // Per-link utilization: measured directly from PhyTxEnd traces attached
  // during link install. devTxBytes[devIdx] is the authoritative on-wire
  // byte count for each device; we aggregate per-edge by summing both
  // directions (edges are full duplex and P2P, so per-edge tx = sum of
  // both endpoints' outgoing bytes / 2 for symmetric reporting, but we
  // report each direction's utilization separately via max/mean).
  double maxLinkUtilPct = 0.0;
  double sumLinkUtilPct = 0.0;
  uint32_t utilSamples = 0;
  for (size_t di = 0; di < devTxBytes->size(); ++di)
  {
    uint32_t ei = (*devEdgeIdx)[di];
    if (ei >= edges.size()) continue;
    const auto& e = edges[ei];
    double rateMbps = (e.kind == "access") ? accessMbps : islMbps;
    if (rateMbps <= 0.0) continue;
    double bits = static_cast<double>((*devTxBytes)[di]) * 8.0;
    double utilPct = (bits / activeDuration) / 1e6 / rateMbps * 100.0;
    if (utilPct > maxLinkUtilPct) maxLinkUtilPct = utilPct;
    sumLinkUtilPct += utilPct;
    ++utilSamples;
  }
  double meanLinkUtilPct = utilSamples ? sumLinkUtilPct / utilSamples : 0.0;

  // Secondary: path-summed upper bound. Useful cross-check — if the PhyTx
  // measurement and this estimate diverge wildly for the max link, a flow
  // probably hit a retransmission storm or a router dropped on arrival.
  struct EdgeKey {
    uint32_t a, b;
    bool operator<(const EdgeKey& o) const {
      if (a != o.a) return a < o.a;
      return b < o.b;
    }
  };
  std::map<EdgeKey, double> pathSumBitsPerEdge;
  std::map<EdgeKey, std::string> kindPerEdge;
  for (const auto& e : edges) {
    EdgeKey k{std::min(e.u, e.v), std::max(e.u, e.v)};
    kindPerEdge[k] = e.kind;
    pathSumBitsPerEdge[k] += 0.0;
  }
  for (const auto& r : perFlowRows) {
    DijkstraResult dj = DijkstraDelay(adjW, r.srcNode, r.dstNode);
    if (std::isinf(dj.delayMs) || dj.delayMs < 0) continue;
    std::vector<uint32_t> path;
    {
      std::vector<double> dist(N, std::numeric_limits<double>::infinity());
      std::vector<int64_t> prev(N, -1);
      using Item = std::pair<double, uint32_t>;
      std::priority_queue<Item, std::vector<Item>, std::greater<Item>> pq;
      dist[r.srcNode] = 0.0; pq.push({0.0, r.srcNode});
      while (!pq.empty()) {
        auto [d, u] = pq.top(); pq.pop();
        if (d > dist[u]) continue;
        if (u == r.dstNode) break;
        for (const auto& [v, w] : adjW[u]) {
          double nd = d + w;
          if (nd < dist[v]) { dist[v] = nd; prev[v] = (int64_t)u; pq.push({nd, v}); }
        }
      }
      if (std::isinf(dist[r.dstNode])) continue;
      for (int64_t u = (int64_t)r.dstNode; u != -1; u = prev[u]) path.push_back((uint32_t)u);
      std::reverse(path.begin(), path.end());
    }
    double bits = static_cast<double>(r.txBytes) * 8.0;
    for (size_t i = 1; i < path.size(); ++i) {
      EdgeKey k{std::min(path[i-1], path[i]), std::max(path[i-1], path[i])};
      auto it = pathSumBitsPerEdge.find(k);
      if (it != pathSumBitsPerEdge.end()) it->second += bits;
    }
  }
  double maxPathSumUtilPct = 0.0;
  for (const auto& [k, bits] : pathSumBitsPerEdge) {
    double rateMbps = (kindPerEdge[k] == "access") ? accessMbps : islMbps;
    if (rateMbps <= 0.0) continue;
    double utilPct = (bits / activeDuration) / 1e6 / rateMbps * 100.0;
    if (utilPct > maxPathSumUtilPct) maxPathSumUtilPct = utilPct;
  }

  std::cout << "\n=== OVERALL RESULTS ===\n";
  std::cout << "Tx load:       " << aggTxLoadMbps << " Mbps\n";
  std::cout << "Goodput:       " << aggGoodputMbps << " Mbps\n";
  std::cout << "Installed cap: " << totalCapacityMbps
            << " Mbps (sum of per-direction link rates)\n";
  std::cout << "Agg cap util:  " << aggCapUtil
            << " %  (aggTxLoad / installedCap; coarse)\n";
  std::cout << "Per-link util: max=" << maxLinkUtilPct << "%, "
            << "mean=" << meanLinkUtilPct
            << "% (measured from PhyTxEnd over " << utilSamples
            << " devices)\n";
  std::cout << "Path-sum util: max=" << maxPathSumUtilPct
            << "% (analytical upper bound; should be >= measured)\n";
  std::cout << "Mean delay:    " << meanDelayMs << " ms\n";
  std::cout << "Retrans:       (see per-flow tcp_retrans_overhead_percent)\n";

  // Write per-flow CSV atomically.
  std::string tmpOut = perFlowOut + ".tmp";
  {
    std::ofstream csv(tmpOut);
    if (!csv.is_open()) NS_FATAL_ERROR("Cannot write " << tmpOut);
    csv << "schema_version=" << SCHEMA_VERSION << "\n";
    // Column notes:
    //  * goodput_mbps:            rxBytes / activeDuration (bits/s -> Mbps).
    //  * tcp_byte_efficiency_percent: rxBytes/txBytes as a coarse transfer
    //    efficiency indicator. NOT an application-layer loss probability;
    //    TCP retransmits transparently at the source so any TCP flow that
    //    completes normally will reach ~100% here even if many segments
    //    were retransmitted. The separate tcp_retrans_overhead_percent
    //    column surfaces retransmission overhead.
    //  * delivery_ratio_percent:  deprecated alias for the same value,
    //    retained for back-compat with schema 2.0.0 consumers.
    //  * hop_count_on_min_delay_path: number of hops counted along the
    //    delay-weighted Dijkstra route that ns-3 actually uses.
    //    hop_count_unweighted is a deprecated alias.
    //  * shortest_delay_ms:       sum of edge propagation delays along
    //    the min-delay path; serialisation/queueing NOT included.
    csv << "flow_index,src_node,dst_node,port,transport,"
           "goodput_mbps,"
           "tcp_byte_efficiency_percent,delivery_ratio_percent,"
           "tcp_retrans_overhead_percent,"
           "mean_delay_ms,mean_jitter_ms,"
           "hop_count_on_min_delay_path,hop_count_unweighted,"
           "shortest_delay_ms,"
           "tx_packets,rx_packets,lost_packets,tx_bytes,rx_bytes\n";
    auto nanToStr = [](double v) {
      if (std::isnan(v)) return std::string("NaN");
      std::ostringstream ss; ss << v; return ss.str();
    };
    for (const auto& r : perFlowRows)
    {
      csv << r.flowIndex << "," << r.srcNode << "," << r.dstNode << ","
          << r.port << ",tcp,"
          << r.goodputMbps << ","
          << r.deliveryRatioPercent << ","
          << r.deliveryRatioPercent << ","  // deprecated alias column
          << nanToStr(r.tcpRetransOverheadPercent) << ","
          << r.meanDelayMs << "," << r.meanJitterMs << ","
          << r.hopCountUnweighted << ","
          << r.hopCountUnweighted << ","    // deprecated alias column
          << r.shortestDelayMs << ","
          << r.txPackets << "," << r.rxPackets << "," << r.lostPackets << ","
          << r.txBytes << "," << r.rxBytes << "\n";
    }
  }
  std::rename(tmpOut.c_str(), perFlowOut.c_str());
  std::cout << "Wrote per-flow metrics to " << perFlowOut << "\n";

  // Run-level metadata JSON (review item 30).
  {
    std::ofstream j(runMetaOut);
    if (j.is_open())
    {
      j << "{\n";
      j << "  \"schema_version\": \"" << SCHEMA_VERSION << "\",\n";
      j << "  \"frozen_time_snapshot\": true,\n";
      j << "  \"sim_time_s\": " << simTime << ",\n";
      j << "  \"app_start_s\": " << appStart << ",\n";
      j << "  \"num_flows\": " << appFlows.size() << ",\n";
      j << "  \"transport\": \"tcp\",\n";
      j << "  \"flow_pattern\": \"" << flowPatternStr << "\",\n";
      j << "  \"packet_size_bytes\": " << packetSize << ",\n";
      j << "  \"isl_rate\": \"" << rate << "\",\n";
      j << "  \"access_rate\": \"" << accessRate << "\",\n";
      j << "  \"queue_size\": \"" << queueSize << "\",\n";
      j << "  \"installed_isl_links\": " << linksISL << ",\n";
      j << "  \"installed_access_links\": " << linksAccess << ",\n";
      j << "  \"installed_capacity_mbps\": " << totalCapacityMbps << ",\n";
      j << "  \"aggregate_goodput_mbps\": " << aggGoodputMbps << ",\n";
      j << "  \"aggregate_tx_load_mbps\": " << aggTxLoadMbps << ",\n";
      j << "  \"aggregate_cap_utilization_percent\": " << aggCapUtil << ",\n";
      j << "  \"per_link_util_max_percent\": " << maxLinkUtilPct << ",\n";
      j << "  \"per_link_util_mean_percent\": " << meanLinkUtilPct << ",\n";
      j << "  \"per_link_util_samples\": " << utilSamples << ",\n";
      j << "  \"per_link_util_source\": \"PhyTxEnd_trace\",\n";
      j << "  \"per_link_util_pathsum_upperbound_max_percent\": "
        << maxPathSumUtilPct << ",\n";
      j << "  \"utilization_metric_note\": \""
        << "aggregate_cap_utilization is aggTxLoad / sum(installed link rates); "
        << "per_link_util is measured per-device from PhyTxEnd traces; "
        << "pathsum_upperbound is an analytical upper bound for cross-check.\"\n";
      j << "}\n";
    }
  }

  monitor->SerializeToXmlFile("results/flowmon.xml", true, true);
  Simulator::Destroy();
  return 0;
}
