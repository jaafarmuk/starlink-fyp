#include "ns3/core-module.h"
#include "ns3/network-module.h"
#include "ns3/internet-module.h"
#include "ns3/point-to-point-module.h"
#include "ns3/applications-module.h"
#include "ns3/flow-monitor-helper.h"
#include "ns3/ipv4-flow-classifier.h"
#include "ns3/netanim-module.h"
#include "ns3/queue-size.h"

#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <algorithm>
#include <limits>
#include <queue>
#include <memory>
#include <map>

using namespace ns3;

struct Edge
{
  uint32_t u;
  uint32_t v;
  double delayMs;
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
  double throughputMbps;
  double meanDelayMs;
  double meanJitterMs;
  double lossRatePercent;
  uint32_t hopCount;
  uint64_t txPackets;
  uint64_t rxPackets;
  uint64_t lostPackets;
};

static std::vector<std::vector<uint32_t>>
BuildAdjacency(uint32_t nodeCount, const std::vector<Edge>& edges)
{
  std::vector<std::vector<uint32_t>> adjacency(nodeCount);
  for (const auto& e : edges)
  {
    adjacency.at(e.u).push_back(e.v);
    adjacency.at(e.v).push_back(e.u);
  }
  return adjacency;
}

static std::vector<std::vector<uint32_t>>
ComputeConnectedComponents(const std::vector<std::vector<uint32_t>>& adjacency)
{
  std::vector<std::vector<uint32_t>> components;
  std::vector<bool> visited(adjacency.size(), false);

  for (uint32_t start = 0; start < adjacency.size(); ++start)
  {
    if (visited[start])
    {
      continue;
    }

    std::vector<uint32_t> component;
    std::queue<uint32_t> q;
    q.push(start);
    visited[start] = true;

    while (!q.empty())
    {
      uint32_t node = q.front();
      q.pop();
      component.push_back(node);

      for (uint32_t neighbor : adjacency[node])
      {
        if (!visited[neighbor])
        {
          visited[neighbor] = true;
          q.push(neighbor);
        }
      }
    }

    std::sort(component.begin(), component.end());
    components.push_back(component);
  }

  std::sort(components.begin(),
            components.end(),
            [](const auto& lhs, const auto& rhs) {
              if (lhs.size() != rhs.size())
              {
                return lhs.size() > rhs.size();
              }
              return lhs.front() < rhs.front();
            });

  return components;
}

static uint32_t
BfsShortestHops(const std::vector<std::vector<uint32_t>>& adjacency,
                uint32_t src, uint32_t dst)
{
  if (src == dst) return 0;
  const uint32_t INF = std::numeric_limits<uint32_t>::max();
  std::vector<uint32_t> dist(adjacency.size(), INF);
  std::queue<uint32_t> q;
  dist[src] = 0;
  q.push(src);
  while (!q.empty())
  {
    uint32_t node = q.front(); q.pop();
    for (uint32_t nbr : adjacency[node])
    {
      if (dist[nbr] == INF)
      {
        dist[nbr] = dist[node] + 1;
        if (nbr == dst) return dist[nbr];
        q.push(nbr);
      }
    }
  }
  return INF;
}

static std::vector<std::pair<uint32_t, uint32_t>>
BuildFlowPairs(const std::vector<uint32_t>& candidateNodes,
               uint32_t numFlows,
               const std::vector<std::vector<uint32_t>>& adjacency)
{
  if (candidateNodes.size() < 2 || numFlows == 0)
    return {};

  const uint32_t INF = std::numeric_limits<uint32_t>::max();
  std::vector<std::tuple<uint32_t, uint32_t, uint32_t>> pairDists;

  for (size_t i = 0; i < candidateNodes.size(); ++i)
  {
    for (size_t j = i + 1; j < candidateNodes.size(); ++j)
    {
      uint32_t src = candidateNodes[i];
      uint32_t dst = candidateNodes[j];
      uint32_t hops = BfsShortestHops(adjacency, src, dst);
      if (hops != INF && hops > 0)
        pairDists.push_back({src, dst, hops});
    }
  }

  std::sort(pairDists.begin(), pairDists.end(),
            [](const auto& a, const auto& b) {
              return std::get<2>(a) > std::get<2>(b);
            });

  std::vector<std::pair<uint32_t, uint32_t>> pairs;
  for (const auto& entry : pairDists)
  {
    if (pairs.size() >= numFlows) break;
    pairs.push_back({std::get<0>(entry), std::get<1>(entry)});
  }

  return pairs;
}

static std::vector<Edge> ReadEdgesCsv(const std::string &path, uint32_t &outMaxId)
{
  std::ifstream f(path);
  if (!f.is_open())
  {
    NS_FATAL_ERROR("Could not open CSV: " << path);
  }

  std::string line;
  std::getline(f, line); // skip header

  std::vector<Edge> edges;
  uint32_t maxId = 0;
  bool anyEdge = false;

  while (std::getline(f, line))
  {
    if (line.empty()) continue;

    std::stringstream ss(line);
    std::string su, sv, sdist, sdelay;

    std::getline(ss, su, ',');
    std::getline(ss, sv, ',');
    std::getline(ss, sdist, ',');
    std::getline(ss, sdelay, ',');

    Edge e;
    e.u = static_cast<uint32_t>(std::stoul(su));
    e.v = static_cast<uint32_t>(std::stoul(sv));
    e.delayMs = std::stod(sdelay);

    maxId = std::max(maxId, std::max(e.u, e.v));
    anyEdge = true;
    edges.push_back(e);
  }

  outMaxId = anyEdge ? maxId : 0;
  return edges;
}

static bool ReadNodesCsv(const std::string &path, uint32_t &outNodeCount)
{
  std::ifstream f(path);
  if (!f.is_open())
  {
    return false;
  }

  std::string line;
  if (!std::getline(f, line))
  {
    return false;
  }

  uint32_t maxId = 0;
  uint32_t count = 0;
  bool any = false;
  while (std::getline(f, line))
  {
    if (line.empty()) continue;
    std::stringstream ss(line);
    std::string sid;
    std::getline(ss, sid, ',');
    try
    {
      uint32_t id = static_cast<uint32_t>(std::stoul(sid));
      maxId = std::max(maxId, id);
      any = true;
      count++;
    }
    catch (const std::exception&)
    {
      continue;
    }
  }

  if (!any)
  {
    return false;
  }

  // The node CSV assigns dense 0..N-1 ids, but we tolerate gaps by trusting
  // the maximum id. Either way, the node count the ns-3 scenario should
  // allocate is max(id)+1.
  outNodeCount = std::max(count, maxId + 1);
  return true;
}

int main(int argc, char *argv[])
{
  std::string edgesPath = "results/snapshot_edges.csv";
  std::string nodesPath = "results/snapshot_nodes.csv";
  double simTime = 10.0;
  std::string rate = "20Mbps";
  uint32_t numFlows = 4;
  double appStart = 1.0;
  uint32_t packetSize = 1000;
  double intervalMs = 1.0;
  uint32_t queuePackets = 20;
  bool enableAnim = true;
  bool useTcp = false;
  std::string perFlowOut = "results/per_flow_metrics.csv";

  CommandLine cmd;
  cmd.AddValue("edges", "CSV edge file", edgesPath);
  cmd.AddValue("nodes", "CSV node file (authoritative node count)", nodesPath);
  cmd.AddValue("simTime", "Simulation time in seconds", simTime);
  cmd.AddValue("rate", "Link data rate", rate);
  cmd.AddValue("numFlows", "Number of flows", numFlows);
  cmd.AddValue("appStart", "Traffic start time in seconds", appStart);
  cmd.AddValue("packetSize", "Packet/segment size in bytes", packetSize);
  cmd.AddValue("intervalMs", "UDP packet interval in milliseconds (ignored when useTcp=true)",
               intervalMs);
  cmd.AddValue("queuePackets", "DropTailQueue size in packets per point-to-point device",
               queuePackets);
  cmd.AddValue("enableAnim", "Enable NetAnim XML output", enableAnim);
  cmd.AddValue("useTcp", "Use TCP BulkSend instead of UDP CBR", useTcp);
  cmd.AddValue("perFlowOut", "CSV file for per-flow metrics", perFlowOut);
  cmd.Parse(argc, argv);

  if (appStart >= simTime)
  {
    NS_FATAL_ERROR("appStart (" << appStart << ") must be < simTime (" << simTime << ")");
  }
  const double activeDuration = simTime - appStart;

  uint32_t maxEdgeId = 0;
  auto edges = ReadEdgesCsv(edgesPath, maxEdgeId);

  uint32_t N = 0;
  const bool nodesFromCsv = ReadNodesCsv(nodesPath, N);
  if (nodesFromCsv)
  {
    if (!edges.empty() && maxEdgeId + 1 > N)
    {
      NS_FATAL_ERROR("Edges reference node id " << maxEdgeId
                     << " but nodes CSV only declares " << N << " nodes");
    }
    std::cout << "Node count source: " << nodesPath << " (N=" << N << ")\n";
  }
  else
  {
    N = edges.empty() ? 0 : maxEdgeId + 1;
    std::cout << "Node count source: edges-only fallback (N=" << N << ")\n";
  }

  if (N == 0)
  {
    NS_FATAL_ERROR("No nodes available; both " << nodesPath
                   << " and " << edgesPath << " appear to be empty");
  }

  auto adjacency = BuildAdjacency(N, edges);

  uint32_t activeNodes = 0;
  for (const auto& neighbors : adjacency)
  {
    if (!neighbors.empty())
    {
      activeNodes++;
    }
  }

  auto components = ComputeConnectedComponents(adjacency);
  const std::vector<uint32_t>* largestComponent = components.empty() ? nullptr : &components.front();

  std::cout << "Nodes: " << N << "\n";
  std::cout << "Edges: " << edges.size() << "\n";
  std::cout << "Active nodes with links: " << activeNodes << "\n";
  std::cout << "Connected components: " << components.size() << "\n";
  std::cout << "Largest component size: "
            << (largestComponent ? largestComponent->size() : 0) << "\n";
  std::cout << "Requested flows: " << numFlows << "\n";
  std::cout << "Traffic start time: " << appStart << " s\n";
  std::cout << "Per-device queue size: " << queuePackets << " packets\n";
  std::cout << "NetAnim enabled: " << (enableAnim ? "true" : "false") << "\n";

  NodeContainer nodes;
  nodes.Create(N);

  InternetStackHelper internet;
  internet.Install(nodes);

  std::unique_ptr<AnimationInterface> anim;
  if (enableAnim)
  {
    anim = std::make_unique<AnimationInterface>("results/starlink-animation.xml");
    anim->EnablePacketMetadata(true);

    for (uint32_t i = 0; i < N; ++i)
    {
      double x = (i % 10) * 50.0;
      double y = (i / 10) * 50.0;
      AnimationInterface::SetConstantPosition(nodes.Get(i), x, y);
      anim->UpdateNodeDescription(nodes.Get(i), "Sat-" + std::to_string(i));
    }
  }

  Ipv4AddressHelper ipv4;
  std::vector<Ipv4Address> nodeAddr(N, Ipv4Address("0.0.0.0"));
  uint32_t subnetIdx = 0;

  for (const auto &e : edges)
  {
    PointToPointHelper p2p;
    p2p.SetDeviceAttribute("DataRate", StringValue(rate));
    p2p.SetChannelAttribute("Delay", TimeValue(MilliSeconds(e.delayMs)));
    p2p.SetQueue("ns3::DropTailQueue<Packet>",
                 "MaxSize",
                 QueueSizeValue(QueueSize(std::to_string(queuePackets) + "p")));

    NodeContainer pair(nodes.Get(e.u), nodes.Get(e.v));
    NetDeviceContainer devs = p2p.Install(pair);

    uint32_t second = 1 + (subnetIdx / 64);
    uint32_t third = (subnetIdx % 64) * 4;

    std::ostringstream net;
    net << "10." << second << "." << third << ".0";
    ipv4.SetBase(Ipv4Address(net.str().c_str()), "255.255.255.252");
    Ipv4InterfaceContainer ifaces = ipv4.Assign(devs);

    nodeAddr[e.u] = ifaces.GetAddress(0);
    nodeAddr[e.v] = ifaces.GetAddress(1);

    subnetIdx++;
  }

  Ipv4GlobalRoutingHelper::PopulateRoutingTables();

  if (!largestComponent || largestComponent->size() < 2)
  {
    std::ostringstream msg;
    msg << "Topology is too small for traffic generation. "
        << "Active nodes with links=" << activeNodes
        << ", connected components=" << components.size()
        << ", largest component size=" << (largestComponent ? largestComponent->size() : 0);
    NS_FATAL_ERROR(msg.str());
  }

  std::vector<uint32_t> candidateNodes;
  for (uint32_t nodeId : *largestComponent)
  {
    if (nodeAddr[nodeId] != Ipv4Address("0.0.0.0"))
    {
      candidateNodes.push_back(nodeId);
    }
  }

  if (candidateNodes.size() < 2)
  {
    std::ostringstream msg;
    msg << "Could not create flow pairs. "
        << "Largest connected component has " << largestComponent->size()
        << " nodes, but only " << candidateNodes.size()
        << " have assigned interface addresses.";
    NS_FATAL_ERROR(msg.str());
  }

  auto flowPairs = BuildFlowPairs(candidateNodes, numFlows, adjacency);

  if (flowPairs.empty())
  {
    std::ostringstream msg;
    msg << "No valid flow pairs could be created from the largest connected component. "
        << "Candidate nodes=" << candidateNodes.size()
        << ", requested flows=" << numFlows << ".";
    NS_FATAL_ERROR(msg.str());
  }

  std::cout << "Created flows: " << flowPairs.size() << "\n";
  std::cout << "Flow candidate component nodes:";
  for (uint32_t nodeId : candidateNodes)
  {
    std::cout << " " << nodeId;
  }
  std::cout << "\n";

  std::cout << "Transport: " << (useTcp ? "TCP (BulkSend)" : "UDP (CBR)") << "\n";

  uint16_t basePort = 9000;
  std::vector<AppFlow> appFlows;

  for (uint32_t i = 0; i < flowPairs.size(); ++i)
  {
    uint32_t src = flowPairs[i].first;
    uint32_t dst = flowPairs[i].second;
    uint16_t port = basePort + i;

    if (useTcp)
    {
      PacketSinkHelper sink("ns3::TcpSocketFactory",
                            InetSocketAddress(Ipv4Address::GetAny(), port));
      ApplicationContainer sinkApp = sink.Install(nodes.Get(dst));
      sinkApp.Start(Seconds(0.0));
      sinkApp.Stop(Seconds(simTime));

      BulkSendHelper source("ns3::TcpSocketFactory",
                            InetSocketAddress(nodeAddr[dst], port));
      source.SetAttribute("MaxBytes", UintegerValue(0));
      source.SetAttribute("SendSize", UintegerValue(packetSize));
      ApplicationContainer sourceApp = source.Install(nodes.Get(src));
      sourceApp.Start(Seconds(appStart));
      sourceApp.Stop(Seconds(simTime));
    }
    else
    {
      PacketSinkHelper sink("ns3::UdpSocketFactory",
                            InetSocketAddress(Ipv4Address::GetAny(), port));
      ApplicationContainer sinkApp = sink.Install(nodes.Get(dst));
      sinkApp.Start(Seconds(0.0));
      sinkApp.Stop(Seconds(simTime));

      UdpClientHelper client(nodeAddr[dst], port);
      client.SetAttribute("MaxPackets", UintegerValue(0));
      client.SetAttribute("Interval", TimeValue(MilliSeconds(intervalMs)));
      client.SetAttribute("PacketSize", UintegerValue(packetSize));
      ApplicationContainer clientApp = client.Install(nodes.Get(src));
      clientApp.Start(Seconds(appStart));
      clientApp.Stop(Seconds(simTime));
    }

    appFlows.push_back({i, src, dst, port});

    std::cout << "Flow " << i
              << ": node " << src
              << " -> node " << dst << "\n";
  }

  FlowMonitorHelper fm;
  Ptr<FlowMonitor> monitor = fm.InstallAll();

  Simulator::Stop(Seconds(simTime));
  Simulator::Run();

  monitor->CheckForLostPackets();
  auto stats = monitor->GetFlowStats();
  Ptr<Ipv4FlowClassifier> classifier = DynamicCast<Ipv4FlowClassifier>(fm.GetClassifier());

  double totalRxBytes = 0.0;
  double totalTxBytes = 0.0;
  uint64_t totalTxPkts = 0;
  uint64_t totalLostPkts = 0;
  double sumDelaySeconds = 0.0;
  uint64_t sumRxPkts = 0;
  std::map<uint16_t, AppFlow> flowByPort;
  for (const auto& flow : appFlows)
  {
    flowByPort[flow.port] = flow;
  }
  std::vector<PerFlowRow> perFlowRows;

  std::cout << "\n=== PER-FLOW RESULTS ===\n";

  for (const auto &kv : stats)
  {
    if (!classifier)
    {
      continue;
    }

    Ipv4FlowClassifier::FiveTuple tuple = classifier->FindFlow(kv.first);
    auto flowIt = flowByPort.find(tuple.destinationPort);
    if (flowIt == flowByPort.end())
    {
      continue;
    }

    const auto &st = kv.second;
    totalRxBytes += st.rxBytes;
    totalTxBytes += st.txBytes;
    totalTxPkts += st.txPackets;
    totalLostPkts += st.lostPackets;
    sumDelaySeconds += st.delaySum.GetSeconds();
    sumRxPkts += st.rxPackets;

    double perFlowThroughputMbps = (st.rxBytes * 8.0) / activeDuration / 1e6;
    // For TCP, lostPackets counts retransmissions, not application-level loss.
    // Use byte-level delivery ratio instead to get a meaningful metric for both modes.
    double perFlowLossRate = st.txBytes
                                 ? (1.0 - static_cast<double>(st.rxBytes) / st.txBytes) * 100.0
                                 : 0.0;
    if (perFlowLossRate < 0.0) perFlowLossRate = 0.0;
    double perFlowMeanDelayMs =
        st.rxPackets ? (st.delaySum.GetSeconds() / st.rxPackets) * 1000.0 : 0.0;
    double perFlowMeanJitterMs = (st.rxPackets > 1)
        ? (st.jitterSum.GetSeconds() / (st.rxPackets - 1)) * 1000.0 : 0.0;

    const auto& flow = flowIt->second;
    uint32_t hops = BfsShortestHops(adjacency, flow.srcNode, flow.dstNode);

    std::cout << "Flow " << flow.flowIndex
              << " route " << flow.srcNode << " -> " << flow.dstNode
              << " (" << (hops == std::numeric_limits<uint32_t>::max() ? 0 : hops) << " hops)"
              << ": Throughput=" << perFlowThroughputMbps << " Mbps"
              << ", Mean Delay=" << perFlowMeanDelayMs << " ms"
              << ", Jitter=" << perFlowMeanJitterMs << " ms"
              << ", Loss Rate=" << perFlowLossRate << " %\n";

    perFlowRows.push_back({flow.flowIndex,
                           flow.srcNode,
                           flow.dstNode,
                           flow.port,
                           perFlowThroughputMbps,
                           perFlowMeanDelayMs,
                           perFlowMeanJitterMs,
                           perFlowLossRate,
                           (hops == std::numeric_limits<uint32_t>::max() ? 0u : hops),
                           st.txPackets,
                           st.rxPackets,
                           st.lostPackets});
  }

  double throughputMbps = (totalRxBytes * 8.0) / activeDuration / 1e6;
  double lossRate = totalTxBytes ? std::max(0.0, 1.0 - totalRxBytes / totalTxBytes) : 0.0;
  double meanDelayMs = sumRxPkts ? (sumDelaySeconds / sumRxPkts) * 1000.0 : 0.0;

  std::cout << "\n=== OVERALL RESULTS ===\n";
  std::cout << "Throughput: " << throughputMbps << " Mbps\n";
  std::cout << "Mean Delay: " << meanDelayMs << " ms\n";
  std::cout << "Loss Rate:  " << lossRate * 100.0 << " %\n";

  std::ofstream perFlowCsv(perFlowOut);
  if (!perFlowCsv.is_open())
  {
    NS_FATAL_ERROR("Could not open per-flow CSV for writing: " << perFlowOut);
  }

  perFlowCsv << "flow_index,src_node,dst_node,port,throughput_mbps,mean_delay_ms,mean_jitter_ms,"
                "loss_rate_percent,hop_count,tx_packets,rx_packets,lost_packets\n";
  for (const auto& row : perFlowRows)
  {
    perFlowCsv << row.flowIndex << ","
               << row.srcNode << ","
               << row.dstNode << ","
               << row.port << ","
               << row.throughputMbps << ","
               << row.meanDelayMs << ","
               << row.meanJitterMs << ","
               << row.lossRatePercent << ","
               << row.hopCount << ","
               << row.txPackets << ","
               << row.rxPackets << ","
               << row.lostPackets << "\n";
  }
  perFlowCsv.close();
  std::cout << "Wrote per-flow metrics to " << perFlowOut << "\n";

  monitor->SerializeToXmlFile("results/flowmon.xml", true, true);

  Simulator::Destroy();
  return 0;
}
