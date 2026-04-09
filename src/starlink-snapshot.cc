#include "ns3/core-module.h"
#include "ns3/network-module.h"
#include "ns3/internet-module.h"
#include "ns3/point-to-point-module.h"
#include "ns3/applications-module.h"
#include "ns3/flow-monitor-helper.h"

#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <algorithm>

using namespace ns3;

struct Edge
{
  uint32_t u;
  uint32_t v;
  double delayMs;
};

static std::vector<Edge> ReadEdgesCsv(const std::string &path, uint32_t &outN)
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
    edges.push_back(e);
  }

  outN = maxId + 1;
  return edges;
}

int main(int argc, char *argv[])
{
  std::string edgesPath = "results/snapshot_edges.csv";
  double simTime = 10.0;
  std::string rate = "20Mbps";

  CommandLine cmd;
  cmd.AddValue("edges", "CSV edge file", edgesPath);
  cmd.AddValue("simTime", "Simulation time in seconds", simTime);
  cmd.AddValue("rate", "Link data rate", rate);
  cmd.Parse(argc, argv);

  uint32_t N = 0;
  auto edges = ReadEdgesCsv(edgesPath, N);

  std::cout << "Nodes: " << N << "\n";
  std::cout << "Edges: " << edges.size() << "\n";

  NodeContainer nodes;
  nodes.Create(N);

  InternetStackHelper internet;
  internet.Install(nodes);

  Ipv4AddressHelper ipv4;
  std::vector<Ipv4Address> nodeAddr(N, Ipv4Address("0.0.0.0"));
  uint32_t subnetIdx = 0;

  for (const auto &e : edges)
  {
    PointToPointHelper p2p;
    p2p.SetDeviceAttribute("DataRate", StringValue(rate));
    p2p.SetChannelAttribute("Delay", TimeValue(MilliSeconds(e.delayMs)));

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

  uint16_t port = 9000;

  PacketSinkHelper sink("ns3::UdpSocketFactory",
                        InetSocketAddress(Ipv4Address::GetAny(), port));
  ApplicationContainer sinkApp = sink.Install(nodes.Get(N - 1));
  sinkApp.Start(Seconds(0.0));
  sinkApp.Stop(Seconds(simTime));

  if (nodeAddr[N - 1] == Ipv4Address("0.0.0.0"))
  {
    NS_FATAL_ERROR("Destination node has no IP address.");
  }

  UdpClientHelper client(nodeAddr[N - 1], port);
  client.SetAttribute("MaxPackets", UintegerValue(0));
  client.SetAttribute("Interval", TimeValue(MilliSeconds(1)));
  client.SetAttribute("PacketSize", UintegerValue(1000));

  ApplicationContainer clientApp = client.Install(nodes.Get(0));
  clientApp.Start(Seconds(1.0));
  clientApp.Stop(Seconds(simTime));

  FlowMonitorHelper fm;
  Ptr<FlowMonitor> monitor = fm.InstallAll();

  Simulator::Stop(Seconds(simTime));
  Simulator::Run();

  monitor->CheckForLostPackets();
  auto stats = monitor->GetFlowStats();

  double totalRxBytes = 0.0;
  uint64_t totalTxPkts = 0;
  uint64_t totalLostPkts = 0;
  double sumDelaySeconds = 0.0;
  uint64_t sumRxPkts = 0;

  for (const auto &kv : stats)
  {
    const auto &st = kv.second;
    totalRxBytes += st.rxBytes;
    totalTxPkts += st.txPackets;
    totalLostPkts += st.lostPackets;
    sumDelaySeconds += st.delaySum.GetSeconds();
    sumRxPkts += st.rxPackets;
  }

  double throughputMbps = (totalRxBytes * 8.0) / simTime / 1e6;
  double lossRate = totalTxPkts ? static_cast<double>(totalLostPkts) / totalTxPkts : 0.0;
  double meanDelayMs = sumRxPkts ? (sumDelaySeconds / sumRxPkts) * 1000.0 : 0.0;

  std::cout << "\n=== OVERALL RESULTS ===\n";
  std::cout << "Throughput: " << throughputMbps << " Mbps\n";
  std::cout << "Mean Delay: " << meanDelayMs << " ms\n";
  std::cout << "Loss Rate:  " << lossRate * 100.0 << " %\n";

  monitor->SerializeToXmlFile("results/flowmon.xml", true, true);

  Simulator::Destroy();
  return 0;
}
