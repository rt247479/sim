/* -*- Mode:C++; c-file-style:"gnu"; indent-tabs-mode:nil; -*- */
/*
 * Hybrid MEC–Fog–Cloud with LRU cache, cache-hit ratio, and Fog cache sharing
 * Usage: ./ns3 run "scratch/hybrid-edge --scenario=2 --cacheSize=20"
 */

#include "ns3/core-module.h"
#include "ns3/network-module.h"
#include "ns3/internet-module.h"
#include "ns3/point-to-point-module.h"
#include "ns3/csma-module.h"
#include "ns3/applications-module.h"
#include "ns3/flow-monitor-module.h"
#include "ns3/udp-socket-factory.h"
#include <unordered_map>
#include <list>
#include <cstring>
#include <vector>
#include <fstream> // Added for event logging

using namespace ns3;

NS_LOG_COMPONENT_DEFINE ("HybridEdge");

// ---------- CLI ----------
uint32_t scenario   = 2;   // 0=cloud,1=mec,2=hybrid
double   simTime    = 30.0;
double   reqIntv    = 0.1; // UE request interval
uint32_t mecCap     = 2;   // max concurrent tasks MEC
uint32_t fogCap     = 5;   // max concurrent tasks Fog
uint32_t cacheSize  = 10;  // LRU entries per node
uint32_t packetSize = 1400; // Large packet size for heavy data
std::string dataRate = "10Mbps"; // Large data rate for heavy data
// Add new global parameters for IoT realism
uint32_t mecMips = 20000; // MEC node capacity in MIPS
uint32_t fogMips = 5000;  // Fog node capacity in MIPS
uint32_t minTaskMi = 5000; // Minimum task size in MI
uint32_t maxTaskMi = 20000; // Maximum task size in MI
uint32_t numFiles = 100;    // Number of unique files in the system
// -------------------------

// Global counters
uint64_t g_cacheHits = 0, g_totalReq = 0, g_peerHits = 0;

// Add a log file for event logging
std::ofstream g_eventLog("hybrid-edge-event-log.txt");

// ---------- LRU cache ----------
template<typename K, typename V>
class LRUCache
{
public:
  LRUCache (uint32_t cap) : m_capacity (cap) {}
  bool Contains (const K& key) const { return m_map.find (key) != m_map.end (); }
  void Insert (const K& key, const V& val)
  {
    auto it = m_map.find (key);
    if (it != m_map.end ())
      {
        it->second->second = val;
        m_list.splice (m_list.begin (), m_list, it->second);
        return;
      }
    if (m_list.size () == m_capacity)
      {
        m_map.erase (m_list.back ().first);
        m_list.pop_back ();
      }
    m_list.emplace_front (key, val);
    m_map[key] = m_list.begin ();
  }
private:
  uint32_t m_capacity;
  std::list<std::pair<K, V>> m_list;
  std::unordered_map<K, typename std::list<std::pair<K, V>>::iterator> m_map;
};

// ---------- Hybrid compute/cache app ----------
class HybridCompute : public Application
{
public:
  static TypeId GetTypeId (void)
  {
    static TypeId tid = TypeId ("HybridCompute")
      .SetParent<Application> ()
      .AddConstructor<HybridCompute> ();
    return tid;
  }
  HybridCompute () : m_socket (nullptr), m_capacity (0), m_current (0) {}
  void Setup (Address upstream, std::string type,
              uint32_t capacity, uint32_t cacheSz,
              std::vector<Address> peerFogs = {});
  void SetComputeDelayBase (Time base);
  void SetComputeDelayPerByte (Time perByte);
  void ReceivePeerQuery(Ptr<Socket> socket);
  void InsertToCache(const std::string& content) {
    m_cache->Insert(content, true);
  }

private:
  virtual void StartApplication (void);
  virtual void StopApplication (void);
  virtual void DoDispose() override;

  void HandleRead (Ptr<Socket> socket);
  void ForwardUpward(const std::string& content, Address from, uint8_t* buf, uint32_t len);
  void ComputeAndStore (std::string content, Address to);
  void SendResponse (std::string content, Address dest);

  struct PendingRequest {
    std::string content;
    Address from;
    uint32_t pendingPeers;
    bool peerHit;
  };
  std::unordered_map<std::string, PendingRequest> m_pendingPeerReqs;

  Ptr<Socket> m_socket;
  Address m_upstream;
  std::string m_type;
  Time m_computeDelayBase{MilliSeconds(5)};
  Time m_computeDelayPerByte{MicroSeconds(10)};
  uint32_t m_capacity, m_current;
  std::unique_ptr<LRUCache<std::string, bool>> m_cache;
  std::vector<Address> m_peerFogs;
};

void HybridCompute::Setup(Address upstream, std::string type,
              uint32_t capacity, uint32_t cacheSz,
              std::vector<Address> peerFogs)
{
  m_upstream  = upstream;
  m_type      = type;
  m_capacity  = capacity;
  m_cache     = std::make_unique<LRUCache<std::string, bool>> (cacheSz);
  m_peerFogs  = peerFogs;
}
void HybridCompute::SetComputeDelayBase(Time base) { m_computeDelayBase = base; }
void HybridCompute::SetComputeDelayPerByte(Time perByte) { m_computeDelayPerByte = perByte; }

void HybridCompute::StartApplication(void)
{
  m_socket = Socket::CreateSocket (GetNode (), UdpSocketFactory::GetTypeId ());
  m_socket->Bind (InetSocketAddress (Ipv4Address::GetAny (), 50000));
  m_socket->SetRecvCallback (MakeCallback (&HybridCompute::HandleRead, this));
}
void HybridCompute::StopApplication(void) { if (m_socket) m_socket->Close (); }

void HybridCompute::HandleRead(Ptr<Socket> socket)
{
  Address from;
  Ptr<Packet> pkt = socket->RecvFrom (from);
  uint8_t buf[2048];
  uint32_t len = pkt->CopyData (buf, sizeof (buf));
  std::string content (reinterpret_cast<char*> (buf), len);
  g_totalReq++;

  // Simulate a real task size (random between minTaskMi and maxTaskMi)
  Ptr<UniformRandomVariable> uv = CreateObject<UniformRandomVariable>();
  uint32_t taskMi = uv->GetInteger(minTaskMi, maxTaskMi);
  uint32_t nodeMips = (m_type == "MEC") ? mecMips : fogMips;
  double computeTimeSec = double(taskMi) / nodeMips;
  Time dynamicDelay = Seconds(computeTimeSec);

  // Peer cache query response (special prefix)
  if (content.rfind("PEERHIT:", 0) == 0) {
    std::string origContent = content.substr(8);
    auto it = m_pendingPeerReqs.find(origContent);
    if (it != m_pendingPeerReqs.end()) {
      it->second.peerHit = true;
      it->second.pendingPeers--;
      if (it->second.pendingPeers == 0) {
        // Serve from peer
        g_peerHits++;
        SendResponse(origContent, it->second.from);
        m_cache->Insert(origContent, true);
        m_pendingPeerReqs.erase(it);
      }
    }
    return;
  }
  if (content.rfind("PEERMISS:", 0) == 0) {
    std::string origContent = content.substr(9);
    auto it = m_pendingPeerReqs.find(origContent);
    if (it != m_pendingPeerReqs.end()) {
      it->second.pendingPeers--;
      if (it->second.pendingPeers == 0 && !it->second.peerHit) {
        // No peer had it, forward upward
        ForwardUpward(origContent, it->second.from, buf, len);
        m_pendingPeerReqs.erase(it);
      }
    }
    return;
  }

  // Local cache check
  bool cacheHit = m_cache->Contains (content);
  if (cacheHit)
    {
      g_cacheHits++;
      // Log event
      g_eventLog << "[REQ] " << Simulator::Now().GetSeconds() << " "
                 << GetNode()->GetId() << " " << content << " MEC/Fog " << m_type
                 << " CACHE_HIT 0 " << computeTimeSec << std::endl;
      SendResponse (content, from);
      return;
    }
  // If Fog, try peer Fogs before going up
  if (m_type == "Fog" && !m_peerFogs.empty())
    {
      // Send peer query to all peers
      PendingRequest req{content, from, (uint32_t)m_peerFogs.size(), false};
      m_pendingPeerReqs[content] = req;
      for (const auto& peer : m_peerFogs) {
        Ptr<Socket> peerSock = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
        std::string peerQuery = "PEERQRY:" + content;
        peerSock->Connect(peer);
        peerSock->Send(Create<Packet>((uint8_t*)peerQuery.c_str(), peerQuery.size()));
        peerSock->Close();
      }
      // Log event (cache miss, peer query)
      g_eventLog << "[REQ] " << Simulator::Now().GetSeconds() << " "
                 << GetNode()->GetId() << " " << content << " Fog CACHE_MISS_PEER 0 " << computeTimeSec << std::endl;
      return;
    }
  // If overloaded, forward upward
  if (m_current >= m_capacity)
    {
      // Log event (offload)
      g_eventLog << "[REQ] " << Simulator::Now().GetSeconds() << " "
                 << GetNode()->GetId() << " " << content << " " << m_type
                 << " OFFLOAD 1 " << computeTimeSec << std::endl;
      ForwardUpward(content, from, buf, len);
      return;
    }
  // Process locally (compute delay proportional to data size)
  m_current++;
  // Log event (cache miss, local compute)
  g_eventLog << "[REQ] " << Simulator::Now().GetSeconds() << " "
             << GetNode()->GetId() << " " << content << " " << m_type
             << " CACHE_MISS_LOCAL 0 " << computeTimeSec << std::endl;
  Simulator::Schedule (dynamicDelay, &HybridCompute::ComputeAndStore, this, content, from);
}

void HybridCompute::ForwardUpward(const std::string& content, Address from, uint8_t* buf, uint32_t len) {
  Ptr<Socket> up = Socket::CreateSocket (GetNode (), UdpSocketFactory::GetTypeId ());
  up->Connect (m_upstream);
  up->Send (Create<Packet> (buf, len));
  up->Close();
}

void HybridCompute::ComputeAndStore (std::string content, Address to)
{
  SendResponse (content, to);
  m_cache->Insert (content, true);
  m_current--;
}

void HybridCompute::SendResponse (std::string content, Address dest)
{
  Ptr<Packet> pkt = Create<Packet> (reinterpret_cast<const uint8_t*> (content.c_str ()), content.size ());
  m_socket->SendTo (pkt, 0, dest);
}

void HybridCompute::DoDispose() {
  m_pendingPeerReqs.clear();
  Application::DoDispose();
}

void HybridCompute::ReceivePeerQuery(Ptr<Socket> socket) {
  Address from;
  Ptr<Packet> pkt = socket->RecvFrom(from);
  uint8_t buf[2048];
  uint32_t len = pkt->CopyData(buf, sizeof(buf));
  std::string content(reinterpret_cast<char*>(buf), len);
  if (content.rfind("PEERQRY:", 0) == 0) {
    std::string origContent = content.substr(8);
    std::string resp;
    if (m_cache->Contains(origContent))
      resp = "PEERHIT:" + origContent;
    else
      resp = "PEERMISS:" + origContent;
    Ptr<Socket> respSock = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    respSock->Connect(from);
    respSock->Send(Create<Packet>((uint8_t*)resp.c_str(), resp.size()));
    respSock->Close();
  }
}

// Prepopulate MEC and Fog caches with random files
void PrepopulateCaches(const std::vector<Ptr<HybridCompute>>& mecApps, const std::vector<Ptr<HybridCompute>>& fogApps, uint32_t numFiles) {
  Ptr<UniformRandomVariable> uv = CreateObject<UniformRandomVariable>();
  for (auto app : mecApps) {
    for (uint32_t i = 0; i < numFiles/2; ++i) { // 50% files cached
      std::string file = "file" + std::to_string(uv->GetInteger(0, numFiles-1));
      app->InsertToCache(file);
      g_eventLog << "[CACHE_POP] MEC " << app->GetNode()->GetId() << " " << file << std::endl;
    }
  }
  for (auto app : fogApps) {
    for (uint32_t i = 0; i < numFiles/2; ++i) {
      std::string file = "file" + std::to_string(uv->GetInteger(0, numFiles-1));
      app->InsertToCache(file);
      g_eventLog << "[CACHE_POP] FOG " << app->GetNode()->GetId() << " " << file << std::endl;
    }
  }
}

// ---------- Topology ----------
void CreateTopology ()
{
  NodeContainer cloud, fog, mec, ue;
  cloud.Create (1); fog.Create (2); mec.Create (4); ue.Create (12);
  InternetStackHelper stack; stack.InstallAll ();

  PointToPointHelper p2pBack, p2pEdge;
  p2pBack.SetDeviceAttribute ("DataRate", StringValue ("1Gbps"));
  p2pBack.SetChannelAttribute ("Delay", StringValue ("10ms"));
  NetDeviceContainer cloudFog;
  for (uint32_t i = 0; i < fog.GetN (); ++i)
    cloudFog.Add (p2pBack.Install (cloud.Get (0), fog.Get (i)));

  p2pEdge.SetDeviceAttribute ("DataRate", StringValue ("100Mbps"));
  p2pEdge.SetChannelAttribute ("Delay", StringValue ("5ms"));
  NetDeviceContainer fogMec;
  for (uint32_t f = 0; f < fog.GetN (); ++f)
    for (uint32_t m = f * 2; m < (f + 1) * 2; ++m)
      fogMec.Add (p2pEdge.Install (fog.Get (f), mec.Get (m)));

  CsmaHelper csma;
  csma.SetChannelAttribute ("DataRate", StringValue ("100Mbps"));
  csma.SetChannelAttribute ("Delay", TimeValue (NanoSeconds (10)));
  NetDeviceContainer csmaDevs;
  for (uint32_t m = 0; m < mec.GetN (); ++m)
    {
      NodeContainer g; g.Add (mec.Get (m));
      for (uint32_t u = m * 3; u < (m + 1) * 3; ++u) g.Add (ue.Get (u));
      csmaDevs.Add (csma.Install (g));
    }

  Ipv4AddressHelper addr;
  addr.SetBase ("10.1.0.0", "255.255.255.0"); addr.Assign (cloudFog);
  addr.SetBase ("10.2.0.0", "255.255.255.0"); addr.Assign (fogMec);
  addr.SetBase ("10.3.0.0", "255.255.255.0"); addr.Assign (csmaDevs);
  Ipv4GlobalRoutingHelper::PopulateRoutingTables ();

  /* ---------- Apps ---------- */
  Address cloudAddr = InetSocketAddress (
      cloud.Get (0)->GetObject<Ipv4> ()->GetAddress (1, 0).GetLocal (), 50000);

  // Fog peer addresses
  std::vector<Address> fogAddrs;
  for (uint32_t f = 0; f < fog.GetN (); ++f)
    fogAddrs.push_back(InetSocketAddress(
      fog.Get(f)->GetObject<Ipv4>()->GetAddress(1,0).GetLocal(), 50000));

  // Install Fog applications with peer addresses
  std::vector<Ptr<HybridCompute>> fogApps;
  for (uint32_t f = 0; f < fog.GetN (); ++f)
    {
      std::vector<Address> peers;
      for (uint32_t pf = 0; pf < fog.GetN (); ++pf)
        if (pf != f) peers.push_back(fogAddrs[pf]);
      Ptr<HybridCompute> fogApp = CreateObject<HybridCompute> ();
      fogApp->Setup (cloudAddr, "Fog", fogCap, cacheSize, peers);
      fogApp->SetComputeDelayBase (MilliSeconds (10));
      fogApp->SetComputeDelayPerByte (MicroSeconds (10));
      fog.Get (f)->AddApplication (fogApp);
      fogApps.push_back(fogApp);
      // Install peer query socket
      Ptr<Socket> peerSock = Socket::CreateSocket(fog.Get(f), UdpSocketFactory::GetTypeId());
      peerSock->Bind(InetSocketAddress(Ipv4Address::GetAny(), 50000));
      peerSock->SetRecvCallback(MakeCallback(&HybridCompute::ReceivePeerQuery, fogApp));
    }

  // Install MEC applications
  std::vector<Ptr<HybridCompute>> mecApps;
  for (uint32_t m = 0; m < mec.GetN (); ++m)
    {
      Address fogAddr = InetSocketAddress (
          (m < 2 ? fog.Get (0) : fog.Get (1))
              ->GetObject<Ipv4> ()->GetAddress (1, 0).GetLocal (), 50000);
      Ptr<HybridCompute> mecApp = CreateObject<HybridCompute> ();
      mecApp->Setup (fogAddr, "MEC", mecCap, cacheSize);
      mecApp->SetComputeDelayBase (MilliSeconds (5));
      mecApp->SetComputeDelayPerByte (MicroSeconds (5));
      mec.Get (m)->AddApplication (mecApp);
      mecApps.push_back(mecApp);
    }
  // Prepopulate caches
  PrepopulateCaches(mecApps, fogApps, numFiles);

  /* ---------- UE clients ---------- */
  OnOffHelper client ("ns3::UdpSocketFactory", Address ());
  client.SetAttribute ("OnTime",  StringValue ("ns3::ConstantRandomVariable[Constant=1]"));
  client.SetAttribute ("OffTime", StringValue ("ns3::ConstantRandomVariable[Constant=0]"));
  client.SetAttribute ("DataRate", DataRateValue (DataRate (dataRate)));
  client.SetAttribute ("PacketSize", UintegerValue (packetSize));

  Ptr<UniformRandomVariable> fileUv = CreateObject<UniformRandomVariable>();
  for (uint32_t u = 0; u < ue.GetN (); ++u)
    {
      Ptr<Node> ueNode = ue.Get (u);
      uint32_t mecId = u / 3;
      Address dest;
      switch (scenario)
        {
        case 0: dest = InetSocketAddress (
            cloud.Get (0)->GetObject<Ipv4> ()->GetAddress (1, 0).GetLocal (), 50000); break;
        case 1: dest = InetSocketAddress (
            mec.Get (mecId)->GetObject<Ipv4> ()->GetAddress (1, 0).GetLocal (), 50000); break;
        default: dest = InetSocketAddress (
            mec.Get (mecId)->GetObject<Ipv4> ()->GetAddress (1, 0).GetLocal (), 50000); break;
        }
      client.SetAttribute ("Remote", AddressValue (dest));
      ApplicationContainer apps = client.Install (ueNode);
      apps.Start (Seconds (1.0 + u * reqIntv));
      // Set the payload for the OnOffApplication
      Ptr<Application> app = apps.Get(0);
      Ptr<OnOffApplication> onoff = DynamicCast<OnOffApplication>(app);
      if (onoff) {
        std::string file = "file" + std::to_string(fileUv->GetInteger(0, numFiles-1));
        Ptr<Packet> pkt = Create<Packet>((uint8_t*)file.c_str(), file.size());
        onoff->SetFill(pkt);
      }
    }
}

int
main (int argc, char *argv[])
{
  CommandLine cmd;
  cmd.AddValue ("scenario",  "0=cloud,1=mec,2=hybrid", scenario);
  cmd.AddValue ("simTime",   "Simulation time (s)", simTime);
  cmd.AddValue ("cacheSize", "Cache entries per node", cacheSize);
  cmd.AddValue ("packetSize", "Packet size (bytes)", packetSize);
  cmd.AddValue ("dataRate", "Data rate for UEs", dataRate);
  cmd.AddValue ("mecMips", "MEC node capacity in MIPS", mecMips);
  cmd.AddValue ("fogMips", "Fog node capacity in MIPS", fogMips);
  cmd.AddValue ("minTaskMi", "Minimum task size in MI", minTaskMi);
  cmd.AddValue ("maxTaskMi", "Maximum task size in MI", maxTaskMi);
  cmd.AddValue ("numFiles", "Number of unique files", numFiles);
  cmd.Parse (argc, argv);

  CreateTopology ();

  FlowMonitorHelper fm;
  Ptr<FlowMonitor> monitor = fm.InstallAll ();

  Simulator::Stop (Seconds (simTime));
  Simulator::Run ();

  monitor->CheckForLostPackets ();
  Ptr<Ipv4FlowClassifier> classifier = DynamicCast<Ipv4FlowClassifier> (fm.GetClassifier ());
  std::map<FlowId, FlowMonitor::FlowStats> stats = monitor->GetFlowStats ();

  double avgLatency = 0.0, totalRx = 0;
  uint64_t totalTx = 0;
  for (const auto &it : stats)
    {
      avgLatency += it.second.delaySum.GetSeconds () * it.second.rxBytes;
      totalRx    += it.second.rxBytes;
      totalTx    += it.second.txBytes;
    }
  if (totalRx > 0) avgLatency /= totalRx;
  double loss = totalTx > 0 ? (totalTx - totalRx) * 100.0 / totalTx : 0.0;
  double hitRatio = g_totalReq > 0 ? g_cacheHits * 100.0 / g_totalReq : 0.0;
  double peerHitRatio = g_totalReq > 0 ? g_peerHits * 100.0 / g_totalReq : 0.0;

  std::cout << "\n=== Scenario " << scenario << " (cacheSize=" << cacheSize << ", packetSize=" << packetSize << ", dataRate=" << dataRate << ") ===\n";
  std::cout << "Avg latency (s): " << avgLatency << "\n";
  std::cout << "Packet loss (%): " << loss << "\n";
  std::cout << "Cache hit (local %):   " << hitRatio << "\n";
  std::cout << "Cache hit (peer %):    " << peerHitRatio << "\n";

  Simulator::Destroy ();
  g_eventLog.close();
  return 0;
}
