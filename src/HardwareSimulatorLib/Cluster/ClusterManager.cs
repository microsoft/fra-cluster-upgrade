using HardwareSimulatorLib.Cluster.Placement;
using HardwareSimulatorLib.Cluster.Upgrade;
using HardwareSimulatorLib.Experiment;
using HardwareSimulatorLib.Predictor;
using HardwareSimulatorLib.Slo;
using HardwareSimulatorLib.Trace;
using System;
using System.Collections.Generic;
using System.Linq;

namespace HardwareSimulatorLib.Cluster
{
    public class ClusterManager
    {
        // All replicaIds below are treated as CanonicalID_TraceID.
        private readonly SloSpecification sloSpec;
        public TraceManager traceMan;

        public ResourceUsage AllocatedRingResourceUsage;
        public ResourceUsage ScaledMaxRingResourceUsage;
        public double scaledMaxUDResourceUsageCpu;

        public double NodeNumCores;
        public double NodeMemorySizeInMB;
        public double NodeDiskSizeInMB;
        public double NodeMemUsageLimitForPlacement;
        public double NodeDiskUsageLimitForPlacement;

        // nodes IDs are 0 to NumNodes - 1
        public readonly int NumNodes;
        public readonly int NumFDs = 5;
        public readonly int NumUDs;
        public static readonly int NumNodesPerUD = 4;

        public HashSet<string> ActiveTenants;

        // all string replica IDs below are ReplicaID with TraceID concatenated.
        public Dictionary<string, TimeSpan> ReplicaIdToPlacementTime;
        public Dictionary<string, int> ReplicaIdToPlacementNodeIdMap;
        public Dictionary<int, HashSet<string>> NodeIdToPlacedReplicasIdMap;
        public Dictionary<string, string[]> TenantIdToReplicaId;

        public double[] NodeIdToCurrCpuUsage;
        public double[] NodeIdToCurrDiskUsage;
        public double[] NodeIdToCurrMemoryUsage;
        public double[] NodeIdToMaxCpuUsage;
        public double[] NodeIdToMaxDiskUsage;
        public double[] NodeIdToMaxMemoryUsage;

        public Dictionary<int, HashSet<int>> FaultDomainToNodeIdMap;

        private readonly PlacementSelector placementSelector;
        public PlacementPreference PlacementPreference;

        private readonly ViolationPredictor predictor;

        public UpgradeScheduleAndState upgradeState;
        public readonly UpgradeExecutor upgradeExecutor;
        public bool considerUpgradesDuringPlacement;

        public readonly ExperimentStatistics statistics;

        public int NumSwaps;
        public int NumMovesToEnablePlacement;
        public int NumMoves;

        public ClusterManager(TraceManager traceMan, TimeSpan simDuration,
            ExperimentParams Params, ExperimentStatistics statistics,
            Random rand, ViolationPredictor predictor)
        {
            this.sloSpec = new SloSpecification(Params.HardwareGeneration);
            this.traceMan = traceMan;

            /* node IDs are 0 to NumNodes - 1 */
            NumNodes = Params.NumNodes;
            NumUDs = NumNodes / NumNodesPerUD;
            AllocatedRingResourceUsage = new ResourceUsage();
            ScaledMaxRingResourceUsage = new ResourceUsage
            {
                Disk = Params.DiskCap *
                       Params.NodeDiskSizeInMB *
                       Params.NumNodes,
                Memory = Params.MemoryCap *
                         Params.NodeMemorySizeInMB *
                         Params.NumNodes,
                Cpu = Params.CpuCap *
                      Params.NodeNumCores *
                      Params.NumNodes *
                      Params.OverbookingRatio
            };
            scaledMaxUDResourceUsageCpu = ScaledMaxRingResourceUsage.Cpu / NumUDs;
            NodeNumCores = Params.NodeNumCores;
            NodeMemorySizeInMB = Params.NodeMemorySizeInMB;
            NodeDiskSizeInMB = Params.NodeDiskSizeInMB;
            NodeMemUsageLimitForPlacement = Params.NodeMemUsageLimitForPlacement;
            NodeDiskUsageLimitForPlacement = Params.NodeDiskUsageLimitForPlacement;

            NodeIdToCurrCpuUsage = new double[NumNodes];
            NodeIdToCurrDiskUsage = new double[NumNodes];
            NodeIdToCurrMemoryUsage = new double[NumNodes];
            NodeIdToMaxCpuUsage = new double[NumNodes];
            NodeIdToMaxDiskUsage = new double[NumNodes];
            NodeIdToMaxMemoryUsage = new double[NumNodes];

            FaultDomainToNodeIdMap = new Dictionary<int, HashSet<int>>();
            for (var FD = 0; FD < NumFDs; FD++)
                FaultDomainToNodeIdMap[FD] = new HashSet<int>();

            if (Params.ApplyFaultDomainConstraints)
            {
                for (var UD = 0; UD < NumUDs; UD++)
                {
                    var FD = UD % NumFDs;
                    for (var i = 0; i < NumNodesPerUD; i++)
                        FaultDomainToNodeIdMap[FD].Add(UD * NumNodesPerUD + i);
                }
            }

            ActiveTenants = new HashSet<string>();
            ReplicaIdToPlacementTime = new Dictionary<string, TimeSpan>();
            ReplicaIdToPlacementNodeIdMap = new Dictionary<string, int>();
            NodeIdToPlacedReplicasIdMap = new Dictionary<int, HashSet<string>>();
            for (var nodeId = 0; nodeId < NumNodes; nodeId++)
                NodeIdToPlacedReplicasIdMap[nodeId] = new HashSet<string>();
            TenantIdToReplicaId = new Dictionary<string, string[]>();

            placementSelector = PlacementSelector.Make(this /*cluster*/,
                Params, rand, predictor);
            this.predictor = predictor;

            upgradeState = new UpgradeScheduleAndState(
                Params.WarmupInHours, Params.IntervalBetweenUpgradesInHours,
                NumUDs, Params.TimeToUpgradeSingleNodeInHours, simDuration);
            upgradeExecutor = UpgradeExecutor.Make(Params, this /*cluster*/);
            PlacementPreference = PlacementPreference.None;

            considerUpgradesDuringPlacement = Params.
                ConsiderUpgradesDuringPlacement;

            this.statistics = statistics;
        }

        public void UpdateAllocatedRingResourceUsageForPrevUD(int UD)
        {
            var prevUD = UD - 1;
            if (prevUD >= 0)
            {
                for (var nodeId = prevUD * NumNodesPerUD;
                         nodeId < (prevUD + 1) * NumNodesPerUD;
                         nodeId++)
                {
                    foreach (var replica in NodeIdToPlacedReplicasIdMap[nodeId])
                    {
                        var tenantId = ReplicaInfo.ExtractTenantId(replica);
                        var slo = traceMan.TenantIdToSloMap[tenantId];
                        AllocatedRingResourceUsage.Cpu += sloSpec.GetMaxCPU(slo);
                        AllocatedRingResourceUsage.Disk += sloSpec.GetDisk(slo);
                        AllocatedRingResourceUsage.Memory += sloSpec.GetMem(slo);
                    }
                }
            }
        }

        public void UpdateAllocatedRingResourceUsageForThisUD(int UD)
        {
            if (UD < NumNodes)
            {
                var startNode = UD * NumNodesPerUD;
                var endNode = (UD + 1) * NumNodesPerUD;
                for (var nodeId = startNode; nodeId < endNode; nodeId++)
                {
                    foreach (var replica in NodeIdToPlacedReplicasIdMap[nodeId])
                    {
                        var tenantId = ReplicaInfo.ExtractTenantId(replica);
                        var slo = traceMan.TenantIdToSloMap[tenantId];
                        AllocatedRingResourceUsage.Cpu -= sloSpec.GetMaxCPU(slo);
                        AllocatedRingResourceUsage.Disk -= sloSpec.GetDisk(slo);
                        AllocatedRingResourceUsage.Memory -= sloSpec.GetMem(slo);
                    }
                }
            }
        }

        public void Place(TimeSpan timeElapsed, string[] replicas)
        {
            var tenantId = ReplicaInfo.ExtractTenantIdWithTrace(replicas[0]);
            TenantIdToReplicaId.Add(tenantId, replicas);
            // The code below has the potential of leading to an infinite loop
            // of trying to place a tenant and failing.
            if (replicas.Length > 1)
            {
                try
                {
                    PlaceReplica(timeElapsed, replicas[1]);
                    PlaceReplica(timeElapsed, replicas[0]);
                    PlaceReplica(timeElapsed, replicas[2]);
                    PlaceReplica(timeElapsed, replicas[3]);
                }
                catch (Exception e)
                {
                    TenantIdToReplicaId.Remove(tenantId);
                    throw new Exception("Can't place it! " + e.Message);
                }
            }
            else
            {
                try
                {
                    PlaceReplica(timeElapsed, replicas[0]);
                }
                catch (Exception e)
                {
                    TenantIdToReplicaId.Remove(tenantId);
                    throw new Exception("Can't place it! " + e.Message);
                }
            }
        }

        public bool IsPrimaryReplica(string replicaId)
        {
            var tenantId = ReplicaInfo.ExtractTenantIdWithTrace(replicaId);
            return TenantIdToReplicaId[tenantId].Length > 1 &&
                    replicaId == TenantIdToReplicaId[tenantId][0];
        }

        public bool IsSecondaryReplica(string replicaId)
        {
            var tenantId = ReplicaInfo.ExtractTenantIdWithTrace(replicaId);
            return TenantIdToReplicaId[tenantId].Length > 1 &&
                    replicaId != TenantIdToReplicaId[tenantId][0];
        }

        public bool IsStandardReplica(string replicaId)
            => TenantIdToReplicaId[ReplicaInfo.
                ExtractTenantIdWithTrace(replicaId)].Length == 1;

        public bool IsCpuCapacityReached()
            => AllocatedRingResourceUsage.Cpu < ScaledMaxRingResourceUsage.Cpu;

        public bool IsAnyNodeInViolation()
        {
            for (var nodeId = 0; nodeId < NumNodes; nodeId++)
                if (IsNodeInViolation(nodeId))
                    return true;
            return false;
        }

        public bool IsNodeInViolation(int nodeId)
            => NodeIdToCurrDiskUsage[nodeId] > NodeDiskSizeInMB ||
                NodeIdToCurrMemoryUsage[nodeId] > NodeMemorySizeInMB;

        public bool IsNodeInMemoryViolation(int nodeId)
            => NodeIdToCurrMemoryUsage[nodeId] > NodeMemorySizeInMB;

        public bool IsNodeInDiskViolation(int nodeId)
            => NodeIdToCurrDiskUsage[nodeId] > NodeDiskSizeInMB;

        public static bool IsNodeReserved(int nodeIndex, int[] nodeIndexes,
            int numNodesReserved)
        {
            /* Nodes in idx [0, numNodesReserved - 1] are reserved */
            for (var nodeId = 0; nodeId < numNodesReserved; nodeId++)
                if (nodeIndex == nodeIndexes[nodeId])
                    return true;
            return false;
        }

        public HashSet<int> GetExcludedNodesForConstraintsOnUDsAndFDsAndUpgrade(
            string replicaId)
        {
            var replicas = TenantIdToReplicaId[
                ReplicaInfo.ExtractTenantIdWithTrace(replicaId)];

            var nodesToExclude = new HashSet<int>();

            // handle upgrade constraints.
            foreach (var NodeIdUnderUpgrade in upgradeState.NodeIdsUnderUpgrade)
                nodesToExclude.Add(NodeIdUnderUpgrade);

            // handle UD and FD constraints for multi-replica tenants.
            if (replicas.Length > 1)
            {
                for (var i = 0; i < replicas.Length; i++)
                {
                    if (replicaId == replicas[i]) continue;

                    // We check if the replicaId is placed becuase the cluster
                    // gets the info of all replicas of a tenant even if not all
                    // replicas have been placed.
                    if (ReplicaIdToPlacementNodeIdMap.ContainsKey(replicas[i]))
                    {
                        var node = ReplicaIdToPlacementNodeIdMap[replicas[i]];
                        var UD = node / NumNodesPerUD;
                        var nodeOffset = UD * NumNodesPerUD;
                        for (var j = 0; j < NumNodesPerUD; j++)
                            nodesToExclude.Add(nodeOffset + j);
                        var FD = UD % NumFDs;
                        foreach (var nodeId in FaultDomainToNodeIdMap[FD])
                            nodesToExclude.Add(nodeId);
                    }
                }
            }
            return nodesToExclude;
        }

        public string[] GetReplicasSortedByResourceUsage(TimeSpan timeElapsed,
            int nodeId, bool sortByMemory, out string[] replicasOnNode,
            out double[] replicasUsage)
        {
            replicasOnNode = NodeIdToPlacedReplicasIdMap[nodeId].ToArray();
            replicasUsage = new double[replicasOnNode.Length];
            for (var i = 0; i < replicasOnNode.Length; i++)
            {
                var elapsedTimeSincePlacement = timeElapsed -
                    ReplicaIdToPlacementTime[replicasOnNode[i]];
                var CanonicalReplicaId = ReplicaInfo.ExtractReplicaId(
                    replicasOnNode[i]);
                var usage = traceMan.ReplicaIdToTraceMap[CanonicalReplicaId].
                    GetResourceUsage(elapsedTimeSincePlacement);

                replicasUsage[i] = sortByMemory ? usage.Memory :
                    /* InDiskViolation */ usage.Disk;
            }
            Array.Sort(replicasUsage, replicasOnNode);
            return replicasOnNode;
        }

        public string[] GetReplicasSortedByResourceUsage(TimeSpan timeElapsed, int UD)
        {
            var nodeIdOffset = UD * NumNodesPerUD;
            var numReplicas = NodeIdToPlacedReplicasIdMap[nodeIdOffset].Count +
                NodeIdToPlacedReplicasIdMap[nodeIdOffset + 1].Count +
                NodeIdToPlacedReplicasIdMap[nodeIdOffset + 2].Count +
                NodeIdToPlacedReplicasIdMap[nodeIdOffset + 3].Count;

            var replicasOnUD = new string[numReplicas];
            var resourceUsage = new double[numReplicas];
            var outputIdx = 0;
            for (var i = 0; i < NumNodesPerUD; i++)
            {
                AppendReplicasAndUsage(timeElapsed, nodeIdOffset + i /* nodeId */,
                    replicasOnUD, resourceUsage, ref outputIdx);
            }
            Array.Sort(resourceUsage, replicasOnUD);
            return replicasOnUD;
        }

        private void AppendReplicasAndUsage(TimeSpan timeElapsed, int nodeId,
            string[] replicasOnUD, double[] resourceUsage, ref int outputIdx)
        {
            var replicasOnNode = NodeIdToPlacedReplicasIdMap[nodeId].ToArray();
            for (var i = 0; i < replicasOnNode.Length; i++)
            {
                replicasOnUD[outputIdx] = replicasOnNode[i];
                var canonicalId = ReplicaInfo.ExtractReplicaId(replicasOnNode[i]);
                var elapsedTimeSincePlacement = timeElapsed -
                    ReplicaIdToPlacementTime[replicasOnNode[i]];
                var usage = traceMan.ReplicaIdToTraceMap[canonicalId].
                    GetResourceUsage(elapsedTimeSincePlacement);
                resourceUsage[outputIdx++] = Math.Max(
                    usage.Memory / NodeMemorySizeInMB,
                    usage.Disk / NodeDiskSizeInMB);
            }
        }

        private readonly HashSet<string> rcReplicasToMerge = new HashSet<string>();
        public string[] GetPrimaryReplicasSortedByResourceUsage(
            TimeSpan timeElapsed, int UD)
            => GetPrimaryOrStandardReplicasSortedByResourceUsage(
                timeElapsed, UD, true /* primary replicas */, rcReplicasToMerge);

        public string[] GetStandardReplicasSortedByResourceUsage(
            TimeSpan timeElapsed, int UD, HashSet<string> replicasToMerge)
            => GetPrimaryOrStandardReplicasSortedByResourceUsage(
                timeElapsed, UD, false /* standard replicas */, replicasToMerge);

        public string[] GetPrimaryAndSecondaryReplicasSortedByResourceUsage(
            TimeSpan timeElapsed, int UD)
        {
            var numReplicas = 0;
            var nodeIdOffset = UD * NumNodesPerUD;
            for (var i = 0; i < NumNodesPerUD; i++)
            {
                var replicas = NodeIdToPlacedReplicasIdMap[nodeIdOffset + i];
                foreach (var replicaId in replicas)
                {
                    if (IsPrimaryReplica(replicaId) ||
                        IsStandardReplica(replicaId))
                    {
                        numReplicas++;
                    }
                }
            }

            var replicasOnUD = new string[numReplicas];
            var resourceUsage = new double[numReplicas];
            var outputIdx = 0;
            for (var j = 0; j < NumNodesPerUD; j++)
            {
                var replicasOnNode = NodeIdToPlacedReplicasIdMap[nodeIdOffset + j].ToArray();
                for (var i = 0; i < replicasOnNode.Length; i++)
                {
                    if (IsPrimaryReplica(replicasOnNode[i]) ||
                        IsStandardReplica(replicasOnNode[i]))
                    {
                        replicasOnUD[outputIdx] = replicasOnNode[i];
                        var canonicalId = ReplicaInfo.ExtractReplicaId(replicasOnNode[i]);
                        var elapsedTimeSincePlacement = timeElapsed -
                            ReplicaIdToPlacementTime[replicasOnNode[i]];
                        var usage = traceMan.ReplicaIdToTraceMap[canonicalId].
                            GetResourceUsage(elapsedTimeSincePlacement);
                        resourceUsage[outputIdx++] = Math.Max(
                            usage.Memory / NodeMemorySizeInMB,
                            usage.Disk / NodeDiskSizeInMB);
                    }
                }
            }

            Array.Sort(resourceUsage, replicasOnUD);
            Array.Reverse(replicasOnUD);
            return replicasOnUD;
        }

        private string[] GetPrimaryOrStandardReplicasSortedByResourceUsage(
            TimeSpan timeElapsed, int UD, bool isPrimary,
            HashSet<string> replicasToMerge)
        {
            var numReplicas = 0;
            var nodeIdOffset = UD * NumNodesPerUD;
            for (var i = 0; i < NumNodesPerUD; i++)
            {
                var replicas = NodeIdToPlacedReplicasIdMap[nodeIdOffset + i];
                foreach (var replicaId in replicas)
                    if (isPrimary ?
                            IsPrimaryReplica(replicaId) :
                            IsStandardReplica(replicaId))
                    {
                        numReplicas++;
                    }
            }
            numReplicas += replicasToMerge.Count;

            var replicasOnUD = new string[numReplicas];
            var resourceUsage = new double[numReplicas];
            var outputIdx = 0;
            for (var j = 0; j < NumNodesPerUD; j++)
            {
                var replicasOnNode = NodeIdToPlacedReplicasIdMap[nodeIdOffset + j].ToArray();
                for (var i = 0; i < replicasOnNode.Length; i++)
                {
                    if (isPrimary ?
                            IsPrimaryReplica(replicasOnNode[i]) :
                            IsStandardReplica(replicasOnNode[i]))
                    {
                        replicasOnUD[outputIdx] = replicasOnNode[i];
                        var canonicalId = ReplicaInfo.ExtractReplicaId(replicasOnNode[i]);
                        var elapsedTimeSincePlacement = timeElapsed -
                            ReplicaIdToPlacementTime[replicasOnNode[i]];
                        var usage = traceMan.ReplicaIdToTraceMap[canonicalId].
                            GetResourceUsage(elapsedTimeSincePlacement);
                        resourceUsage[outputIdx++] = Math.Max(
                            usage.Memory / NodeMemorySizeInMB,
                            usage.Disk / NodeDiskSizeInMB);
                    }
                }
            }
            foreach (var replica in replicasToMerge)
            {
                replicasOnUD[outputIdx] = replica;
                var canonicalId = ReplicaInfo.ExtractReplicaId(replica);
                var elapsedTimeSincePlacement = timeElapsed -
                    ReplicaIdToPlacementTime[replica];
                var usage = traceMan.ReplicaIdToTraceMap[canonicalId].
                    GetResourceUsage(elapsedTimeSincePlacement);
                resourceUsage[outputIdx++] = Math.Max(
                    usage.Memory / NodeMemorySizeInMB,
                    usage.Disk / NodeDiskSizeInMB);
            }

            Array.Sort(resourceUsage, replicasOnUD);
            Array.Reverse(replicasOnUD);
            return replicasOnUD;
        }

        public void EvictTenantsIfLifetimeElapsed(TimeSpan timeElapsed)
        {
            // Release resource used by replicas evicted
            var tenantsToEvict = new HashSet<string>();
            foreach (var activeTenantId in ActiveTenants)
            {
                var replica = TenantIdToReplicaId[activeTenantId][0];
                var activeTime = timeElapsed - ReplicaIdToPlacementTime[replica];
                var replicaId = ReplicaInfo.ExtractReplicaId(replica);
                if (!traceMan.ReplicaIdToTraceMap[replicaId].IsActive(activeTime))
                    _ = tenantsToEvict.Add(activeTenantId);
            }

            foreach (var tenantToEvict in tenantsToEvict)
            {
                ActiveTenants.Remove(tenantToEvict);
                foreach (var replica in TenantIdToReplicaId[tenantToEvict])
                    Evict(replica);
                TenantIdToReplicaId.Remove(tenantToEvict);
            }
        }

        private void Evict(string replicaToEvict)
        {
            var placementNodeId = ReplicaIdToPlacementNodeIdMap[replicaToEvict];
            ReplicaIdToPlacementNodeIdMap.Remove(replicaToEvict);
            NodeIdToPlacedReplicasIdMap[placementNodeId].Remove(replicaToEvict);

            if (!upgradeState.NodeIdsUnderUpgrade.Contains(placementNodeId))
            {
                var tenantId = ReplicaInfo.ExtractTenantId(replicaToEvict);
                var slo = traceMan.TenantIdToSloMap[tenantId];
                AllocatedRingResourceUsage.Cpu -= sloSpec.GetMaxCPU(slo);
                AllocatedRingResourceUsage.Disk -= sloSpec.GetDisk(slo);
                AllocatedRingResourceUsage.Memory -= sloSpec.GetMem(slo);
            }
        }

        public void UpdateResourceUsageWithNewReports(TimeSpan timeElapsed)
        {
            for (var nodeId = 0; nodeId < NumNodes; nodeId++)
            {
                NodeIdToCurrCpuUsage[nodeId] = 0;
                NodeIdToCurrDiskUsage[nodeId] = 0;
                NodeIdToCurrMemoryUsage[nodeId] = 0;
                NodeIdToMaxCpuUsage[nodeId] = 0;
                NodeIdToMaxDiskUsage[nodeId] = 0;
                NodeIdToMaxMemoryUsage[nodeId] = 0;
            }

            foreach (var activeTenantId in ActiveTenants)
            {
                foreach (var activeReplicaId in TenantIdToReplicaId[activeTenantId])
                {
                    var placementTime = ReplicaIdToPlacementTime[activeReplicaId];
                    var placementNodeId = ReplicaIdToPlacementNodeIdMap[activeReplicaId];
                    var replicaId = ReplicaInfo.ExtractReplicaId(activeReplicaId);
                    var trace = traceMan.ReplicaIdToTraceMap[replicaId];

                    var usage = trace.GetResourceUsage(timeElapsed - placementTime);
                    NodeIdToCurrCpuUsage[placementNodeId] += usage.Cpu;
                    NodeIdToCurrDiskUsage[placementNodeId] += usage.Disk;
                    NodeIdToCurrMemoryUsage[placementNodeId] += usage.Memory;
                    NodeIdToMaxCpuUsage[placementNodeId] += trace.GetMaxCpuUsage();
                    NodeIdToMaxDiskUsage[placementNodeId] += trace.GetMaxDiskUsage();
                    NodeIdToMaxMemoryUsage[placementNodeId] += trace.GetMaxMemUsage();
                }
            }
        }

        public void Upgrade(TimeSpan timeElapsed)
        {
            try
            { upgradeExecutor.Upgrade(timeElapsed); }
            catch (Exception e)
            { throw new Exception("Failed - cluster.Upgrade - " + e.Message); }
        }

        public int ChooseNodeToMoveToForViolationFix(
            TimeSpan timeElapsed, string replicaToMove)
            => placementSelector.ChooseNodeToMoveReplicaTo(
                timeElapsed, replicaToMove);

        public void FindReplicasToMoveToFixViolationOnNode(
            TimeSpan timeElapsed, int nodeId, out List<string> replicasToMove,
            out List<int> dstNodeIds)
        {
            try
            {
                var nodeIdToCpuUsage = new double[NumNodes];
                var nodeIdToDiskUsage = new double[NumNodes];
                var nodeIdToMemUsage = new double[NumNodes];

                var emptyUsage = new UsageInfo() { Cpu = 0, Memory = 0, Disk = 0 };
                placementSelector.AttemptClearingSpaceOnNode(timeElapsed, nodeId,
                    nodeIdToCpuUsage, nodeIdToDiskUsage, nodeIdToMemUsage,
                    ref emptyUsage, out List<string> replicasToClear,
                    out List<int> dstNodeIdsToClearTo);
                replicasToMove = replicasToClear;
                dstNodeIds = dstNodeIdsToClearTo;

            }
            catch (Exception e)
            { throw new Exception("Failed - FindReplicasToMoveToFixViolationOnNode. " + e.Message); }
        }

        public void Swap(TimeSpan timeElapsed, string replica1, string replica2)
        {
            NumSwaps++;
            var dstNode1 = ReplicaIdToPlacementNodeIdMap[replica2];
            var dstNode2 = ReplicaIdToPlacementNodeIdMap[replica1];
            MoveReplica(timeElapsed, replica1, dstNode1, false /*forPlacement*/, true /*forSwap*/);
            MoveReplica(timeElapsed, replica2, dstNode2, false /*forPlacement*/, true /*forSwap*/);
        }

        public void MoveReplica(TimeSpan timeElapsed, string replicaId, int dstNode,
            bool forPlacement = false, bool forSwap = false)
        {
            if (!forSwap)
            {
                if (!forPlacement) NumMoves++;
                else if (forPlacement) NumMovesToEnablePlacement++;
            }

            var trace = traceMan.ReplicaIdToTraceMap[
                ReplicaInfo.ExtractReplicaId(replicaId)];
            var usage = trace.GetResourceUsage(
                timeElapsed - ReplicaIdToPlacementTime[replicaId]);
            var srcNodeId = ReplicaIdToPlacementNodeIdMap[replicaId];

            PlaceAndUpdateResourceUsage(replicaId, trace, ref usage, dstNode);

            NodeIdToPlacedReplicasIdMap[srcNodeId].Remove(replicaId);
            NodeIdToCurrDiskUsage[srcNodeId] -= usage.Disk;
            NodeIdToCurrMemoryUsage[srcNodeId] -= usage.Memory;
            NodeIdToCurrCpuUsage[srcNodeId] -= usage.Cpu;
            NodeIdToMaxDiskUsage[srcNodeId] -= trace.GetMaxDiskUsage();
            NodeIdToMaxCpuUsage[srcNodeId] -= trace.GetMaxCpuUsage();
            NodeIdToMaxMemoryUsage[srcNodeId] -= trace.GetMaxMemUsage();

            statistics.MemoryMoved += usage.Memory;
            statistics.DiskMoved += usage.Disk;
            statistics.CpuMoved += usage.Cpu;
            statistics.Moves++;
        }

        private void PlaceReplica(TimeSpan timeElapsed, string replicaToPlace)
        {
            var tenantId = ReplicaInfo.ExtractTenantId(replicaToPlace);
            var slo = traceMan.TenantIdToSloMap[tenantId];

            var replicaId = ReplicaInfo.ExtractReplicaId(replicaToPlace);
            var trace = traceMan.ReplicaIdToTraceMap[replicaId];
            var replicaToPlaceUsage = trace.GetResourceUsage(
                TimeSpan.Zero, (int)sloSpec.GetMaxCPU(slo));
            predictor.UpdateWithPredictedMaxResourceUsage(
                ref replicaToPlaceUsage, slo);

            var placementNode = placementSelector.ChooseNodeToPlaceReplica(
                timeElapsed, replicaToPlace, slo, ref replicaToPlaceUsage);

            var usage = trace.GetResourceUsage(TimeSpan.Zero);

            ActiveTenants.Add(ReplicaInfo.ExtractTenantIdWithTrace(replicaToPlace));
            AllocatedRingResourceUsage.Cpu += sloSpec.GetMaxCPU(slo);
            AllocatedRingResourceUsage.Disk += sloSpec.GetDisk(slo);
            AllocatedRingResourceUsage.Memory += sloSpec.GetMem(slo);
            ReplicaIdToPlacementTime[replicaToPlace] = timeElapsed;

            PlaceAndUpdateResourceUsage(replicaToPlace, trace,
                ref usage, placementNode);
        }

        private void PlaceAndUpdateResourceUsage(string ReplicaID_TraceID,
            ReplicaTrace trace, ref UsageInfo usage, int dstNode)
        {
            NodeIdToPlacedReplicasIdMap[dstNode].Add(ReplicaID_TraceID);
            ReplicaIdToPlacementNodeIdMap[ReplicaID_TraceID] = dstNode;

            NodeIdToCurrCpuUsage[dstNode] += usage.Cpu;
            NodeIdToCurrDiskUsage[dstNode] += usage.Disk;
            NodeIdToCurrMemoryUsage[dstNode] += usage.Memory;
            NodeIdToMaxCpuUsage[dstNode] += trace.GetMaxCpuUsage();
            NodeIdToMaxDiskUsage[dstNode] += trace.GetMaxDiskUsage();
            NodeIdToMaxMemoryUsage[dstNode] += trace.GetMaxMemUsage();
        }
    }
}
