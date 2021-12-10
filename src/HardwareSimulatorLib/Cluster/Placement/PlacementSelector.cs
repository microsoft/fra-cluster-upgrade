using HardwareSimulatorLib.Cluster.Placement.Impl;
using HardwareSimulatorLib.Cluster.Upgrade;
using HardwareSimulatorLib.Config;
using HardwareSimulatorLib.Experiment;
using HardwareSimulatorLib.Predictor;
using HardwareSimulatorLib.Trace;
using System;
using System.Collections.Generic;

namespace HardwareSimulatorLib.Cluster.Placement
{
    public abstract class PlacementSelector
    {
        protected ClusterManager cluster;

        protected readonly int nodesToReserve;
        protected readonly MetricWeightingSchemeEnum metricWeightingScheme;
        protected bool ApplyPlacementPreference;

        public readonly Random rand;

        public static PlacementSelector Make(ClusterManager cluster,
            ExperimentParams experimentParams, Random rand,
            ViolationPredictor predictor)
        {
            switch (experimentParams.PlacementHeuristic)
            {
                default:
                case PlacementHeuristicEnum.WorstFit:
                    return new WorstFitSelector(cluster, experimentParams, rand);
                case PlacementHeuristicEnum.WorstFitUpgrade:
                    return new WorstFitUpgradeSelector(cluster, experimentParams, rand);
                case PlacementHeuristicEnum.Bestfit:
                    return new BestFitSelector(cluster, experimentParams, rand);
                case PlacementHeuristicEnum.SumOfSquares:
                    return new SumOfSquaresSelector(cluster, experimentParams, rand);
                case PlacementHeuristicEnum.MinStdDiv:
                    return new MinStdDivSelector(cluster, experimentParams, rand);
                case PlacementHeuristicEnum.Penalties:
                    return new PenaltiesSelector(cluster, experimentParams, rand);
                case PlacementHeuristicEnum.InnerProduct:
                    return new InnerProductSelector(cluster, experimentParams, rand);
                case PlacementHeuristicEnum.WorstFitProbabilityViolation:
                    return new ProbabilityViolationWorstFitSelector(cluster, experimentParams, rand, predictor);
                case PlacementHeuristicEnum.WorstFitProbabilityViolationUpgrade:
                    return new ProbabilityViolationWorstFitUpgradeSelector(cluster, experimentParams, rand, predictor);
                case PlacementHeuristicEnum.BestFitProbabilityViolation:
                    return new ProbabilityViolationBestFitSelector(cluster, experimentParams, rand, predictor);
                case PlacementHeuristicEnum.DotProduct:
                    return new DotProductSelector(cluster, experimentParams, rand);
                case PlacementHeuristicEnum.L2Norm:
                    return new L2NormSelector(cluster, experimentParams, rand);
            }
        }

        protected PlacementSelector(ClusterManager cluster,
            ExperimentParams experimentParams, Random rand)
        {
            this.cluster = cluster;

            this.nodesToReserve = experimentParams.NodesToReserve;
            this.metricWeightingScheme = experimentParams.MetricWeightingScheme;
            this.ApplyPlacementPreference = experimentParams.ApplyPlacementPreference;

            this.rand = rand;
        }

        protected abstract void ComputeNodeScores(string replicaId,
            double[] nodeIdToCpuUsage, double[] nodeIdToMemoryUsage,
            double[] nodeIdToDiskUsage, double replicaCpuUsage,
            double replicaMemoryUsage, double replicaDiskUsage, int srcNodeId,
            out double[] nodeIdToScore, out double optimalScore);

        private UsageInfo rcUsageInfo = new UsageInfo();
        public string ChooseReplicaToSwapWith(TimeSpan timeElapsed,
            string replicaToSwap, PlacementPreference placementPreference)
        {
            var srcNodeId = cluster.ReplicaIdToPlacementNodeIdMap[replicaToSwap];
            var placementTime = cluster.ReplicaIdToPlacementTime[replicaToSwap];
            var replicaToSwapUsage = cluster
                .traceMan
                .ReplicaIdToTraceMap[ReplicaInfo.ExtractReplicaId(replicaToSwap)]
                .GetResourceUsage(timeElapsed - placementTime);

            var replicas = cluster.TenantIdToReplicaId[
                ReplicaInfo.ExtractTenantIdWithTrace(replicaToSwap)];

            string replicaChoice = null;
            var swapNode = placementPreference == PlacementPreference.
                LowerUpgradeDomains ? cluster.NumNodes : -1;

            var random = new Random();
            foreach (var replicaToSwapWith in replicas)
            {
                if (replicaToSwap == replicaToSwapWith) continue;

                var trace = cluster.traceMan.ReplicaIdToTraceMap[
                    ReplicaInfo.ExtractReplicaId(replicaToSwapWith)];
                var replicaToSwapWithUsage = trace.GetResourceUsage(
                    timeElapsed - cluster.ReplicaIdToPlacementTime[replicaToSwapWith]);

                var dstNodeId = cluster.ReplicaIdToPlacementNodeIdMap[replicaToSwapWith];

                // TODO: refactor code below.
                rcUsageInfo.Cpu = replicaToSwapUsage.Cpu - replicaToSwapWithUsage.Cpu;
                rcUsageInfo.Memory = replicaToSwapUsage.Memory - replicaToSwapWithUsage.Memory;
                rcUsageInfo.Disk = replicaToSwapUsage.Disk - replicaToSwapWithUsage.Disk;
                if (cluster.NodeIdToCurrDiskUsage[dstNodeId] + rcUsageInfo.Disk > cluster.NodeDiskSizeInMB ||
                    cluster.NodeIdToCurrMemoryUsage[dstNodeId] + rcUsageInfo.Memory > cluster.NodeMemorySizeInMB ||
                    cluster.NodeIdToCurrCpuUsage[dstNodeId] + rcUsageInfo.Cpu > cluster.NodeNumCores * 10000)
                {
                    continue;
                }

                rcUsageInfo.Cpu = replicaToSwapWithUsage.Cpu - replicaToSwapUsage.Cpu;
                rcUsageInfo.Memory = replicaToSwapWithUsage.Memory - replicaToSwapUsage.Memory;
                rcUsageInfo.Disk = replicaToSwapWithUsage.Disk - replicaToSwapUsage.Disk;
                if (cluster.NodeIdToCurrDiskUsage[srcNodeId] + rcUsageInfo.Disk > cluster.NodeDiskSizeInMB ||
                    cluster.NodeIdToCurrMemoryUsage[srcNodeId] + rcUsageInfo.Memory > cluster.NodeMemorySizeInMB ||
                    cluster.NodeIdToCurrCpuUsage[srcNodeId] + rcUsageInfo.Cpu > cluster.NodeNumCores * 10000)
                {
                    continue;
                }

                if (ApplyPlacementPreference)
                {
                if (swapNode == cluster.NumNodes || swapNode == -1 ||
                    (placementPreference == PlacementPreference.LowerUpgradeDomains && dstNodeId < swapNode) ||
                    (placementPreference == PlacementPreference.UpperUpgradeDomains && dstNodeId > swapNode))
                {
                    replicaChoice = replicaToSwapWith;
                    swapNode = dstNodeId;
                }
                }
                else if (swapNode == cluster.NumNodes || swapNode == -1 || random.NextDouble() > 0.5)
                {
                    replicaChoice = replicaToSwapWith;
                    swapNode = dstNodeId;
                }
            }
            return replicaChoice;
        }

        public virtual int ChooseNodeToPlaceReplica(TimeSpan timeElapsed,
            string replicaToMove, string slo, ref UsageInfo replicaToPlaceUsage)
        {
            // 1. Try directly placing the replica on a node.
            var dstNodeId = FindNodeToPlaceOrMoveTo(replicaToMove,
                cluster.NodeIdToCurrCpuUsage, cluster.NodeIdToCurrMemoryUsage,
                cluster.NodeIdToCurrDiskUsage, replicaToPlaceUsage.Cpu,
                replicaToPlaceUsage.Memory, replicaToPlaceUsage.Disk);

            // 2. Try moving already placed replicas to clear
            //      space for this replica.
            if (dstNodeId == -1)
            {
                dstNodeId = cluster.NodeIdsUnderUpgrade.Count == 0 ||
                            !ApplyPlacementPreference ?
                    FindNodeToMoveToAfterClearingSpace(timeElapsed,
                        replicaToMove, ref replicaToPlaceUsage, -1 /*srcNodeId*/,
                        true /* for placement */) :
                    FindNodeToMoveToAfterClearingSpaceDuringUpgrade(
                        timeElapsed, replicaToMove, ref replicaToPlaceUsage,
                        -1 /*srcNodeId*/, true /* for placement */);
            }
            return dstNodeId;
        }

        public virtual int ChooseNodeToMoveReplicaTo(TimeSpan timeElapsed,
            string replicaToMove)
        {
            var canonicalId = ReplicaInfo.ExtractReplicaId(replicaToMove);
            var trace = cluster.traceMan.ReplicaIdToTraceMap[canonicalId];
            var replicaToMoveUsage = trace.GetResourceUsage(
                timeElapsed - cluster.ReplicaIdToPlacementTime[replicaToMove]);
            var srcNodeId = cluster.ReplicaIdToPlacementNodeIdMap[replicaToMove];

            // 1. Try directly placing the replica on a node.
            var dstNodeId = FindNodeToPlaceOrMoveTo(replicaToMove,
                cluster.NodeIdToCurrCpuUsage, cluster.NodeIdToCurrMemoryUsage,
                cluster.NodeIdToCurrDiskUsage, replicaToMoveUsage.Cpu,
                replicaToMoveUsage.Memory, replicaToMoveUsage.Disk, srcNodeId);

            // 2. Try moving already placed tenants to clear
            //      space for this replica.
            if (dstNodeId == -1)
            {
                dstNodeId = cluster.NodeIdsUnderUpgrade.Count == 0 ||
                            !ApplyPlacementPreference ?
                    FindNodeToMoveToAfterClearingSpace(timeElapsed,
                        replicaToMove, ref replicaToMoveUsage, srcNodeId) :
                    FindNodeToMoveToAfterClearingSpaceDuringUpgrade(
                        timeElapsed, replicaToMove, ref replicaToMoveUsage,
                        srcNodeId);
            }
            return dstNodeId;
        }

        protected virtual int FindNodeToPlaceOrMoveTo(string replicaId,
            double[] nodeIdToCpuUsage, double[] nodeIdToMemoryUsage,
            double[] nodeIdToDiskUsage, double replicaCpuUsage,
            double replicaMemoryUsage, double replicaDiskUsage,
            int srcNodeId = -1)
        {
            ComputeNodeScores(replicaId, nodeIdToCpuUsage, nodeIdToMemoryUsage,
                nodeIdToDiskUsage, replicaCpuUsage, replicaMemoryUsage,
                replicaDiskUsage, srcNodeId, out double[] nodeIdToScore,
                out double optimalScore);

            if (UpgradeScheduler.IsUpgrading)
            {
                UpdateScoresBasedOnUDPreferences(nodeIdToScore, ref optimalScore);
            }

            return ChooseNode(nodeIdToScore, optimalScore);
        }

        private void UpdateScoresBasedOnUDPreferences(double[] nodeIdToScore, ref double optimalScore)
        {
            if (cluster.PlacementPreference == PlacementPreference.None)
            {
                throw new ArgumentException("Cluster is in an upgrade. Placement preference cannot be None.");
            }

            // if preference is to go to lower UDs yet the lowest UD is under upgrade OR
            // if preference is to go to upper UDs yet the highest UD is under upgrade then
            // preferences can be ignored as they have no impact on these failovers.
            var isPreferenceForLowerUD = cluster.PlacementPreference == PlacementPreference.LowerUpgradeDomains;
            if (isPreferenceForLowerUD && cluster.DomainUnderUpgrade == 0 ||
                !isPreferenceForLowerUD && cluster.DomainUnderUpgrade == cluster.NumUDs - 1)
            {
                return;
            }

            // Each node is in either: 1) already upgraded; 2) under upgrade; or 3) to be upgraded.
            // Due to the direction of ugprades from 0 to cluster.NumNodes or from cluster.NumNodes to 0,
            //   we can split nodes into 2 sets containing continuous nodeIds:
            //   1. nodes already upgraded (either left or right section dependent on Upgrade direction)
            //   2. nodes to upgrade (either left or right section dependent on Upgrade direction)
            var smallestNodeIdUnderUpgrade = cluster.DomainUnderUpgrade * ClusterManager.NumNodesPerUD;
            var largestNodeIdUnderUpgrade  = smallestNodeIdUnderUpgrade + ClusterManager.NumNodesPerUD - 1;
            var startNodeIdUpgraded = isPreferenceForLowerUD ?
                0 : largestNodeIdUnderUpgrade + 1;
            var endNodeIdUpgraded = isPreferenceForLowerUD ?
                smallestNodeIdUnderUpgrade - 1 : cluster.NumNodes - 1;
            var startNodeIdToUpgrade = isPreferenceForLowerUD ?
                largestNodeIdUnderUpgrade + 1 : 0;
            var endNodeIdToUpgrade = isPreferenceForLowerUD ?
                cluster.NumNodes - 1 : smallestNodeIdUnderUpgrade - 1;

            // Find the best score for the nodes already upgraded.
            var newOptimalScoreDueToPreference = double.MaxValue;
            for (var nodeId = startNodeIdUpgraded; nodeId < endNodeIdUpgraded; nodeId++)
            {
                if (nodeIdToScore[nodeId] < newOptimalScoreDueToPreference)
                {
                    newOptimalScoreDueToPreference = nodeIdToScore[nodeId];
                }
            }

            // If there is at least a single node that's already upgraded on which we can place,
            //   we overwrite all of the scores for nodes to upgrade
            if (newOptimalScoreDueToPreference != double.MaxValue)
            {
                for (var nodeId = startNodeIdToUpgrade; nodeId < endNodeIdToUpgrade; nodeId++)
                {
                    nodeIdToScore[nodeId] = double.MaxValue;
                }
            }
        }

        private int FindNodeToMoveToAfterClearingSpace(
            TimeSpan timeElapsed, string replicaId,
            ref UsageInfo replicaToMoveUsage, int srcNodeId = -1,
            bool forPlacement = false)
        {
            var chosenNodeToClear = -1;
            var minNumMoves = int.MaxValue;
            List<string> chosenReplicasToClear = new List<string>();
            List<int> chosenDstNodeIdsToClearTo = new List<int>();

            // Attempt to clear placed replicas in other nodes to make space.
            var nodeIdToCpuUsageCopy = new double[cluster.NumNodes];
            var nodeIdToDiskUsageCopy = new double[cluster.NumNodes];
            var nodeIdToMemUsageCopy = new double[cluster.NumNodes];

            var nodesToExclude = cluster.
                GetExcludedNodesForConstraintsOnUDsAndFDsAndUpgrade(replicaId);
            for (var nodeIdToClear = 0;
                     nodeIdToClear < cluster.NumNodes;
                     nodeIdToClear++)
            {
                if (srcNodeId == nodeIdToClear ||
                        nodesToExclude.Contains(nodeIdToClear))
                    // We ignore nodeId since that's the node we are moving a
                    //    replica from as well as nodes that don't meet UD
                    //    constraints.
                    continue;

                AttemptClearingSpaceOnNodeEnsuringMinNumMoves(timeElapsed,
                    nodeIdToClear, nodeIdToCpuUsageCopy, nodeIdToDiskUsageCopy,
                    nodeIdToMemUsageCopy, ref replicaToMoveUsage,
                    ref chosenNodeToClear, ref minNumMoves,
                    ref chosenReplicasToClear, ref chosenDstNodeIdsToClearTo);
            }

            // Attempt to actually empty a node now.
            if (chosenNodeToClear != -1)
            {
                for (var i = 0; i < minNumMoves; i++)
                    cluster.MoveReplica(timeElapsed, chosenReplicasToClear[i],
                        chosenDstNodeIdsToClearTo[i], forPlacement);
                cluster.statistics.NumMovesDueToClearSpace += minNumMoves;

                AssertNodeClearedIsChosenAfterClearing(replicaId,
                    ref replicaToMoveUsage, srcNodeId, chosenNodeToClear);
                return chosenNodeToClear;
            }
            throw new Exception("Choose node after clearing space failed.");
        }

        private int FindNodeToMoveToAfterClearingSpaceDuringUpgrade(
            TimeSpan timeElapsed, string replicaId,
            ref UsageInfo replicaToMoveUsage, int srcNodeId = 1,
            bool forPlacement = false)
        {
            var chosenNodeToClear = -1;
            var minNumMoves = int.MaxValue;
            List<string> chosenReplicasToClear = new List<string>();
            List<int> chosenDstNodeIdsToClearTo = new List<int>();

            // Attempt to clear placed replicas in other nodes to make space.
            var nodeIdToCpuUsageCopy = new double[cluster.NumNodes];
            var nodeIdToDiskUsageCopy = new double[cluster.NumNodes];
            var nodeIdToMemUsageCopy = new double[cluster.NumNodes];

            var nodesToExclude = cluster.
                GetExcludedNodesForConstraintsOnUDsAndFDsAndUpgrade(replicaId);
            nodesToExclude.Add(srcNodeId);

            var startNodeIdToClear =
                cluster.PlacementPreference == PlacementPreference.LowerUpgradeDomains ?
                    0 :
                    (cluster.DomainUnderUpgrade + 1) *
                        ClusterManager.NumNodesPerUD;
            var endNodeIdToClear =
                cluster.PlacementPreference == PlacementPreference.LowerUpgradeDomains ?
                    cluster.DomainUnderUpgrade * ClusterManager.NumNodesPerUD :
                    cluster.NumNodes;

            for (var nodeIdToClear = startNodeIdToClear;
                     nodeIdToClear < endNodeIdToClear;
                     nodeIdToClear++)
            {
                if (nodesToExclude.Contains(nodeIdToClear)) continue;

                AttemptClearingSpaceOnNodeEnsuringMinNumMoves(timeElapsed,
                    nodeIdToClear, nodeIdToCpuUsageCopy, nodeIdToDiskUsageCopy,
                    nodeIdToMemUsageCopy, ref replicaToMoveUsage,
                    ref chosenNodeToClear, ref minNumMoves,
                    ref chosenReplicasToClear, ref chosenDstNodeIdsToClearTo);
            }

            if (chosenNodeToClear == -1)
            {
                startNodeIdToClear = cluster.PlacementPreference ==
                    PlacementPreference.LowerUpgradeDomains ?
                        (cluster.DomainUnderUpgrade + 1) *
                            ClusterManager.NumNodesPerUD :
                        0;
                endNodeIdToClear = cluster.PlacementPreference ==
                    PlacementPreference.LowerUpgradeDomains ?
                        cluster.NumNodes :
                        cluster.DomainUnderUpgrade *
                            ClusterManager.NumNodesPerUD;

                for (var nodeIdToClear = startNodeIdToClear;
                         nodeIdToClear < endNodeIdToClear;
                         nodeIdToClear++)
                {
                    if (nodesToExclude.Contains(nodeIdToClear)) continue;

                    AttemptClearingSpaceOnNodeEnsuringMinNumMoves(timeElapsed,
                        nodeIdToClear, nodeIdToCpuUsageCopy,
                        nodeIdToDiskUsageCopy, nodeIdToMemUsageCopy,
                        ref replicaToMoveUsage, ref chosenNodeToClear,
                        ref minNumMoves, ref chosenReplicasToClear,
                        ref chosenDstNodeIdsToClearTo);
                }
            }

            // Attempt to actually empty a node now.
            if (chosenNodeToClear != -1)
            {
                for (var i = 0; i < minNumMoves; i++)
                    cluster.MoveReplica(timeElapsed, chosenReplicasToClear[i],
                        chosenDstNodeIdsToClearTo[i], forPlacement);
                cluster.statistics.NumMovesDueToClearSpace += minNumMoves;

                AssertNodeClearedIsChosenAfterClearing(replicaId,
                    ref replicaToMoveUsage, srcNodeId, chosenNodeToClear);
                return chosenNodeToClear;
            }
            throw new Exception("FindNodeToMoveToAfterClearingSpaceDuringUpgrade failed.");
        }

        private void AttemptClearingSpaceOnNodeEnsuringMinNumMoves(
            TimeSpan timeElapsed, int nodeIdToClear, double[] nodeIdToCpuUsage,
            double[] nodeIdToDiskUsage, double[] nodeIdToMemUsage,
            ref UsageInfo replicaToMoveUsage, ref int chosenNodeToClear,
            ref int minNumMoves, ref List<string> chosenReplicasToClear,
            ref List<int> chosenDstNodeIdsToClearTo)
        {
            if (AttemptClearingSpaceOnNode(timeElapsed, nodeIdToClear,
                    nodeIdToCpuUsage, nodeIdToDiskUsage, nodeIdToMemUsage,
                    ref replicaToMoveUsage, out List<string> replicasToClear,
                    out List<int> dstNodeIdsToClearTo)
                && minNumMoves > replicasToClear.Count)
            {
                minNumMoves = replicasToClear.Count;
                chosenNodeToClear = nodeIdToClear;
                chosenReplicasToClear = replicasToClear;
                chosenDstNodeIdsToClearTo = dstNodeIdsToClearTo;
            }
        }

        public bool AttemptClearingSpaceOnNode(
            TimeSpan timeElapsed, int nodeIdToClear, double[] nodeIdToCpuUsage,
            double[] nodeIdToDiskUsage, double[] nodeIdToMemUsage,
            ref UsageInfo replicaToMoveUsage, out List<string> replicasToClear,
            out List<int> dstNodeIdsToClearTo)
        {
            Array.Copy(cluster.NodeIdToCurrCpuUsage,
                nodeIdToCpuUsage, cluster.NumNodes);
            Array.Copy(cluster.NodeIdToCurrDiskUsage,
                nodeIdToDiskUsage, cluster.NumNodes);
            Array.Copy(cluster.NodeIdToCurrMemoryUsage,
                nodeIdToMemUsage, cluster.NumNodes);

            // At least memory or disk need to be cleared.
            // We attempt to clear the space needed for both.

            replicasToClear = new List<string>();
            dstNodeIdsToClearTo = new List<int>();

            var clearedSpace = true;
            for (var attempt = 0; attempt < 2; attempt++)
            {
                var clearMemory = attempt == 0;
                var amountToClear = clearMemory /* else disk */ ?
                    replicaToMoveUsage.Memory - (cluster.NodeMemorySizeInMB
                        - nodeIdToMemUsage[nodeIdToClear]) :
                    replicaToMoveUsage.Disk - (cluster.NodeDiskSizeInMB
                        - nodeIdToDiskUsage[nodeIdToClear]);
                if (amountToClear > 0)
                {
                    cluster.GetReplicasSortedByResourceUsage(timeElapsed,
                        nodeIdToClear, clearMemory /* sort by Mem usage */,
                        out string[] replicasOnNode,
                        out double[] replicasUsage);
                    var i = 0;
                    while (i < replicasUsage.Length - 1 &&
                        amountToClear > replicasUsage[i]) i++;
                    for (var idx = i; idx >= 0; idx--)
                    {
                        // Attempt clearing the replica.
                        var replica = replicasOnNode[idx];
                        // if already chosen to move, skip.
                        if (replicasToClear.Contains(replica))
                            continue;

                        var trace = cluster.traceMan.ReplicaIdToTraceMap[
                            ReplicaInfo.ExtractReplicaId(replica)];
                        var usage = trace.GetResourceUsage(timeElapsed -
                            cluster.ReplicaIdToPlacementTime[replica]);

                        var dstNodeId = FindNodeToPlaceOrMoveTo(replica,
                            nodeIdToCpuUsage, nodeIdToMemUsage,
                            nodeIdToDiskUsage, usage.Cpu, usage.Memory,
                            usage.Disk, nodeIdToClear /* srcNodeId */);

                        if (dstNodeId == -1) continue;

                        replicasToClear.Add(replica);
                        dstNodeIdsToClearTo.Add(dstNodeId);
                        nodeIdToCpuUsage[nodeIdToClear] -= usage.Cpu;
                        nodeIdToMemUsage[nodeIdToClear] -= usage.Memory;
                        nodeIdToDiskUsage[nodeIdToClear] -= usage.Disk;
                        nodeIdToCpuUsage[dstNodeId] += usage.Cpu;
                        nodeIdToMemUsage[dstNodeId] += usage.Memory;
                        nodeIdToDiskUsage[dstNodeId] += usage.Disk;

                        amountToClear -= clearMemory ?
                            usage.Memory : usage.Disk;

                        if ((clearMemory && replicaToMoveUsage.Memory <=
                                (cluster.NodeMemorySizeInMB -
                                    nodeIdToMemUsage[nodeIdToClear])) ||
                            (!clearMemory && replicaToMoveUsage.Disk <=
                                (cluster.NodeDiskSizeInMB -
                                    nodeIdToDiskUsage[nodeIdToClear])))
                            break;

                        // Shift and skip on large replicas that need
                        //   not be considered.
                        var idxMoved = idx;
                        while (idxMoved > 0 &&
                                amountToClear <= replicasUsage[idxMoved - 1])
                            idxMoved--;
                        if (idxMoved != idx)
                            // due to -- in the outer loop, we increment by 1.
                            idx = idxMoved + 1;
                    }

                    if ((clearMemory && replicaToMoveUsage.Memory >
                                (cluster.NodeMemorySizeInMB -
                                    nodeIdToMemUsage[nodeIdToClear])) ||
                        (!clearMemory && replicaToMoveUsage.Disk >
                                (cluster.NodeDiskSizeInMB -
                                    nodeIdToDiskUsage[nodeIdToClear])))
                    {
                        clearedSpace = false;
                        break;
                    }
                }
            }

            return clearedSpace;
        }

        protected int ChooseNode(double[] nodeIdToScore, double optimalScore)
        {
            if (optimalScore == double.MaxValue)
                return -1;

            var candidateNodesNum = 0;
            // TODO: this allocation can be avoided.
            var candidateNodes = new int[cluster.NumNodes];
            for (var nodeId = 0; nodeId < cluster.NumNodes; nodeId++)
            {
                if (nodeIdToScore[nodeId] == optimalScore)
                {
                    candidateNodes[candidateNodesNum++] = nodeId;
                }
            }

            if (candidateNodesNum > 0)
                return candidateNodes[rand.Next(candidateNodesNum)];

            return -1;
        }

        protected void ComputeMeanAndVarianceResourceUsage(double[] nodeIdToCpuUsage,
            double[] nodeIdToMemoryUsage, double[] nodeIdToDiskUsage,
            double replicaCpuUsage, double replicaMemoryUsage,
            double replicaDiskUsage, int srcNodeId, out double diskMean,
            out double memoryMean, out double cpuMean,
            out double diskVariance, out double memoryVariance)
        {
            ComputeMeanResourceUsage(nodeIdToCpuUsage, nodeIdToMemoryUsage,
                nodeIdToDiskUsage, replicaCpuUsage, replicaMemoryUsage,
                replicaDiskUsage, srcNodeId, out diskMean, out memoryMean,
                out cpuMean);

            diskVariance = 0.0;
            memoryVariance = 0.0;
            for (var nodeId = 0; nodeId < cluster.NumNodes; nodeId++)
            {
                diskVariance += Math.Pow(nodeIdToDiskUsage[nodeId] - diskMean, 2);
                memoryVariance += Math.Pow(nodeIdToMemoryUsage[nodeId] - memoryMean, 2);
            }
        }

        protected void ComputeMeanResourceUsage(double[] nodeIdToCpuUsage,
            double[] nodeIdToMemoryUsage, double[] nodeIdToDiskUsage,
            double replicaCpuUsage, double replicaMemoryUsage,
            double replicaDiskUsage, int srcNodeId, out double diskMean,
            out double memoryMean, out double cpuMean)
        {
            // Compute mean resource usage across nodes
            diskMean = 0.0;
            memoryMean = 0.0;
            cpuMean = 0.0;

            //  For initial placement, resource usage is added to the means.
            if (srcNodeId == -1)
            {
                diskMean = replicaDiskUsage;
                memoryMean = replicaMemoryUsage;
                cpuMean = replicaCpuUsage;
            }

            // Current usage metrics.
            for (var nodeId = 0; nodeId < cluster.NumNodes; nodeId++)
            {
                diskMean += nodeIdToDiskUsage[nodeId];
                memoryMean += nodeIdToMemoryUsage[nodeId];
                cpuMean += nodeIdToCpuUsage[nodeId];
            }

            diskMean /= cluster.NumNodes;
            memoryMean /= cluster.NumNodes;
            cpuMean /= cluster.NumNodes;
        }

        // Computes the aggregate difference given a weighting function
        protected double ComputeAggregateDiff(double diskDiff,
            double memDiff, double cpuDiff, double diskMean,
            double memoryMean, double cpuMean)
        {
            switch (metricWeightingScheme)
            {
                case MetricWeightingSchemeEnum.MinWeight:
                    return Math.Min(Math.Min(diskDiff, memDiff), cpuDiff);
                case MetricWeightingSchemeEnum.UnweightedAvg:
                    return (diskDiff + memDiff + cpuDiff) / 3;
                case MetricWeightingSchemeEnum.FFSumWeight:
                    // TODO: Change this.
                    return
                        (cpuMean / (cluster.NodeNumCores * 10000) * cpuDiff +
                         diskMean / cluster.NodeDiskSizeInMB * diskDiff +
                         memoryMean / cluster.NodeMemorySizeInMB * memDiff) / 3;
                default:
                    return Math.Min(Math.Min(diskDiff, memDiff), cpuDiff);
            }
        }

        private void AssertNodeClearedIsChosenAfterClearing(string replicaId,
            ref UsageInfo replicaToMoveUsage, int srcNodeId,
            int chosenDstNodeId)
        {
            var dstNodeId = FindNodeToPlaceOrMoveTo(replicaId,
                cluster.NodeIdToCurrCpuUsage, cluster.NodeIdToCurrMemoryUsage,
                cluster.NodeIdToCurrDiskUsage, replicaToMoveUsage.Cpu,
                replicaToMoveUsage.Memory, replicaToMoveUsage.Disk, srcNodeId);

            if (dstNodeId == -1)
                throw new Exception("AssertNodeClearedIsChosenAfterClearing " +
                    "Failed - (dstNodeId == -1).");

            if (dstNodeId != chosenDstNodeId)
                throw new Exception("AssertNodeClearedIsChosenAfterClearing " +
                    "Failed - (dstNodeId != chosenNodeToClear).");
        }
    }
}
