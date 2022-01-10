using HardwareSimulatorLib.Experiment;
using HardwareSimulatorLib.Predictor;
using HardwareSimulatorLib.Trace;
using System;

namespace HardwareSimulatorLib.Cluster.Placement.Impl
{
    class ProbabilityViolationWorstFitUpgradeSelector
            : ProbabilityViolationWorstFitSelector
    {
        public ProbabilityViolationWorstFitUpgradeSelector(
            ClusterManager cluster, ExperimentParams experimentParams,
            Random rand, ViolationPredictor predictor) :
                base(cluster, experimentParams, rand, predictor)
        { }

        protected override void ComputeNodeScores(string replicaId,
            double[] nodeIdToCpuUsage, double[] nodeIdToMemoryUsage,
            double[] nodeIdToDiskUsage, double replicaCpuUsage,
            double replicaMemoryUsage, double replicaDiskUsage,
            int srcNodeId, out double[] nodeIdToScore,
            out double optimalScore)
        {
            ComputeMeanResourceUsage(nodeIdToCpuUsage, nodeIdToMemoryUsage,
                nodeIdToDiskUsage, replicaCpuUsage, replicaMemoryUsage,
                replicaDiskUsage, srcNodeId, out double diskMean,
                out double memoryMean, out double cpuMean);

            var nodeAggregateDiffs = new double[cluster.NumNodes];
            var nodeIndexes = new int[cluster.NumNodes];
            // find the loaded node with min resources.
            for (var nodeId = 0; nodeId < cluster.NumNodes; nodeId++)
            {
                // Ratio of empty space
                var diskDiff = 1 - (nodeIdToDiskUsage[nodeId] / cluster.NodeDiskSizeInMB);
                var memDiff = 1 - (nodeIdToMemoryUsage[nodeId] / cluster.NodeMemorySizeInMB);
                var cpuDiff = 1 - (nodeIdToCpuUsage[nodeId] / (cluster.NodeNumCores * 10000));

                // Pick the resource which is most constrained
                nodeAggregateDiffs[nodeId] = -1 * ComputeAggregateDiff(diskDiff,
                    memDiff, cpuDiff, diskMean, memoryMean, cpuMean);
                nodeIndexes[nodeId] = nodeId;
            }
            Array.Sort(nodeAggregateDiffs, nodeIndexes);

            var excludedNodeIds = cluster.
                GetExcludedNodesForConstraintsOnUDsAndFDsAndUpgrade(replicaId);

            var anyNodeWithSmallProbabilityViolation = false;
            for (var nodeId = 0; nodeId < cluster.NumNodes; nodeId++)
            {
                if (excludedNodeIds.Contains(nodeId) ||
                    ClusterManager.IsNodeReserved(
                        nodeId, nodeIndexes, nodesToReserve))
                {
                    continue;
                }
                if (NodeIdToViolationProbability[nodeId] < probabilityOfViolationThreshold)
                {
                    anyNodeWithSmallProbabilityViolation = true;
                    break;
                }
            }

            // Nodes whose score equals to optimals score are candidates
            optimalScore = double.MaxValue;
            nodeIdToScore = new double[cluster.NumNodes];
            for (var nodeId = 0; nodeId < cluster.NumNodes; nodeId++)
            {
                // Initialize a node's score to an invalid value
                nodeIdToScore[nodeId] = double.MaxValue;

                // Check if node can accomodate the new tenant's resource usage
                if (excludedNodeIds.Contains(nodeId) ||
                    nodeIdToDiskUsage[nodeId] + replicaDiskUsage > cluster.NodeDiskSizeInMB ||
                    nodeIdToMemoryUsage[nodeId] + replicaMemoryUsage > cluster.NodeMemorySizeInMB ||
                    nodeIdToCpuUsage[nodeId] + replicaCpuUsage > cluster.NodeNumCores * 10000 ||
                    nodeId == srcNodeId)
                {
                    continue;
                }

                if (ClusterManager.IsNodeReserved(nodeId, nodeIndexes, nodesToReserve) ||
                    (anyNodeWithSmallProbabilityViolation &&
                        NodeIdToViolationProbability[nodeId] >= probabilityOfViolationThreshold))
                    // This one is off limits unless we have to use it
                    continue;

                var diskDiff = 1 - ((nodeIdToDiskUsage[nodeId] + replicaDiskUsage) / cluster.NodeDiskSizeInMB);
                var memoryDiff = 1 - ((nodeIdToMemoryUsage[nodeId] + replicaMemoryUsage) / cluster.NodeMemorySizeInMB);
                var cpuDiff = 1 - ((nodeIdToCpuUsage[nodeId] + replicaCpuUsage) / (cluster.NodeNumCores * 10000));
                nodeIdToScore[nodeId] = -1 * ComputeAggregateDiff(diskDiff,
                    memoryDiff, cpuDiff, diskMean, memoryMean, cpuMean);

                // Add extra update move as a weighted sum.
                if (cluster.IsStandardReplica(replicaId) &&
                        nodeId < ClusterManager.NumNodesPerUD)
                {
                    nodeIdToScore[nodeId]++;
                }
                else if (cluster.IsPrimaryReplica(replicaId))
                {
                    var isReplicaLowestUD = true;
                    var tenantId = ReplicaInfo.ExtractTenantIdWithTrace(replicaId);
                    foreach (var replica in cluster.TenantIdToReplicaId[tenantId])
                    {
                        if (replica == replicaId) continue;
                        if (cluster.ReplicaIdToPlacementNodeIdMap.ContainsKey(replica) &&
                                cluster.ReplicaIdToPlacementNodeIdMap[replica] < nodeId)
                        {
                            isReplicaLowestUD = false;
                            break;
                        }
                    }
                    nodeIdToScore[nodeId] += isReplicaLowestUD ? 1 : 0;
                }

                if (optimalScore == double.MaxValue ||
                    optimalScore > nodeIdToScore[nodeId]) // Minimize score
                {
                    optimalScore = nodeIdToScore[nodeId];
                }
            }
        }
    }
}
