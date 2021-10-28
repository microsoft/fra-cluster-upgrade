using HardwareSimulatorLib.Experiment;
using System;

namespace HardwareSimulatorLib.Cluster.Placement.Impl
{
    class WorstFitSelector : PlacementSelector
    {
        public WorstFitSelector(ClusterManager clusterManager,
            ExperimentParams experimentParams, Random rand) :
            base(clusterManager, experimentParams, rand)
        { }

        protected override void CompteNodeScores(string replicaId,
            double[] nodeIdToCpuUsage, double[] nodeIdToMemoryUsage,
            double[] nodeIdToDiskUsage, double replicaCpuUsage,
            double replicaMemoryUsage, double replicaDiskUsage,
            int srcNodeId, out double[] nodeIdToScore,
            out double optimalScore)
        {
            ComputeMeanResourceUsage(nodeIdToCpuUsage,
                nodeIdToMemoryUsage, nodeIdToDiskUsage, replicaCpuUsage,
                replicaMemoryUsage, replicaDiskUsage, srcNodeId,
                out double diskMean, out double memoryMean, out double cpuMean);

            // Nodes whose score equals to optimals score are candidates
            optimalScore = double.MaxValue;
            nodeIdToScore = new double[cluster.NumNodes];
            var excludedNodeIds = cluster.
                GetExcludedNodesForConstraintsOnUDsAndFDsAndUpgrade(replicaId);
            for (var nodeId = 0; nodeId < cluster.NumNodes; nodeId++)
            {
                // Initialize a node's score to an invalid value.
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

                var diskDiff = 1 - ((nodeIdToDiskUsage[nodeId] + replicaDiskUsage) / cluster.NodeDiskSizeInMB);
                var memoryDiff = 1 - ((nodeIdToMemoryUsage[nodeId] + replicaMemoryUsage) / cluster.NodeMemorySizeInMB);
                var cpuDiff = 1 - ((nodeIdToCpuUsage[nodeId] + replicaCpuUsage) / (cluster.NodeNumCores * 10000));
                nodeIdToScore[nodeId] = (-1) * ComputeAggregateDiff(diskDiff,
                    memoryDiff, cpuDiff, diskMean, memoryMean, cpuMean);

                if (optimalScore == double.MaxValue ||
                    optimalScore > nodeIdToScore[nodeId])  // Minimize score
                {
                    optimalScore = nodeIdToScore[nodeId];
                }
            }
        }
    }
}
