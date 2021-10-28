using HardwareSimulatorLib.Config;
using HardwareSimulatorLib.Experiment;
using System;

namespace HardwareSimulatorLib.Cluster.Placement.Impl
{
    class L2NormSelector : PlacementSelector
    {
        public L2NormSelector(ClusterManager cluster, ExperimentParams experimentParams, Random rand) :
            base(cluster, experimentParams, rand)
        { }

        protected override void CompteNodeScores(string replicaId,
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

            // Nodes whose score equals to optimals score are candidates
            optimalScore = double.MaxValue;
            nodeIdToScore = new double[cluster.NumNodes];
            var excludedNodeIds = cluster.
                GetExcludedNodesForConstraintsOnUDsAndFDsAndUpgrade(replicaId);
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

                var diskDemand = replicaDiskUsage / cluster.NodeDiskSizeInMB;
                var memoryDemand = replicaMemoryUsage / cluster.NodeMemorySizeInMB;
                var cpuDemand = replicaCpuUsage / (cluster.NodeNumCores * 10000);

                var diskAvailable = 1 - (nodeIdToDiskUsage[nodeId] / cluster.NodeDiskSizeInMB);
                var memoryAvailable = 1 - (nodeIdToMemoryUsage[nodeId] / cluster.NodeMemorySizeInMB);
                var cpuAvailable = 1 - (nodeIdToCpuUsage[nodeId] / (cluster.NodeNumCores * 10000));

                switch (metricWeightingScheme)
                {   // a||I - R||
                    default:
                    case MetricWeightingSchemeEnum.FFSumWeight:
                        nodeIdToScore[nodeId] =
                            ((diskMean / cluster.NodeDiskSizeInMB) * Math.Pow(diskDemand - diskAvailable, 2) +
                             (memoryMean / cluster.NodeMemorySizeInMB) * Math.Pow(memoryDemand - memoryAvailable, 2) +
                             (cpuMean / (cluster.NodeNumCores * 10000)) * Math.Pow(cpuDemand - cpuAvailable, 2));
                        break;
                    case MetricWeightingSchemeEnum.UnweightedAvg:
                        nodeIdToScore[nodeId] = Math.Pow(diskDemand - diskAvailable, 2) +
                            Math.Pow(memoryDemand - memoryAvailable, 2) + Math.Pow(cpuDemand - cpuAvailable, 2);
                        break;
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
