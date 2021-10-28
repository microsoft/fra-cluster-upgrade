using HardwareSimulatorLib.Config;
using HardwareSimulatorLib.Experiment;
using System;

namespace HardwareSimulatorLib.Cluster.Placement.Impl
{
    class MinStdDivSelector : PlacementSelector
    {
        public MinStdDivSelector(ClusterManager cluster,
            ExperimentParams experimentParams, Random rand) :
            base(cluster, experimentParams, rand)
        { }

        protected override void CompteNodeScores(string replicaId,
            double[] nodeIdToCpuUsage, double[] nodeIdToMemoryUsage,
            double[] nodeIdToDiskUsage, double replicaCpuUsage,
            double replicaMemoryUsage, double replicaDiskUsage,
            int srcNodeId, out double[] nodeIdToScore,
            out double optimalScore)
        {
            ComputeMeanAndVarianceResourceUsage(nodeIdToCpuUsage,
                nodeIdToMemoryUsage, nodeIdToDiskUsage, replicaCpuUsage,
                replicaMemoryUsage, replicaDiskUsage, srcNodeId,
                out double diskMean, out double memoryMean, out double cpuMean,
                out double diskVariance, out double memoryVariance);

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

                var diskDiff = diskVariance - Math.Pow(nodeIdToDiskUsage[nodeId] - diskMean, 2) +
                    Math.Pow(nodeIdToDiskUsage[nodeId] + replicaDiskUsage - diskMean, 2);
                var memoryDiff = memoryVariance - Math.Pow(nodeIdToMemoryUsage[nodeId] - memoryMean, 2) +
                    Math.Pow(nodeIdToMemoryUsage[nodeId] + replicaMemoryUsage - memoryMean, 2);

                var diskWeight = diskMean / cluster.NodeDiskSizeInMB;
                var memoryWeight = memoryMean / cluster.NodeMemorySizeInMB;

                if (metricWeightingScheme == MetricWeightingSchemeEnum.FFSumWeight)
                    nodeIdToScore[nodeId] = diskWeight * diskDiff + memoryWeight * memoryDiff;
                else nodeIdToScore[nodeId] = diskDiff + memoryDiff;

                if (optimalScore == double.MaxValue ||
                    optimalScore > diskDiff + memoryDiff) // Minimize score
                {
                    if (metricWeightingScheme == MetricWeightingSchemeEnum.FFSumWeight)
                        optimalScore = diskWeight * diskDiff +
                            memoryWeight * memoryDiff;
                    else optimalScore = diskDiff + memoryDiff;
                }
            }
        }
    }
}
