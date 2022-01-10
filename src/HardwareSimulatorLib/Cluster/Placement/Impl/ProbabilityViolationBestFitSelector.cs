using HardwareSimulatorLib.Experiment;
using HardwareSimulatorLib.Predictor;
using System;

namespace HardwareSimulatorLib.Cluster.Placement.Impl
{
    class ProbabilityViolationBestFitSelector : ProbabilityViolationSelector
    {
        public ProbabilityViolationBestFitSelector(ClusterManager cluster,
            ExperimentParams experimentParams, Random rand, ViolationPredictor predictor) :
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

            var fNodeWithSmallPrVExists = false;
            for (var nodeId = 0; nodeId < cluster.NumNodes; nodeId++)
            {
                if (NodeIdToViolationProbability[nodeId] < probabilityOfViolationThreshold)
                {
                    fNodeWithSmallPrVExists = true;
                    break;
                }
            }

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

                if (fNodeWithSmallPrVExists &&
                    NodeIdToViolationProbability[nodeId] >= probabilityOfViolationThreshold)
                    // Note: this can't happen if we allow for temporary
                    //   overbooking since the 2nd condition can't hold any more
                    continue;

                if (fNodeWithSmallPrVExists)
                {
                    var diskDiff = 1 - ((nodeIdToDiskUsage[nodeId] + replicaDiskUsage) / cluster.NodeDiskSizeInMB);
                    var memoryDiff = 1 - ((nodeIdToMemoryUsage[nodeId] + replicaMemoryUsage) / cluster.NodeMemorySizeInMB);
                    var cpuDiff = 1 - ((nodeIdToCpuUsage[nodeId] + replicaCpuUsage) / (cluster.NodeNumCores * 10000));
                    nodeIdToScore[nodeId] = ComputeAggregateDiff(diskDiff,
                        memoryDiff, cpuDiff, diskMean, memoryMean, cpuMean);
                }
                else nodeIdToScore[nodeId] = NodeIdToViolationProbability[nodeId];

                if (optimalScore == double.MaxValue ||
                    optimalScore > nodeIdToScore[nodeId]) // Maximize score
                {
                    optimalScore = nodeIdToScore[nodeId];
                }
            }
        }
    }
}
