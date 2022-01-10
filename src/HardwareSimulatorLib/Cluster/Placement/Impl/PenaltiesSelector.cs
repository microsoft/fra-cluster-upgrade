using HardwareSimulatorLib.Experiment;
using System;

namespace HardwareSimulatorLib.Cluster.Placement.Impl
{
    // TODO: broken due to the use of reserved nodes and should be similar to
    //       WorstFitProbabilityViolationSelector.
    class PenaltiesSelector : PlacementSelector
    {
        private readonly double penaltiesParameterThreshold;

        public PenaltiesSelector(ClusterManager cluster,
            ExperimentParams experimentParams, Random rand) :
            base(cluster, experimentParams, rand)
        {
            this.penaltiesParameterThreshold = experimentParams.PenaltiesParameterThreshold;
        }

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

            var nodeDiffs = new double[cluster.NumNodes];
            var nodeIndexes = new int[cluster.NumNodes];

            for (var nodeId = 0; nodeId < cluster.NumNodes; nodeId++)
            {
                // Ratio of empty space
                var diskDiff = 1 - (nodeIdToDiskUsage[nodeId] / cluster.NodeDiskSizeInMB);
                var memDiff = 1 - (nodeIdToMemoryUsage[nodeId] / cluster.NodeMemorySizeInMB);
                var cpuDiff = 1 - (nodeIdToCpuUsage[nodeId] / (cluster.NodeNumCores * 10000));

                // Pick the resource which is most constrained
                nodeDiffs[nodeId] = -1 * ComputeAggregateDiff(diskDiff,
                    memDiff, cpuDiff, diskMean, memoryMean, cpuMean);
                nodeIndexes[nodeId] = nodeId;
            }
            Array.Sort(nodeDiffs, nodeIndexes);

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

                if (ClusterManager.IsNodeReserved(nodeId, nodeIndexes, nodesToReserve))
                    // This one is off limits unless we have to use it
                    continue;

                var diskDiff = 1 - ((nodeIdToDiskUsage[nodeId] + replicaDiskUsage) / cluster.NodeDiskSizeInMB);
                var memoryDiff = 1 - ((nodeIdToMemoryUsage[nodeId] + replicaMemoryUsage) / cluster.NodeMemorySizeInMB);
                var cpuDiff = 1 - ((nodeIdToCpuUsage[nodeId] + replicaCpuUsage) / (cluster.NodeNumCores * 10000));

                var aggregateDiff = -1 * ComputeAggregateDiff(diskDiff,
                    memoryDiff, cpuDiff, diskMean, memoryMean, cpuMean);

                nodeIdToScore[nodeId] = aggregateDiff;
                if (optimalScore == double.MaxValue ||
                    optimalScore > aggregateDiff) // Minimize score
                {
                    optimalScore = aggregateDiff;
                }
            }

            // Placement increases node load beyond 75%.
            if (Math.Abs(optimalScore) < penaltiesParameterThreshold)
                // || optimalScore == double.MaxValue) // Haven't been able to place this
            {
                for (var j = 0; j < Math.Min(nodesToReserve, cluster.NumNodes); j++)
                {
                    int nodeId = nodeIndexes[j];
                    // Check if the node can accomodate the new tenant's resource usage
                    if ((nodeIdToDiskUsage[nodeId] + replicaDiskUsage > cluster.NodeDiskSizeInMB ||
                        nodeIdToMemoryUsage[nodeId] + replicaMemoryUsage > cluster.NodeMemorySizeInMB ||
                        nodeIdToCpuUsage[nodeId] + replicaCpuUsage > cluster.NodeNumCores * 10000 ||
                        nodeId == srcNodeId))
                    {
                        continue;
                    }

                    // Consider the held out node(s)
                    var diskDiff = 1 - ((nodeIdToDiskUsage[nodeId] + replicaDiskUsage) / cluster.NodeDiskSizeInMB);
                    var memoryDiff = 1 - ((nodeIdToMemoryUsage[nodeId] + replicaMemoryUsage) / cluster.NodeMemorySizeInMB);
                    var cpuDiff = 1 - ((nodeIdToCpuUsage[nodeId] + replicaCpuUsage) / (cluster.NodeNumCores * 10000));
                    nodeIdToScore[nodeId] = -1 * ComputeAggregateDiff(diskDiff,
                        memoryDiff, cpuDiff, diskMean, memoryMean, cpuMean);

                    if (optimalScore == double.MaxValue ||
                        optimalScore > nodeIdToScore[nodeId]) // Minimize score
                    {
                        optimalScore = nodeIdToScore[nodeId];
                    }
                }
            }
        }
    }
}
