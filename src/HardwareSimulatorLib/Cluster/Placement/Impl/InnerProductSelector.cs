using HardwareSimulatorLib.Experiment;
using System;

namespace HardwareSimulatorLib.Cluster.Placement.Impl
{
    class InnerProductSelector : PlacementSelector
    {
        public InnerProductSelector(ClusterManager cluster,
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

                nodeIdToScore[nodeId] = diskDemand * diskAvailable +
                    memoryDemand * memoryAvailable + cpuDemand * cpuAvailable;
                if (optimalScore == double.MaxValue ||
                    optimalScore < nodeIdToScore[nodeId]) // Maximize score
                {
                    optimalScore = nodeIdToScore[nodeId];
                }
            }
        }
    }
}
