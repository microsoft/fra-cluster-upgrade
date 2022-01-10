using HardwareSimulatorLib.Config;
using HardwareSimulatorLib.Experiment;
using System;

namespace HardwareSimulatorLib.Cluster.Placement.Impl
{
    class SumOfSquaresSelector : PlacementSelector
    {
        public SumOfSquaresSelector(ClusterManager cluster,
            ExperimentParams experimentParams, Random rand) :
            base(cluster, experimentParams, rand)
        { }

        protected override void ComputeNodeScores(string replicaId,
            double[] nodeIdToCpuUsage, double[] nodeIdToMemoryUsage,
            double[] nodeIdToDiskUsage, double replicaCpuUsage,
            double replicaMemoryUsage, double replicaDiskUsage,
            int srcNodeId, out double[] nodeIdToScore,
            out double optimalScore)
        {
            const int NumberOfIntervals = 10; // <- We can revisit this later
            var MinBoundaries = new double[NumberOfIntervals];
            var MaxBoundaries = new double[NumberOfIntervals];
            var IntervalCounts = new int[NumberOfIntervals];
            var IntervalWeights = new double[NumberOfIntervals];

            // Compute the current potential function
            for (var i = 1; i <= NumberOfIntervals; i++)
            {
                MinBoundaries[i - 1] = (((float)i) - 1.0)
                                            / (float)NumberOfIntervals;
                IntervalCounts[i - 1] = 0;
                IntervalWeights[i - 1] = i * 10;
            }
            for (int i = 1; i < NumberOfIntervals; i++)
                MaxBoundaries[i - 1] = MinBoundaries[i];
            MaxBoundaries[9] = 1;

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

                // Update the potential function for the case where we have an incoming tenant at nodeIndex
                // Revisit these. They seem to be buggy.
                var diskDiff = 1 - ((nodeIdToDiskUsage[nodeId] + replicaDiskUsage) / cluster.NodeDiskSizeInMB);
                var memoryDiff = 1 - ((nodeIdToMemoryUsage[nodeId] + replicaMemoryUsage) / cluster.NodeMemorySizeInMB);
                var cpuDiff = 1 - ((nodeIdToCpuUsage[nodeId] + replicaCpuUsage) / (cluster.NodeNumCores * 10000));
                var oldDiskDiff = 1 - ((nodeIdToDiskUsage[nodeId] + replicaDiskUsage) / cluster.NodeDiskSizeInMB);
                var oldMemoryDiff = 1 - ((nodeIdToMemoryUsage[nodeId] + replicaMemoryUsage) / cluster.NodeMemorySizeInMB);
                var oldCpuDiff = 1 - ((nodeIdToCpuUsage[nodeId] + replicaCpuUsage) / (cluster.NodeNumCores * 10000));

                // Removing a tenant from the source also has effects
                var diskDiffSrc = 0.0;
                var memoryDiffSrc = 0.0;
                var cpuDiffSrc = 0.0;
                var oldDiskDiffSrc = 0.0;
                var oldMemoryDiffSrc = 0.0;
                var oldCpuDiffSrc = 0.0;

                if (srcNodeId != -1)
                {
                    // TODO: Revisit these. They seem to be buggy.
                    diskDiffSrc = 1 - ((nodeIdToDiskUsage[srcNodeId] + replicaDiskUsage) / cluster.NodeDiskSizeInMB);
                    memoryDiffSrc = 1 - ((nodeIdToMemoryUsage[srcNodeId] + replicaMemoryUsage) / cluster.NodeMemorySizeInMB);
                    cpuDiffSrc = 1 - ((nodeIdToCpuUsage[srcNodeId] + replicaCpuUsage) / (cluster.NodeNumCores * 10000));
                    oldDiskDiffSrc = 1 - ((nodeIdToDiskUsage[srcNodeId] + replicaDiskUsage) / cluster.NodeDiskSizeInMB);
                    oldMemoryDiffSrc = 1 - ((nodeIdToMemoryUsage[srcNodeId] + replicaMemoryUsage) / cluster.NodeMemorySizeInMB);
                    oldCpuDiffSrc = 1 - ((nodeIdToCpuUsage[nodeId] + replicaCpuUsage) / (cluster.NodeNumCores * 10000));
                }

                var aggregateDiffSrc = 0.0;
                var oldAggregateDiffSrc = 0.0;

                var aggregateDiff = Math.Max(Math.Max(diskDiff, memoryDiff), cpuDiff); // This is something we may want to revisit
                var oldAggregateDiff = Math.Max(Math.Max(oldDiskDiff, oldMemoryDiff), oldCpuDiff); // This is something we may want to revisit
                if (srcNodeId != -1)
                {
                    aggregateDiffSrc = Math.Max(Math.Max(diskDiffSrc, memoryDiffSrc), cpuDiffSrc); // This is something we may want to revisit
                    oldAggregateDiffSrc = Math.Max(Math.Max(oldDiskDiffSrc, oldMemoryDiffSrc), oldCpuDiffSrc); // This is something we may want to revisit
                }

                var CurrentIntervalCounts = new int[NumberOfIntervals];
                for (var i = 0; i < NumberOfIntervals; i++)
                {
                    CurrentIntervalCounts[i] = IntervalCounts[i];
                    if (aggregateDiff > MinBoundaries[i] && aggregateDiff <= MaxBoundaries[i])
                        IntervalCounts[i]++;
                    if (oldAggregateDiff > MinBoundaries[i] && oldAggregateDiff <= MaxBoundaries[i])
                        IntervalCounts[i]--;
                    if (srcNodeId != -1)
                    {
                        if (aggregateDiffSrc > MinBoundaries[i] && aggregateDiffSrc <= MaxBoundaries[i])
                            IntervalCounts[i]++;
                        if (oldAggregateDiffSrc > MinBoundaries[i] && oldAggregateDiffSrc <= MaxBoundaries[i])
                            IntervalCounts[i]--;
                    }
                }

                nodeIdToScore[nodeId] = 0.0;
                for (var i = 0; i < NumberOfIntervals; i++)
                    nodeIdToScore[nodeId] += Math.Pow(CurrentIntervalCounts[i] * IntervalWeights[i], 2);

                if (optimalScore == double.MaxValue ||
                    optimalScore > nodeIdToScore[nodeId]) // Minimize score
                {
                    optimalScore = nodeIdToScore[nodeId];
                }
            }
        }
    }
}
