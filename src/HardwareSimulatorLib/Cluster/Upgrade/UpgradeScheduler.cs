using HardwareSimulatorLib.Config;
using HardwareSimulatorLib.Experiment;
using HardwareSimulatorLib.Trace;
using System;
using System.Collections.Generic;

namespace HardwareSimulatorLib.Cluster.Upgrade
{
    public abstract class UpgradeScheduler
    {
        protected ClusterManager cluster;
        public int MinNumberSwapsNeededGivenReplicas;
        public int MinNumberMovesNeededGivenReplicas;
        public string TimeToUpgrade;

        protected bool isNextUpgradeLowerToHigherUDs;
        protected int numberFailedUdUpgrades;

        public static Dictionary<string, int> premUpgradeMoveCount = new Dictionary<string, int>();
        public static Dictionary<string, int> stdUpgradeMoveCount = new Dictionary<string, int>();
        public static Dictionary<string, int> secUpgradeMoveCount = new Dictionary<string, int>();
        public static Dictionary<string, int> premRegularMovesCount = new Dictionary<string, int>();
        public static Dictionary<string, int> stdRegularMoveCount = new Dictionary<string, int>();
        public static Dictionary<string, int> secRegularMoveCount = new Dictionary<string, int>();
        public static bool IsUpgrading = false;

        protected int[] numReplicasInDomainUnderUpgrade = new int[] { -1, -1, -1, -1 };

        public int NumMoves;

        public static TimeSpan UpgradeStartTime = new TimeSpan(
            (24 + 336) /* hrs */, 0 /* mins */, 0 /* secs */);
        public static TimeSpan UpgradeEndTime = new TimeSpan(
            (24 + 376) /* hrs */, 0 /* mins */, 0 /* secs */);
        public static TimeSpan UpgradeLength = new TimeSpan(
                    4 /* hrs */, 0 /* mins */, 0 /* secs */);

        public static void InitializeDataDistributionCounters()
        {
            premUpgradeMoveCount.Clear();
            stdUpgradeMoveCount.Clear();
            secUpgradeMoveCount.Clear();
            premRegularMovesCount.Clear();
            stdRegularMoveCount.Clear();
            secRegularMoveCount.Clear();
        }

        public static UpgradeScheduler Make(ExperimentParams Params,
                ClusterManager cluster)
        {
            switch (Params.UpgradeHeuristic)
            {
                case UpgradeHeuristicEnum.GreedilyFailover:
                    return new GreedilyFailover(cluster, Params);
                case UpgradeHeuristicEnum.GreedilyFailoverWithSort:
                    return new GreedilyFailoverWithSort(cluster, Params);
                case UpgradeHeuristicEnum.GreedilyFailoverPrimThenStd:
                    return new GreedilyFailoverPrimThenStd(cluster, Params);
                default:
                case UpgradeHeuristicEnum.GreedilyFailoverPrimThenStdWithSort:
                    return new GreedilyFailoverPrimThenStdWithSort(cluster, Params);
            }
        }

        public abstract void Upgrade(TimeSpan timeElapsed);

        public UpgradeScheduler(ClusterManager cluster)
        {
            this.cluster = cluster;
            // First upgrade domain goes from 0 to m.
            // Second goes from m to 0 and so on.
            this.isNextUpgradeLowerToHigherUDs = true;
        }

        protected void AssertNoNewlyPlacedReplicasOnPreviouslyUpgradedUD(int UD)
        {
            if (UD < 0) return;

            var startNodeId = UD * ClusterManager.NumNodesPerUD;
            var endNodeId = (UD + 1) * ClusterManager.NumNodesPerUD;
            var i = 0;
            for (var nodeId = startNodeId; nodeId < endNodeId; nodeId++)
            {
                if (numReplicasInDomainUnderUpgrade[i] != -1 &&
                    numReplicasInDomainUnderUpgrade[i] <
                        cluster.NodeIdToPlacedReplicasIdMap[nodeId].Count)
                    throw new Exception("AssertNoNewlyPlacedReplicasOnPreviouslyUpgradedUD Failed.");
                i++;
            }
        }

        protected void ResetNumReplicasOnDomainUnderUpgrade()
        {
            numReplicasInDomainUnderUpgrade[0] =
                numReplicasInDomainUnderUpgrade[1] =
                    numReplicasInDomainUnderUpgrade[2] =
                        numReplicasInDomainUnderUpgrade[3] = -1;
        }

        protected void SetNumReplicasOnDomainUnderUpgrade(int UD)
        {
            var startNodeId = UD * ClusterManager.NumNodesPerUD;
            var endNodeId = (UD + 1) * ClusterManager.NumNodesPerUD;
            var i = 0;
            for (var nodeId = startNodeId; nodeId < endNodeId; nodeId++)
            {
                numReplicasInDomainUnderUpgrade[i++] =
                    cluster.NodeIdToPlacedReplicasIdMap[nodeId].Count;
            }
        }

        private int[] rcSecondariesNodes = new int[3];
        public void ComputeMinNumberSwapsNeeded(int UD)
        {
            var startNodeId = UD * ClusterManager.NumNodesPerUD;
            var endNodeId = (UD + 1) * ClusterManager.NumNodesPerUD;
            for (var nodeId = startNodeId; nodeId < endNodeId; nodeId++)
            {
                foreach (var replica in cluster.NodeIdToPlacedReplicasIdMap[nodeId])
                {
                    if (cluster.IsPrimaryReplica(replica))
                    {
                        var tenantId = ReplicaInfo.ExtractTenantIdWithTrace(replica);
                        var i = 0;
                        foreach (var secondary in cluster.TenantIdToReplicaId[tenantId])
                        {
                            if (replica == secondary) continue;
                            rcSecondariesNodes[i++] = cluster.
                                ReplicaIdToPlacementNodeIdMap[secondary];
                        }

                        // Hardcoded to count nodes on left.
                        // Needs to be fixed for future runs with multiple upgrades.
                        var nodesOnLeft = 0;
                        foreach (var node in rcSecondariesNodes)
                            if (node < nodeId)
                                nodesOnLeft++;

                        MinNumberSwapsNeededGivenReplicas++;
                    }
                }
            }
        }

        public void ComputeMinNumberMovesNeeded(int UD)
        {
            var startNodeId = UD * ClusterManager.NumNodesPerUD;
            var endNodeId = (UD + 1) * ClusterManager.NumNodesPerUD;
            for (var nodeId = startNodeId; nodeId < endNodeId; nodeId++)
            {
                var replicas = cluster.NodeIdToPlacedReplicasIdMap[nodeId];
                foreach (var replica in replicas)
                    if (cluster.IsStandardReplica(replica))
                        MinNumberMovesNeededGivenReplicas++;
            }
        }

        protected void AssertNoStandardsOrPrimariesLeft(int UD)
        {
            var startNodeId = UD * ClusterManager.NumNodesPerUD;
            var endNodeId = (UD + 1) * ClusterManager.NumNodesPerUD;
            for (var nodeId = startNodeId; nodeId < endNodeId; nodeId++)
            {
                foreach (var replica in cluster.NodeIdToPlacedReplicasIdMap[nodeId])
                {
                    if (cluster.IsPrimaryReplica(replica) ||
                        cluster.IsStandardReplica(replica))
                        throw new Exception("NoStandardsOrPrimariesLeft Failed.");
                }
            }
        }
    }
}
