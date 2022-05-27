using HardwareSimulatorLib.Config;
using HardwareSimulatorLib.Experiment;
using HardwareSimulatorLib.Trace;
using System;

namespace HardwareSimulatorLib.Cluster.Upgrade
{
    public abstract class UpgradeExecutor
    {
        protected ClusterManager cluster;
        public string TimeToUpgrade;

        public int NumMoves;

        public static UpgradeExecutor Make(ExperimentParams Params,
                ClusterManager cluster)
        {
            switch (Params.UpgradeHeuristic)
            {
                case UpgradeHeuristicEnum.GreedilyFailoverPrimThenStdWithSort:
                default:
                    return new GreedilyFailoverPrimThenStdWithSort(cluster, Params);
            }
        }

        public abstract void Upgrade(TimeSpan timeElapsed);

        public UpgradeExecutor(ClusterManager cluster)
        {
            this.cluster = cluster;
        }

        protected void SetNumReplicasOnDomainUnderUpgrade(int UD)
        {
            var startNodeId = UD * ClusterManager.NumNodesPerUD;
            var endNodeId = (UD + 1) * ClusterManager.NumNodesPerUD;
            var i = 0;
            for (var nodeId = startNodeId; nodeId < endNodeId; nodeId++)
            {
                cluster.upgradeState
                       .numReplicasInDomainUnderUpgrade[i++] =
                            cluster.NodeIdToPlacedReplicasIdMap[nodeId].Count;
            }
        }

        protected void PreUpgradeAsserts(int UD, UpgradeScheduleAndState state)
        {
            if (UD != state.GetInitialUD())
            {
                AssertNoNewlyPlacedReplicasOnNodesUnderUpgrade(
                    state.GetPreviouslyUpgradedUD(UD));
            }
        }

        protected void PostUpgradeAsserts(int UD)
        {
            AssertNoStandardsOrPrimaries(UD);
            SetNumReplicasOnDomainUnderUpgrade(UD);
        }

        protected void AssertNoNewlyPlacedReplicasOnNodesUnderUpgrade(int UD)
        {
            if (UD < 0) return;

            var startNodeId = UD * ClusterManager.NumNodesPerUD;
            var endNodeId = (UD + 1) * ClusterManager.NumNodesPerUD;
            var i = 0;
            for (var nodeId = startNodeId; nodeId < endNodeId; nodeId++)
            {
                if (cluster.upgradeState.
                        numReplicasInDomainUnderUpgrade[i] != -1 &&
                    cluster.upgradeState.
                        numReplicasInDomainUnderUpgrade[i] <
                            cluster.NodeIdToPlacedReplicasIdMap[nodeId].Count)
                {
                    throw new Exception("AssertNoNewlyPlacedReplicasOnNodesUnderUpgrade Failed.");
                }
                i++;
            }
        }

        protected void AssertNoStandardsOrPrimaries(int UD)
        {
            var startNodeId = UD * ClusterManager.NumNodesPerUD;
            var endNodeId = (UD + 1) * ClusterManager.NumNodesPerUD;
            for (var nodeId = startNodeId; nodeId < endNodeId; nodeId++)
            {
                foreach (var replica in cluster.NodeIdToPlacedReplicasIdMap[nodeId])
                {
                    if (cluster.IsPrimaryReplica(replica) ||
                            cluster.IsStandardReplica(replica))
                    {
                        throw new Exception("AssertNoStandardsOrPrimaries Failed.");
                    }
                }
            }
        }
    }
}
