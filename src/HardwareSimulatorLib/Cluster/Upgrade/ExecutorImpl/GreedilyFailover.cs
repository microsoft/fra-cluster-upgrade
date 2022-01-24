using HardwareSimulatorLib.Cluster.Placement.Impl;
using HardwareSimulatorLib.Experiment;
using System;
using System.Diagnostics;
using System.Linq;

namespace HardwareSimulatorLib.Cluster.Upgrade
{
    // Warning: Do not use at the moment.
    public class GreedilyFailover : UpgradeExecutor
    {
        readonly WorstFitSelector placementSelector;

        public GreedilyFailover(ClusterManager cluster,
            ExperimentParams experimentParams) : base(cluster)
        {
            placementSelector = new WorstFitSelector(cluster, experimentParams,
                new Random(0 /* fixed seed */));
        }

        public override void Upgrade(TimeSpan timeElapsed)
        {
            var state = cluster.upgradeState;
            var UD = state.GetUDToUpgrade(timeElapsed);
            if (UD != state.GetInitialUD())
                AssertNoNewlyPlacedReplicasOnNodesUnderUpgrade(state.GetPreviouslyUpgradedUD(UD));

            AssertNoNewlyPlacedReplicasOnNodesUnderUpgrade(UD - 1);

            var currNumMoves = cluster.NumMoves;
            cluster.upgradeState.SetDomainToUpgrade(UD);
            cluster.UpdateAllocatedRingResourceUsageForPrevUD(UD);

            var stopWatch = Stopwatch.StartNew();

            // cluster.PlacementPreference = state.GetPlacementPreference(UD);

            var rnd = new Random(0);
            var replicas = cluster
                .GetPrimaryAndSecondaryReplicasSortedByResourceUsage(
                    timeElapsed, UD)
                .OrderBy(replica => rnd.Next())
                .ToArray();
            foreach (var replica in replicas)
            {
                if (cluster.IsPrimaryReplica(replica))
                {
                    var replicaToSwapWith = placementSelector.
                        ChooseReplicaToSwapWith(timeElapsed, replica,
                            state.GetSwapPlacementPreference(UD));
                    if (replicaToSwapWith != null)
                    {
                        cluster.Swap(timeElapsed, replica, replicaToSwapWith);
                    }
                    else
                    {
                        var dstNodeId = placementSelector.
                            ChooseNodeToMoveReplicaTo(timeElapsed, replica);
                        cluster.MoveReplica(timeElapsed, replica, dstNodeId);
                    }
                }
                else if (cluster.IsStandardReplica(replica))
                {
                    var dstNodeId = placementSelector.
                        ChooseNodeToMoveReplicaTo(timeElapsed, replica);
                    cluster.MoveReplica(timeElapsed, replica, dstNodeId);
                }
            }

            AssertNoStandardsOrPrimaries(UD);
            SetNumReplicasOnDomainUnderUpgrade(UD);
            cluster.UpdateAllocatedRingResourceUsageForThisUD(UD);

            stopWatch.Stop();

            NumMoves += (cluster.NumMoves - currNumMoves);
            cluster.NumMoves = currNumMoves;

            TimeToUpgrade = $"{stopWatch.Elapsed.TotalMilliseconds:0.0}";
            Console.WriteLine("Cluster upgraded UD " + UD + " in " +
                TimeToUpgrade + " ms");
        }
    }
}
