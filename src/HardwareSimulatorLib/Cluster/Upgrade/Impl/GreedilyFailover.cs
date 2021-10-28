using HardwareSimulatorLib.Cluster.Placement;
using HardwareSimulatorLib.Cluster.Placement.Impl;
using HardwareSimulatorLib.Experiment;
using System;
using System.Diagnostics;
using System.Linq;

namespace HardwareSimulatorLib.Cluster.Upgrade
{
    public class GreedilyFailover : UpgradeScheduler
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
            var UD = (int)(
                (timeElapsed - UpgradeStartTime).Ticks / UpgradeLength.Ticks);

            if (UD == cluster.NumUDs)
            {
                cluster.NodeIdsUnderUpgrade.Clear();
                cluster.PlacementPreference = PlacementPreference.None;
                cluster.ScaledMaxRingResourceUsage.Cpu += cluster.scaledMaxUDResourceUsageCpu;
                cluster.UpdateAllocatedRingResourceUsageForPrevUD(UD);
                // Setup direction for the next scheduled upgrade.
                isNextUpgradeLowerToHigherUDs = !isNextUpgradeLowerToHigherUDs;
                ResetNumReplicasOnDomainUnderUpgrade();
                return;
            }
            AssertNoNewlyPlacedReplicasOnPreviouslyUpgradedUD(UD - 1);

            var currNumMoves = cluster.NumMoves;

            ComputeMinNumberSwapsNeeded(UD);
            ComputeMinNumberMovesNeeded(UD);
            cluster.DenoteDomainToUpgrade(UD);
            cluster.UpdateAllocatedRingResourceUsageForPrevUD(UD);

            var stopWatch = Stopwatch.StartNew();

            var initialUD = isNextUpgradeLowerToHigherUDs ?
                0 : cluster.NumUDs - 1;
            if (UD == initialUD)
            {
                cluster.ScaledMaxRingResourceUsage.Cpu -=
                        cluster.scaledMaxUDResourceUsageCpu;
            }

            cluster.PlacementPreference = UD == initialUD ?
                (PlacementPreference.None) :
                (isNextUpgradeLowerToHigherUDs ?
                    PlacementPreference.LowerUpgradeDomains :
                    PlacementPreference.UpperUpgradeDomains);
            var placementForSwap = UD == initialUD ?
                (isNextUpgradeLowerToHigherUDs ?
                    PlacementPreference.UpperUpgradeDomains :
                    PlacementPreference.LowerUpgradeDomains) :
                (isNextUpgradeLowerToHigherUDs ?
                    PlacementPreference.LowerUpgradeDomains :
                    PlacementPreference.UpperUpgradeDomains);

            IsUpgrading = true;

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
                            placementForSwap);
                    if (replicaToSwapWith != null)
                        cluster.Swap(timeElapsed, replica, replicaToSwapWith);
                    else
                    {
                        var dstNodeId = placementSelector.
                            ChooseNodeToMoveReplicaTo(
                                timeElapsed, replica);
                        cluster.MoveReplica(timeElapsed, replica, dstNodeId);
                    }
                }
                else if (cluster.IsStandardReplica(replica))
                {
                    var dstNodeId = placementSelector.
                        ChooseNodeToMoveReplicaTo(
                            timeElapsed, replica);
                    cluster.MoveReplica(timeElapsed, replica, dstNodeId);
                }
            }

            AssertNoStandardsOrPrimariesLeft(UD);
            SetNumReplicasOnDomainUnderUpgrade(UD);
            cluster.UpdateAllocatedRingResourceUsageForThisUD(UD);

            stopWatch.Stop();

            NumMoves += (cluster.NumMoves - currNumMoves);
            cluster.NumMoves = currNumMoves;

            IsUpgrading = false;

            TimeToUpgrade = $"{stopWatch.Elapsed.TotalMilliseconds:0.0}";
            Console.WriteLine("Cluster upgraded UD " + UD + " in " +
                TimeToUpgrade + " ms");
        }
    }
}
