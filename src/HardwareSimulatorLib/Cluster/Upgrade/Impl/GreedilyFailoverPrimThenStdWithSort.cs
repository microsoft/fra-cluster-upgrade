using HardwareSimulatorLib.Cluster.Placement;
using HardwareSimulatorLib.Cluster.Placement.Impl;
using HardwareSimulatorLib.Experiment;
using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace HardwareSimulatorLib.Cluster.Upgrade
{
    public class GreedilyFailoverPrimThenStdWithSort : UpgradeScheduler
    {
        readonly WorstFitSelector placementSelector;

        public GreedilyFailoverPrimThenStdWithSort(ClusterManager cluster,
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

            var primaryReplicasOnUD = cluster.
                GetPrimaryReplicasSortedByResourceUsage(timeElapsed, UD);
            var primaryReplicasToMove = new HashSet<string>();
            foreach (var primaryReplica in primaryReplicasOnUD)
            {
                var replicaToSwapWith = placementSelector.
                    ChooseReplicaToSwapWith(timeElapsed, primaryReplica,
                        placementForSwap);
                if (replicaToSwapWith != null)
                    cluster.Swap(timeElapsed, primaryReplica, replicaToSwapWith);
                else
                    primaryReplicasToMove.Add(primaryReplica);
            }

            var replicasOnUD = cluster.
                GetStandardReplicasSortedByResourceUsage(
                    timeElapsed, UD, primaryReplicasToMove);
            foreach (var standardReplica in replicasOnUD)
            {
                var dstNodeId = placementSelector.
                    ChooseNodeToMoveReplicaTo(timeElapsed, standardReplica);
                cluster.MoveReplica(timeElapsed, standardReplica, dstNodeId);
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
