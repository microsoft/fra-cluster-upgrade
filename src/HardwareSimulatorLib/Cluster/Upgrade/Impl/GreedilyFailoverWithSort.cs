using HardwareSimulatorLib.Cluster.Placement;
using HardwareSimulatorLib.Cluster.Placement.Impl;
using HardwareSimulatorLib.Experiment;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace HardwareSimulatorLib.Cluster.Upgrade
{
    public class GreedilyFailoverWithSort : UpgradeScheduler
    {
        readonly WorstFitSelector placementSelector;

        public GreedilyFailoverWithSort(ClusterManager cluster,
            ExperimentParams experimentParams) : base(cluster)
        {
            placementSelector = new WorstFitSelector(cluster, experimentParams,
                new Random(0 /* fixed seed */));
        }

        public override void Upgrade(TimeSpan timeElapsed)
        {
            var stopWatch = Stopwatch.StartNew();

            var initialUD = isNextUpgradeLowerToHigherUDs ? 0 : cluster.NumUDs - 1;
            var UD = initialUD;
            while ((isNextUpgradeLowerToHigherUDs && UD < cluster.NumUDs) ||
                (!isNextUpgradeLowerToHigherUDs && UD >= 0))
            {
                // UD upgrade code goes here.
                var placementPreference = UD == initialUD ?
                    (isNextUpgradeLowerToHigherUDs ?
                        PlacementPreference.UpperUpgradeDomains :
                        PlacementPreference.LowerUpgradeDomains) :
                    (isNextUpgradeLowerToHigherUDs ?
                        PlacementPreference.LowerUpgradeDomains :
                        PlacementPreference.UpperUpgradeDomains);

                var replicas = cluster
                    .GetPrimaryAndSecondaryReplicasSortedByResourceUsage(
                        timeElapsed, UD);
                foreach (var replica in replicas)
                {
                    if (cluster.IsPrimaryReplica(replica))
                    {
                        var replicaToSwapWith = placementSelector.
                            ChooseReplicaToSwapWith(timeElapsed, replica,
                                placementPreference);
                        if (replicaToSwapWith != null)
                            cluster.Swap(timeElapsed, replica, replicaToSwapWith);
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
                            ChooseNodeToMoveReplicaTo(
                                timeElapsed, replica);
                        cluster.MoveReplica(timeElapsed, replica, dstNodeId);
                    }
                }

                AssertNoStandardsOrPrimariesLeft(UD);
                if (isNextUpgradeLowerToHigherUDs) UD++; else UD--;
            }

            stopWatch.Stop();
            TimeToUpgrade = $"{stopWatch.Elapsed.TotalMilliseconds:0.0}";
            Console.WriteLine("Cluster upgrade took " + TimeToUpgrade + " ms");

            // Setup for the next scheduled upgrade.
            isNextUpgradeLowerToHigherUDs = !isNextUpgradeLowerToHigherUDs;
        }
    }
}
