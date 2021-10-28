using HardwareSimulatorLib.Cluster.Placement;
using HardwareSimulatorLib.Cluster.Placement.Impl;
using HardwareSimulatorLib.Experiment;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace HardwareSimulatorLib.Cluster.Upgrade
{
    public class GreedilyFailoverPrimThenStd : UpgradeScheduler
    {
        readonly PlacementSelector placementSelector;

        public GreedilyFailoverPrimThenStd(ClusterManager cluster,
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

                var rnd = new Random(0);
                var primaryReplicasOnUD = cluster
                    .GetPrimaryReplicasSortedByResourceUsage(timeElapsed, UD)
                    .OrderBy(replica => rnd.Next())
                    .ToArray();
                var primaryReplicasToMove = new HashSet<string>();
                foreach (var primaryReplica in primaryReplicasOnUD)
                {
                    var replicaToSwapWith = placementSelector.
                        ChooseReplicaToSwapWith(timeElapsed,
                            primaryReplica, placementPreference);
                    if (replicaToSwapWith != null)
                        cluster.Swap(timeElapsed, primaryReplica, replicaToSwapWith);
                    else
                        primaryReplicasToMove.Add(primaryReplica);
                }

                rnd = new Random(1);
                var replicasOnUD = cluster
                    .GetStandardReplicasSortedByResourceUsage(
                        timeElapsed, UD, primaryReplicasToMove)
                    .OrderBy(replica => rnd.Next())
                    .ToArray();
                foreach (var standardReplica in replicasOnUD)
                {
                    var dstNodeId = placementSelector.
                        ChooseNodeToMoveReplicaTo(
                            timeElapsed, standardReplica);
                    cluster.MoveReplica(timeElapsed, standardReplica, dstNodeId);
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
