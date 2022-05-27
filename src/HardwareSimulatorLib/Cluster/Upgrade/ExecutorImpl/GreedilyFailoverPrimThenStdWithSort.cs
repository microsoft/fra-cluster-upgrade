using HardwareSimulatorLib.Cluster.Placement;
using HardwareSimulatorLib.Cluster.Placement.Impl;
using HardwareSimulatorLib.Experiment;
using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace HardwareSimulatorLib.Cluster.Upgrade
{
    public class GreedilyFailoverPrimThenStdWithSort : UpgradeExecutor
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
            var state = cluster.upgradeState;
            var UD = state.GetUDToUpgrade(timeElapsed);
            cluster.upgradeState.SetDomainToUpgrade(UD);

            PreUpgradeAsserts(UD, state);

            var currNumMoves = cluster.NumMoves;

            var stopWatch = Stopwatch.StartNew();
            var nonSwappedPrimaries = SwapPrimariesAndGetNonSwappedOnes(timeElapsed, UD);
            MoveStandardAndNonSwappedPrimaries(timeElapsed, UD, nonSwappedPrimaries);
            stopWatch.Stop();

            NumMoves += (cluster.NumMoves - currNumMoves);
            cluster.NumMoves = currNumMoves;
            
            PostUpgradeAsserts(UD);

            TimeToUpgrade = $"{stopWatch.Elapsed.TotalMilliseconds:0.0}";
            Console.WriteLine("Cluster upgraded UD " + UD + " in " +
                TimeToUpgrade + " ms");
        }

        private HashSet<string> SwapPrimariesAndGetNonSwappedOnes(TimeSpan timeElapsed, int UD)
        {
            var primaryReplicasOnUD = cluster.
                GetPrimaryReplicasSortedByResourceUsage(timeElapsed, UD);
            var primaryReplicasNotSwapped = new HashSet<string>();
            foreach (var primaryReplica in primaryReplicasOnUD)
            {
                var replicaToSwapWith = placementSelector.ChooseReplicaToSwapWith(
                    timeElapsed, primaryReplica, cluster.upgradeState.
                        GetSwapPlacementPreference(UD));
                if (replicaToSwapWith != null)
                    cluster.Swap(timeElapsed, primaryReplica, replicaToSwapWith);
                else primaryReplicasNotSwapped.Add(primaryReplica);
            }
            return primaryReplicasNotSwapped;
        }

        private void MoveStandardAndNonSwappedPrimaries(TimeSpan timeElapsed, int UD,
            HashSet<string> primaryReplicasToMove)
        {
            // At the moment, we merge primaries and standards.
            // Do we want to move all primaries first or keep the merge as is.
            var replicasOnUD = cluster.GetStandardReplicasSortedByResourceUsage(
                timeElapsed, UD, primaryReplicasToMove);
            foreach (var standardReplica in replicasOnUD)
            {
                var dstNodeId = placementSelector.ChooseNodeToMoveReplicaTo(
                    timeElapsed, standardReplica);
                cluster.MoveReplica(timeElapsed, standardReplica, dstNodeId);
            }
        }
    }
}
