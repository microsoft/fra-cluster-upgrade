using System;
using System.Collections.Generic;
using System.IO;
using HardwareSimulatorLib.Predictor;
using HardwareSimulatorLib.Trace;
using HardwareSimulatorLib.Cluster;
using HardwareSimulatorLib.Cluster.Placement;

namespace HardwareSimulatorLib.Experiment
{
    public class ExperimentRunner
    {
        public const int NumMinutesPerSimInterval = 10;
        public static TimeSpan SimulationIntervalLength = TimeSpan.FromMinutes(
            NumMinutesPerSimInterval);

        private TimeSpan simDuration;
        private readonly ExperimentParams Params;
        readonly ExperimentStatistics statistics;

        private readonly ClusterManager cluster;

        public int NumCpuViolations;
        public int NumMemViolations;
        public int NumDiskViolations;

        private string[] Preupgradelogs;
        private string[] Upgradelogs;
        public string LogFailovers;

        public ExperimentRunner(TimeSpan simDuration, ExperimentParams Params,
            TraceManager traceMan, ViolationPredictor predictor, int runIdx)
        {
            this.simDuration = simDuration;
            this.Params = Params;
            this.statistics = new ExperimentStatistics();

            cluster = new ClusterManager(traceMan, simDuration, Params,
                statistics, new Random(runIdx), predictor);

            var NumUpgradesPlanned = cluster.upgradeState.
                GetNumUpgradesPlanned();
            Preupgradelogs = new string[NumUpgradesPlanned];
            Upgradelogs = new string[NumUpgradesPlanned];
        }

        public IEnumerable<ExperimentStatistics> RunExperiment()
        {
            // We keep track of total num traces included in experiment so far
            //   and use it to generate traceIDs.
            var numTracesUsed = 0;

            Console.WriteLine("Experiment time elapsed in hours: ");
            for (var timeElapsed = TimeSpan.Zero;
                     timeElapsed <= simDuration;
                     timeElapsed += SimulationIntervalLength)
            {
                if (timeElapsed.TotalHours % 10 == 0)
                    Console.WriteLine("\t" + timeElapsed.TotalHours + " hrs");

                HandleUpgradesIfAny(timeElapsed);

                cluster.EvictTenantsIfLifetimeElapsed(timeElapsed);
                cluster.UpdateResourceUsageWithNewReports(timeElapsed);

                // Fill the cluster with new tenants picked at random until
                //   cluster reaches its Cpu capacity. Assumption is that
                //   other resources scale the same as cpu.
                // Each randomly picked replica is placed based on predicted
                //   max values.
                statistics.NumPlacementFailures = 0;
                while (cluster.IsCpuCapacityReached())
                {
                    var tenant = cluster.traceMan.
                        GetNonHistoricalTenantsAtRandom(numTracesUsed++);
                    cluster.Place(timeElapsed, tenant);
                }

                // Resolve resource violations if any are found.

                // The 1st iteration of packing is to obtain an initial state of
                //   the cluster. Hence no stats are reported.
                if (timeElapsed != TimeSpan.Zero)
                    GetAndResetEssentialStatsAndLogViolations(timeElapsed);

                /* Go over each node i.e., in a loop in order starting with
                 *   node 0 to node (NumNodes - 1) and attempt to fix any found
                 *   memory or disk violation.
                 * We run the above loop 3 times guided by:
                 *  'Once or twice, though you should fail,
                 *   Try, try again.'
                 *  from 'Try, try Again' - William Edward Hickson' */
                for (var attempt = 0; attempt < 3; attempt++)
                {
                    statistics.NumReplacementFailures = 0;
                    for (var nodeId = 0; nodeId < Params.NumNodes; nodeId++)
                    {
                        if (!cluster.IsNodeInViolation(nodeId))
                            continue;

                        cluster.FindReplicasToMoveToFixViolationOnNode(
                            timeElapsed, nodeId,
                            out List<string> replicasToMove,
                            out List<int> dstNodeIds);
                        for (var i = 0; i < replicasToMove.Count; i++)
                            cluster.MoveReplica(timeElapsed,
                                replicasToMove[i], dstNodeIds[i]);
                        statistics.MovesAfterViolation += replicasToMove.Count;
                    }
                }
                LogViolations(timeElapsed, Params.outputDirectory +
                    @"NotFixed-Violations.txt");

                if (timeElapsed == simDuration)
                {
                    LogFailovers = ConcatAndGetLog();
                }

                if (timeElapsed != TimeSpan.Zero)
                    yield return statistics;
            }
        }

        private void HandleUpgradesIfAny(TimeSpan timeElapsed)
        {
            // Check for the start of upgrading a domain 
            if (cluster.upgradeState
                       .IsTimeToStartDomainUpgrade(timeElapsed))
            {
                var upgradeIdx = cluster.upgradeState.nextIdx;
                if (timeElapsed == cluster.upgradeState
                                          .UpgradeStartElapsedTime[upgradeIdx])
                {
                    cluster.PlacementPreference = cluster
                        .upgradeState.GetPlacementPreference();
                    var numViolations = NumMemViolations + NumDiskViolations;
                    Preupgradelogs[upgradeIdx] = NumMemViolations + "," +
                        NumDiskViolations + "," + numViolations + "," +
                        cluster.NumMoves + "," + cluster.NumMovesToEnablePlacement;
                    NumMemViolations = NumDiskViolations =
                        cluster.NumMoves = cluster.NumSwaps = 0;
                }

                cluster.Upgrade(timeElapsed);
            }

            if (cluster.upgradeState
                       .IsTimeToEndUpgrade(timeElapsed))
            {
                cluster.PlacementPreference = PlacementPreference.None;
                var numViolations = NumMemViolations + NumDiskViolations;
                var upgradeIdx = cluster.upgradeState.nextIdx;
                Upgradelogs[upgradeIdx] = NumMemViolations + "," +
                    NumDiskViolations + "," + numViolations + "," +
                    cluster.NumMoves + "," + cluster.NumMovesToEnablePlacement +
                    "," + cluster.NumSwaps + "," + cluster.upgradeExecutor.NumMoves;
                NumMemViolations = NumDiskViolations =
                    cluster.NumMoves = cluster.NumSwaps =
                        cluster.upgradeExecutor.NumMoves = 0;
                cluster.upgradeState.nextIdx++;
            }
        }

        private string ConcatAndGetLog()
        {
            var Log = "";
            for (var i = 0; i < Upgradelogs.Length; i++)
                Log += (Preupgradelogs[i] + "\n" + Upgradelogs[i] + "\n");

            var numViolations = NumMemViolations + NumDiskViolations;
            Log += (NumMemViolations + "," + NumDiskViolations + "," +
                numViolations + "," + cluster.NumMoves + "," +
                cluster.NumMovesToEnablePlacement);
            return Log;
        }

        public void GetAndResetEssentialStatsAndLogViolations(
            TimeSpan timeElapsed)
        {
            statistics.minutesElapsed = timeElapsed.Days * 24 * 60 +
                timeElapsed.Hours * 60 + timeElapsed.Minutes;
            statistics.NumReplicas = cluster.ActiveTenants.Count;

            var totalMem = 0.0;
            var totalDisk = 0.0;
            var totalCPU = 0.0;
            for (var nodeId = 0; nodeId < Params.NumNodes; nodeId++)
            {
                totalMem += cluster.NodeIdToCurrMemoryUsage[nodeId];
                totalDisk += cluster.NodeIdToCurrDiskUsage[nodeId];
                totalCPU += cluster.NodeIdToCurrCpuUsage[nodeId];
            }
            statistics.VCoreUtilRatio = totalCPU / 10000 /
                (Params.NodeNumCores * Params.NumNodes) * 100;
            statistics.DiskUtilRatio = totalDisk /
                (Params.NodeDiskSizeInMB * Params.NumNodes) * 100;
            statistics.MemUtilRatio = totalMem /
                (Params.NodeMemorySizeInMB * Params.NumNodes) * 100;

            statistics.MaxVCoreUtilRatio = cluster.
                AllocatedRingResourceUsage.Cpu / 10000 /
                (Params.NodeNumCores * Params.NumNodes) * 100;

            // Count and log violations.
            statistics.NumDiskViolations = 0;
            statistics.NumMemViolations = 0;
            statistics.NumCpuViolations = 0;
            LogViolations(timeElapsed, Params.outputDirectory +
                @"Violations.txt");
            statistics.Moves =
                statistics.MovesAfterViolation =
                    statistics.NumMovesDueToClearSpace = 0;
            statistics.MemoryMoved =
                statistics.DiskMoved =
                    statistics.CpuMoved = 0.0;
        }

        private void LogViolations(TimeSpan timeElapsed, string fileName)
        {
            using (var sw = new StreamWriter(fileName, true))
            {
                if (cluster.IsAnyNodeInViolation())
                    sw.WriteLine("Time Elapsed: " +
                        timeElapsed.TotalMinutes + " mins");
                for (var nodeId = 0; nodeId < Params.NumNodes; nodeId++)
                {
                    var diskUsage = cluster.NodeIdToCurrDiskUsage[nodeId];
                    if (diskUsage > Params.NodeDiskSizeInMB)
                    {
                        NumDiskViolations++;
                        statistics.NumDiskViolations++;
                        sw.WriteLine("Disk Violation on node " + nodeId + ": " +
                            diskUsage / Params.NodeDiskSizeInMB * 100 +
                                "% of capacity used");
                    }

                    var memUsage = cluster.NodeIdToCurrMemoryUsage[nodeId];
                    if (memUsage > Params.NodeMemorySizeInMB)
                    {
                        NumMemViolations++;
                        statistics.NumMemViolations++;
                        sw.WriteLine("Mem. Violation on node " + nodeId + ": " +
                            memUsage / Params.NodeMemorySizeInMB * 100 +
                                "% of capacity used");
                    }
                }
            }
        }
    }
}
