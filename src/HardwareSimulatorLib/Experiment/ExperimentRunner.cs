using System;
using System.Collections.Generic;
using System.IO;
using HardwareSimulatorLib.Predictor;
using HardwareSimulatorLib.Trace;
using HardwareSimulatorLib.Cluster;
using HardwareSimulatorLib.Cluster.Upgrade;

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

        private string preupgradelog;
        private string upgradelog;
        private string postupgradelog;
        public string Log;
        private int runIdx;

        public ExperimentRunner(TimeSpan simDuration, ExperimentParams Params,
            TraceManager traceMan, ViolationPredictor predictor, int runIdx)
        {
            this.simDuration = simDuration;
            this.Params = Params;
            this.statistics = new ExperimentStatistics();
            this.runIdx = runIdx;

            cluster = new ClusterManager(traceMan, Params, statistics,
                new Random(runIdx), predictor);
        }

        public static int MapToNumSimIntervals(TimeSpan time)
            => Convert.ToInt32(time.TotalMinutes / NumMinutesPerSimInterval);

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
                if (timeElapsed.TotalMinutes % 600 == 0)
                    Console.WriteLine("\t" + timeElapsed.TotalMinutes / 60 +
                        " hours");

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

                if (cluster.IsTimeToUpgrade(timeElapsed))
                {
                    if (timeElapsed == UpgradeScheduler.UpgradeStartTime)
                    {
                        preupgradelog =
                            NumMemViolations + "," +
                            NumDiskViolations + "," +
                            (NumMemViolations + NumDiskViolations) + "," +
                            cluster.NumMoves;
                        NumMemViolations = NumDiskViolations = cluster.NumMoves = 0;

                        LogRegularMovesDataDistribution("pre-");
                        UpgradeScheduler.InitializeDataDistributionCounters();
                    }
                    // Log here.

                    cluster.Upgrade(timeElapsed);

                    if (timeElapsed == UpgradeScheduler.UpgradeEndTime)
                    {
                        upgradelog =
                            NumMemViolations + "," +
                            NumDiskViolations + "," +
                            (NumMemViolations + NumDiskViolations) + "," +
                            cluster.NumMoves + "," +
                            cluster.NumSwaps + "," +
                            cluster.upgradeScheduler.NumMoves;
                        NumMemViolations = NumDiskViolations =
                            cluster.NumMoves = cluster.NumSwaps = 0;
                        var data = LogUpgradeMovesDataDistribution();
                        LogRegularMovesDataDistribution("up-", data);
                        UpgradeScheduler.InitializeDataDistributionCounters();
                    }
                }

                if (timeElapsed == simDuration)
                {
                    postupgradelog =
                        NumMemViolations + "," +
                        NumDiskViolations + "," +

                            (NumMemViolations + NumDiskViolations) + "," + cluster.NumMoves;
                    Log = preupgradelog + "," +
                        postupgradelog + "," + upgradelog;

                    LogRegularMovesDataDistribution("post-");
                }

                if (timeElapsed != TimeSpan.Zero)
                    yield return statistics;
            }
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

        private void LogRegularMovesDataDistribution(string prefix,
            Dictionary<string, int> numTenantMovesToCountFromUpgrades = null)
        {
            var dir = Params.outputDirectory + $"\\regular_moves\\";
            if (!Directory.Exists(dir))
                Directory.CreateDirectory(dir);

            var numTenantMovesToCount = new Dictionary<string, int>();

            var numMovesToCount = new Dictionary<int, int>();
            using (var sw = new StreamWriter(dir + $"{prefix}StdRegularMoves_{runIdx}.txt", true))
            {
                foreach (var replicaId in UpgradeScheduler.stdRegularMoveCount.Keys)
                {
                    var tenantId = ReplicaInfo.ExtractTenantIdWithTrace(replicaId);
                    if (!numTenantMovesToCount.ContainsKey(tenantId))
                        numTenantMovesToCount[tenantId] = 0;
                    numTenantMovesToCount[tenantId] = numTenantMovesToCount[tenantId] + 1;

                    var moves = UpgradeScheduler.stdRegularMoveCount[replicaId];
                    if (!numMovesToCount.ContainsKey(moves))
                        numMovesToCount[moves] = 0;
                    numMovesToCount[moves]++;
                    sw.WriteLine(replicaId + "," + moves);
                }
            }
            using (var sw = new StreamWriter(dir + $"{prefix}StdRegularMovesAgg_{runIdx}.txt", true))
            {
                foreach (var numMoves in numMovesToCount.Keys)
                    sw.WriteLine(numMoves + "," + numMovesToCount[numMoves]);
            }

            numMovesToCount = new Dictionary<int, int>();
            using (var sw = new StreamWriter(dir + $"{prefix}PremRegularMoves_{runIdx}.txt", true))
            {
                foreach (var replicaId in UpgradeScheduler.premRegularMovesCount.Keys)
                {
                    var tenantId = ReplicaInfo.ExtractTenantIdWithTrace(replicaId);
                    if (!numTenantMovesToCount.ContainsKey(tenantId))
                        numTenantMovesToCount[tenantId] = 0;
                    numTenantMovesToCount[tenantId]++;

                    var moves = UpgradeScheduler.premRegularMovesCount[replicaId];
                    if (!numMovesToCount.ContainsKey(moves))
                        numMovesToCount[moves] = 0;
                    numMovesToCount[moves]++;
                    sw.WriteLine(replicaId + "," + moves);
                }
            }
            using (var sw = new StreamWriter(dir + $"{prefix}PremRegularMovesAgg_{runIdx}.txt", true))
            {
                foreach (var numMoves in numMovesToCount.Keys)
                    sw.WriteLine(numMoves + "," + numMovesToCount[numMoves]);
            }

            numMovesToCount = new Dictionary<int, int>();
            using (var sw = new StreamWriter(dir + $"{prefix}SecRegularMoves_{runIdx}.txt", true))
            {
                foreach (var replicaId in UpgradeScheduler.secRegularMoveCount.Keys)
                {
                    var tenantId = ReplicaInfo.ExtractTenantIdWithTrace(replicaId);
                    if (!numTenantMovesToCount.ContainsKey(tenantId))
                        numTenantMovesToCount[tenantId] = 0;
                    numTenantMovesToCount[tenantId]++;

                    var moves = UpgradeScheduler.secRegularMoveCount[replicaId];
                    if (!numMovesToCount.ContainsKey(moves))
                        numMovesToCount[moves] = 0;
                    numMovesToCount[moves]++;
                    sw.WriteLine(replicaId + "," + moves);
                }
            }
            using (var sw = new StreamWriter(dir + $"{prefix}SecRegularMovesAgg_{runIdx}.txt", true))
            {
                foreach (var numMoves in numMovesToCount.Keys)
                    sw.WriteLine(numMoves + "," + numMovesToCount[numMoves]);
            }

            numMovesToCount = new Dictionary<int, int>();
            using (var sw = new StreamWriter(dir + $"{prefix}TenantRegularMoves_{runIdx}.txt", true))
            {
                foreach (var tenant in numTenantMovesToCount.Keys)
                {
                    var moves = numTenantMovesToCount[tenant];

                    if (!numMovesToCount.ContainsKey(moves))
                        numMovesToCount[moves] = 0;
                    numMovesToCount[moves]++;

                    sw.WriteLine(tenant + "," + moves);
                }
            }
            using (var sw = new StreamWriter(dir + $"{prefix}TenantRegularMovesAgg_{runIdx}.txt", true))
            {
                foreach (var numMoves in numMovesToCount.Keys)
                    sw.WriteLine(numMoves + "," + numMovesToCount[numMoves]);
            }

            if (numTenantMovesToCountFromUpgrades != null)
            {
                foreach (var tenant in numTenantMovesToCountFromUpgrades.Keys)
                {
                    if (!numTenantMovesToCount.ContainsKey(tenant))
                        numTenantMovesToCount[tenant] = numTenantMovesToCountFromUpgrades[tenant];
                    else
                        numTenantMovesToCount[tenant] += numTenantMovesToCountFromUpgrades[tenant];
                }
                numMovesToCount = new Dictionary<int, int>();
                using (var sw = new StreamWriter(dir + $"{prefix}TenantMoves_{runIdx}.txt", true))
                {
                    foreach (var tenant in numTenantMovesToCount.Keys)
                    {
                        var moves = numTenantMovesToCount[tenant];

                        if (!numMovesToCount.ContainsKey(moves))
                            numMovesToCount[moves] = 0;
                        numMovesToCount[moves]++;

                        sw.WriteLine(tenant + "," + moves);
                    }
                }
                using (var sw = new StreamWriter(dir + $"{prefix}TenantMovesAgg_{runIdx}.txt", true))
                {
                    foreach (var numMoves in numMovesToCount.Keys)
                        sw.WriteLine(numMoves + "," + numMovesToCount[numMoves]);
                }
            }
        }

        private Dictionary<string, int> LogUpgradeMovesDataDistribution()
        {
            var dir = Params.outputDirectory + $"\\upgrade_moves\\";
            if (!Directory.Exists(dir))
                Directory.CreateDirectory(dir);

            var numTenantMovesToCount = new Dictionary<string, int>();

            var numMovesToCount = new Dictionary<int, int>();
            using (var sw = new StreamWriter(dir + $"StdUpgradeMoves_{runIdx}.txt", true))
            {
                foreach (var replicaId in UpgradeScheduler.stdUpgradeMoveCount.Keys)
                {
                    var tenantId = ReplicaInfo.ExtractTenantIdWithTrace(replicaId);
                    if (!numTenantMovesToCount.ContainsKey(tenantId))
                        numTenantMovesToCount[tenantId] = 0;
                    numTenantMovesToCount[tenantId]++;

                    var moves = UpgradeScheduler.stdUpgradeMoveCount[replicaId];
                    if (!numMovesToCount.ContainsKey(moves))
                        numMovesToCount[moves] = 0;
                    numMovesToCount[moves]++;
                    sw.WriteLine(replicaId + "," + moves);
                }
            }
            using (var sw = new StreamWriter(dir + $"StdUpgradeMovesAgg_{runIdx}.txt", true))
            {
                foreach (var numMoves in numMovesToCount.Keys)
                    sw.WriteLine(numMoves + "," + numMovesToCount[numMoves]);
            }

            numMovesToCount = new Dictionary<int, int>();
            using (var sw = new StreamWriter(dir + $"PremUpgradeMoves_{runIdx}.txt", true))
            {
                foreach (var replicaId in UpgradeScheduler.premUpgradeMoveCount.Keys)
                {
                    var tenantId = ReplicaInfo.ExtractTenantIdWithTrace(replicaId);
                    if (!numTenantMovesToCount.ContainsKey(tenantId))
                        numTenantMovesToCount[tenantId] = 0;
                    numTenantMovesToCount[tenantId]++;

                    var moves = UpgradeScheduler.premUpgradeMoveCount[replicaId];
                    if (!numMovesToCount.ContainsKey(moves))
                        numMovesToCount[moves] = 0;
                    numMovesToCount[moves]++;
                    sw.WriteLine(replicaId + "," + moves);
                }
            }
            using (var sw = new StreamWriter(dir + $"PremUpgradeMovesAgg_{runIdx}.txt", true))
            {
                foreach (var numMoves in numMovesToCount.Keys)
                    sw.WriteLine(numMoves + "," + numMovesToCount[numMoves]);
            }

            numMovesToCount = new Dictionary<int, int>();
            using (var sw = new StreamWriter(dir + $"SecUpgradeMoves_{runIdx}.txt", true))
            {
                foreach (var replicaId in UpgradeScheduler.secUpgradeMoveCount.Keys)
                {
                    var tenantId = ReplicaInfo.ExtractTenantIdWithTrace(replicaId);
                    if (!numTenantMovesToCount.ContainsKey(tenantId))
                        numTenantMovesToCount[tenantId] = 0;
                    numTenantMovesToCount[tenantId]++;

                    var moves = UpgradeScheduler.secUpgradeMoveCount[replicaId];
                    if (!numMovesToCount.ContainsKey(moves))
                        numMovesToCount[moves] = 0;
                    numMovesToCount[moves]++;
                    sw.WriteLine(replicaId + "," + moves);
                }
            }
            using (var sw = new StreamWriter(dir + $"SecUpgradeMovesAgg_{runIdx}.txt", true))
            {
                foreach (var numMoves in numMovesToCount.Keys)
                    sw.WriteLine(numMoves + "," + numMovesToCount[numMoves]);
            }

            numMovesToCount = new Dictionary<int, int>();
            using (var sw = new StreamWriter(dir + $"TenantUpgradeMoves_{runIdx}.txt", true))
            {
                foreach (var tenant in numTenantMovesToCount.Keys)
                {
                    var moves = numTenantMovesToCount[tenant];

                    if (!numMovesToCount.ContainsKey(moves))
                        numMovesToCount[moves] = 0;
                    numMovesToCount[moves]++;

                    sw.WriteLine(tenant + "," + moves);
                }
            }
            using (var sw = new StreamWriter(dir + $"TenantUpgradeMovesAgg_{runIdx}.txt", true))
            {
                foreach (var numMoves in numMovesToCount.Keys)
                    sw.WriteLine(numMoves + "," + numMovesToCount[numMoves]);
            }

            return numTenantMovesToCount;
        }
    }
}
