using System;
using System.IO;
using System.Web.Script.Serialization;
using HardwareSimulatorLib.Slo;
using HardwareSimulatorLib.Config;
using HardwareSimulatorLib.Experiment;
using System.Diagnostics;
using HardwareSimulatorLib.Trace;
using HardwareSimulatorLib.Predictor;

namespace HardwareSimulator
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args == null || args.Length != 4 ||
                args[0] != "-SimulationConfiguration" ||
                !File.Exists(args[1]) || args[2] != "-Output")
            {
                PrintExpectedArgsHelp();
                return;
            }

            var jsonSerializer = new JavaScriptSerializer();
            var simConfig = jsonSerializer.Deserialize<
                SimulationConfiguration>(File.ReadAllText(
                    args[1]/*-SimulationConfiguration*/));
            var outputDirectory = GetOutputDirectoryAndCreateIfNecessary(
                args[3]/*-Output*/, ref simConfig);

            var stopWatch = Stopwatch.StartNew();

            SloSpecification.InitializeSloNameToSpecifications();
            var traceMan = new TraceManager(simConfig.DataFile.Trim(),
                simConfig.UseOnlyNewTenants);
            var violationPredictor = new ViolationPredictor(traceMan);
            traceMan.SplitReplicasPerSLO();

            stopWatch.Stop();

            Console.WriteLine($"Parsing of simulation config, trace file, " +
                $"and creation of violation predictor " +
                $"completed in {stopWatch.Elapsed.TotalSeconds:0.0} seconds");

            var stopWatchAll = Stopwatch.StartNew();

            if (simConfig.EnableSloOutput)
                LogReplicaIdToSloIdMap(outputDirectory, traceMan);

            var numExperiments = simConfig.GetNumExperiments();
            var totalNumRuns = numExperiments *
                simConfig.RunsPerConfiguration;
            var actualNumExperiments = numExperiments -
                simConfig.StartConfigurationId /*NumExperiementsToSkip*/;

            var searchSpaceSize = simConfig.GetSearchSpaceSize();
            var multiRunStats = new SimulationStatistics(actualNumExperiments);
            var simDuration = TimeSpan.FromHours(
                simConfig.SimulationDurationInHours);
            foreach (var experimentParams in
                simConfig.GetAllExperimentsParams())
            {
                experimentParams.outputDirectory =
                    $"{outputDirectory}Config_{experimentParams.ID}\\";
                if (!Directory.Exists(experimentParams.outputDirectory))
                    Directory.CreateDirectory(experimentParams.outputDirectory);
                using (var file = new StreamWriter(
                        experimentParams.outputDirectory + "Configuration.tsv"))
                    file.WriteLine(jsonSerializer.Serialize(experimentParams));

                var statisticsAvgsOverAllRuns = new ExperimentStatistics();
                var totalSimIntervalsOverAllRuns = 0L;
                for (var runIdx = 0;
                         runIdx < simConfig.RunsPerConfiguration;
                         runIdx++)
                {
                    stopWatch = Stopwatch.StartNew();

                    using (var sw = new StreamWriter(
                        experimentParams.outputDirectory + @"Violations.txt",
                            true /* append */))
                    {
                        sw.WriteLine("Run number: " + (runIdx + 1));
                    }
                    Console.WriteLine("Run number: " + (runIdx + 1) + "/ {0}", totalNumRuns);

                    try {
                        var runner = new ExperimentRunner(simDuration,
                            experimentParams, traceMan, violationPredictor, runIdx);
                    traceMan.RandomChooseInstance = new Random(runIdx);

                    using (var ringStatsFile =
                        new StreamWriter(experimentParams.outputDirectory +
                            @"ring_stats_" + runIdx + ".tsv"))
                    {
                        var numIntervals = 0;
                        foreach (var stats in runner.RunExperiment())
                        {
                            if (numIntervals >= simConfig.WarmupInHours)
                            {
                                ringStatsFile.WriteLine(stats.ToTSV());
                                statisticsAvgsOverAllRuns.Add(stats);
                                totalSimIntervalsOverAllRuns++;
                            }
                            numIntervals++;
                        }
                    }

                    using (var ExperimentsLogFile =
                        new StreamWriter(experimentParams.outputDirectory +
                            @"experiment_" + runIdx + ".log", true))
                        ExperimentsLogFile.WriteLine(runner.LogFailovers);

                    stopWatch.Stop();
                    Console.WriteLine("Run finished in " +
                        $"{stopWatch.Elapsed.TotalSeconds:0.0} secs");

                    } catch (Exception e)
                    {
                        using (var ExperimentsLogFile =
                        new StreamWriter(experimentParams.outputDirectory +
                            @"experiment_" + runIdx + ".log", true))
                            ExperimentsLogFile.WriteLine(e.Message);
                    }
                }

                statisticsAvgsOverAllRuns.Divide(simConfig.
                    RunsPerConfiguration, totalSimIntervalsOverAllRuns);
                statisticsAvgsOverAllRuns.Log(
                    experimentParams.outputDirectory + "Summary.tsv");
                var placementOffset = experimentParams.ID % searchSpaceSize;
                multiRunStats.Append(placementOffset, experimentParams,
                    simConfig, statisticsAvgsOverAllRuns);
            }

        multiRunStats.Log(outputDirectory);
            stopWatchAll.Stop();
            Console.WriteLine($"Completed {totalNumRuns} experiments in " +
                $"{stopWatchAll.Elapsed.TotalMinutes:0.0} minutes " +
                $"(average {stopWatchAll.Elapsed.TotalSeconds / totalNumRuns:0.0} " +
                $"sec/experiment)");
        }

    public static string GetTraceFilename(
        ref SimulationConfiguration simulationConfig)
    {
        var tracefile = "";
        var dataPull = new DataPull(simulationConfig.ScopeSDK,
            simulationConfig.TraceSourceTable);
        if (!simulationConfig.SkipDataGeneration)
        {
            var success = dataPull.TrySubmitDataPull(simulationConfig.
                DataRange.Region,
                DateTime.Parse(simulationConfig.DataRange.StartDate),
                DateTime.Parse(simulationConfig.DataRange.EndDate),
                simulationConfig.DataRange.HardwareGeneration,
                out string resultPath)
                && dataPull.TryDownloadFile(resultPath, out tracefile);
            if (!success)
            {
                Console.WriteLine("Failed to get data from Cosmos");
                Environment.Exit(-1);
            }
        }
        else
        {
            if (string.IsNullOrWhiteSpace(simulationConfig.DataFile))
                tracefile = dataPull.GetLocalDataFile(
                    dataPull.GetCosmosDataFile(simulationConfig.DataRange
                        .Region.ToLower(),
                    DateTime.Parse(simulationConfig.DataRange.StartDate),
                    DateTime.Parse(simulationConfig.DataRange.EndDate)));
            else
                tracefile = simulationConfig.DataFile.Trim();

            if (!File.Exists(tracefile))
                throw new Exception($"{tracefile} does not exist." +
                    $" Set 'SkipDataGeneration' to false" +
                    $" to generate simulation data.");
        }
        return tracefile;
    }

    private static string GetOutputDirectoryAndCreateIfNecessary(
        string output, ref SimulationConfiguration simulationConfig)
    {
        var outputDirectory = $"{output.TrimEnd('\\')}" +
            $"\\{simulationConfig.DataRange.Region}\\";
        if (!Directory.Exists(outputDirectory))
            Directory.CreateDirectory(outputDirectory);
        return outputDirectory;
    }

    private static void LogReplicaIdToSloIdMap(string outputDirectory,
        TraceManager traceManager)
    {
        using (var file = new StreamWriter(outputDirectory +
                @"TenantSlo.tsv"))
            foreach (var replicaId in traceManager.TenantIdToSloMap)
                file.WriteLine(string.Join(",", replicaId.Key,
                    replicaId.Value));
    }

    private static void PrintExpectedArgsHelp()
    {
        var processName = Process.GetCurrentProcess().ProcessName;
        Console.WriteLine("Expecting Simulation configuration json file," +
            " please specify a path for the configuration file");
        Console.WriteLine($"\r\nUsage: {processName} " +
            $"-SimulationConfiguration <configuration.json> " +
            $"-Output <output path>");
        Console.WriteLine("\r\nExample:");
        Console.WriteLine($"\t{processName} " +
            $"-SimulationConfiguration .\\SimConfig.json " +
            $"-Output .\\output\\");
    }
}
}
