using HardwareSimulatorLib.Config;
using System;
using System.IO;

namespace HardwareSimulatorLib.Experiment
{
    public class SimulationStatistics
    {
        private readonly double[] AvgMovesPerConfiguration;
        private readonly double[] AvgDiskViolationsPerConfiguration;
        private readonly double[] AvgMemViolationsPerConfiguration;
        private readonly double[] AvgIDSUMovedPerConfiguration;
        private readonly double[] AvgAMUMovedPerConfiguration;
        private readonly int[] ConfigurationIDs;
        private readonly double[] OverbookingRatios;
        private readonly string[] PlacementAlgorithm;
        private readonly string[] MetricToUseForPlacement;
        private readonly int[] NodesToReserve;
        private readonly double[] PenaltiesParameterThreshold;
        private readonly double[] ProbabilityOfViolationThreshold;
        private readonly int[] ProbabilityOfViolationMCRepetitions;

        public SimulationStatistics(long numExperiments)
        {
            AvgMovesPerConfiguration = new double[numExperiments];
            AvgDiskViolationsPerConfiguration = new double[numExperiments];
            AvgMemViolationsPerConfiguration = new double[numExperiments];
            AvgIDSUMovedPerConfiguration = new double[numExperiments];
            AvgAMUMovedPerConfiguration = new double[numExperiments];
            ConfigurationIDs = new int[numExperiments];
            OverbookingRatios = new double[numExperiments];
            PlacementAlgorithm = new string[numExperiments];
            MetricToUseForPlacement = new string[numExperiments];
            NodesToReserve = new int[numExperiments];
            PenaltiesParameterThreshold = new double[numExperiments];
            ProbabilityOfViolationThreshold = new double[numExperiments];
            ProbabilityOfViolationMCRepetitions = new int[numExperiments];
        }

        public void Append(ExperimentParams experimentParams, SimulationConfiguration simulationConfig,
            ExperimentStatistics statisticsAvgsOverAllRuns)
        {
            var idx = experimentParams.ID;
            var numIDsSkipped = simulationConfig.StartConfigurationId;
            var placementAlgo = simulationConfig.PlacementAlgorithm;
            PlacementAlgorithm[idx] = placementAlgo.PlacementHeuristic;
            NodesToReserve[idx] = placementAlgo.NodesToReserve;
            PenaltiesParameterThreshold[idx] = placementAlgo.PenaltiesParameterThreshold;
            ProbabilityOfViolationThreshold[idx] = placementAlgo.ProbabilityOfViolationThreshold;
            OverbookingRatios[idx] = experimentParams.OverbookingRatio;
            AvgMovesPerConfiguration[idx - numIDsSkipped] = statisticsAvgsOverAllRuns.Moves;
            AvgDiskViolationsPerConfiguration[idx - numIDsSkipped] = statisticsAvgsOverAllRuns.NumDiskViolations;
            AvgMemViolationsPerConfiguration[idx - numIDsSkipped] = statisticsAvgsOverAllRuns.NumMemViolations;
            AvgIDSUMovedPerConfiguration[idx - numIDsSkipped] = statisticsAvgsOverAllRuns.DiskMoved;
            AvgAMUMovedPerConfiguration[idx - numIDsSkipped] = statisticsAvgsOverAllRuns.MemoryMoved;
            MetricToUseForPlacement[idx] = placementAlgo.MetricToUseForPlacement;
            ProbabilityOfViolationMCRepetitions[idx] = placementAlgo.ProbabilityOfViolationMCRepetitions;
            ConfigurationIDs[idx - numIDsSkipped] = idx;
        }

        public void Log(string outputDirectory)
        {
            // Output in an Excel - friendly format
            using (var file = new StreamWriter(outputDirectory + "ConfigurationSummary.tsv"))
            {
                for (var i = 0; i < AvgMovesPerConfiguration.Length; i++)
                {
                    file.WriteLine(
                        PlacementAlgorithm[i] +
                        "(" +
                        NodesToReserve[i] + " " +
                        (PlacementAlgorithm[i] == "Penalties" ? PenaltiesParameterThreshold[i].ToString() : "") +
                        (PlacementAlgorithm[i] == "WorstFitProbabilityViolation" ? ProbabilityOfViolationThreshold[i].ToString() : "") +
                        (PlacementAlgorithm[i] == "BestFitProbabilityViolation" ? ProbabilityOfViolationThreshold[i].ToString() : "") +
                        " ), " +
                         OverbookingRatios[i] + ", " +
                         MetricToUseForPlacement[i] + ", " +
                         AvgMovesPerConfiguration[i] + ", " +
                         AvgDiskViolationsPerConfiguration[i] + ", " +
                         AvgMemViolationsPerConfiguration[i] + ", " +
                         AvgIDSUMovedPerConfiguration[i] + ", " +
                         AvgAMUMovedPerConfiguration[i] + ", " +
                         MetricToUseForPlacement[i] + ", " +
                         ProbabilityOfViolationMCRepetitions[i]
                    );
                }
            }

            // Output sorted by # moves
            Array.Sort(AvgMovesPerConfiguration, ConfigurationIDs);
            using (var rank_File = new StreamWriter(outputDirectory + "ConfigurationRanks.tsv"))
            {
                for (int k = 0; k < AvgMovesPerConfiguration.Length; k++)
                {
                    rank_File.WriteLine("Configuration: " + ConfigurationIDs[k] +
                        " [ OR: " + OverbookingRatios[ConfigurationIDs[k]] +
                        " Alg: " + PlacementAlgorithm[ConfigurationIDs[k]] +
                        ((PlacementAlgorithm[ConfigurationIDs[k]] == "Penalties" ||
                          PlacementAlgorithm[ConfigurationIDs[k]] == "WorstFitProbabilityViolation") ?
                            ("(" + NodesToReserve[ConfigurationIDs[k]] + ";") : "") +
                        (PlacementAlgorithm[ConfigurationIDs[k]] == "Penalties" ?
                            (PenaltiesParameterThreshold[ConfigurationIDs[k]] + ")") : "") +
                        ((PlacementAlgorithm[ConfigurationIDs[k]] == "WorstFitProbabilityViolation") ?
                            (ProbabilityOfViolationThreshold[ConfigurationIDs[k]] + ")") : "") +
                        ((PlacementAlgorithm[ConfigurationIDs[k]] == "BestFitProbabilityViolation") ?
                            ("(" + ProbabilityOfViolationThreshold[ConfigurationIDs[k]] + ")") : "") +
                        " PlacementMetric: " + MetricToUseForPlacement[ConfigurationIDs[k]] + " ] " +
                        " AvgMoves: " + AvgMovesPerConfiguration[k] +
                        " DiskViolations:  " + AvgDiskViolationsPerConfiguration[ConfigurationIDs[k]] +
                        " Mem Violations: " + AvgMemViolationsPerConfiguration[ConfigurationIDs[k]] +
                        " AvgIDSUMoved:  " + AvgIDSUMovedPerConfiguration[ConfigurationIDs[k]] +
                        " AvgAMUMoved: " + AvgAMUMovedPerConfiguration[ConfigurationIDs[k]] +
                        " MetricToUseForPlacement: " + MetricToUseForPlacement[ConfigurationIDs[k]] +
                        " MCRepetitions " + ProbabilityOfViolationMCRepetitions[ConfigurationIDs[k]]
                    );
                }
            }
        }
    }
}
