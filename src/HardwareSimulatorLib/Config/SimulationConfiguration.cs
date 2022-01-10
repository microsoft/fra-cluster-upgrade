using HardwareSimulatorLib.Experiment;
using System;
using System.Collections.Generic;

namespace HardwareSimulatorLib.Config
{
    // A simulation contains multiple experiments each using a different
    // 'PlacementAlgorithm'. An experiment is ran 'RunsPerConfiguration'
    //  times over a set of possible 'ClusterConfiguration(s)'.
    public class SimulationConfiguration
    {
        public string ScopeSDK;
        public SearchSpace SearchSpace;
        public ClusterConfiguration ClusterConfiguration;
        public DataRange DataRange;
        public PlacementAlgo PlacementAlgorithm;
        public UpgradeAlgo UpgradeAlgorithm;
        public int RunsPerConfiguration;
        public int SimulationDurationInHours;
        public int WarmupInHours;
        public bool SkipDataGeneration;
        public bool EnableSloOutput;
        public string DataFile;
        public string SloRatioFile;
        public long StartConfigurationId; /*numExperimentsSkipped*/
        public bool UseOnlyNewTenants;
        public string TraceSourceTable;
        public bool ExtrapolateGrowingTenants;
        public bool AllowTenantPlacementFailures;
        public bool OnlyPremiumTenants;
        public bool ApplyFaultDomainConstraints;
        public int LookAheadOffset;
        public int MinPlacementTimeForLookAhead;

        public int GetNumExperiments()
        {
            return SearchSpace.DiskCaps.Length *
                   SearchSpace.MemoryCaps.Length *
                   SearchSpace.VCoresPerNode.Length *
                   SearchSpace.DiskSizesInGB.Length *
                   SearchSpace.MemorySizesInGB.Length *
                   SearchSpace.OverbookingRatios.Length *
                   SearchSpace.NodeMemMaxUsageRatiosForPlacement.Length * 
                   SearchSpace.NodeDiskMaxUsageRatiosForPlacement.Length * 
                   PlacementAlgorithm.PlacementHeuristic.Length;
        }

        public int GetSearchSpaceSize()
        {
            return SearchSpace.DiskCaps.Length *
                   SearchSpace.MemoryCaps.Length *
                   SearchSpace.VCoresPerNode.Length *
                   SearchSpace.DiskSizesInGB.Length *
                   SearchSpace.MemorySizesInGB.Length *
                   SearchSpace.OverbookingRatios.Length *
                   SearchSpace.NodeMemMaxUsageRatiosForPlacement.Length * 
                   SearchSpace.NodeDiskMaxUsageRatiosForPlacement.Length;
        }

        public IEnumerable<ExperimentParams> GetAllExperimentsParams()
        {
            var experimentParamsID = 0;
            foreach (var cpuCap in SearchSpace.CpuCaps)
            foreach (var diskCap in SearchSpace.DiskCaps)
            foreach (var memoryCap in SearchSpace.MemoryCaps)
            foreach (var vCoresPerNode in SearchSpace.VCoresPerNode)
            foreach (var memorySizeInGB in SearchSpace.MemorySizesInGB)
            foreach (var diskSizeInGB in SearchSpace.DiskSizesInGB)
            foreach (var overbookingRatio in SearchSpace.OverbookingRatios)
            foreach (var nodeMemMaxUsageRatioForPlacement in SearchSpace.NodeMemMaxUsageRatiosForPlacement)
            foreach (var nodeDiskMaxUsageRatioForPlacement in SearchSpace.NodeDiskMaxUsageRatiosForPlacement)
            /* We fix values over the search space then loop over the set of
             * possible experiments based on the 'PlacementAlgorithm' */
            {
                for (var i = 0;
                         i < PlacementAlgorithm.PlacementHeuristic.Length; i++)
                {
                    /* We skip StartConfigurationId experiments if a crash
                     * happens in a large num of experiments in a given sim */
                    if (experimentParamsID < StartConfigurationId)
                    {
                        experimentParamsID++;
                        continue;
                    }
                    yield return new ExperimentParams
                    {
                        // First, set search space params.
                        ID = experimentParamsID,
                        CpuCap = cpuCap,
                        DiskCap = diskCap,
                        MemoryCap = memoryCap,
                        OverbookingRatio = overbookingRatio,
                        NodeNumCores = Math.Floor(vCoresPerNode * (1.0 - 0.07)),
                        NodeMemorySizeInMB = 1024 * (memorySizeInGB - 
                            ClusterConfiguration.MemoryOverheadPerNodeInGB),
                        NodeDiskSizeInMB   = 1024 * (diskSizeInGB -
                            ClusterConfiguration.DiskOverheadPerNodeInGB),
                        NodeMemUsageLimitForPlacement = 1024 * (memorySizeInGB -
                            ClusterConfiguration.MemoryOverheadPerNodeInGB) * 
                            nodeMemMaxUsageRatioForPlacement,
                        NodeDiskUsageLimitForPlacement = 1024 * (diskSizeInGB -
                            ClusterConfiguration.DiskOverheadPerNodeInGB) *
                            nodeDiskMaxUsageRatioForPlacement,

                        // Second, set base experiment params.
                        NumNodes = ClusterConfiguration.Nodes,
                        HardwareGeneration = DataRange.HardwareGeneration,
                        NodesToReserve = PlacementAlgorithm.NodesToReserve[i],
                        PenaltiesParameterThreshold = PlacementAlgorithm.
                            PenaltiesParameterThreshold[i],
                        ProbabilityOfViolationThreshold = PlacementAlgorithm.
                            ProbabilityOfViolationThreshold[i],
                        ProbabilityOfViolationMCRepetitions =
                            PlacementAlgorithm.
                            ProbabilityOfViolationMCRepetitions[i],
                        ExtrapolateGrowingTenants = ExtrapolateGrowingTenants,
                        AllowTenantPlacementFailures =
                            AllowTenantPlacementFailures,
                        OnlyPremiumTenants = OnlyPremiumTenants,
                        LookAheadOffset = LookAheadOffset,
                        MinPlacementTimeForLookAhead =
                            MinPlacementTimeForLookAhead,
                        ApplyFaultDomainConstraints = ApplyFaultDomainConstraints,

                        // Finally, set placement algo params
                        PlacementHeuristic = PlacementAlgo
                            .GetPlacementHeuristicAsEnum(
                                PlacementAlgorithm.PlacementHeuristic[i]),
                        MetricToUseForPlacement = PlacementAlgo
                            .GetMetricToUseForPlacementAsEnum(
                                PlacementAlgorithm.MetricToUseForPlacement[i]),
                        MetricToUseForNodeLoad = PlacementAlgo
                            .GetMetricToUseForNodeLoadAsEnum(
                                PlacementAlgorithm.MetricToUseForNodeLoad[i]),
                        ConflictResolutionHeuristic = PlacementAlgo
                            .GetConflictResolutionHeuristicAsEnum(
                                PlacementAlgorithm
                                .ConflictResolutionHeuristic[i]),
                        MetricWeightingScheme = PlacementAlgo.
                            GetMetricWeightingSchemeAsEnum(
                            PlacementAlgorithm.MetricWeightingScheme[i]),

                        UpgradeHeuristic = UpgradeAlgo
                            .GetUpgradeHeuristicAsEnum(UpgradeAlgorithm.Heuristic),
                        UpgradeIntervalInHours = UpgradeAlgorithm.IntervalInHours,
                        ApplyPlacementPreference = UpgradeAlgorithm.ApplyPlacementPreference
                    };
                    experimentParamsID++;
                }
            }
        }
    }
}
