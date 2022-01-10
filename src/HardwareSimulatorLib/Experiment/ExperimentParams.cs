using HardwareSimulatorLib.Config;

namespace HardwareSimulatorLib.Experiment
{
    public class ExperimentParams
    {
        public int ID;

        public double CpuCap;
        public double DiskCap;
        public double MemoryCap;
        public double NodeNumCores;
        public double NodeDiskSizeInMB;
        public double NodeMemorySizeInMB;
        public double OverbookingRatio;
        public double NodeMemUsageLimitForPlacement;
        public double NodeDiskUsageLimitForPlacement;

        public int NumNodes;
        public string HardwareGeneration;
        public int NodesToReserve;
        public double PenaltiesParameterThreshold;
        public double ProbabilityOfViolationThreshold;
        public int ProbabilityOfViolationMCRepetitions;
        public bool ExtrapolateGrowingTenants;
        public bool AllowTenantPlacementFailures;
        public bool OnlyPremiumTenants;
        public int LookAheadOffset;
        public int MinPlacementTimeForLookAhead;
        public bool ApplyFaultDomainConstraints;

        public PlacementHeuristicEnum PlacementHeuristic;
        public MetricToUseForPlacementEnum MetricToUseForPlacement;
        public MetricToUseForNodeLoadEnum MetricToUseForNodeLoad;
        public MetricWeightingSchemeEnum MetricWeightingScheme;
        public ConflictResolutionHeuristicEnum ConflictResolutionHeuristic;

        public UpgradeHeuristicEnum UpgradeHeuristic;
        public int UpgradeIntervalInHours;
        public bool ApplyPlacementPreference;

        public string outputDirectory;
    }
}
