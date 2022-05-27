namespace HardwareSimulatorLib.Config
{
    public enum PlacementHeuristicEnum
    {
        Bestfit,
        WorstFit,
        VMPlacement,
        SumOfSquares,
        MinStdDiv,
        Penalties,
        InnerProduct,
        WorstFitProbabilityViolation,
        BestFitProbabilityViolation,
        DotProduct,
        L2Norm
    };

    // This class contains the different metric aggregates to use for placement
    public enum MetricToUseForPlacementEnum
    {
        MaxValue,
        OnedayValue,
        TwodayValue,
        ThreedayValue,
        FourdayValue,
        InitialValue,
        CurrentValue,
        DefaultLoad,
        PredictedMaxValue
    };

    public enum MetricToUseForNodeLoadEnum
    {
        OneHourLookahead,
        SixHourLookahead,
        TwelveHourLookahead,
        OneDayLookahead,
        TwoDayLookahead,
        FourDayLookahead,
        SevenDayLookahead,
        LookaheadByLifetime,
        CumulativeMaxLoad,
        CurrentValue
    };

    public enum MetricWeightingSchemeEnum
    {
        MinWeight, 
        FFSumWeight,
        UnweightedAvg
    }

    public enum ConflictResolutionHeuristicEnum
    {
        Random,
        MinimumMoves,
        LeastCongested,
        LongestTimeToFailure
    };

    public enum MetricToUseEnum
    {
        All,
        IDSUonly,
        AMUonly
    };

    public enum UsageCurveEnum
    {
        Real,
        MaxValue
    };

    // Algo contains the default config params for the sim's search space.
    // The parameters will be passed-in as a JSON file to the program.
    public class PlacementAlgo
    {
        public string PlacementHeuristic;
        public string MetricToUseForPlacement;
        public string MetricToUseForNodeLoad;
        public string MetricWeightingScheme;
        public string ConflictResolutionHeuristic;
        public string MetricToUse;
        public string UsageCurve;
        public int NodesToReserve;
        public double PenaltiesParameterThreshold;
        public double ProbabilityOfViolationThreshold;
        public int ProbabilityOfViolationMCRepetitions;
        public bool ConsiderUpgrades;

        public static PlacementHeuristicEnum GetPlacementHeuristicAsEnum(
            string PlacementHeuristic)
        {
            switch (PlacementHeuristic)
            {
                case "BestFit":
                    return PlacementHeuristicEnum.Bestfit;
                case "BestFitProbabilityViolation":
                    return PlacementHeuristicEnum.BestFitProbabilityViolation;
                case "WorstFitProbabilityViolation":
                    return PlacementHeuristicEnum.WorstFitProbabilityViolation;
                case "InnerProduct":
                    return PlacementHeuristicEnum.InnerProduct;
                case "WorstFit":
                    return PlacementHeuristicEnum.WorstFit;
                case "Penalties":
                    return PlacementHeuristicEnum.Penalties;
                case "MinStdDiv":
                    return PlacementHeuristicEnum.MinStdDiv;
                case "VMPlacement":
                    return PlacementHeuristicEnum.VMPlacement;
                case "SumOfSquares":
                    return PlacementHeuristicEnum.SumOfSquares;
                case "DotProduct":
                    return PlacementHeuristicEnum.DotProduct;
                case "L2Norm":
                    return PlacementHeuristicEnum.L2Norm;
                default:
                    return PlacementHeuristicEnum.Bestfit;
            }
        }

        public static MetricToUseForPlacementEnum
            GetMetricToUseForPlacementAsEnum(
                string MetricToUseForPlacement)
        {
            switch (MetricToUseForPlacement)
            {
                case "DefaultLoad":
                    return MetricToUseForPlacementEnum.DefaultLoad;
                case "MaxValue":
                    return MetricToUseForPlacementEnum.MaxValue;
                case "PredictedMaxValue":
                    return MetricToUseForPlacementEnum.PredictedMaxValue;
                case "InitialValue":
                    return MetricToUseForPlacementEnum.InitialValue;
                case "CurrentValue":
                    return MetricToUseForPlacementEnum.InitialValue;
                case "OnedayValue":
                    return MetricToUseForPlacementEnum.OnedayValue;
                case "TwodayValue":
                    return MetricToUseForPlacementEnum.TwodayValue;
                case "ThreedayValue":
                    return MetricToUseForPlacementEnum.ThreedayValue;
                case "FourdayValue":
                    return MetricToUseForPlacementEnum.FourdayValue;
                default:
                    return MetricToUseForPlacementEnum.MaxValue;
            }
        }

        public static MetricWeightingSchemeEnum GetMetricWeightingSchemeAsEnum(
            string MetricWeightingScheme)
        {
            switch (MetricWeightingScheme)
            {
                case "MinWeight":
                    return MetricWeightingSchemeEnum.MinWeight;
                case "FFSumWeight":
                    return MetricWeightingSchemeEnum.FFSumWeight;
                case "UnweightedAvg":
                    return MetricWeightingSchemeEnum.UnweightedAvg;
                default:
                    return MetricWeightingSchemeEnum.MinWeight;
            }
        }

        public static MetricToUseForNodeLoadEnum
            GetMetricToUseForNodeLoadAsEnum(
                string MetricToUseForNodeLoad)
        {
            switch (MetricToUseForNodeLoad)
            {
                case "CurrentValue":
                    return MetricToUseForNodeLoadEnum.CurrentValue;
                case "OneHourLookahead":
                    return MetricToUseForNodeLoadEnum.OneHourLookahead;
                case "SixHourLookahead":
                    return MetricToUseForNodeLoadEnum.SixHourLookahead;
                case "TwelveHourLookahead":
                    return MetricToUseForNodeLoadEnum.TwelveHourLookahead;
                case "OneDayLookahead":
                    return MetricToUseForNodeLoadEnum.OneDayLookahead;
                case "TwoDayLookahead":
                    return MetricToUseForNodeLoadEnum.TwoDayLookahead;
                case "FourDayLookahead":
                    return MetricToUseForNodeLoadEnum.FourDayLookahead;
                case "SevenDayLookahead":
                    return MetricToUseForNodeLoadEnum.SevenDayLookahead;
                case "LookaheadByLifetime":
                    return MetricToUseForNodeLoadEnum.LookaheadByLifetime;
                case "CumulativeMaxLoad":
                    return MetricToUseForNodeLoadEnum.CumulativeMaxLoad;
                default:
                    return MetricToUseForNodeLoadEnum.CurrentValue;
            }
        }

        public static ConflictResolutionHeuristicEnum
            GetConflictResolutionHeuristicAsEnum(
                string ConflictResolutionHeuristic)
        {
            switch (ConflictResolutionHeuristic)
            {
                case "MinimumMoves":
                    return ConflictResolutionHeuristicEnum.MinimumMoves;
                case "Random":
                    return ConflictResolutionHeuristicEnum.Random;
                case "LeastCongested":
                    return ConflictResolutionHeuristicEnum.LeastCongested;
                case "LongestTimeToFailure":
                    return ConflictResolutionHeuristicEnum.LongestTimeToFailure;
                default:
                    return ConflictResolutionHeuristicEnum.LeastCongested;
            }
        }

        public static MetricToUseEnum GetMetricToUseAsEnum(string MetricToUse)
        {
            switch (MetricToUse)
            {
                case "All":
                    return MetricToUseEnum.All;
                case "IDSUonly":
                    return MetricToUseEnum.IDSUonly;
                case "AMUonly":
                    return MetricToUseEnum.AMUonly;
                default:
                    return MetricToUseEnum.All;
            }
        }

        public static UsageCurveEnum GetUsageCurveAsEnum(string UsageCurve)
        {
            switch (UsageCurve)
            {
                case "Real":
                    return UsageCurveEnum.Real;
                case "MaxValue":
                    return UsageCurveEnum.MaxValue;
                default:
                    return UsageCurveEnum.Real;
            }
        }
    }
}
