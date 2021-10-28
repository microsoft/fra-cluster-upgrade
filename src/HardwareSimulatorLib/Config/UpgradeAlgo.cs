namespace HardwareSimulatorLib.Config
{
    public enum UpgradeHeuristicEnum
    {
        GreedilyFailover,
        GreedilyFailoverWithSort,
        GreedilyFailoverPrimThenStd,
        GreedilyFailoverPrimThenStdWithSort,
    };

    public class UpgradeAlgo
    {
        public string Heuristic;
        public int IntervalInHours;
        public bool ApplyPlacementPreference;

        public static UpgradeHeuristicEnum GetUpgradeHeuristicAsEnum(
            string PlacementHeuristic)
        {
            switch (PlacementHeuristic)
            {
                case "GreedilyFailover":
                    return UpgradeHeuristicEnum.GreedilyFailover;
                case "GreedilyFailoverWithSort":
                    return UpgradeHeuristicEnum.GreedilyFailoverWithSort;
                case "GreedilyFailoverPrimThenStd":
                    return UpgradeHeuristicEnum.GreedilyFailoverPrimThenStd;
                case "GreedilyFailoverPrimThenStdWithSort":
                default:
                    return UpgradeHeuristicEnum.GreedilyFailoverPrimThenStdWithSort;
            }
        }
    }
}
