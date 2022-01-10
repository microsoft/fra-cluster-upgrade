using System.Collections.Generic;

namespace HardwareSimulatorLib.Slo.Reader
{
    // Application configuration
    public class ApplicationConfiguration
    {
        #region Fields declrations
        // Service type, such as Worker.ISO.
        public string Type { get; private set; }

        // The size (or SLO) of a service type.
        public string Size { get; private set; }

        // The hardware generations of an application.
        public HashSet<string> VMSizes { get; private set; }

        public PlacementConfiguration PlacementConfiguration { get; private set; }
        #endregion

        public bool IsPremium { get; set; }

        public ApplicationConfiguration(string type, string size,
            PlacementConfiguration configuration, HashSet<string> vmSizes, bool isPremium)
        {
            Type = type;
            Size = size;
            PlacementConfiguration = configuration;
            VMSizes = vmSizes;
            IsPremium = isPremium;
        }

        public double GetMaxCpuUsage(string hardware)
        {
            return PlacementConfiguration.GetValueOrDefault("MaxCpuUsage",
                hardware);
        }

        public double GetInstanceDiskSpaceUsed(string hardware)
        {
            return PlacementConfiguration.GetValueOrDefault(
                "InstanceDiskSpaceUsed", hardware);
        }

        public double GetAppMemoryUsageMB(string hardware)
        {
            return PlacementConfiguration.GetValueOrDefault("AppMemoryUsageMB",
                hardware);
        }
    }
}
