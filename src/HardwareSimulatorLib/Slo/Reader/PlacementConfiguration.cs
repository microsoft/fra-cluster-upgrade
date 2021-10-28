using System.Collections.Generic;

namespace HardwareSimulatorLib.Slo.Reader
{
    // Placement configuration:
    // It contains one default configuration and different hardware generation configuration if exists.
    public class PlacementConfiguration
    {
        // Mappings of hardware generation and its resource setting.
        Dictionary<string, Dictionary<string, double>> hardwareGenMappings = new Dictionary<string, Dictionary<string, double>>();

        // Add a hardware and its accociated values.
        public void AddHardwareGenValue(string hardwareGeneration, Dictionary<string, double> val)
        {
            if (!hardwareGenMappings.ContainsKey(hardwareGeneration))
                hardwareGenMappings.Add(hardwareGeneration, val);
        }

        // Check if a hardware exists.
        public bool ContainsHardware(string hardware)
        {
            return hardwareGenMappings.ContainsKey(hardware);
        }

        // Get the resource value of a hardware generation or default if not existed.
        public double GetValueOrDefault(string name,
            string hardwareGeneration = null)
        {
            var useDefaultName = string.IsNullOrWhiteSpace(hardwareGeneration) ||
                !hardwareGenMappings.ContainsKey(hardwareGeneration);
            var config = hardwareGenMappings[useDefaultName ?
                 XDBSqlInstancePlacementMappingsXmlReader.DefaultName : hardwareGeneration];
            return config.ContainsKey(name) ? config[name] : 0.0;
        }
    }
}
