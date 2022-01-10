using System.Collections.Generic;
using System.Linq;
using System.Xml.Linq;

namespace HardwareSimulatorLib.Slo.Reader
{
    public static class XDBSqlInstancePlacementMappingsXmlReader
    {
        #region Fields declarations
        public const string DefaultName = "Default";
        public const string NameAttributeName = "name";
        public const string TypeAttributeName = "type";
        public const string ValueAttributeName = "value";
        public const string VmOverrideElementName = "VmOverride";
        public const string SLOAttributeName = "serviceLevelObjective";
        public const string PlacementMappingElement = "PlacementMapping";
        public const string PlacementMappingsElement = "PlacementMappings";
        public const string AvailableVmSizeElementName = "AvailableVmSize";
        public const string AvailableVmSizesElementName = "AvailableVmSizes";
        public const string FabricLoadMetricElementName = "FabricLoadMetric";
        public const string FabricLoadMetricsElementName = "FabricLoadMetrics";
        public const string EditionElementName = "Edition";
        #endregion

        public static List<ApplicationConfiguration>
            ReadXDBSqlInstancePlacementMappingsFile()
        {
            var assembly = System.Reflection.Assembly.GetExecutingAssembly();
            var mappings = XElement.Load(assembly.GetManifestResourceStream(
                "HardwareSimulatorLib.Slo.Reader." +
                    "XDBSqlInstancePlacementMappings.xml"));

            var appConfigs = (
                from item in mappings.Descendants(PlacementMappingElement)
                select ProcessMappingElement(item)
            ).ToList();
            return appConfigs;
        }

        private static ApplicationConfiguration ProcessMappingElement(
            XElement el)
        {
            var type = (string) el.Attribute(TypeAttributeName);
            var size = (string) el.Attribute(SLOAttributeName);
            var vmSizes = new HashSet<string>();
            foreach (var elem in el.Element(
                AvailableVmSizesElementName).
                Descendants(AvailableVmSizeElementName))
            {
                vmSizes.Add((string)elem.Attribute(NameAttributeName));
            }

            string edition = (string)el.Element(EditionElementName);
            bool isPremium = false;
            if(edition != null && edition.CompareTo("Premium") == 0)
            {
                isPremium = true;
            }

            var config = ProcessFabricLoadMetricsElement(
                el.Element(FabricLoadMetricsElementName));
            return new ApplicationConfiguration(type, size, config, vmSizes, isPremium);
        }

        private static PlacementConfiguration ProcessFabricLoadMetricsElement(
            XElement element)
        {
            var placementConfig = new PlacementConfiguration();
            var defaultConfig = ProcessFabricLoadMetricCollection(element);
            placementConfig.AddHardwareGenValue(DefaultName, defaultConfig);
            foreach (var vm in element.Descendants(VmOverrideElementName))
            {
                var hwName = (string)vm.Attribute(NameAttributeName);
                var hwConfig = ProcessFabricLoadMetricCollection(vm);
                // use default config values if hwConfig does not have it.
                foreach (var key in defaultConfig.Keys)
                    if (!hwConfig.ContainsKey(key))
                        hwConfig.Add(key, defaultConfig[key]);

                if (!placementConfig.ContainsHardware(hwName))
                    placementConfig.AddHardwareGenValue(hwName, hwConfig);
            }
            return placementConfig;
        }

        private static Dictionary<string, double>
            ProcessFabricLoadMetricCollection(XElement el)
        {
            var ret = new Dictionary<string, double>();
            foreach (var child in el.Descendants(FabricLoadMetricElementName))
            {
                var metricName = (string) child.Attribute(NameAttributeName);
                var value = double.Parse((string)
                    child.Attribute(ValueAttributeName));
                if (!ret.ContainsKey(metricName))
                    ret.Add(metricName, value);
            }
            return ret;
        }
    }
}
