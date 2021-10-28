using System;
using System.Collections.Generic;
using HardwareSimulatorLib.Slo.Reader;

namespace HardwareSimulatorLib.Slo
{
    public class SloSpecification
    {
        public static Dictionary<
            string, ApplicationConfiguration> SloIdToSpecificationMap { get; set; }

        private readonly string HardwareGen;

        public SloSpecification(string hardwareGen)
        {
            if (!string.IsNullOrWhiteSpace(hardwareGen))
                HardwareGen = hardwareGen.Trim();
        }

        public static void InitializeSloNameToSpecifications()
        {
            SloIdToSpecificationMap = new Dictionary<
                string, ApplicationConfiguration>();
            var sloSpecifications = XDBSqlInstancePlacementMappingsXmlReader.
                ReadXDBSqlInstancePlacementMappingsFile();
            foreach (var spec in sloSpecifications)
            {
                if (string.IsNullOrWhiteSpace(spec.Size) &&
                    spec.Type.Contains("LogicalServer"))
                    continue;

                if (!SloIdToSpecificationMap.ContainsKey(spec.Size))
                    SloIdToSpecificationMap.Add(spec.Size, spec);
                else
                    Console.WriteLine(string.Format("Duplicate Size {0}, " +
                        "with the type {1}, in the configuration file.",
                        spec.Size, spec.Type));
            }
        }

        public static bool IsSloAllowedInSim(string SloId)
        {
            return SloIdToSpecificationMap.ContainsKey(SloId);
        }

        public double GetMem(string SloId)
        {
            ValidateSloIdExists(SloId);
            return SloIdToSpecificationMap[SloId].GetAppMemoryUsageMB(
                HardwareGen);
        }

        public double GetDisk(string SloId)
        {
            ValidateSloIdExists(SloId);
            return SloIdToSpecificationMap[SloId].GetInstanceDiskSpaceUsed(
                HardwareGen);
        }

        public double GetMaxCPU(string SloId)
        {
            ValidateSloIdExists(SloId);
            return SloIdToSpecificationMap[SloId].GetMaxCpuUsage(HardwareGen)
                / 100000.0;
        }

        private void ValidateSloIdExists(string SloId)
        {
            if (!SloIdToSpecificationMap.ContainsKey(SloId))
            {
                var message = string.Format("{0} does not exist!", SloId);
                throw new Exception(message);
            }
        }
    }
}
