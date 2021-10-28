using System;
using System.Collections.Generic;
using System.Linq;
using HardwareSimulatorLib.Config;
using HardwareSimulatorLib.Predictor;

namespace HardwareSimulatorLib.Trace
{
    public struct UsageInfo
    {
        public TimeSpan TimeSinceInsertion;
        public double Memory;
        public double Disk;
        public double Cpu;
    };

    public class ReplicaTrace
    {
        public DateTime InsertionTime;
        private int offset = 0;
        private ResourceUsage MaxUsage;
        private readonly List<UsageInfo> UsageData;

        // Lookahead at the granularity of 10 minutes
        public double[] cpuUsageLookahead;
        public double[] memUsageLookahead;
        public double[] diskUsageLookahead;

        public ReplicaTrace()
        {
            UsageData = new List<UsageInfo>();
            MaxUsage.Disk = MaxUsage.Memory = MaxUsage.Cpu = -1;
        }

        public void AddDataPoint(MonRgManagerInput entry)
        {
            var data = new UsageInfo()
            {
                Disk = entry.InstanceDiskSpaceUsed,
                Memory = entry.AppMemoryUsageMB,
                Cpu = entry.AppCpuUsage
            };
            if (UsageData.Count == 0)
                InsertionTime = entry.timestamp;
            else
                data.TimeSinceInsertion = entry.timestamp - InsertionTime;
            UsageData.Add(data);
        }

        public TimeSpan GetLifetime() =>
            UsageData[UsageData.Count - 1].TimeSinceInsertion;

        public bool IsActive(TimeSpan timeDelta)
            => UsageData.Count > 0 &&
                UsageData[UsageData.Count - 1]
                    .TimeSinceInsertion >= timeDelta;

        public bool IsInBounds(double value, double low, double high)
            => !(value < low) && (value < high);

        public double GetMaxDiskUsage()
        {
            if (MaxUsage.Disk >= 0)
                return MaxUsage.Disk;
            MaxUsage.Disk = UsageData.Max(ui => ui.Disk);
            return MaxUsage.Disk;
        }

        public double GetMaxMemUsage()
        {
            if (MaxUsage.Memory >= 0)
                return MaxUsage.Memory;
            MaxUsage.Memory = UsageData.Max(ui => ui.Memory);
            return MaxUsage.Memory;
        }

        public double GetMaxCpuUsage()
        {
            if (MaxUsage.Cpu >= 0)
                return MaxUsage.Cpu;
            MaxUsage.Cpu = UsageData.Max(ui => ui.Cpu);
            return MaxUsage.Cpu;
        }

        public UsageInfo GetResourceUsage(TimeSpan timeDelta,
            int coreReservation)
        {
            if (UsageData.Count == 0)
                throw new Exception("Expected at least one record in UsageData");

            /* MetricToUseForPlacement !=
             *  MetricToUseForPlacementEnum.InitialValue */
            while (true) // Move to the right
            {
                if (offset == UsageData.Count - 1) break;
                if (UsageData[offset + 1].TimeSinceInsertion <= timeDelta)
                    offset++;
                else break; // We can't go further
            }
            while (true)
            {
                if (offset == 0) break;
                if (UsageData[offset].TimeSinceInsertion > timeDelta)
                    offset--;
                else break; // We can't go further
            }

            /* MetricToUseForPlacement ==
             *  MetricToUseForPlacementEnum.PredictedMaxValue */
            var usage = new UsageInfo
            {
                Cpu = 0,
                Disk = 0,
                Memory = 0,
                TimeSinceInsertion = TimeSpan.Zero
            };

            if (IsInBounds(coreReservation, 0, 20000))
            {
                usage.Disk = 2; usage.Cpu = 1; usage.Memory = 3072;
            }
            if (IsInBounds(coreReservation, 20000, 10 * 10000))
            {
                usage.Disk = 6; usage.Cpu = 2; usage.Memory = 5120;
            }
            if (IsInBounds(coreReservation, 10 * 10000, 18 * 10000))
            {
                usage.Disk = 8; usage.Cpu = 3; usage.Memory = 5120;
            }
            if (IsInBounds(coreReservation, 18 * 10000, double.MaxValue))
            {
                usage.Disk = 10; usage.Cpu = 5; usage.Memory = 5120;
            }
            usage.Disk *= 25000 /* DiskFactor 2.5 GB */;
            usage.Cpu *= 10000  /* CpuFactor         */;
            if (usage.Cpu >= 0)
                return usage;
            
            throw new Exception("Invalid MetricToUseForPlacement " +
                    "parameter in GetUsageForPlacement");
        }

        public UsageInfo CandidateUsageAccoutingForGrowth(TimeSpan timeElapsed)
        {
            if (timeElapsed.TotalHours <= 5)
            {
                var candidateUsage = GetResourceUsage(timeElapsed);
                candidateUsage.Disk *= 2.5;
                candidateUsage.Memory *= 2.5;
                return candidateUsage;
            }
            else
            {
                var priorCandidateUsage = new UsageInfo();
                var candidateUsage = GetAllUsageAndPriorUsage(
                    timeElapsed, TimeSpan.FromHours(5),
                    ref priorCandidateUsage);

                if (candidateUsage.Disk - priorCandidateUsage.Disk >
                        priorCandidateUsage.Disk * 0.01)
                    candidateUsage.Disk *= 2.5;
                if (candidateUsage.Memory - priorCandidateUsage.Memory >
                        priorCandidateUsage.Memory * 0.01)
                    candidateUsage.Memory *= 2.5;
                return candidateUsage;
            }
        }

        public UsageInfo GetResourceUsage(TimeSpan timeDelta)
        {
            if (UsageData.Count == 0)
                throw new Exception("Expected at least 1 record in UsageData");

            /* usageCurve != UsageCurveEnum.MaxValue &&
                  usageCurve == UsageCurveEnum.Real */
            while (true) // Move to the right
            {
                if (offset == UsageData.Count - 1) break;
                if (UsageData[offset + 1].TimeSinceInsertion <= timeDelta)
                    offset++;
                else break; // We can't go further 
            }
            while (true)
            {
                if (offset == 0) break;
                if (UsageData[offset].TimeSinceInsertion > timeDelta)
                    offset--;
                else break; // We can't go further
            }

            if (offset == UsageData.Count - 1)
                // We always terminate every trace with 0s
                return new UsageInfo
                {
                    Cpu = 0,
                    Disk = 0,
                    Memory = 0,
                    TimeSinceInsertion = timeDelta
                };
            else
                return new UsageInfo
                {
                    Cpu = UsageData[offset].Cpu,
                    Disk = UsageData[offset].Disk,
                    Memory = UsageData[offset].Memory,
                    TimeSinceInsertion = UsageData[offset].TimeSinceInsertion
                };
        }

        public UsageInfo GetAllUsageAndPriorUsage(TimeSpan timeDelta,
            TimeSpan timeDelta2, ref UsageInfo priorUsageInfo)
        {
            // advance to the right
            if (UsageData.Count == 0)
                throw new Exception("Expected at least 1 record in UsageData");

            // How many indexes do we have to go back?
            var indexes_to_skip = 0; // If there are not enought data points, we won't skip.
            if (UsageData.Count > 1)
                indexes_to_skip = (int)timeDelta2.TotalMinutes /
                    (int)UsageData[1].TimeSinceInsertion.TotalMinutes;

            /* usageCurve == UsageCurveEnum.MaxValue &&
             * usageCurve == UsageCurveEnum.Real */
            while (true) // Move to the right
            {
                if (offset == UsageData.Count - 1) break;
                if (UsageData[offset + 1].TimeSinceInsertion <= timeDelta)
                    offset++;
                else break; // We can't go further
            }
            while (true)
            {
                if (offset == 0) break;
                if (UsageData[offset].TimeSinceInsertion > timeDelta)
                    offset--;
                else break; // We can't go further
            }

            var prioroffset = offset - indexes_to_skip;
            if (offset - indexes_to_skip > 0)
                priorUsageInfo = new UsageInfo
                {
                    Cpu = UsageData[prioroffset].Cpu,
                    Disk = UsageData[prioroffset].Disk,
                    Memory = UsageData[prioroffset].Memory,
                    TimeSinceInsertion = UsageData[prioroffset].
                                            TimeSinceInsertion
                };
            else
                priorUsageInfo = new UsageInfo
                {
                    Cpu = UsageData[0].Cpu,
                    Disk = UsageData[0].Disk,
                    Memory = UsageData[0].Memory,
                    TimeSinceInsertion = UsageData[0].TimeSinceInsertion
                };

            if (offset == UsageData.Count - 1)
                // We always terminate every trace with 0s
                return new UsageInfo
                {
                    Cpu = 0,
                    Disk = 0,
                    Memory = 0,
                    TimeSinceInsertion = timeDelta
                };
            else
                return new UsageInfo
                {
                    Cpu = UsageData[offset].Cpu,
                    Disk = UsageData[offset].Disk,
                    Memory = UsageData[offset].Memory,
                    TimeSinceInsertion = UsageData[offset].TimeSinceInsertion
                };
        }
    }
}
