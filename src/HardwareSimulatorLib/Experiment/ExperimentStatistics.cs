using System.IO;

namespace HardwareSimulatorLib.Experiment
{
    public class ExperimentStatistics
    {
        public int minutesElapsed;

        public double VCoreUtilRatio { get; set; }
        public double DiskUtilRatio { get; set; }
        public double MemUtilRatio { get; set; }
        public double MaxVCoreUtilRatio { get; set; }

        public double NumCpuViolations { get; set; }
        public double NumDiskViolations { get; set; }
        public double NumMemViolations { get; set; }

        public double Moves { get; set; }
        public double MovesAfterViolation { get; set; }
        public double NumMovesDueToClearSpace { get; set; }
        public double CpuMoved { get; set; }
        public double DiskMoved { get; set; }
        public double MemoryMoved { get; set; }

        public double NumPlacementFailures { get; set; }
        public double NumReplacementFailures { get; set; }

        public double NumReplicas { get; set; }

        public string ToTSV() => string.Join(",", minutesElapsed,
                VCoreUtilRatio, DiskUtilRatio, MemUtilRatio, MaxVCoreUtilRatio,
                NumCpuViolations, NumDiskViolations, NumMemViolations, NumReplicas,
                CpuMoved, MemoryMoved, DiskMoved, Moves, NumPlacementFailures,
                NumReplacementFailures);

        public void Add(ExperimentStatistics Statistic)
        {
            VCoreUtilRatio += Statistic.VCoreUtilRatio;
            DiskUtilRatio += Statistic.DiskUtilRatio;
            MemUtilRatio += Statistic.MemUtilRatio;
            MaxVCoreUtilRatio += Statistic.MaxVCoreUtilRatio;
            NumCpuViolations += Statistic.NumCpuViolations;
            NumDiskViolations += Statistic.NumDiskViolations;
            NumMemViolations += Statistic.NumMemViolations;
            Moves += Statistic.Moves;
            MovesAfterViolation += Statistic.MovesAfterViolation;
            NumMovesDueToClearSpace += Statistic.NumMovesDueToClearSpace;
            CpuMoved += Statistic.CpuMoved;
            DiskMoved += Statistic.DiskMoved;
            MemoryMoved += Statistic.MemoryMoved;
            NumPlacementFailures += Statistic.NumPlacementFailures;
            NumReplacementFailures += Statistic.NumReplacementFailures;
            NumReplicas += Statistic.NumReplicas;
        }

        public void Divide(int numRuns, long totalSimIntervalsOverAllRuns)
        {
            VCoreUtilRatio /= totalSimIntervalsOverAllRuns;
            DiskUtilRatio /= totalSimIntervalsOverAllRuns;
            MemUtilRatio /= totalSimIntervalsOverAllRuns;
            MaxVCoreUtilRatio /= totalSimIntervalsOverAllRuns;
            NumCpuViolations /= numRuns;
            NumDiskViolations /= numRuns;
            NumMemViolations /= numRuns;
            Moves /= numRuns;
            MovesAfterViolation /= numRuns;
            NumMovesDueToClearSpace /= numRuns;
            CpuMoved /= numRuns;
            DiskMoved /= numRuns;
            MemoryMoved /= numRuns;
            NumPlacementFailures /= numRuns;
            NumReplacementFailures /= numRuns;
            NumReplicas /= totalSimIntervalsOverAllRuns;
        }

        public void Log(string filename)
        {
            using (var sw = new StreamWriter(filename))
            {
                sw.WriteLine("Avg vCore Util Percentage: " + VCoreUtilRatio.ToString());
                sw.WriteLine("Avg MaxvCore Util Percentage: " + MaxVCoreUtilRatio.ToString());
                sw.WriteLine("Avg Disk util Percentage: " + DiskUtilRatio.ToString());
                sw.WriteLine("Avg Mem Util Percentage: " + MemUtilRatio.ToString());
                sw.WriteLine("CPU violations: " + NumCpuViolations.ToString());
                sw.WriteLine("Disk Violations: " + NumDiskViolations.ToString());
                sw.WriteLine("Mem Violations: " + NumMemViolations.ToString());
                sw.WriteLine("Avg Total DB Num: " + NumReplicas.ToString());
                sw.WriteLine("Moves: " + Moves.ToString());
                sw.WriteLine("Moves (violation): " + MovesAfterViolation.ToString());
                sw.WriteLine("Moves (clearing Space): " + NumMovesDueToClearSpace.ToString());
                sw.WriteLine("Placement failures: " + NumPlacementFailures.ToString());
                sw.WriteLine("Replacement Failures: " + NumReplacementFailures.ToString());
                sw.WriteLine("CpuMoved: " + CpuMoved.ToString());
                sw.WriteLine("MemoryMoved: " + MemoryMoved.ToString());
                sw.WriteLine("DiskMoved: " + DiskMoved.ToString());
            }
        }
    }
}
