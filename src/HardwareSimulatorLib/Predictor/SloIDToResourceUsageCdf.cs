using System;
using System.Collections.Generic;

namespace HardwareSimulatorLib.Predictor
{
    [Serializable]
    public class CumulativeDistributionFunction
    {
        public List<double> data = new List<double>();

        public void Add(double dataPoint) => data.Add(dataPoint);

        public void FinalizeCdf() => data.Sort();

        public double GetPercentile(int percentile) =>
            data[Math.Max(data.Count * percentile / 100 - 1, 0)];
    }

    [Serializable]
    public class SloIdToResourceUsageCdf
    {
        public Dictionary<string /* SloId */, CumulativeDistributionFunction>
            SloIdToResourceUsageCdfMap =
                new Dictionary<string, CumulativeDistributionFunction>();

        public void AddDataPoint(string SloId, double dataPoint)
        {
            if (!SloIdToResourceUsageCdfMap.ContainsKey(SloId))
                SloIdToResourceUsageCdfMap[SloId] =
                    new CumulativeDistributionFunction();
            SloIdToResourceUsageCdfMap[SloId].Add(dataPoint);
        }

        public void FinalizeCdfs()
        {
            foreach (var cdf in SloIdToResourceUsageCdfMap.Values)
                cdf.FinalizeCdf();
        }

        public double GetPercentile(string SloId, int percentile)
        {
            if (!SloIdToResourceUsageCdfMap.ContainsKey(SloId))
                return -1;
            return SloIdToResourceUsageCdfMap[SloId].GetPercentile(percentile);
        }
    }
}
