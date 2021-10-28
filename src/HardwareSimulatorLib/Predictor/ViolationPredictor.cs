using System;
using System.Collections.Generic;
using System.Linq;
using HardwareSimulatorLib.Config;
using HardwareSimulatorLib.Experiment;
using HardwareSimulatorLib.Slo;
using HardwareSimulatorLib.Trace;

namespace HardwareSimulatorLib.Predictor
{
    public struct ResourceUsage
    {
        public double Disk;
        public double Memory;
        public double Cpu;
    };

    public struct ResourceUsageTrace
    {
        public double MaxMemoryUsage;
        public double MaxDiskUsage;

        // A usage curve is resource usage over time in constant time intervals
        public ResourceUsage[] usageCurve;
    }

    public class ResourceUsageComparer : IComparer<ResourceUsageTrace>
    {
        public int Compare(ResourceUsageTrace l, ResourceUsageTrace r)
        {
            return l.MaxDiskUsage.CompareTo(r.MaxDiskUsage);
        }
    }

    public struct SampledTenants
    {
        // Min/Max over sampled tenants.
        public double minlifetime;
        public double minIDSU;
        public double minAMU;
        public double maxIDSU;
        public double maxAMU;
        public int[] sampledOffsets;
        public bool no_match;
    }

    public class ViolationPredictor
    {
        private const int MaxPredictorSize = 215000;
        private const int PercentileOfReplicaIdsToUse = 40;
        private static readonly ResourceUsageComparer resourceUsageComparer =
            new ResourceUsageComparer();

        public SloIdToResourceUsageCdf SloIdToMaxDiskUsageCdf;
        public SloIdToResourceUsageCdf SloIdToMaxMemoryUsageCdf;
        public SloIdToResourceUsageCdf SloIdToMaxCpuUsageCdf;

        public Dictionary<string, List<ResourceUsageTrace>> SloIdToHistoricalUsageCurveMap;
        public Dictionary<string, List<ResourceUsageTrace>> SloIdTiPredictedUsageCurveMap;
        public Dictionary<string, ResourceUsage> MaxDemandsPerSLO;
        public static Random rand = new Random(0);

        public ViolationPredictor(TraceManager traceManager)
        {
            SloIdToHistoricalUsageCurveMap = new Dictionary<string, List<ResourceUsageTrace>>();
            MaxDemandsPerSLO = new Dictionary<string, ResourceUsage>();
            SloIdToMaxDiskUsageCdf = new SloIdToResourceUsageCdf();
            SloIdToMaxMemoryUsageCdf = new SloIdToResourceUsageCdf();
            SloIdToMaxCpuUsageCdf = new SloIdToResourceUsageCdf();

            var rand = new Random(1);

            // if too large, we use the max predictor size.
            var numToSample = Math.Min(MaxPredictorSize,
                PercentileOfReplicaIdsToUse * traceManager.TenantIds.Count() / 100);

            var tenantIdsSampled = new HashSet<string>();
            var numSampled = 0;
            for (; ;)
            {
                var idx = rand.Next(traceManager.TenantIds.Count());
                var tenantId = traceManager.TenantIds[idx];
                if (!tenantIdsSampled.Contains(tenantId))
                {
                    tenantIdsSampled.Add(tenantId);
                    numSampled += traceManager.TenantIdToReplicaIds[tenantId].Count;
                }
                if (numSampled >= numToSample)
                    break;
            }

            foreach (var tenantId in tenantIdsSampled)
            {
                foreach (var replicaId in traceManager.TenantIdToReplicaIds[tenantId])
                {
                    var trace = traceManager.ReplicaIdToTraceMap[replicaId];
                    var slo = traceManager.TenantIdToSloMap[replicaId.
                        Substring(replicaId.LastIndexOf('_') + 1)];
                    if (SloSpecification.IsSloAllowedInSim(slo))
                    {
                        SloIdToMaxDiskUsageCdf.AddDataPoint(slo, trace.GetMaxDiskUsage());
                        SloIdToMaxMemoryUsageCdf.AddDataPoint(slo, trace.GetMaxMemUsage());
                        SloIdToMaxCpuUsageCdf.AddDataPoint(slo, trace.GetMaxCpuUsage());
                        AddHistoricalUsageCurve(slo, trace, UsageCurveEnum.Real, MetricToUseEnum.All);
                    }
                }
            }
            SloIdToMaxDiskUsageCdf.FinalizeCdfs();
            SloIdToMaxMemoryUsageCdf.FinalizeCdfs();
            SloIdToMaxCpuUsageCdf.FinalizeCdfs();
            FinalizePredictor();

            foreach (var tenantId in tenantIdsSampled)
            {
                traceManager.TenantIdToReplicaIds.Remove(tenantId);
                traceManager.TenantIds.Remove(tenantId);
            }

            Console.WriteLine("Sampled and removed replicas for violation predictor.");
            var numPremiumTenants = 0;
            var numStandardTenants = 0;
            foreach (var tenantId in traceManager.TenantIdToReplicaIds.Keys)
            {
                if (traceManager.TenantIdToReplicaIds[tenantId].Count == 1)
                    numStandardTenants++;
                else // TenantIdToReplicaIds[tenantId].Count == 4
                {
                    numPremiumTenants++;
                }
            }
            Console.WriteLine("Left with " + numPremiumTenants + " Premium " +
                "tenants and " + numStandardTenants + " Standard tenants.");
        }

        public void UpdateWithPredictedMaxResourceUsage(
        ref UsageInfo usage, string Slo)
        {
            usage.Memory = Math.Max(usage.Memory,
                SloIdToMaxMemoryUsageCdf.GetPercentile(Slo, 99));
            usage.Disk = Math.Max(usage.Disk,
                SloIdToMaxDiskUsageCdf.GetPercentile(Slo, 99));
            usage.Cpu = Math.Max(usage.Cpu,
                SloIdToMaxCpuUsageCdf.GetPercentile(Slo, 99));
        }

        public UsageInfo PredictMaxResourceUsage(ref UsageInfo defaulUsage,
            string SLO)
        {
            return new UsageInfo
            {
                Memory = Math.Max(SloIdToMaxMemoryUsageCdf
                    .GetPercentile(SLO, 99), defaulUsage.Memory),
                Disk = Math.Max(SloIdToMaxDiskUsageCdf
                    .GetPercentile(SLO, 99), defaulUsage.Disk),
                Cpu = Math.Max(SloIdToMaxCpuUsageCdf
                    .GetPercentile(SLO, 99), defaulUsage.Cpu)
            };
        }

        public void AddHistoricalUsageCurve(string sloId, ReplicaTrace trace,
            UsageCurveEnum usageCurve, MetricToUseEnum metricToUse)
        {
            if (!SloIdToHistoricalUsageCurveMap.ContainsKey(sloId))
                SloIdToHistoricalUsageCurveMap[sloId] =
                    new List<ResourceUsageTrace>();

            // Get the curve at the appropriate level of resolution
            var tmpUsage = new List<UsageInfo>();
            for (var timeElapsed = TimeSpan.Zero;
                     timeElapsed <= trace.GetLifetime() +
                                    ExperimentRunner.SimulationIntervalLength;
                     timeElapsed += ExperimentRunner.SimulationIntervalLength)
            {
                tmpUsage.Add(trace.GetResourceUsage(timeElapsed));
            }

            var D = new ResourceUsage[tmpUsage.Count];
            double maxAMU = 0; double maxIDSU = 0;
            for (int i = 0; i < tmpUsage.Count; i++)
            {
                D[i].Disk = tmpUsage[i].Disk;
                D[i].Memory = tmpUsage[i].Memory;
                D[i].Cpu = tmpUsage[i].Cpu;
                maxAMU = Math.Max(maxAMU, tmpUsage[i].Memory);
                maxIDSU = Math.Max(maxIDSU, tmpUsage[i].Disk);
            }
            SloIdToHistoricalUsageCurveMap[sloId].Add(new ResourceUsageTrace
            {
                usageCurve = D,
                MaxMemoryUsage = maxAMU,
                MaxDiskUsage = maxIDSU
            });
        }

        private void FinalizePredictor()
        {
            SloIdTiPredictedUsageCurveMap = new Dictionary<string, List<ResourceUsageTrace>>();
            // Iterate over all buckets
            foreach (string bucket in SloIdToHistoricalUsageCurveMap.Keys) // Iterate over all SLOs
            {
                var DemandCurves = new List<ResourceUsageTrace>();
                SloIdTiPredictedUsageCurveMap[bucket] = DemandCurves;

                // Sort
                SloIdToHistoricalUsageCurveMap[bucket].Sort(resourceUsageComparer);
                foreach (var DemandCurve in SloIdToHistoricalUsageCurveMap[bucket])
                {
                    ResourceUsage[] DemandCurveModel = new ResourceUsage[DemandCurve.usageCurve.Length];
                    double maxDiskUsage = 0;
                    double maxMemUsage = 0;
                    bool end_of_growth_Disk = false;
                    bool end_of_growth_Memory = false;

                    // Compute the max usages for this demand curve
                    for (int i = 0; i < DemandCurve.usageCurve.Length; i++)
                    {
                        maxDiskUsage = Math.Max(DemandCurve.usageCurve[i].Disk, maxDiskUsage);
                        maxMemUsage = Math.Max(DemandCurve.usageCurve[i].Memory, maxMemUsage);
                    }

                    for (int i = 0; i < DemandCurve.usageCurve.Length; i++)
                    {
                        DemandCurveModel[i].Cpu = DemandCurve.usageCurve[i].Cpu; // We'll ignore CPUUsage for now, since it's irrelevant
                        if (i == 0)
                        {
                            DemandCurveModel[i].Disk = DemandCurve.usageCurve[i].Disk;
                            DemandCurveModel[i].Memory = DemandCurve.usageCurve[i].Memory;
                        }
                        else
                        {
                            if (i == DemandCurve.usageCurve.Length - 1)        // The last value is always 0
                            {
                                DemandCurveModel[i].Disk = 0;
                                DemandCurveModel[i].Memory = 0;
                            }
                            else
                            {
                                if (end_of_growth_Disk)
                                    DemandCurveModel[i].Disk = maxDiskUsage;// ((maxDiskUsage * 95) / 100);
                                if (end_of_growth_Memory)
                                    DemandCurveModel[i].Memory = maxMemUsage;// ((maxMemUsage * 95) / 100);
                            }
                        }

                        if ((!end_of_growth_Disk) && (DemandCurve.usageCurve[i].Disk >= (maxDiskUsage * 98) / 100))
                        {
                            end_of_growth_Disk = true;
                            for (int j = 1; j <= i; j++)
                                DemandCurveModel[j].Disk = (DemandCurveModel[0].Disk + (((double)j) / ((double)i)) * (maxDiskUsage - DemandCurveModel[0].Disk));
                        }
                        if ((!end_of_growth_Memory) && (DemandCurve.usageCurve[i].Memory >= (maxMemUsage * 97) / 100))
                        {
                            end_of_growth_Memory = true;
                            for (int j = 1; j <= i; j++)
                                DemandCurveModel[j].Memory = (DemandCurveModel[0].Memory + (((double)j) / ((double)i)) * (maxMemUsage - DemandCurveModel[0].Memory));
                        }
                    }

                    // Finally, update all the information we use for filtering of appropriate traces
                    ResourceUsageTrace trace = new ResourceUsageTrace();
                    trace.usageCurve = DemandCurveModel;
                    double maxAMU = 0; double maxIDSU = 0;
                    for (int j = 0; j < DemandCurve.usageCurve.Length; j++)
                    {
                        maxAMU = Math.Max(maxAMU, DemandCurve.usageCurve[j].Memory);
                        maxIDSU = Math.Max(maxIDSU, DemandCurve.usageCurve[j].Disk);
                    }
                    trace.MaxDiskUsage = maxIDSU;
                    trace.MaxMemoryUsage = maxAMU;

                    // We're done poulating the model - add it
                    SloIdTiPredictedUsageCurveMap[bucket].Add(trace);
                }
                SloIdTiPredictedUsageCurveMap[bucket].Sort(resourceUsageComparer);
            }

            foreach (string bucket in SloIdToHistoricalUsageCurveMap.Keys) // Iterate over all SLOs to compute the max demand per SLO
            {
                ResourceUsage MaxDemands = new ResourceUsage { Cpu = 0, Disk = 0, Memory = 0 };
                foreach (ResourceUsageTrace T in SloIdToHistoricalUsageCurveMap[bucket])
                {
                    MaxDemands.Disk = Math.Max(MaxDemands.Cpu, T.MaxDiskUsage);
                    MaxDemands.Memory = Math.Max(MaxDemands.Memory, T.MaxMemoryUsage);
                }
                MaxDemandsPerSLO[bucket] = MaxDemands;
            }
        }

        public double ComputeProbabilityViolation(
            List<TenantConstraints> constraints, bool fUseModel,
            List<int> Time_Offsets, int numRepetitions,
            List<ResourceUsage> currentUsages, double maxDisk, double maxMem,
            bool useSparseEvaluation, ref List<SampledTenants> MCSample)
        {
            // Stores the offsets of traces that satisfy the constraints;
            //  this is the set of tenants we subsequently sample from
            var qualifiedTenants = new List<int[]>();
            var qualifiedTimeOffset = new List<int>();
            var qualifiedConstraints = new List<TenantConstraints>();
            // List of the indexes of selected tenants in the original list
            var mcSampleOffset = new List<int>();

            var fReuseExistingSamples = new bool[constraints.Count];

            // populate the set of tenants to sample from
            for (int i = 0; i < constraints.Count; i++)
            {
                // update the constraints to reflect current usage
                constraints[i].minIDSU = Math.Max(constraints[i].minIDSU, currentUsages[i].Disk);
                constraints[i].minAMU = Math.Max(constraints[i].minAMU, currentUsages[i].Memory);

                bool fReuseExistingSample = false;
                List<int> offsets = new List<int>();
                // First, check if we can re-use the existing sampled traces
                if (MCSample[i].sampledOffsets[0] != -1 || MCSample[i].no_match)
                {   // These are prepopulated  
                    if ((MCSample[i].minAMU >= currentUsages[i].Memory && MCSample[i].minIDSU >= currentUsages[i].Disk) || MCSample[i].no_match || MCSample[i].minlifetime < (Time_Offsets[i] * -1))
                    {
                        fReuseExistingSample = true;
                    }    // can we re-use the existing sample
                }

                fReuseExistingSamples[i] = fReuseExistingSample;

                if (!fReuseExistingSample)
                {
                    if (SloIdToHistoricalUsageCurveMap.ContainsKey(constraints[i].slo))
                    {
                        ResourceUsageTrace X = new ResourceUsageTrace
                        {
                            MaxDiskUsage = constraints[i].minIDSU
                        };

                        if (!fUseModel)
                        {
                            // First, do a binary search to find the subset of traces that satisfy the MaxIDSU constraint
                            int index = SloIdToHistoricalUsageCurveMap[constraints[i].slo].BinarySearch(X, resourceUsageComparer);
                            if (index < 0)
                                index = ~index;

                            //foreach (DemandInfo[] DemandCurve in TrainingData[constraints[i].SLO])
                            for (int l = index; l < SloIdToHistoricalUsageCurveMap[constraints[i].slo].Count; l++)
                            {
                                // Does this tenant qualify
                                if ((constraints[i].minAMU <= SloIdToHistoricalUsageCurveMap[constraints[i].slo][l].MaxMemoryUsage && constraints[i].maxAMU >= SloIdToHistoricalUsageCurveMap[constraints[i].slo][l].MaxMemoryUsage)
                                     && SloIdToHistoricalUsageCurveMap[constraints[i].slo][l].usageCurve.Length > constraints[i].minLifetime &&
                                    (constraints[i].minIDSU <= SloIdToHistoricalUsageCurveMap[constraints[i].slo][l].MaxDiskUsage && constraints[i].maxIDSU >= SloIdToHistoricalUsageCurveMap[constraints[i].slo][l].MaxDiskUsage))
                                    offsets.Add(l);
                            }

                        }
                        else
                        {
                            // First, do a binary search to find the subset of traces that satisfy the MaxIDSU constraint
                            int index = SloIdTiPredictedUsageCurveMap[constraints[i].slo].BinarySearch(X, resourceUsageComparer);
                            if (index < 0)
                                index = ~index;

                            //foreach (DemandInfo[] DemandCurve in TrainingData[constraints[i].SLO])
                            for (int l = index; l < SloIdTiPredictedUsageCurveMap[constraints[i].slo].Count; l++)
                            {
                                // Does this tenant qualify
                                if ((constraints[i].minAMU <= SloIdTiPredictedUsageCurveMap[constraints[i].slo][l].MaxMemoryUsage && constraints[i].maxAMU >= SloIdTiPredictedUsageCurveMap[constraints[i].slo][l].MaxMemoryUsage)
                                     && SloIdTiPredictedUsageCurveMap[constraints[i].slo][l].usageCurve.Length > constraints[i].minLifetime &&
                                    (constraints[i].minIDSU <= SloIdTiPredictedUsageCurveMap[constraints[i].slo][l].MaxDiskUsage && constraints[i].maxIDSU >= SloIdTiPredictedUsageCurveMap[constraints[i].slo][l].MaxDiskUsage))
                                    offsets.Add(l);
                            }
                        }

                        int[] offsetArray = offsets.ToArray();
                        qualifiedTenants.Add(offsetArray);
                        qualifiedTimeOffset.Add(Time_Offsets[i]);
                        qualifiedConstraints.Add(constraints[i]);
                        mcSampleOffset.Add(i);

                        // If we don't have a matching trace, we have to mark this
                        if (offsets.Count == 0)
                        {
                            fReuseExistingSamples[i] = true;
                            SampledTenants temp = MCSample[i];
                            temp.no_match = true;
                            temp.maxAMU = currentUsages[i].Memory;
                            temp.minAMU = currentUsages[i].Memory;
                            temp.maxIDSU = currentUsages[i].Disk;
                            temp.minIDSU = currentUsages[i].Disk;
                            temp.minlifetime = constraints[i].minLifetime;
                            MCSample[i] = temp;
                        }
                    }
                    else
                    {
                        fReuseExistingSamples[i] = true;
                        SampledTenants temp = MCSample[i];
                        temp.no_match = true;
                        temp.maxAMU = currentUsages[i].Memory;
                        temp.minAMU = currentUsages[i].Memory;
                        temp.maxIDSU = currentUsages[i].Disk;
                        temp.minIDSU = currentUsages[i].Disk;
                        temp.minlifetime = constraints[i].minLifetime;
                        MCSample[i] = temp;
                    }
                }
                else
                {
                    qualifiedTenants.Add(MCSample[i].sampledOffsets);
                    qualifiedTimeOffset.Add(Time_Offsets[i]);
                    qualifiedConstraints.Add(constraints[i]);
                    mcSampleOffset.Add(i);
                }
                //  No need to compute the subset of qualified tenants, if we're re-using the cached offsets
            }

            int numViolations = 0;

            // Early out if the node is already overloaded
            double CurrentTotalDiskUsage = 0;
            double CurrentTotalMemUsage = 0;
            double SampleBoundOnTotalDiskUsage = 0;
            double SampleBoundOnTotalMemUsage = 0;
            bool fAllTenantsReuseTheirSamples = true;

            for (int j = 0; j < qualifiedConstraints.Count; j++)
            {
                CurrentTotalDiskUsage += currentUsages[j].Disk;
                CurrentTotalMemUsage += currentUsages[j].Memory;
                fAllTenantsReuseTheirSamples = fAllTenantsReuseTheirSamples && fReuseExistingSamples[j];
                SampleBoundOnTotalDiskUsage += MCSample[mcSampleOffset[j]].maxIDSU;
                SampleBoundOnTotalMemUsage += MCSample[mcSampleOffset[j]].maxAMU;
            }

            if (CurrentTotalDiskUsage > maxDisk || CurrentTotalMemUsage > maxMem)
                // Is this node already overloaded?
                return 1;   // We're done here :)

            // Quick check if a violation is even possible
            double UpperBoundOnDiskUsage = 0;
            double UpperBoundOnMemoryUsage = 0;

            for (var j = 0; j < qualifiedConstraints.Count; j++)
            {
                if (MaxDemandsPerSLO.ContainsKey(qualifiedConstraints[j].slo))
                {
                    UpperBoundOnDiskUsage += MaxDemandsPerSLO[qualifiedConstraints[j].slo].Disk;
                    UpperBoundOnMemoryUsage += MaxDemandsPerSLO[qualifiedConstraints[j].slo].Memory;
                }
                else
                {
                    UpperBoundOnDiskUsage += currentUsages[j].Disk;
                    UpperBoundOnMemoryUsage += currentUsages[j].Memory;
                }   // if we don't have any training data for an SLO, we use the current usages in the simulation
            }

            var fViolationNotPossible = UpperBoundOnDiskUsage <= maxDisk &&
                UpperBoundOnMemoryUsage <= maxMem;
            if (fAllTenantsReuseTheirSamples)
                fViolationNotPossible = fViolationNotPossible ||
                    (SampleBoundOnTotalDiskUsage <= maxDisk &&
                        SampleBoundOnTotalMemUsage <= maxMem);

            if (!fViolationNotPossible)   // if this doesn't hold, there's no chance of a violation
            {
                int[] current_sample = new int[qualifiedConstraints.Count];
                double[] minIDSU = new double[qualifiedConstraints.Count]; //double.MaxValue;
                double[] minAMU = new double[qualifiedConstraints.Count]; //double.MaxValue;
                double[] maxIDSU = new double[qualifiedConstraints.Count]; //0;
                double[] maxAMU = new double[qualifiedConstraints.Count]; //0;
                double[] minlifetime = new double[qualifiedConstraints.Count]; //double.MaxValue;
                for (int i = 0; i < numRepetitions; i++)
                {
                    for (int j = 0; j < qualifiedConstraints.Count; j++)
                    {
                        if (!fReuseExistingSamples[j])
                        {
                            if (qualifiedTenants[j].Count() > 0)
                            {
                                int offset = rand.Next(qualifiedTenants[j].Count());
                                current_sample[j] = qualifiedTenants[j][offset];
                                MCSample[mcSampleOffset[j]].sampledOffsets[i] = current_sample[j];
                            }
                            else current_sample[j] = -1;    // No qualified tenant
                        }
                        else
                        {
                            if (MCSample[mcSampleOffset[j]].no_match)
                                current_sample[j] = -1;
                            else
                            {
                                int offset = (MCSample[mcSampleOffset[j]].sampledOffsets[i]);
                                current_sample[j] = offset;
                            }
                        }
                    }

                    for (int j = 0; j < qualifiedConstraints.Count; j++)
                    {
                        if (!fReuseExistingSamples[j])  // We need to update the min/max values associated with the precomputed sample
                        {
                            if (i == 0)
                            {
                                if (fUseModel)
                                {
                                    double DiskUsage = SloIdTiPredictedUsageCurveMap[constraints[j].slo][current_sample[j]].usageCurve[SloIdTiPredictedUsageCurveMap[constraints[j].slo][current_sample[j]].usageCurve.Length - 2].Disk;
                                    double MemoryUsage = SloIdTiPredictedUsageCurveMap[constraints[j].slo][current_sample[j]].usageCurve[SloIdTiPredictedUsageCurveMap[constraints[j].slo][current_sample[j]].usageCurve.Length - 2].Memory;
                                    minIDSU[j] = DiskUsage;
                                    minAMU[j] = MemoryUsage;
                                    maxIDSU[j] = DiskUsage;
                                    maxAMU[j] = MemoryUsage;
                                    minlifetime[j] = SloIdTiPredictedUsageCurveMap[constraints[j].slo][current_sample[j]].usageCurve.Length;
                                    if (minAMU[j] < currentUsages[j].Memory || minIDSU[j] < currentUsages[j].Disk)
                                        Console.WriteLine("Error in bounding logic!");
                                }
                                else
                                {
                                    double DiskUsage = 0;
                                    double MemoryUsage = 0;
                                    for (int k = 0; k < SloIdToHistoricalUsageCurveMap[constraints[j].slo][current_sample[j]].usageCurve.Length; k++)
                                    {
                                        DiskUsage = Math.Max(SloIdToHistoricalUsageCurveMap[constraints[j].slo][current_sample[j]].usageCurve[k].Disk, DiskUsage);
                                        MemoryUsage = Math.Max(SloIdToHistoricalUsageCurveMap[constraints[j].slo][current_sample[j]].usageCurve[k].Memory, MemoryUsage);
                                    }
                                    minIDSU[j] = DiskUsage;
                                    minAMU[j] = MemoryUsage;
                                    maxIDSU[j] = DiskUsage;
                                    maxAMU[j] = MemoryUsage;
                                    minlifetime[j] = SloIdToHistoricalUsageCurveMap[constraints[j].slo][current_sample[j]].usageCurve.Length;
                                    if (minAMU[j] < currentUsages[j].Memory || minIDSU[j] < currentUsages[j].Disk)
                                        Console.WriteLine("Error in bounding logic!");
                                }
                            }
                            else
                            {
                                if (fUseModel)
                                {
                                    double DiskUsage = SloIdTiPredictedUsageCurveMap[constraints[j].slo][current_sample[j]].usageCurve[SloIdTiPredictedUsageCurveMap[constraints[j].slo][current_sample[j]].usageCurve.Length - 2].Disk;
                                    double MemoryUsage = SloIdTiPredictedUsageCurveMap[constraints[j].slo][current_sample[j]].usageCurve[SloIdTiPredictedUsageCurveMap[constraints[j].slo][current_sample[j]].usageCurve.Length - 2].Memory;
                                    minIDSU[j] = Math.Min(DiskUsage, minIDSU[j]);
                                    minAMU[j] = Math.Min(MemoryUsage, minAMU[j]);
                                    maxIDSU[j] = Math.Max(DiskUsage, maxIDSU[j]);
                                    maxAMU[j] = Math.Max(MemoryUsage, maxAMU[j]);
                                    minlifetime[j] = Math.Min(SloIdTiPredictedUsageCurveMap[constraints[j].slo][current_sample[j]].usageCurve.Length, minlifetime[j]);
                                    if (minAMU[j] < currentUsages[j].Memory || minIDSU[j] < currentUsages[j].Disk)
                                        Console.WriteLine("Error in bounding logic!");
                                }
                                else
                                {
                                    double DiskUsage = 0;
                                    double MemoryUsage = 0;
                                    for (int k = 0; k < SloIdToHistoricalUsageCurveMap[constraints[j].slo][current_sample[j]].usageCurve.Length; k++)
                                    {
                                        DiskUsage = Math.Max(SloIdToHistoricalUsageCurveMap[constraints[j].slo][current_sample[j]].usageCurve[k].Disk, DiskUsage);
                                        MemoryUsage = Math.Max(SloIdToHistoricalUsageCurveMap[constraints[j].slo][current_sample[j]].usageCurve[k].Memory, MemoryUsage);
                                    }
                                    minIDSU[j] = Math.Min(DiskUsage, minIDSU[j]);
                                    minAMU[j] = Math.Min(MemoryUsage, minAMU[j]);
                                    maxIDSU[j] = Math.Max(DiskUsage, maxIDSU[j]);
                                    maxAMU[j] = Math.Max(MemoryUsage, maxAMU[j]);
                                    minlifetime[j] = Math.Min(SloIdToHistoricalUsageCurveMap[constraints[j].slo][current_sample[j]].usageCurve.Length, minlifetime[j]);
                                    if (minAMU[j] < currentUsages[j].Memory || minIDSU[j] < currentUsages[j].Disk)
                                        Console.WriteLine("Error in bounding logic!");
                                }
                            }
                        }
                    }

                    var fViolation = false;

                    var maxLength = 0;
                    //Get the max length of every trace
                    if (!useSparseEvaluation || !fUseModel)
                    {
                        for (int j = 0; j < qualifiedTenants.Count; j++)
                        {
                            if (current_sample[j] != -1)
                            {
                                if (!fUseModel) maxLength = Math.Max(maxLength, SloIdToHistoricalUsageCurveMap[qualifiedConstraints[j].slo][current_sample[j]].usageCurve.Length + qualifiedTimeOffset[j] /* Note: these have to be negative or 0 */);
                                else maxLength = Math.Max(maxLength, SloIdTiPredictedUsageCurveMap[qualifiedConstraints[j].slo][current_sample[j]].usageCurve.Length + qualifiedTimeOffset[j] /* Note: these have to be negative or 0 */);
                            }
                        }

                        for (var time = 0; time < maxLength; time++)
                        {
                            var currentDiskUsage = 0.0;
                            var currentMemoryUsage = 0.0;
                            // Again, we're ignoring CPU for now
                            for (int j = 0; j < qualifiedTenants.Count; j++)
                            {
                                if (current_sample[j] != -1)
                                {
                                    int trace_offset = time + (-1 * qualifiedTimeOffset[j]);

                                    if (!fUseModel)
                                    {
                                        if (trace_offset < SloIdToHistoricalUsageCurveMap[qualifiedConstraints[j].slo][current_sample[j]].usageCurve.Length)
                                        {
                                            currentDiskUsage += SloIdToHistoricalUsageCurveMap[qualifiedConstraints[j].slo][current_sample[j]].usageCurve[trace_offset].Disk;
                                            currentMemoryUsage += SloIdToHistoricalUsageCurveMap[qualifiedConstraints[j].slo][current_sample[j]].usageCurve[trace_offset].Memory;
                                        }
                                        else
                                        {
                                            currentDiskUsage += currentUsages[j].Disk;
                                            currentMemoryUsage += currentUsages[j].Memory;
                                        }
                                    }
                                    else
                                    {
                                        if (trace_offset < SloIdTiPredictedUsageCurveMap[qualifiedConstraints[j].slo][current_sample[j]].usageCurve.Length)
                                        {
                                            currentDiskUsage += SloIdTiPredictedUsageCurveMap[qualifiedConstraints[j].slo][current_sample[j]].usageCurve[trace_offset].Disk;
                                            currentMemoryUsage += SloIdTiPredictedUsageCurveMap[qualifiedConstraints[j].slo][current_sample[j]].usageCurve[trace_offset].Memory;
                                        }
                                        else
                                        {
                                            currentDiskUsage += currentUsages[j].Disk;
                                            currentMemoryUsage += currentUsages[j].Memory;
                                        }
                                    }
                                }
                                else
                                {
                                    // If we don't have a fitting trace, we will use the current usage 
                                    // Typically, tenants grow very little after a while, so this is not a bad heuristic
                                    currentDiskUsage += currentUsages[j].Disk;
                                    currentMemoryUsage += currentUsages[j].Memory;
                                }
                            }

                            // Check for a violation
                            if (currentDiskUsage > maxDisk || currentMemoryUsage > maxMem)
                            {
                                fViolation = true; break;
                            }
                        }
                        if (fViolation)
                            numViolations++;
                    }
                    else
                    {
                        // enumerate the different timepoints where we need to evaluate
                        List<int> Time_offsets = new List<int>();
                        for (int j = 0; j < qualifiedTenants.Count; j++)
                        {
                            if (current_sample[j] != -1)
                            {
                                int time_offsetToEvaluate = (SloIdTiPredictedUsageCurveMap[qualifiedConstraints[j].slo][current_sample[j]].usageCurve.Length - 2) + (qualifiedTimeOffset[j]);
                                Time_offsets.Add(time_offsetToEvaluate);
                            }
                        }

                        // Evaluate at these time-points and no other
                        // Note: we know that we use the model here; sparse evaluation doesn't make sense for non-model traces
                        foreach (int time_offset in Time_offsets)
                        {
                            double currentDiskUsage = 0;
                            double currentMemoryUsage = 0;
                            // Again, we're ignoring CPU for now

                            for (int j = 0; j < qualifiedTenants.Count; j++)
                            {
                                if (current_sample[j] != -1)
                                {
                                    int trace_offset = time_offset + (-1 * qualifiedTimeOffset[j]);
                                    if (trace_offset < SloIdTiPredictedUsageCurveMap[qualifiedConstraints[j].slo][current_sample[j]].usageCurve.Length
                                        && trace_offset > 0)  // This tenant is still active
                                    {
                                        currentDiskUsage += SloIdTiPredictedUsageCurveMap[qualifiedConstraints[j].slo][current_sample[j]].usageCurve[trace_offset].Disk;
                                        currentMemoryUsage += SloIdTiPredictedUsageCurveMap[qualifiedConstraints[j].slo][current_sample[j]].usageCurve[trace_offset].Memory;
                                    }
                                }
                                else
                                {
                                    // If we don't have a fitting trace, we will use the current usage 
                                    // Typically, tenants grow very little after a while, so this is not a bad heuristic
                                    currentDiskUsage += currentUsages[j].Disk;
                                    currentMemoryUsage += currentUsages[j].Memory;
                                }
                            }

                            // Check for a violation
                            if (currentDiskUsage > maxDisk || currentMemoryUsage > maxMem)
                            {
                                fViolation = true; break;
                            }
                        }
                        if (fViolation)
                            numViolations++;
                    }
                }
                for (int j = 0; j < qualifiedConstraints.Count; j++)
                {
                    if (!fReuseExistingSamples[j])
                    {
                        SampledTenants S_temp = MCSample[mcSampleOffset[j]];
                        S_temp.maxAMU = maxAMU[j];
                        S_temp.minAMU = minAMU[j];
                        S_temp.maxIDSU = maxIDSU[j];
                        S_temp.minIDSU = minIDSU[j];
                        S_temp.minlifetime = minlifetime[j];
                        MCSample[mcSampleOffset[j]] = S_temp;
                    }
                }

                // Double-check that we re-populated all data structures correctly.
                for (int i = 0; i < constraints.Count; i++)
                {
                    bool fReuseExistingSample = false;
                    // First, check if we can re-use the existing sampled traces
                    if (MCSample[i].sampledOffsets[0] != -1 || MCSample[i].no_match)
                    {   // These are prepopulated  
                        if ((MCSample[i].minAMU >= currentUsages[i].Memory && MCSample[i].minIDSU >= currentUsages[i].Disk) || MCSample[i].no_match || MCSample[i].minlifetime < (Time_Offsets[i] * -1))
                        {
                            fReuseExistingSample = true;
                        }    // can we re-use the existing sample
                    }
                    if ((!fReuseExistingSample) && (MCSample[i].sampledOffsets[0] != -1) && (!MCSample[i].no_match))
                        Console.WriteLine("Error in Probability of Violation Computation");
                }
            }

            return numViolations / ((double)numRepetitions);
        }

    }
}
