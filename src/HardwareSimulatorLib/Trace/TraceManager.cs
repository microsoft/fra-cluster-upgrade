using HardwareSimulatorLib.Config;
using HardwareSimulatorLib.Slo;
using System;
using System.Collections.Generic;
using System.IO;

namespace HardwareSimulatorLib.Trace
{
    public class TraceManager
    {
        public List<string> TenantIds { get; set; }
        public Dictionary<string, ReplicaTrace> ReplicaIdToTraceMap { get; set; }

        public Dictionary<string, string> TenantIdToSloMap { get; set; }
        public Dictionary<string, List<string>> TenantIdToReplicaIds { get; set; }

        public Dictionary<string, List<string>> SloToPremiumTenantIds { get; set; }

        public Dictionary<string, List<string>> SloToStandardTenantIds { get; set; }

        public Random RandomChooseInstance { get; set; }

        private List<string> StdSlos;
        private List<double> StdSloCdf;
        private List<string> PremSlos;
        private List<double> PremSloCdf;

        public TraceManager(string traceFilename, bool UseOnlyNewTenants)
        {
            FilterTraceFile(traceFilename);
            FilterInvalidTraces(UseOnlyNewTenants);
            GroupReplicasInTenants();
        }

        public string[] GetNonHistoricalTenantsAtRandom(int numTracesUsed)
        {
            var rand = RandomChooseInstance.NextDouble() /* between 0 and 1 */ *
                    PremSloCdf[PremSloCdf.Count - 1] /* max val to scale*/;
            var i = 0;
            var slos = StdSlos;
            var dist = StdSloCdf;
            while (rand > dist[i] && i < dist.Count)
            {
                i++;
                if (i == dist.Count && slos == StdSlos)
                {
                    i = 0;
                    slos = PremSlos;
                    dist = PremSloCdf;
                }
            }
            var sloToTenantIds = slos == StdSlos ?
                SloToStandardTenantIds : SloToPremiumTenantIds;
            var tenants = sloToTenantIds[slos[i]];
            var tenantId = tenants[RandomChooseInstance.Next(tenants.Count)];

            var replicas = TenantIdToReplicaIds[tenantId];
            var numberReplicas = replicas.Count == 1 ? 1 : 4;
            var replicasToPlace = new string[numberReplicas];
            for (var j = 0; j < numberReplicas; j++)
                replicasToPlace[j] = TenantIdToReplicaIds[tenantId][j] +
                    "$" + numTracesUsed;
            return replicasToPlace;
        }

        public void SplitReplicasPerSLO()
        {
            SloToPremiumTenantIds = new Dictionary<string, List<string>>();
            SloToStandardTenantIds = new Dictionary<string, List<string>>();
            foreach (var tenantId in TenantIds)
            {
                var SloToTenantIds = TenantIdToReplicaIds[tenantId].Count == 1 ?
                    SloToStandardTenantIds : SloToPremiumTenantIds;
                var slo = TenantIdToSloMap[tenantId];
                if (!SloToTenantIds.ContainsKey(slo))
                    SloToTenantIds[slo] = new List<string>();
                SloToTenantIds[slo].Add(tenantId);
            }

            StdSlos = new List<string>();
            StdSloCdf = new List<double>();
            PremSlos = new List<string>();
            PremSloCdf = new List<double>();

            /* if (SloToProb.Count != SloToStandardTenantIds.Count + SloToPremiumTenantIds.Count)
                throw new Exception("SloToProb.Count != " +
                    "SloToStandardTenantIds.Count + SloToPremiumTenantIds.Count"); */

            var unallowedSlo = new List<string>();
            foreach (var slo in SloToProb.Keys)
            {
                if (!SloSpecification.IsSloAllowedInSim(slo))
                {
                    unallowedSlo.Add(slo);
                    continue;
                }

                var dist = SloToProb[slo];
                if (SloToPremiumTenantIds.ContainsKey(slo))
                {
                    PremSlos.Add(slo);
                    PremSloCdf.Add(dist);
                }
                else if (SloToStandardTenantIds.ContainsKey(slo))
                {
                    StdSlos.Add(slo);
                    StdSloCdf.Add(dist);
                }
            }

            for (var i = 1; i < StdSloCdf.Count; i++)
                StdSloCdf[i] += StdSloCdf[i - 1];
            PremSloCdf[0] += StdSloCdf[StdSloCdf.Count - 1];
            for (var i = 1; i < PremSloCdf.Count; i++)
                PremSloCdf[i] += PremSloCdf[i - 1];
        }

        private void FilterTraceFile(string traceFilename)
        {
            Console.WriteLine("Reading trace records...");
            TenantIdToSloMap = new Dictionary<string, string>();
            ReplicaIdToTraceMap = new Dictionary<string, ReplicaTrace>();

            // The same tenant trace might be plugged in at various time points
            // to reach target OB ratio, but with a tenant ID alias pointing to
            // the same trace data each time.
            var prevClusterId = string.Empty;
            var prevAppName = string.Empty;
            var prevMachineName = string.Empty;
            ReplicaTrace trace = null;

            var lineNum = 0;
            var rcItem = new MonRgManagerInput();
            foreach (var line in File.ReadLines(traceFilename))
            {
                lineNum++;
                if (lineNum % 1000000 == 0)
                {
                    Console.WriteLine((lineNum / 1000000).ToString() +
                        "M lines read");
                    Console.WriteLine(ReplicaIdToTraceMap.Count +
                        " traces detected");
                }

                MonRgManagerInput.Parse(line, ref rcItem);
                if (prevClusterId != rcItem.ClusterId ||
                    prevAppName != rcItem.AppName ||
                    prevMachineName != rcItem.MachineName)
                {
                    if (!TenantIdToSloMap.ContainsKey(rcItem.MachineName))
                        TenantIdToSloMap[rcItem.MachineName] =
                            rcItem.SloName;

                    var CanonicalreplicaID = string.Join("_",
                        rcItem.ClusterId, rcItem.AppName, rcItem.MachineName);
                    trace = new ReplicaTrace();
                    ReplicaIdToTraceMap.Add(CanonicalreplicaID, trace);
                    prevClusterId = rcItem.ClusterId;
                    prevAppName = rcItem.AppName;
                    prevMachineName = rcItem.MachineName;
                }

                trace.AddDataPoint(rcItem);
            }
        }

        private void FilterInvalidTraces(bool UseOnlyNewTenants)
        {
            var invalidEntries = new HashSet<string>();
            // Remove all tenants with old start times
            foreach (var traceID in ReplicaIdToTraceMap.Keys)
            {
                if (ReplicaIdToTraceMap[traceID].InsertionTime <
                        (DateTime.Now - TimeSpan.FromDays(1000)))
                    invalidEntries.Add(traceID);
            }
            Console.WriteLine("Traces removed due to invalid timestamps: " +
                invalidEntries.Count);

            foreach (var traceID in invalidEntries)
            {
                ReplicaIdToTraceMap.Remove(traceID);
            }

            // Remove tenant's that didn't come in on day one
            if (UseOnlyNewTenants)
            {
                var MinStartDate = DateTime.MaxValue;
                foreach (var trace in ReplicaIdToTraceMap.Values)
                    if (MinStartDate > trace.InsertionTime)
                        MinStartDate = trace.InsertionTime;

                // Throw traces that start within an hour of the min start date
                var TracesToRemove = new HashSet<string>();
                foreach (var replicaId in ReplicaIdToTraceMap.Keys)
                {
                    if (ReplicaIdToTraceMap[replicaId].
                           InsertionTime - TimeSpan.FromHours(1) < MinStartDate)
                        TracesToRemove.Add(replicaId);
                }
                foreach (var key in TracesToRemove)
                {
                    ReplicaIdToTraceMap.Remove(key);
                }
            }
        }

        TimeSpan rcTenMins = new TimeSpan(0 /* hrs */, 10 /* mins */, 0 /* secs */);
        private bool IsFailedOver(ReplicaTrace trace0, ReplicaTrace trace1)
        {
            return IsWithin10Mins(trace0.InsertionTime, trace1.InsertionTime);
        }

        private Dictionary<string, double> SloToProb = new Dictionary<string, double>();
        private void GroupReplicasInTenants()
        {
            TenantIdToReplicaIds = new Dictionary<string, List<string>>();
            var slos = new HashSet<string>();
            foreach (var replicaId in ReplicaIdToTraceMap.Keys)
            {
                var tenantId = replicaId.Substring(
                    replicaId.LastIndexOf('_') + 1);
                if (!TenantIdToReplicaIds.ContainsKey(tenantId))
                {
                    TenantIdToReplicaIds[tenantId] = new List<string>();
                    var slo = TenantIdToSloMap[tenantId];
                    slos.Add(slo);
                    if (!SloToProb.ContainsKey(slo))
                        SloToProb[slo] = 0.0;
                    SloToProb[slo]++;
                }
                TenantIdToReplicaIds[tenantId].Add(replicaId);
            }

            foreach (var slo in slos)
                SloToProb[slo] /= TenantIdToReplicaIds.Count;

            var numPremTenants = 0;
            var numStdTenants = 0;
            var filteredNumPremTenants = 0;
            var filteredNumStdTenants = 0;
            var premTenantsToFix = new Dictionary<string, List<string>>();

            var filteredTenantIdToReplicaIds = new Dictionary<string, List<string>>();
            foreach (var tenantId in TenantIdToReplicaIds.Keys)
            {
                if (TenantIdToReplicaIds[tenantId].Count == 1)
                {
                    numStdTenants++;
                    filteredTenantIdToReplicaIds[tenantId] =
                        TenantIdToReplicaIds[tenantId];
                }
                else if (TenantIdToReplicaIds[tenantId].Count < 4)
                    filteredNumStdTenants++;
                else if (TenantIdToReplicaIds[tenantId].Count == 4)
                {
                    // Ensure not a standard failed 3 times.
                    var replicas = TenantIdToReplicaIds[tenantId];
                    var trace0 = ReplicaIdToTraceMap[replicas[0]];
                    var trace1 = ReplicaIdToTraceMap[replicas[1]];
                    var trace2 = ReplicaIdToTraceMap[replicas[2]];
                    var trace3 = ReplicaIdToTraceMap[replicas[3]];
                    if (IsFailedOver(trace0, trace1) || IsFailedOver(trace0, trace2) ||
                        IsFailedOver(trace0, trace3) || IsFailedOver(trace1, trace2) ||
                        IsFailedOver(trace1, trace3) || IsFailedOver(trace2, trace3))
                    {
                        filteredNumStdTenants++;
                    }
                    else
                    {
                        numPremTenants++;
                        SetPrimaryAndSecondaryReplicas(tenantId, replicas);
                        filteredTenantIdToReplicaIds[tenantId] = replicas;
                    }
                }
                else if (TenantIdToReplicaIds[tenantId].Count > 4)
                {
                    var replicas = TenantIdToReplicaIds[tenantId];

                    var earliestInsertionTime = ReplicaIdToTraceMap[replicas[0]].InsertionTime;
                    for (var i = 1; i < replicas.Count; i++)
                    {
                        var insertionTime = ReplicaIdToTraceMap[replicas[i]].InsertionTime;
                        if (insertionTime < earliestInsertionTime)
                            earliestInsertionTime = insertionTime;
                    }
                    var earlyReplicas = new List<string>();
                    foreach (var replica in replicas)
                    {
                        var insertionTime = ReplicaIdToTraceMap[replica].InsertionTime;
                        if (IsWithin10Mins(earliestInsertionTime, insertionTime))
                            earlyReplicas.Add(replica);
                    }

                    if (earlyReplicas.Count < 2)
                    {
                        filteredNumStdTenants++;
                    }
                    else
                    {
                        var largestLifetime = ReplicaIdToTraceMap[earlyReplicas[0]].GetLifetime();
                        for (var i = 1; i < earlyReplicas.Count; i++)
                        {
                            var lifetime = ReplicaIdToTraceMap[earlyReplicas[i]].GetLifetime();
                            if (lifetime > largestLifetime)
                                largestLifetime = lifetime;
                        }
                        var finalReplicas = new List<string>();
                        foreach (var replica in earlyReplicas)
                        {
                            var lifetime = ReplicaIdToTraceMap[replica].GetLifetime();
                            if (IsWithin10Mins(lifetime, largestLifetime))
                                finalReplicas.Add(replica);
                        }
                        if (finalReplicas.Count < 2)
                        {
                            filteredNumPremTenants++;
                        }
                        else // if (finalReplicas.Count in {2, 3, 4} )
                        {
                            numPremTenants++;
                            SetPrimaryAndSecondaryReplicas(tenantId, finalReplicas);
                            filteredTenantIdToReplicaIds[tenantId] = finalReplicas;
                        }
                    }
                }
            }

            TenantIdToReplicaIds = filteredTenantIdToReplicaIds;
            Console.WriteLine("Initially, total of " + (numPremTenants + 
                filteredNumPremTenants) + " Premium tenants and " +
                (numStdTenants + filteredNumStdTenants) + " Standard tenants.");


            Console.WriteLine("After failover filtering, Total of " +
                numPremTenants + " Premium tenants and " + numStdTenants +
                " Standard tenants.");

            TenantIds = new List<string>();
            foreach (var tenantId in TenantIdToReplicaIds.Keys)
                TenantIds.Add(tenantId);

            foreach (var tenantId in TenantIdToReplicaIds.Keys)
            {
                var replicas = TenantIdToReplicaIds[tenantId];
                if (replicas.Count >= 4)
                {
                    var secondaryTrace = ReplicaIdToTraceMap[replicas[1]];
                    for (var i = 2; i < 4; i++)
                    {
                        if (secondaryTrace != ReplicaIdToTraceMap[replicas[i]])
                            throw new Exception("secondaryTrace != " +
                                "ReplicaIdToTraceMap[replicas[i]] - Failed");
                    }
                }
            }
        }

        private void SetPrimaryAndSecondaryReplicas(string tenantId, List<string> replicas)
        {
            var primaryIdx = 0;
            var trace = ReplicaIdToTraceMap[replicas[primaryIdx]];
            // Simple hack - typical hardware SKU has ~10x the disk size.
            var avgUsagePrimary = trace.GetMaxMemUsage() / 10 + trace.GetMaxMemUsage();
            for (var i = 1; i < replicas.Count; i++)
            {
                var replica = replicas[i];
                var trace_i = ReplicaIdToTraceMap[replica];
                if (avgUsagePrimary <
                        trace_i.GetMaxMemUsage() / 10 +
                            trace_i.GetMaxMemUsage())
                {
                    avgUsagePrimary = trace_i.GetMaxMemUsage() / 10
                        + trace_i.GetMaxMemUsage();
                    primaryIdx = i;
                }
            }

            if (primaryIdx != 0)
            {
                var temp = replicas[0];
                replicas[0] = replicas[primaryIdx];
                replicas[primaryIdx] = temp;
            }

            if (replicas.Count < 4)
            {
                while (replicas.Count < 4)
                {
                    foreach (var replica in TenantIdToReplicaIds[tenantId])
                    {
                        var addedAlready = false;
                        foreach (var addedReplica in replicas)
                        {
                            if (replica == addedReplica)
                            {
                                addedAlready = true;
                                break;
                            }
                        }
                        if (!addedAlready)
                        {
                            replicas.Add(replica);
                        }
                        if (replicas.Count == 4) break;
                    }
                }
            }

            // treat the replica at i = 1 as the secondary 
            var secondaryTrace = ReplicaIdToTraceMap[replicas[1]];
            for (var i = 2; i < 4; i++)
                ReplicaIdToTraceMap[replicas[i]] = secondaryTrace;
        }

        private bool IsWithin10Mins(DateTime lDateTime, DateTime rDateTime)
        {
            return (lDateTime < rDateTime &&
                    (rDateTime - lDateTime).Ticks > rcTenMins.Ticks) ||
                   (rDateTime < lDateTime &&
                    (lDateTime - rDateTime).Ticks > rcTenMins.Ticks);
        }

        private bool IsWithin10Mins(TimeSpan lTime, TimeSpan rTime)
        {
            return (lTime < rTime && (rTime - lTime).Ticks > rcTenMins.Ticks) ||
                   (rTime < lTime && (lTime - rTime).Ticks > rcTenMins.Ticks);
        }
    }
}
