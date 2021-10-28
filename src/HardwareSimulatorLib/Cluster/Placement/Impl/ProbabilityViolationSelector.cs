using HardwareSimulatorLib.Config;
using HardwareSimulatorLib.Experiment;
using HardwareSimulatorLib.Predictor;
using HardwareSimulatorLib.Trace;
using System;
using System.Collections.Generic;
using System.Linq;

namespace HardwareSimulatorLib.Cluster.Placement.Impl
{
    public abstract class ProbabilityViolationSelector : PlacementSelector
    {
        protected readonly double probabilityOfViolationThreshold;
        protected readonly int probabilityOfViolationMCRepetitions;
        protected readonly bool extrapolateGrowingTenants;

        protected ViolationPredictor predictor;
        readonly Dictionary<string, SampledTenants> MCSCache;
        public double[] NodeIdToViolationProbability;

        public ProbabilityViolationSelector(
            ClusterManager clusterManager, ExperimentParams experimentParams,
            Random rand, ViolationPredictor predictor) :
            base(clusterManager, experimentParams, rand)
        {
            this.probabilityOfViolationThreshold =
                experimentParams.ProbabilityOfViolationThreshold;
            this.probabilityOfViolationMCRepetitions =
                experimentParams.ProbabilityOfViolationMCRepetitions;
            this.extrapolateGrowingTenants =
                experimentParams.ExtrapolateGrowingTenants;

            this.predictor = predictor;
            this.MCSCache = new Dictionary<string, SampledTenants>();
            this.NodeIdToViolationProbability = new double[cluster.NumNodes];
        }

        public override int ChooseNodeToPlaceReplica(TimeSpan timeElapsed,
            string replicaToMove, string slo, ref UsageInfo replicaToPlaceUsage)
        {
            ComputeProbabilityOfViolationPerNodeForPlacement(timeElapsed,
                replicaToMove, slo, replicaToPlaceUsage);
            return base.ChooseNodeToPlaceReplica(timeElapsed, replicaToMove,
                slo, ref replicaToPlaceUsage);
        }

        public override int ChooseNodeToMoveReplicaTo(TimeSpan timeElapsed,
            string replicaToMove)
        {
            ComputeProbabilityOfViolationPerNodeForMove(timeElapsed, replicaToMove);
            return base.ChooseNodeToMoveReplicaTo(timeElapsed, replicaToMove);
        }

        public void ComputeProbabilityOfViolationPerNodeForPlacement(
                TimeSpan timeElapsed, string replicaToMove, string slo,
                UsageInfo replicaToPlaceResourceUsage)
        {
            //Create an empty entry in the MCS cache
            var sample = new SampledTenants
            {
                no_match = false,
                minAMU = -1,
                minIDSU = -1,
                maxAMU = double.MaxValue,
                maxIDSU = double.MaxValue,
                minlifetime = -1,
                sampledOffsets = new int[probabilityOfViolationMCRepetitions]
            };
            sample.sampledOffsets[0] = -1;
            MCSCache[replicaToMove] = sample;

            for (var nodeId = 0; nodeId < cluster.NumNodes; nodeId++)
            {
                if (cluster.NodeIdToPlacedReplicasIdMap[nodeId].Count == 0)
                {
                    NodeIdToViolationProbability[nodeId] = 0;
                    continue;
                }

                var constraints = new List<TenantConstraints>();
                var Time_Offsets = new List<int>();
                var tenants = cluster.NodeIdToPlacedReplicasIdMap[nodeId];
                var TenantsArray = tenants.ToArray();
                var currentUsages = new List<ResourceUsage>();
                var MCSample = new List<SampledTenants>();

                for (var j = 0; j < TenantsArray.Length; j++)
                {
                    ReplicaInfo.ExtractIDs(TenantsArray[j],
                        out string tenantID1, out string tenant_real_id);
                    var candidateSlo = cluster.traceMan.TenantIdToSloMap[tenantID1];
                    var candidateTrace = cluster.traceMan.ReplicaIdToTraceMap[tenant_real_id];

                    var candidateUsage = extrapolateGrowingTenants ?
                        candidateTrace.CandidateUsageAccoutingForGrowth(
                            timeElapsed - cluster.ReplicaIdToPlacementTime[TenantsArray[j]]) :
                        candidateTrace.GetResourceUsage(
                            timeElapsed - cluster.ReplicaIdToPlacementTime[TenantsArray[j]]);

                    currentUsages.Add(new ResourceUsage
                    {
                        Disk = candidateUsage.Disk,
                        Memory = candidateUsage.Memory,
                        Cpu = candidateUsage.Cpu
                    });

                    // Extract the tenant constraints
                    // Not using this for perf. reasons
                    constraints.Add(new TenantConstraints
                    {
                        minLifetime = (timeElapsed - cluster.ReplicaIdToPlacementTime[TenantsArray[j]]).
                            TotalMinutes / ExperimentRunner.SimulationIntervalLength.TotalMinutes,
                        minIDSU = candidateUsage.Disk, // refine this later to be the max usage up to this point
                        minAMU = candidateUsage.Memory,
                        maxIDSU = double.MaxValue,
                        maxAMU = double.MaxValue,
                        slo = candidateSlo
                    });

                    // Compute the time offset
                    var time_offset = -1 * (int)Math.Round(
                        (timeElapsed - cluster.ReplicaIdToPlacementTime[TenantsArray[j]]).TotalMinutes /
                            ExperimentRunner.SimulationIntervalLength.TotalMinutes);
                    Time_Offsets.Add(time_offset);

                    // Add the pre-computed sample offsets if they exist
                    //if (MCSCache.ContainsKey(tenant_real_id))
                    MCSample.Add(MCSCache[TenantsArray[j]]); // These should always exist
                }

                // Add the tenant to be placed
                currentUsages.Add(new ResourceUsage
                {
                    Disk = replicaToPlaceResourceUsage.Disk,
                    Memory = replicaToPlaceResourceUsage.Memory,
                    Cpu = replicaToPlaceResourceUsage.Cpu
                });
                constraints.Add(new TenantConstraints
                {
                    slo = slo,
                    maxIDSU = double.MaxValue,
                    maxAMU = double.MaxValue,
                    minIDSU = 0,
                    minAMU = 0,
                    minLifetime = 0
                });
                Time_Offsets.Add(0); // We're placing the tenant at the current time

                MCSample.Add(MCSCache[replicaToMove]);
                NodeIdToViolationProbability[nodeId] =
                    predictor.ComputeProbabilityViolation(
                        constraints, true /*use the model*/, Time_Offsets,
                        probabilityOfViolationMCRepetitions,
                        currentUsages, cluster.NodeDiskSizeInMB,
                        cluster.NodeMemorySizeInMB,
                        true /* use sparce evaluation */, ref MCSample);

                // Now write the pertinent information back into the cache data structure
                for (int j = 0; j < TenantsArray.Length; j++)
                    MCSCache[TenantsArray[j]] = MCSample[j];
                MCSCache[replicaToMove] = MCSample[MCSample.Count - 1];
            }
        }

        public void ComputeProbabilityOfViolationPerNodeForMove(
            TimeSpan timeElapsed, string replicaToMove)
        {
            ReplicaInfo.ExtractIDs(replicaToMove, out string tenantId,
                out string canonicalId);
            var trace = cluster.traceMan.ReplicaIdToTraceMap[canonicalId];
            var replicaToMoveResourceUsage = trace.GetResourceUsage(
                timeElapsed - cluster.ReplicaIdToPlacementTime[replicaToMove]);
            var slo = cluster.traceMan.TenantIdToSloMap[tenantId];

            for (int nodeId = 0; nodeId < cluster.NumNodes; nodeId++)
            {
                var constraints = new List<TenantConstraints>();
                var Time_Offsets = new List<int>();
                var tenants = cluster.NodeIdToPlacedReplicasIdMap[nodeId];
                var TenantsArray = tenants.ToArray();
                var currentUsages = new List<ResourceUsage>();
                var MCSample = new List<SampledTenants>();

                for (var j = 0; j < TenantsArray.Length; j++)
                {
                    ReplicaInfo.ExtractIDs(TenantsArray[j], out string tenantID1, out string tenant_real_id);
                    var candidateSlo = cluster.traceMan.TenantIdToSloMap[tenantID1];
                    var candidateTrace = cluster.traceMan.ReplicaIdToTraceMap[tenant_real_id];

                    var candidateUsage = extrapolateGrowingTenants ?
                        candidateTrace.
                            CandidateUsageAccoutingForGrowth(timeElapsed -
                                cluster.ReplicaIdToPlacementTime[TenantsArray[j]]) :
                        candidateTrace.GetResourceUsage(
                            timeElapsed - cluster.ReplicaIdToPlacementTime[TenantsArray[j]]);

                    currentUsages.Add(new ResourceUsage
                    {
                        Disk = candidateUsage.Disk,
                        Memory = candidateUsage.Memory,
                        Cpu = candidateUsage.Cpu
                    });

                    // Extract the tenant constraints
                    constraints.Add(new TenantConstraints
                    {
                        minLifetime = timeElapsed.TotalMinutes / ExperimentRunner.SimulationIntervalLength.TotalMinutes,
                        minIDSU = candidateUsage.Disk, // refine this later to be the max usage up to this point
                        minAMU = candidateUsage.Memory,
                        maxIDSU = double.MaxValue,
                        maxAMU = double.MaxValue,
                        slo = candidateSlo
                    });

                    // Compute the time offset
                    var time_offset = -1 * (int)Math.Round(
                        (timeElapsed - cluster.ReplicaIdToPlacementTime[TenantsArray[j]]).
                            TotalMinutes / ExperimentRunner.SimulationIntervalLength.TotalMinutes);
                    Time_Offsets.Add(time_offset);

                    MCSample.Add(MCSCache[TenantsArray[j]]);
                }

                // Add the tenant to be placed
                currentUsages.Add(new ResourceUsage
                {
                    Disk = replicaToMoveResourceUsage.Disk,
                    Memory = replicaToMoveResourceUsage.Memory,
                    Cpu = replicaToMoveResourceUsage.Cpu
                });
                constraints.Add(new TenantConstraints
                {
                    slo = slo,
                    maxIDSU = double.MaxValue,
                    maxAMU = double.MaxValue,
                    minIDSU = replicaToMoveResourceUsage.Disk,
                    minAMU = replicaToMoveResourceUsage.Memory,
                    minLifetime = 0
                });
                Time_Offsets.Add(0);    // We're placing the tenant now

                MCSample.Add(MCSCache[replicaToMove]);
                NodeIdToViolationProbability[nodeId] =
                    predictor.ComputeProbabilityViolation(
                        constraints, true /*use the model*/, Time_Offsets,
                        probabilityOfViolationMCRepetitions,
                        currentUsages, cluster.NodeDiskSizeInMB,
                        cluster.NodeMemorySizeInMB, true, ref MCSample);
            }
        }
    }
}
