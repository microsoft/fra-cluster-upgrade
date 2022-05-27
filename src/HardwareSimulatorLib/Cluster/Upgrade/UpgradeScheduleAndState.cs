﻿using HardwareSimulatorLib.Cluster.Placement;
using System;
using System.Collections.Generic;

namespace HardwareSimulatorLib.Cluster.Upgrade
{
    public class UpgradeScheduleAndState
    {
        public static TimeSpan IntervalBetweenUpgrades;
        public static TimeSpan TimeToUpgradeUDs;
        public static TimeSpan TimeToUpgradeSingleUD;
        public static TimeSpan TimeToStartUpgradeLastUD;

        public int nextIdx;
        public TimeSpan[] UpgradeStartElapsedTime;
        public TimeSpan[] UpgradeEndElapsedTime;

        public HashSet<int> NodeIdsUnderUpgrade;
        public int DomainUnderUpgrade;

        public readonly bool IsUpgradeUnidirectional;
        private readonly int NumUDs;

        public bool IsUpgradeLowerToHigherUDs
        {
            get
            {
                // If upgrade is unidirectional, then all upgrades are from low to high UDs.
                // Otherwise:
                //    1st, 3rd, 5th etc. are from low to high UDs.
                //    2nd, 4th, 6th etc. are from high to low UDs.
                return IsUpgradeUnidirectional || (nextIdx % 2 == 0);
            }
        }

        public int[] numReplicasInDomainUnderUpgrade = new int[] { -1, -1, -1, -1 };

        public UpgradeScheduleAndState(int WarmupInHours,
            int IntervalBetweenUpgradesInHours, int NumUDs, bool IsUpgradeUnidirectional,
            int TimeToUpgradeSingleNodeInHours, TimeSpan simDuration)
        {
            this.NumUDs = NumUDs;
            this.IsUpgradeUnidirectional = IsUpgradeUnidirectional;

            var initialTime = TimeSpan.FromHours(WarmupInHours);
            IntervalBetweenUpgrades = TimeSpan.FromHours(IntervalBetweenUpgradesInHours);
            TimeToUpgradeUDs = TimeSpan.FromHours(TimeToUpgradeSingleNodeInHours * NumUDs);
            TimeToUpgradeSingleUD = TimeSpan.FromHours(TimeToUpgradeSingleNodeInHours);
            TimeToStartUpgradeLastUD = TimeSpan.FromHours((NumUDs - 1) * TimeToUpgradeSingleNodeInHours);

            var numUpgrades = 0;
            for (var timeElapsed = initialTime + IntervalBetweenUpgrades;
                     timeElapsed <= simDuration;
                     timeElapsed += IntervalBetweenUpgrades)
            {
                numUpgrades++;
            }

            UpgradeStartElapsedTime = new TimeSpan[numUpgrades];
            UpgradeEndElapsedTime = new TimeSpan[numUpgrades];
            UpgradeStartElapsedTime[0] = initialTime + IntervalBetweenUpgrades;
            UpgradeEndElapsedTime[0] = UpgradeStartElapsedTime[0] + TimeToUpgradeUDs;
            for (var i = 1; i < numUpgrades; i++)
            {
                UpgradeStartElapsedTime[i] = UpgradeStartElapsedTime[i - 1] + IntervalBetweenUpgrades;
                UpgradeEndElapsedTime[i] = UpgradeStartElapsedTime[i] + TimeToUpgradeUDs;
            }

            NodeIdsUnderUpgrade = new HashSet<int>();
        }

        public int GetNumUpgradesPlanned() => UpgradeStartElapsedTime.Length;

        public int GetInitialUD() => IsUpgradeLowerToHigherUDs ? 0 : NumUDs - 1;

        public int GetPreviouslyUpgradedUD(int UD)
            => IsUpgradeLowerToHigherUDs ? UD - 1 : UD + 1;

        public PlacementPreference GetPlacementPreference()
            => IsUpgradeUnidirectional ?
                PlacementPreference.MaximizeUpgradeDomainsWithBound :
                (IsUpgradeLowerToHigherUDs ?
                    PlacementPreference.MinimizeUpgradeDomains :
                    PlacementPreference.MaximizeUpgradeDomains);

        public PlacementPreference GetSwapPlacementPreference(int UD)
            => UD == GetInitialUD() ?
                (IsUpgradeLowerToHigherUDs ?
                    PlacementPreference.MaximizeUpgradeDomains :
                    PlacementPreference.MinimizeUpgradeDomains) :
                GetPlacementPreference();

        public bool IsInInitialUD(int nodeId)
        {
            return IsUpgradeLowerToHigherUDs ?
                (nodeId < ClusterManager.NumNodesPerUD) :
                (nodeId >= ClusterManager.NumNodesPerUD * 9 /* TODO: remove hardcoding */ );
        }

        public bool IsUpgrading(TimeSpan timeElapsed)
            => nextIdx < UpgradeStartElapsedTime.Length        &&
               timeElapsed >= UpgradeStartElapsedTime[nextIdx] &&
               timeElapsed <= UpgradeEndElapsedTime[nextIdx];

        public void SetDomainToUpgrade(int UD)
        {
            DomainUnderUpgrade = UD;
            var startNodeId = UD * ClusterManager.NumNodesPerUD;
            var endNodeId = (UD + 1) * ClusterManager.NumNodesPerUD;
            NodeIdsUnderUpgrade.Clear();
            for (var nodeId = startNodeId; nodeId < endNodeId; nodeId++)
                NodeIdsUnderUpgrade.Add(nodeId);
        }

        public int GetUDToUpgrade(TimeSpan timeElapsed)
        {
            // Warning: assumes correct call for now. TODO: add assertion.
            var UD = (int)((timeElapsed - UpgradeStartElapsedTime[nextIdx])
                .Ticks / TimeToUpgradeSingleUD.Ticks);
            return IsUpgradeLowerToHigherUDs ? UD : NumUDs - 1 - UD;
        }

        public bool IsTimeToStartDomainUpgrade(TimeSpan timeElapsed)
        {
            if (nextIdx >= UpgradeStartElapsedTime.Length ||
                timeElapsed < UpgradeStartElapsedTime[nextIdx] ||
                timeElapsed > UpgradeStartElapsedTime[nextIdx] + TimeToStartUpgradeLastUD)
            {
                return false;
            }

            var timeElapsedSinceStart = timeElapsed - UpgradeStartElapsedTime[nextIdx];
            return timeElapsedSinceStart.Ticks % TimeToUpgradeSingleUD.Ticks == 0;
        }

        public bool IsTimeToEndUpgrade(TimeSpan elapsedTime)
            => nextIdx < UpgradeEndElapsedTime.Length &&
                elapsedTime == UpgradeEndElapsedTime[nextIdx];
    }
}
