using HardwareSimulatorLib.Cluster.Upgrade;
using NUnit.Framework;
using System;

namespace HardwareSimulatorLibTest
{
    public class Tests
    {
        [SetUp]
        public void Setup() { }

        [Test]
        public void TestUpgradeScheduleAndStateTest()
        {
            var numUDs = 10;

            var warmupInHours = 24;
            var intervalBetweenUpgradesInHours = 4 /* weeks */
                                               * 7 /* days  */
                                               * 24 /* hours */;
            var timeToUpgradeSingleNodeInHours = 2;
            var simDuration = TimeSpan.FromHours(3 *
                4 /* weeks */ * 7 /* days */ * 24 /* hours */ +
                48 /* warump and extra day */);

            var state = new UpgradeScheduleAndState(warmupInHours,
                intervalBetweenUpgradesInHours, numUDs,
                timeToUpgradeSingleNodeInHours, simDuration);

            Assert.True(state.UpgradeStartElapsedTime[0] == TimeSpan.FromDays(29));
            Assert.True(state.UpgradeStartElapsedTime[1] == TimeSpan.FromDays(57));
            Assert.True(state.UpgradeStartElapsedTime[2] == TimeSpan.FromDays(85));

            var twentyMins = TimeSpan.FromHours(20);
            Assert.True(state.UpgradeEndElapsedTime[0] == TimeSpan.FromDays(29) + twentyMins);
            Assert.True(state.UpgradeEndElapsedTime[1] == TimeSpan.FromDays(57) + twentyMins);
            Assert.True(state.UpgradeEndElapsedTime[2] == TimeSpan.FromDays(85) + twentyMins);

            for (var timeElapsed = TimeSpan.Zero;
                     timeElapsed < simDuration;
                     timeElapsed += TimeSpan.FromMinutes(10))
            {
                var isUnderUpgrade = false;
                for (var i = 0; i < state.UpgradeStartElapsedTime.Length; i++)
                {
                    if (timeElapsed >= state.UpgradeStartElapsedTime[i] &&
                        timeElapsed <= state.UpgradeEndElapsedTime[i])
                    {
                        isUnderUpgrade = true;
                        Assert.True(state.IsUpgrading(timeElapsed));
                        var ExpectedUD = -1;
                        for (var UD = 0; UD < numUDs; UD++)
                        {
                            if (timeElapsed == state.UpgradeStartElapsedTime[i]
                                + TimeSpan.FromHours(2 * UD))
                            {
                                ExpectedUD = UD;
                            }
                        }
                        if (ExpectedUD != -1)
                        {
                            Assert.True(state.IsTimeToStartDomainUpgrade(timeElapsed));
                            Assert.AreEqual(i == 1 ? numUDs - 1 - ExpectedUD :
                                ExpectedUD, state.GetUDToUpgrade(timeElapsed));
                        }
                        else
                        {
                            Assert.False(state.IsTimeToStartDomainUpgrade(timeElapsed));
                        }
                        if (timeElapsed == state.UpgradeEndElapsedTime[i])
                        {
                            state.nextIdx++;
                        }
                    }
                }

                if (!isUnderUpgrade)
                {
                    Assert.False(state.IsUpgrading(timeElapsed));
                }
            }
        }
    }
}
