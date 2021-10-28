using System;

namespace HardwareSimulatorLib.Config
{
    // Reads in a processed daily MonRgManager file created by "Pull Data.script"
    // See "https://cosmos11.osdinfra.net/cosmos/SQLDB.AdHoc/shares/SQLDB.Prod/local/SqlAzure/Production/ssDaily/MonRgManager/20191222.ss?property=info"
    public struct MonRgManagerInput
    {
        public DateTime timestamp;
        public string ClusterId; // tr1
        public string MachineName; // DB39
        public string AppName; // a3a8f83f106f
        public int AppCpuUsage;
        public int InstanceDiskSpaceUsed;
        public int AppMemoryUsageMB;
        public string SloName;

        public static void Parse(string line, ref MonRgManagerInput output)
        {
            var columns = line.Split('\t');

            if (columns.Length != 8)
                throw new ArgumentException(string.Format(
                    "Expected 8 columns; got {0}", columns.Length));

            output.timestamp = DateTime.Parse(columns[0]);
            output.ClusterId = columns[1];
            // need to confirm the nomenclature on these:
            // Note : MachineName actually is appname and appname is machine name.
            output.MachineName = columns[3];
            output.AppName = columns[2];
            output.AppCpuUsage = int.Parse(columns[4]);
            output.InstanceDiskSpaceUsed = int.Parse(columns[5]);
            output.AppMemoryUsageMB = int.Parse(columns[6]);
            output.SloName = columns[7];
        }
    }
}
