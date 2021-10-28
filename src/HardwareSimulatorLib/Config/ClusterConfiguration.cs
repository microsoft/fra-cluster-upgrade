namespace HardwareSimulatorLib.Config
{
    // This class contains the default config parameters for the cluster.
    // The parameters will be passed-in as a JSON file to the program.
    // The default values may be subject to change.
    public class ClusterConfiguration
    {
        public int Nodes = 40;
        public int vCoreOverheadPerNode = 2;
        public int MemoryOverheadPerNodeInGB = 20;
        public int DiskOverheadPerNodeInGB = 200;
    }
}
