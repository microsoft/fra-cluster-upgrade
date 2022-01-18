namespace HardwareSimulatorLib.Config
{
    // This class contains the default config params for the sim's search space.
    // The parameters will be passed-in as a JSON file to the program.
    // The default values may be subject to change.
    public class SearchSpace
    {
        public double[] OverbookingRatios = { 1 };

        // const double sku_base_mem_size_gb = 4085; // for i in 0 to 5
        // Deducting non-db usage:
        //   memorySizes.Add((sku_base_mem_size_gb + i * 250) * 1024 - 40000);
        public double[] MemorySizesInGB =
        {
            4143040, 4399040, 4655040, 4911040, 5167040
        };

        // const double sku_base_disk_size_tb = 14.2;
        // Deducting non-db usage
        //   diskSizes.Add((sku_base_disk_size_tb + i) * 1024^2 - 200 * 1024);
        public double[] DiskSizesInGB =
        {
            14684979.2, 15733555.2, 16782131.2, 17830707.2, 18879283.2
        };
        public double[] VCoresPerNode = { 128 };
        public double[] DiskCaps = { 1.0, 0.70, 0.75, 0.8 };
        public double[] MemoryCaps = { 0.9, 0.85 };
        public double[] CpuCaps = { 0.9 };
        public double[] NodeMemMaxUsageRatiosForPlacement = { 1.0 /*0.9*/ };
        public double[] NodeDiskMaxUsageRatiosForPlacement = { 1.0 /*0.9*/ };
    }
}
