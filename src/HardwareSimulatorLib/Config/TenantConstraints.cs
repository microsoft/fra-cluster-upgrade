namespace HardwareSimulatorLib.Config
{
    public class TenantConstraints
    {
        // Average memory use : AMU
        // IDSU is the same but for Disk.
        public string slo;
        public double minIDSU;
        public double minAMU;
        public double minLifetime;
        public double maxIDSU;
        public double maxAMU;
    }
}
