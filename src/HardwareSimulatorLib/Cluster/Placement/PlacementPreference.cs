namespace HardwareSimulatorLib.Cluster.Placement
{
    public enum PlacementPreference
    {
        /* No preference regarding upgrade domains (UDs) */
        None,

        /* Failover a replica placed on an upgrade domain (UD) i
           to a node on an upgrade domain (UD) j where j < i  */
        LowerUpgradeDomains,

        /* Failover a replica placed on an upgrade domain (UD) i
           to a node on an upgrade domain (UD) j where j > i  */
        UpperUpgradeDomains,
    };
}
