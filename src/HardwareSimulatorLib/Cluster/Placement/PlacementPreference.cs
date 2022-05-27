namespace HardwareSimulatorLib.Cluster.Placement
{
    public enum PlacementPreference
    {
        /* No preference regarding upgrade domains (UDs) */
        None,

        /* Failover a replica
           1. currently placed on a UD i
           2. to a node on a UD j where j < i  */
        MinimizeUpgradeDomains,

        /* Failover a replica
           1. currently placed on a UD i
           2. to a node on a UD j where j > i  */
        MaximizeUpgradeDomains,

        /* Failover a replica 
           1. currently placed on a UD i
           2. to a node on a UD j where j > i 
           3. with an upper bound on j */
        MaximizeUpgradeDomainsWithBound
    };
}
