# TR-Overbooking Simulator
This simulator estimates the lower bound of the performance of a tenant ring with various overbooking ratios. It simulates a fully packed tenant ring by replaying resource usage trace collected from production TRs. Performance metrics include the number of resource violations, resource utilization ratios, and etc. Besides overbooking ratio, users can also configure the resource capacity of a tenant ring such as SKU of a node, the number of nodes, and various resource limits.

At each step, first the simulator randomly admits tenants from the resource usage trace until the ring is full. A ring is full when its overbooking ratio has been reached, or resource limits are hit. Second, for each newly admitted and existing tenant, the simulator will replay its resource usage. Finally, the simulator will resolve resource violation on each node in a similar way as PLB by minimizing the variance of resource utilization across nodes. The width of step is configurable, such as 1 minute, 5 mins and etc. as long as it is not smaller than the granularity of the resource usage trace. Also, the same tenant in resource usage trace might be admitted mutiple times into the ring as different tenants.

# Jumpstart

  1. Downloading resource usage trace from production TRs
  2. Downloading DB-to-SLO mappings
  3. Configuring TR-Overbooking Simulator
  4. Building and running TR-Overbooking Simulator
  5. Collecting Performance Metrics

## 1. Downloading Resource Usage Trace from Production TRs
Resource usage trace is downloaded from Cosmos. A sample scope script that collects resource usage trace on 08/05/2019 from all TRs in koreasouth1-a is shown below:
~~~~
#DECLARE cl string = "koreasouth1-a";
#DECLARE outputFilePathDisk string = "/my/output/AzureResourceUsage/Test_"+@cl+"_2019-08-05_AllTR_DiskMem.txt";

rs1 = VIEW "/shares/SQLDB.Prod/local/SqlAzure/Production/Views/Public/MonRgManager.view"
PARAMS (startDate="2019-08-05", endDate="2019-08-05");

rs3 =
    SELECT 
                                timestamp,
                                ClusterName,
                                NodeRole,
                                MachineName,
                                AppName,
                                application_name,
                                AppTypeName,
                                LogicalServerName,
                                SubscriptionId,
                                ResourceGroup,
                                replica_id,
                                is_primary_replica,
                                metric_name,
                                metric_value,
                                metric_frq,
                             application_correlation_id
                                
    FROM rs1
    WHERE 
                ClusterName.Contains("koreasouth1-a")
          && eventName == "sql_plb_metrics_reporting"
                      && (metric_name == "AppCpuUsage" || metric_name == "InstanceDiskSpaceUsed" || metric_name == "AppMemoryUsageMB")
    ORDER BY ClusterName, application_name, AppName, MachineName, timestamp,  metric_name   ASC;
OUTPUT rs3 TO @outputFilePathDisk;
~~~~

Upon query completion, the output file should be downloaded to the local machine where the simulator is running.

## 2. Downloading DB-to-SLO Mapping
DB-to-SLO mapping is downloaded separately from Cosmos as well. A sample scope script that collects the mapping of all tenants in July 2019 from all TRs in koreasouth1-a is shown below:
~~~~
#DECLARE outputFilePathDisk string = "/my/output/AzureResourceUsage/AllSLOs_koreasouth_August.txt";
rs1 = VIEW "/shares/SQLDB.Prod/local/SqlAzure/Production/Views/Public/MonDmRealTimeResourceStats.view"
PARAMS (startDate="2019-07-01", endDate="2019-07-30");

rs2 =
    SELECT ClusterName,
           NodeName,
           slo_name,
           AppName
    FROM rs1
    WHERE ClusterName.Contains("koreasouth1-a") && NodeRole == "DB" 
    GROUP BY ClusterName, NodeName, slo_name, AppName;

OUTPUT rs2 TO @outputFilePathDisk;
~~~~
Upon query completion, the output file should be downloaded to the local machine where the simulator is running. The downloaded file needs to be reformatted to be consumed by the simulator. Sample c# code for reformatting is shown below:
~~~~
StreamReader reader = new StreamReader(original_mapping_file);
StreamWriter writer = new StreamWriter(reformatted_mapping_file);
string line;

while ((line = reader.ReadLine()) != null)
{
    string[] fields = line.Split('\t');
    string output = "";

    output += fields[0] + "\t";
    output += fields[1].Substring(0, fields[1].IndexOf('.')) + fields[1].Substring(fields[1].IndexOf('.') + 1) + "\t";
    output += "RgManagerTenant" + "\t";
    output += "fabric:/Worker.ISO/" + fields[3] + "\t";
    output += fields[2].Substring(0, fields[2].LastIndexOf('_'));
    writer.WriteLine(output);
}
reader.Close();
writer.Close();
~~~~

## 3. Configuring TR-Overbooking Simulator
All configurable parameters are hard-coded in the simulator. These include sku of TR capacity, paths to files in Step 1 and 2, and etc. Please make changes in the code and rebuild as appropriate. It is also desirable to use external configuration XMLs to avoid frequent rebuilding the simulator, if necessary.

## 4. Building and Running TR-Overbooking Simulator
The simulator is written in C#, and requires .Net framework 4.5+ to compile and run.

## 5. Collecting Performance Metrics
The output CSV file after each run is in the following format. Please make changes in the simulator code if more detailed performance metrics are needed.
~~~~
ring_vCore_util_ratio,ring_disk_util_ratio,ring_mem_util_ratio,cpu_violation_num,disk_violation_num,mem_violation_num,total_db_num
~~~~