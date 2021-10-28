using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using ScopeExe;

namespace HardwareSimulator
{
    class DataPull
    {
        private const string CosmosVC =
            "https://cosmos11.osdinfra.net/cosmos/SQLDB.AdHoc";
        private const int JobPollingSeconds = 30;
        private readonly string sourceTable;

        private readonly ScopeExeWrapper Wrapper;

        // Set of known regions as of 2019-12-23.
        private static readonly string[] KnownRegions = new[]
        {
            "australiacentral1-a",
            "australiacentral2-a",
            "australiaeast1-a",
            "australiasoutheast1-a",
            "brazilsouth1-a",
            "canadacentral1-a",
            "canadaeast1-a",
            "centralus1-a",
            "chinaeast1-a",
            "chinaeast2-a",
            "chinanorth1-a",
            "chinanorth2-a",
            "eastasia1-a",
            "eastus1-a",
            "eastus2-a",
            "francecentral1-a",
            "francesouth1-a",
            "germanycentral1-a",
            "germanynorth1-a",
            "germanynortheast1-a",
            "germanywestcentral1-a",
            "indiacentral1-a",
            "indiasouth1-a",
            "indiawest1-a",
            "japaneast1-a",
            "japanwest1-a",
            "koreacentral1-a",
            "koreasouth1-a",
            "lkgtst1-a",
            "lkgtst2-a",
            "lkgtst3-a",
            "northcentralus1-a",
            "northeurope1-a",
            "northeurope1-b",
            "norwayeast1-a",
            "norwaywest1-a",
            "southafricanorth1-a",
            "southafricawest1-a",
            "southcentralus1-a",
            "southeastasia1-a",
            "switzerlandnorth1-a",
            "switzerlandwest1-a",
            "uaecentral1-a",
            "uaenorth1-a",
            "uknorth1-a",
            "uksouth1-a",
            "uksouth2-a",
            "ukwest1-a",
            "usdodcentral1-a",
            "usdodeast1-a",
            "useuapcentral1-a",
            "useuapeast2-a",
            "usgovcentral1-a",
            "usgoveast1-a",
            "usgovsouthcentral1-a",
            "usgovsouthwest1-a",
            "westcentralus1-a",
            "westeurope1-a",
            "westus1-a",
            "westus2-a"
        };

        public DataPull(string scopeSdkPath, string sourceTable)
        {
            if (string.IsNullOrEmpty(scopeSdkPath))
                throw new ArgumentNullException(nameof(scopeSdkPath));

            if (!scopeSdkPath.ToLower().EndsWith("scope.exe"))
                throw new ArgumentException(
                    "The ScopeSDK path must be the path to the Scope.exe file");

            if (!File.Exists(scopeSdkPath))
                throw new FileNotFoundException(
                    "The ScopeSDK's Scope.exe file was not found");

            Wrapper = new ScopeExeWrapper(scopeSdkPath, CosmosVC);
            this.sourceTable = sourceTable;
        }

        public static string GetLoggedInUsername()
        {
            return System.Security.Principal.WindowsIdentity
                .GetCurrent()
                .Name
                .Split('\\')[1];
        }

        // Submits a data pull job to Cosmos with params:
        // - inputRegion: The requested region; this is free-form,
        //                but ideally should be one of "KnownRegions".
        // - The start date (inclusive) of data to pull.
        // - The end date (inclusive) of data to pull.
        // - The hardware generation.
        // Returns True iff the job ran successfully.
        public bool TrySubmitDataPull(string inputRegion, DateTime startDate,
            DateTime endDate, string hardwareGeneration, out string resultPath)
        {
            resultPath = null;
            var region = inputRegion.ToLower();
            if (!KnownRegions.Contains(region)
                && !KnownRegions.Contains(region + "-a")
                && !KnownRegions.Contains(region + "-b"))
            {
                Console.Write(
                    $"The '{region}' region is not known. " +
                    $"Do you still want to execute a data pull? [y/N]: ");
                var response = Console.ReadLine().Trim().ToLower();
                if (response != "y" && response != "yes")
                    return false;
            }

            // get the embedded resource
            string scopeTemplate;
            if (sourceTable != null && sourceTable.CompareTo("MonRgLoad") == 0)
                scopeTemplate = "HardwareSimulator.MonRgManagerTrace.script";
            else
                scopeTemplate = "HardwareSimulator.Pull Data Template.script";

            using (Stream stream = Assembly.GetExecutingAssembly()
                .GetManifestResourceStream(scopeTemplate))
            using (StreamReader reader = new StreamReader(stream))
            {
                var username = GetLoggedInUsername();
                var script = reader.ReadToEnd()
                    .Replace("%start_date%", startDate.ToString("yyyy-MM-dd"))
                    .Replace("%end_date%", endDate.ToString("yyyy-MM-dd"))
                    .Replace("%region%", region)
                    .Replace("%hardware_generation%", hardwareGeneration)
                    .Replace("%username%", username);

                var stopWatch = System.Diagnostics.Stopwatch.StartNew();

                var submittedJob = Wrapper.SubmitJob(script,
                    name: $"{region} data pull " +
                        $"{startDate:yyyy-MM-dd} to {endDate:yyyy-MM-dd}",
                    notify: username);

                // Were there any issues submitting?
                if (submittedJob.Status == JobStatus.CompletedFailure)
                {
                    Console.WriteLine("There was an error submitting the job." +
                        " Please see the raw Cosmos response for info:\n\n{0}",
                        submittedJob.jobInfoEntriesLog);
                    return false;
                }

                // the file will be here when done
                var jobPath = $"{CosmosVC}/_Jobs/{submittedJob.JobId.ToString()}";
                resultPath = GetCosmosDataFile(region, startDate, endDate);

                Console.WriteLine("Job submitted in {0} seconds; " +
                    "polling every {1} seconds for the result. " +
                    "You can manually monitor for the job and file at " +
                    "the following locations:\n\n\t{2}\n\n\t{3}?property=info",
                    (int)stopWatch.Elapsed.TotalSeconds, JobPollingSeconds,
                    jobPath, resultPath);

                stopWatch = System.Diagnostics.Stopwatch.StartNew();

                while (true)
                {
                    Thread.Sleep(JobPollingSeconds * 1000);
                    Console.Write(".");

                    var refreshedJob = Wrapper.JobStatus(submittedJob.JobId);
                    switch (refreshedJob.Status)
                    {
                        case JobStatus.Cancelled:
                            Console.WriteLine("\n\tThe job was cancelled.");
                            return false;
                        case JobStatus.CompletedFailure:
                            Console.WriteLine("\n\tThe job failed. " +
                                "See the raw Cosmos response for info:\n\n{0}",
                                refreshedJob.jobInfoEntriesLog);
                            return false;
                        case JobStatus.CompletedSuccess:
                            Console.WriteLine("\n\tThe job succeeded. " +
                                "Approximate time taken was {0} seconds.",
                                (int)stopWatch.Elapsed.TotalSeconds);
                            return true;
                        default:
                            break;
                    }
                }
            }
        }

        public string GetCosmosDataFile(string region, DateTime startDate,
            DateTime endDate)
        {
            return $"{ CosmosVC}/local/users/{GetLoggedInUsername()}/" +
                $"HardwareSimulatorData/RgLoad_Data_{region}_" +
                $"{startDate:yyyyMMdd}-{endDate:yyyyMMdd}.tsv";
        }

        public string GetLocalDataFile(string resultPath)
        {
            // get the folder in which this .exe lives
            var currentDirectory = Directory.GetParent(
                Assembly.GetExecutingAssembly().Location).FullName;

            // get the filename from the full path
            return currentDirectory + "\\" + resultPath.Split('/').Last();
        }

        public bool TryDownloadFile(string resultPath, out string localFile)
        {
            try
            {
                // get info on this Cosmos stream
                localFile = GetLocalDataFile(resultPath);
                Console.WriteLine($"Attempting to " +
                    $"download remote file from Cosmos...");
                var results = Wrapper.CopyFileDown(resultPath, localFile);

                var streamLen = Wrapper.StreamInfo(resultPath)["Length"];
                return long.Parse(streamLen) == new FileInfo(localFile).Length;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                localFile = null;
                return false;
            }
        }
    }
}
