using System;
using System.Collections.Generic;
using System.Linq;

namespace ScopeExe
{
    public enum JobStatus
    {
        Unknown,
        CompletedSuccess,
        CompletedFailure,
        Running,
        Queued,
        Yielded,
        Cancelled
    };

    public class JobInfo
    {
        public readonly Guid JobId;
        public readonly string JobName;
        public readonly JobStatus Status;
        public readonly DateTime SubmitTime;
        public readonly DateTime StartTime;
        public readonly DateTime EndTime;
        public readonly DateTime UploadStarted;
        public readonly DateTime UploadEnded;
        public readonly string jobInfoEntriesLog;

        internal JobInfo(string jobInfoEntriesLog)
        {
            this.jobInfoEntriesLog = jobInfoEntriesLog;
            var jobInfoEntries = jobInfoEntriesLog
                .Split(new[] { '\r', '\n' },
                    StringSplitOptions.RemoveEmptyEntries)
                .Where(line => line.Contains(": "))
                .Select(line => line.Split(new[] { ": " },
                    StringSplitOptions.None))
                .Select(cols => new KeyValuePair<string, string>(
                    cols[0].Trim(), cols[1]));

            foreach (var jobInfoEntry in jobInfoEntries)
            {
                switch (jobInfoEntry.Key)
                {
                    case "Job ID":
                        JobId = Guid.Parse(jobInfoEntry.Value);
                        break;
                    case "Script submitted, job Id":
                        var guid = jobInfoEntry.Value
                            .Remove(0, 1)
                            .Remove(36, 2);
                        JobId = Guid.Parse(guid);
                        break;
                    case "Job Name":
                        JobName = jobInfoEntry.Value;
                        break;
                    case "Job Status":
                        Status = (JobStatus) Enum.Parse(typeof(JobStatus),
                            jobInfoEntry.Value);
                        break;
                    case "Submit Time":
                        SubmitTime = DateTime.Parse(jobInfoEntry.Value);
                        break;
                    case "Start Time":
                        DateTime d;
                        if (DateTime.TryParse(jobInfoEntry.Value, out d))
                            StartTime = d;
                        break;
                    case "End Time":
                        EndTime = DateTime.Parse(jobInfoEntry.Value);
                        break;
                    case "Upload started at":
                        UploadStarted = DateTime.Parse(jobInfoEntry.Value);
                        break;
                    case "Upload ended at":
                        UploadEnded = DateTime.Parse(jobInfoEntry.Value);
                        break;
                    default:
                        break;
                }
            }
        }
    }
}
