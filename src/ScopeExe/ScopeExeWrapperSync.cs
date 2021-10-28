using System;
using System.Collections.Generic;
using System.Linq;

namespace ScopeExe
{
    using System.Diagnostics;
    using System.IO;

    public partial class ScopeExeWrapper
    {
        public string CopyFileDown(string cosmosPath, string localFile)
        {
            return Invoke("copy", cosmosPath, localFile, "-overwrite");
        }

        public Dictionary<string, string> StreamInfo(string cosmosPath)
        {
            return Invoke("streaminfo", cosmosPath) /*results*/
                .Split(new[] { '\r', '\n' },
                    StringSplitOptions.RemoveEmptyEntries)
                .Where(line => line.Contains(':'))
                .Select(line => line.Split(new[] { ':' }, 2))
                .ToDictionary(cols => cols[0].Trim(), cols => cols[1].Trim());
        }

        public JobInfo SubmitJob(string scopeText, int priority = 1000,
            string name = null, string notify = null,
            IDictionary<string, object> @params = null)
        {
            // if scopeText is not an existing file,
            // assume it's scope code, write to a temp file and use it.
            if (!File.Exists(scopeText))
            {
                var tmpFile = Path.GetTempFileName();
                using (var sw = new StreamWriter(tmpFile))
                    sw.Write(scopeText);
                scopeText = tmpFile;
            }

            var args = new List<string>
            {
                $"-i \"{scopeText}\"",
                $"-vc {this.VC}",
                $"-p {priority}"
            };

            if (string.IsNullOrWhiteSpace(name))
                name = DateTime.Now.ToShortTimeString();
            args.Add($"-f \"{name}\"");

            if (!string.IsNullOrWhiteSpace(notify))
                args.Add($"-notify {notify}");

            if (@params != null)
            {
                foreach (var param in @params)
                {
                    if (param.Value is string)
                        args.Add($"-params {param.Key}=\\\"{param.Value}\\\"");
                    else
                        args.Add($"-params {param.Key}={param.Value}");
                }
            }
            return new JobInfo(Invoke("submit", args.ToArray()) /*response*/);
        }

        public JobInfo JobStatus(Guid jobId)
        {
            return new JobInfo(Invoke(
                "jobstatus", jobId.ToString(), "-vc", VC));
        }

        private string Invoke(string command, params string[] args)
        {
            var username = System.Security.Principal.WindowsIdentity.
                GetCurrent().Name.Split('\\')[1];
            var allArgs = new[] { command, "-on UseAadAuthentication",
                    $"-u {username}@microsoft.com" }
                .Concat(args)
                .ToArray();

            var proc = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = ScopeExe,
                    // TODO: make sure we escape arguments
                    Arguments = string.Join(" ", allArgs),
                    UseShellExecute = false /*not sure*/,
                    RedirectStandardOutput = true,
                    // capture this so we can read it.
                    CreateNoWindow = true
                }
            };
            proc.Start();
            return proc.StandardOutput.ReadToEnd();
        }
    }
}
