using System.IO;

namespace ScopeExe
{
    public partial class ScopeExeWrapper
    {
        // Make sure we encode credentials!
        // > Scope.exe encode redmond\adashi
        private readonly string ScopeExe;
        private string _VC;

        // The VC to target. Trailing slash is always trimmed.
        public string VC
        {
            get { return _VC; }
            set { _VC = value?.TrimEnd('/'); }
        }

        public ScopeExeWrapper(string scopeExePath, string vc)
        {
            if (!File.Exists(scopeExePath))
                throw new FileNotFoundException("Scope.exe not found",
                    scopeExePath);
            ScopeExe = scopeExePath;
            VC = vc;
        }
    }
}
