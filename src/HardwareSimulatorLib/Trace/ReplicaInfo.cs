using System;

namespace HardwareSimulatorLib.Trace
{
    public struct ReplicaInfo
    {
        private readonly static string specialDelimeter = "$";

        // ID is: ReplicaId + '$' + TraceID e.g., "tr1_DB40_d7dbaa5725f8$0".
        public static void ExtractIDs(string ID, out string TenantId,
            out string ReplicaId)
        {
            if (ID.IndexOf(specialDelimeter) != -1)
                ID = ID.Substring(0, ID.IndexOf(specialDelimeter));

            TenantId = ID.Substring(ID.LastIndexOf('_') + 1);
            ReplicaId = ID;
        }

        // Ring + '_' + MachineId + '_' + TenantId e.g., "tr1_DB40_d7dbaa5725f8"
        public static string ExtractReplicaId(string ID)
        {
            if (ID.IndexOf(specialDelimeter) != -1)
                return ID.Substring(0, ID.IndexOf(specialDelimeter));
            return ID;
        }

        // TenantId as found in MonRgManager called AppName e.g., "d7dbaa5725f8"
        public static string ExtractTenantIdWithTrace(string ID)
        {
            return ID.Substring(ID.LastIndexOf('_') + 1);
        }

        public static string ExtractTenantId(string ID)
        {
            if (ID.IndexOf(specialDelimeter) != -1)
                ID = ID.Substring(0, ID.IndexOf(specialDelimeter));

            return ID.Substring(ID.LastIndexOf('_') + 1);
        }
    }
}
