namespace HardwareSimulatorLib.Config
{
    // This class contains the default configuration parameters for the simulation's input data range.
    // The parameters will be passed-in as a JSON file to the program.
    public class DataRange
    {
        public string Region;
        public string StartDate;
        public string EndDate;
        public string HardwareGeneration;
    }
}
