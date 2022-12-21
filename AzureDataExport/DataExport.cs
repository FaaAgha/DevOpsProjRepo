using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Extensions.Logging;
using Microsoft.Data.SqlClient;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using System.Text;
using System.IO;
using Newtonsoft.Json;
using System.Data;
using System.Linq;
using Renci.SshNet;

namespace narrativewave_fa
{
    public class SFTPQueueType
    {
        public string Host { get; set; }
        public int Port { get; set; }
        public string User { get; set; }
        public string Password { get; set; }
        public string Filename { get; set; }
        public string Content { get; set; }
    }

    public static class HelperClass
    {
        public static DateTime Trim(this DateTime date, long roundTicks)
        {
            return new DateTime(date.Ticks - date.Ticks % roundTicks, date.Kind);
        }

        public static async Task<string> CreateCSV(SqlDataReader dataReader)
        {
            // Build CSV
            StringBuilder sb = new StringBuilder();

            //Get All column 
            var columnNames = Enumerable.Range(0, dataReader.FieldCount)
                                       .Select(dataReader.GetName) //OR .Select("\""+  reader.GetName"\"") 
                                       .ToList();

            //Create headers
            sb.Append(string.Join(",", columnNames));

            //Append Line
            sb.AppendLine();

            while (await dataReader.ReadAsync())
            {
                for (int i = 0; i < dataReader.FieldCount; i++)
                {
                    string value = dataReader[i].ToString();
                    if (value.Contains(","))
                        value = "\"" + value + "\"";

                    sb.Append(value.Replace(Environment.NewLine, " ") + ",");
                }
                sb.Length--; // Remove the last comma
                sb.AppendLine();
            }

            return sb.ToString();
        }
    }

    public static class NWFaultResponseExport_RhylFlats
    {
        [FunctionName("NWFaultResponseExport_RhylFlats")]
        [FixedDelayRetry(5, "00:05:00")]
        public static async Task Run([TimerTrigger("0 5 */1 * * *")] TimerInfo myTimer,
            ILogger log,
            Binder binder,
            ExecutionContext context)
        {
            try
            {
                // Get function configuration
                var config = new ConfigurationBuilder()
                    .SetBasePath(context.FunctionAppDirectory)
                    .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                    .AddEnvironmentVariables()
                    .Build();

                // Check config
                if (config.GetConnectionString("SQLFCPROD") == null) throw new Exception("SQLFCPROD connection string missing in application settings!");

                // Get SQL connection string
                var connStr = config.GetConnectionString("SQLFCPROD");

                string csvstr;
                DateTime from = DateTime.UtcNow.Trim(TimeSpan.TicksPerHour).AddHours(-1);
                DateTime to = DateTime.UtcNow.Trim(TimeSpan.TicksPerHour);

                // Log
                log.LogInformation($"NWFaultResponseExport from: {from} to: {to}");

                using (SqlConnection conn = new SqlConnection(connStr))
                {
                    using (SqlCommand cmd = new SqlCommand("dbo.sp_NarrativeWaveAlarmsExport_LogSiemenstblAlarmLog", conn))
                    {
                        // Configure stored procedure command
                        cmd.CommandType = System.Data.CommandType.StoredProcedure;

                        cmd.Parameters.Add("@siteIdi", System.Data.SqlDbType.Int).Value = 237;
                        cmd.Parameters.Add("@from", System.Data.SqlDbType.SmallDateTime).Value = from;
                        cmd.Parameters.Add("@to", System.Data.SqlDbType.SmallDateTime).Value = to;

                        // Open connection and read data
                        conn.Open();
                        SqlDataReader dataReader = await cmd.ExecuteReaderAsync();

                        // Check, if rows are available else return
                        if (dataReader.HasRows) csvstr = await HelperClass.CreateCSV(dataReader);
                        else
                        {
                            // Log
                            log.LogInformation($"NWFaultResponseExport nothing to export at: {DateTime.Now}");

                            return;
                        }
                    }
                }

                // Get block blob refeernce
                var filename = "faults_rhylflats_" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".csv";
                var cloudBlockBlob = await binder.BindAsync<CloudBlockBlob>(new BlobAttribute("testsa4100rhk/data-exchange/" + filename, FileAccess.Write));

                // Upload block blob
                await cloudBlockBlob.UploadFromStreamAsync(new MemoryStream(Encoding.UTF8.GetBytes(csvstr)));

                // Log
                log.LogInformation($"NWFaultResponseExport function exported {filename} at: {DateTime.Now}");
            }
            catch (Exception e)
            {
                // Log
                log.LogInformation($"NWFaultResponseExport function failed at: {DateTime.Now}");

                throw;
            }
        }
    }
}
