using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

using Newtonsoft.Json;



//############################################### Type Equivalences, local for this unit ########################################
using JsonDictionary = System.Collections.Generic.Dictionary<string, object>;
using JsonTableDictionary = System.Collections.Generic.Dictionary<string, System.Collections.Generic.Dictionary<string, object> >;
using JsonDictionaryItem = System.Collections.Generic.KeyValuePair<string,System.Collections.Generic.Dictionary<string,object>>;
// using IColumns = System.Collections.Generic.IEnumerable<System.Data.DataColumn>;
//#################################################################################################################################

namespace FirebirdTest1
{
    public partial class Form1 : Form
    {
        
        public Form1()
        {
            InitializeComponent();
        
        }

        public void EnumerateDataSetsAndColumns() 
        {
            var LoggingAndConfigDataSet = new LoggingAndConfig();
            var sb = new StringBuilder();
            foreach (DataTable table in LoggingAndConfigDataSet.Tables)
            {   sb.AppendLine( "Table:" + table.TableName);
            int ColIdx = 0;
                foreach (DataColumn column in table.Columns)
                {
                    sb.Append("   "+column.ColumnName + ':' + column.DataType + ' ');
                    ColIdx ++;
                    if  (ColIdx >= 3) {
                        sb.AppendLine(" ");
                        ColIdx = 0;
                    }

                };
                sb.AppendLine("");
                sb.AppendLine("");
            }
            richTextBox1.Text = sb.ToString();
        }

        public void GetConfigurationItems()
        {
            var Config = new LoggingAndConfig.CONFIGDataTable();
            var ConfigAd = new LoggingAndConfigTableAdapters.CONFIGTableAdapter();

            ConfigAd.Fill(Config);
            var sb = new StringBuilder();
            int lines = 0;
            foreach (var row in Config)
            {

                string strValue;
                if (row.IsSTRVALUENull())
                {
                    if (row.IsINTVALUENull())
                    {
                        if (row.IsBOOLVALUENull())
                        {
                            if (row.IsBLOBSTRINGNull())
                            {

                                strValue = "???";
                            }
                            else
                                strValue = row.BLOBSTRING;

                        }
                        else
                            strValue = row.BOOLVALUE.ToString();

                    }
                    else
                        strValue = row.INTVALUE.ToString();
                }
                else
                {
                    strValue = row.STRVALUE;
                    if (strValue == "")
                    {
                        strValue = "<empty-string>";
                    }
                }
                sb.AppendLine(row.USERNAME + ":: " + row.FIRSTCAT + '.' + row.SECONDCAT + '.' + row.ITEM + " = " + strValue);
                lines++;

                if (lines > 300)
                {
                    break;
                }


            }
            richTextBox1.Text = sb.ToString();
        }

        // Helper used when converting DataRow to a Dictionary, which we then can convert to JSON, used if you want to include nulls.
        private JsonTableDictionary DataTableToDictionary(DataTable dt, string prefix, string id)
        {
            var cols = dt.Columns.Cast<DataColumn>().Where( c => c.ColumnName != id );
            return dt.Rows.Cast<DataRow>()
                     .ToDictionary(r => prefix+r[id].ToString(),
                                   r => cols.ToDictionary(c => c.ColumnName, c => r[c.ColumnName]));
        }


        // Helper used when converting DataRow to a Dictionary, which we then can convert to JSON, used if you want to exclude nulls.
        private JsonTableDictionary DataTableToSparseDictionary(DataTable dt, string prefix, string id)
        {
            var cols = dt.Columns.Cast<DataColumn>().Where(c => c.ColumnName != id);
            return dt.Rows.Cast<DataRow>()
                     .ToDictionary(r => prefix + r[id].ToString(),
                                   r => cols.Where(c => !Convert.IsDBNull(r[c.ColumnName])).ToDictionary
                                       (
                                                c => c.ColumnName, c => r[c.ColumnName]
                                       )
                                  );
        }


        public void RunACustomQueryOnApplicationLogAndConvertToJSON()
        {

            //var APPOINTMENTLOG = new LoggingAndConfig.APPOINTMENTLOGDataTable(); // <-- This is to get ALL ROWS.
            // But to only get certain rows, we use GetDataBy[SomeCustomQueryHere]

            var apptAd = new LoggingAndConfigTableAdapters.APPOINTMENTLOGTableAdapter();
            var table = apptAd.GetDataByStudyId(618);
            //var sb = new StringBuilder();

            // here's our key: Transform the rows to documents, with a prefix, and a primary key value.
            // The prefix is how we know the TYPE of the document.
            var dict = DataTableToDictionary(table, "APPOINTMENTLOGENTRY.", "ENTRYID");

            foreach (JsonDictionaryItem item in dict) 
            {
                item.Value["_DOCUMENT_TYPE"] = "APPOINTMENTLOGENTRY";
                item.Value["_DOCUMENT_REV"]  = "1";
                item.Value["_DOCUMENT_ORIGIN"] = "FBIMPORT";
            };

            var jsonSerializerSettings = new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore };

            

            richTextBox1.Text = JsonConvert.SerializeObject(
                                            dict, 
                                            Newtonsoft.Json.Formatting.Indented,
                                            jsonSerializerSettings);
            // Newtonsoft.Json.Formatting.Indented 
            
        }

        public void RunACustomQueryOnStudyAccessLogAndConvertToJSON()
        {

            //var StudyAccess = new LoggingAndConfig.STUDYACCESSDataTable(); 
            //var StudyAccessAd = new LoggingAndConfigTableAdapters.STUDYACCESSTableAdapter();
            //StudyAccessAd.Fill(StudyAccess);
            var studyAd = new LoggingAndConfigTableAdapters.STUDYACCESSTableAdapter();
            var startDate = System.DateTime.Now.AddDays(-7);
            var endDate = System.DateTime.Now.AddDays(7);

            var StudyAccess = studyAd.GetDataByAccessTimeRange( startDate, endDate);


            // here's our key: Transform the rows to documents, with a prefix, and a primary key value.
            // The prefix is how we know the TYPE of the document.

            // DataTableToSparseDictionary -> Do not include nulls.
            // DataTableToDictionary -> Include nulls.
            var dict = DataTableToSparseDictionary(StudyAccess, "STUDYACCESS.",  "ENTRYID");

            var writer = new CouchbaseExportWriter("http://couchbase1.ramsoft.biz:8091/pools");

            writer.openBucket("default");

            foreach (var item in dict)
            {
                item.Value["_DOCUMENT_TYPE"] = "STUDYACCESS";
                item.Value["_DOCUMENT_REV"] = "1";
                item.Value["_DOCUMENT_ORIGIN"] = "FBIMPORT";

                writer.upsert(item.Key, item.Value);

            };
            var jsonSerializerSettings = new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore };


            richTextBox1.Text = JsonConvert.SerializeObject(
                                            dict,
                                            Newtonsoft.Json.Formatting.Indented,
                                            jsonSerializerSettings);

            




        }
        private void button1_Click(object sender, EventArgs e)
        {
           // DEMO1
           // EnumerateDataSetsAndColumns();

           // DEMO2
           // GetConfigurationItems();

           // DEMO3
           // RunACustomQueryOnApplicationLogAndConvertToJSON();

           // DEMO4
           RunACustomQueryOnStudyAccessLogAndConvertToJSON();
        }
    }
}
