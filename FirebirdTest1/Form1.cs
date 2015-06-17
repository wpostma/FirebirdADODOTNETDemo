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

// Type Equivalences for this unit:
using IColumns = System.Collections.Generic.IEnumerable<System.Data.DataColumn>;

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

        // Helper used when converting DataRow to a JSON.
        private Dictionary<string, Dictionary<string, object>> DataTableToDictionary(DataTable dt, string prefix, string id)
        {
            var cols = dt.Columns.Cast<DataColumn>().Where( c => c.ColumnName != id );
            return dt.Rows.Cast<DataRow>()
                     .ToDictionary(r => prefix+r[id].ToString(),
                                   r => cols.ToDictionary(c => c.ColumnName, c => r[c.ColumnName]));
        }

        private Dictionary<string, object> EraseNulls(Dictionary<string, object> Dict) 
        {  
            Dictionary<string, object> SparseDict = new Dictionary<string,object>();
            foreach(var item in Dict.Keys)
            {
                if (Dict[item] != DBNull.Value)
                    SparseDict[item] = Dict[item];
            }
            return SparseDict;
        }
        
        private Dictionary<string, Dictionary<string, object>> DataTableToSparseDictionary(DataTable dt, string prefix, string id)
        {
            IColumns cols = dt.Columns.Cast<DataColumn>().Where(c => c.ColumnName != id);
            return dt.Rows.Cast<DataRow>()
                     .ToDictionary(r => prefix + r[id].ToString(),
                      r => EraseNulls( cols.ToDictionary(c => c.ColumnName, c => r[c.ColumnName]) ));
                                  // r => SparseDictionary(r,cols) );
            
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

            foreach (var item in dict) 
            {
                item.Value["_DOCUMENT_TYPE"] = "APPOINTMENTLOGENTRY";
                item.Value["_DOCUMENT_REV"]  = "1";
                item.Value["_DOCUMENT_ORIGIN"] = "FBIMPORT";
            };
            

            richTextBox1.Text = JsonConvert.SerializeObject(
                                            dict, 
                                            Newtonsoft.Json.Formatting.Indented);
            
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
            var dict = DataTableToSparseDictionary(StudyAccess, "STUDYACCESS.",  "ENTRYID");

            foreach (var item in dict)
            {
                item.Value["_DOCUMENT_TYPE"] = "STUDYACCESS";
                item.Value["_DOCUMENT_REV"] = "1";
                item.Value["_DOCUMENT_ORIGIN"] = "FBIMPORT";
            };


            richTextBox1.Text = JsonConvert.SerializeObject(
                                            dict,
                                            Newtonsoft.Json.Formatting.Indented);

        }
        private void button1_Click(object sender, EventArgs e)
        {
           // EnumerateDataSetsAndColumns();
           // GetConfigurationItems();

           // RunACustomQueryOnApplicationLogAndConvertToJSON();

            RunACustomQueryOnStudyAccessLogAndConvertToJSON();
        

        }
    }
}
