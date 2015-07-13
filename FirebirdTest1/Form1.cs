
using System;
using System.Configuration;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.Diagnostics;
using Newtonsoft.Json;


using FirebirdSql.Data.FirebirdClient;


//############################################### Type Equivalences, local for this unit ########################################
using JsonDictionary = System.Collections.Generic.Dictionary<string, object>;
using JsonTableDictionary = System.Collections.Generic.Dictionary<string, System.Collections.Generic.Dictionary<string, object> >;
using JsonDictionaryItem = System.Collections.Generic.KeyValuePair<string,System.Collections.Generic.Dictionary<string,object>>;
using System.Data.SqlClient;
// using IColumns = System.Collections.Generic.IEnumerable<System.Data.DataColumn>;
//#################################################################################################################################

namespace FirebirdTest1
{
      [Serializable()]
    public class CommunicationFailureException :  System.ApplicationException
    {
        public CommunicationFailureException() { }
        public CommunicationFailureException(string message) : base(message) { } 
    }

    public partial class Form1 : Form
    {
        
        public Form1()
        {
            InitializeComponent();
        
        }

        /*SqlConnection getConnection()
        {
            // This is the MSSQL connection logic.
            var connection = new SqlConnection(  ConfigurationManager.ConnectionStrings["FirebirdTest1.Properties.Settings.mssql"].ConnectionString);
            connection.Open();
            return connection;
        }
        */
        
        // This is the Firebird connection logic.
        FbConnection getConnection()
        {
            var connectStr = ConfigurationManager.ConnectionStrings["FirebirdTest1.Properties.Settings.pacsdb"].ConnectionString;
            FbConnection connect = new FbConnection(connectStr);
            connect.Open();
            return connect;

        }

         int executeCommand(FbConnection connection, FbTransaction transaction, string commandSql)
         {
             var cmd = new FbCommand(commandSql);
             cmd.Connection = connection;
             cmd.Transaction = transaction;
             return cmd.ExecuteNonQuery();
         }

         FbDataReader executeQuery(FbConnection connection, FbTransaction transaction, string querySql)
         {
             var qry = new FbCommand(querySql);
             qry.Connection = connection;
             qry.Transaction = transaction;
             return qry.ExecuteReader(); // the command generates an FbDataReader.
         }

        FbDataReader executeQueryParam1(FbConnection connection, FbTransaction transaction, string querySql, string param1)
        {
            var qry = new FbCommand(querySql);
            qry.Connection = connection;
            qry.Transaction = transaction;
            qry.Prepare();
            qry.Parameters.AddWithValue( "1", param1);
            return qry.ExecuteReader(); // the command generates an FbDataReader.
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

        // we want json fields to be in lowercase.
        // we want the primary key from firebird to become _fbidentity in the converted record.
        private string Normalize(string value, string id)
        {
            if (value.ToUpper() == id.ToUpper())
                return "_fbidentity";
            else
                return value.ToLower();
        }

        /* 
          private string NormalizeKeyValue(object value, string ColumnName)
          { 
          }
         */

        // Helper used when converting DataRow to a Dictionary, which we then can convert to JSON, used if you want to include nulls.
        private JsonTableDictionary DataTableToDictionary(DataTable dt, string prefix, string id)
        {
            var cols = dt.Columns.Cast<DataColumn>().Where( c => c.ColumnName != id );
            return dt.Rows.Cast<DataRow>()
                     .ToDictionary(r => prefix+r[id].ToString(),
                                   r => cols.ToDictionary(c => Normalize( c.ColumnName, id ), c => r[c.ColumnName]));
        }


        // Helper used when converting DataRow to a Dictionary, which we then can convert to JSON, used if you want to exclude nulls.
        private JsonTableDictionary DataTableToSparseDictionary(DataTable dt, string prefix, string id)
        {
            var cols = dt.Columns.Cast<DataColumn>();// .Where(c => c.ColumnName != id);
            return dt.Rows.Cast<DataRow>()
                     .ToDictionary(r => prefix + r[id].ToString(),
                                   r => cols.Where(c => !Convert.IsDBNull(r[c.ColumnName])).ToDictionary
                                       (
                                                c => Normalize(c.ColumnName,id), c => r[c.ColumnName]
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
                item.Value["_document_type"] = "APPOINTMENTLOGENTRY";
                item.Value["_document_rev"]  = "1";
                item.Value["_document_origin"] = "FBIMPORT";
            };

            var jsonSerializerSettings = new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore };

            

            richTextBox1.Text = JsonConvert.SerializeObject(
                                            dict, 
                                            Newtonsoft.Json.Formatting.Indented,
                                            jsonSerializerSettings);
            // Newtonsoft.Json.Formatting.Indented 
            label1.Text = "count: " + dict.Count.ToString();
        }

        public void RunACustomQueryOnStudyAccessLogAndConvertToJSON()
        {
            richTextBox1.Text = "Please wait...";
            


            //var StudyAccess = new LoggingAndConfig.STUDYACCESSDataTable(); 
            //var StudyAccessAd = new LoggingAndConfigTableAdapters.STUDYACCESSTableAdapter();
            //StudyAccessAd.Fill(StudyAccess);
            var studyAd = new LoggingAndConfigTableAdapters.STUDYACCESSTableAdapter();
            var startDate = System.DateTime.Now.AddDays(-365);
            var endDate = System.DateTime.Now.AddDays(7);

            
            var StudyAccess = studyAd.GetDataByAccessTimeRange( startDate, endDate);


            // here's our key: Transform the rows to documents, with a prefix, and a primary key value.
            // The prefix is how we know the TYPE of the document.

            // DataTableToSparseDictionary -> Do not include nulls.
            // DataTableToDictionary -> Include nulls.
            var dict = DataTableToSparseDictionary(StudyAccess, "STUDYACCESS.",  "ENTRYID");

            var writer = new CouchbaseExportWriter("http://rscouchbase01.ramsoft.com:8091/pools"); // couchbase1.ramsoft.biz

            var partialdict = new JsonTableDictionary();

            writer.openBucket("default");

            label1.Text = "bucket item count= " + writer.getBucketItemCount().ToString();


            int count = 0;

            var sw = new Stopwatch();

            sw.Start();

            // Add some metadata
            foreach (var item in dict)
            {
                item.Value["_document_type"] = "STUDYACCESS";
                item.Value["_document_rev"] = "2";
                item.Value["_document_origin"] = "FBIMPORT";

                writer.upsert(item.Key, item.Value);

                count++;

                if (count<10)
                {
                    partialdict.Add(item.Key, item.Value);
                }

                if (sw.ElapsedMilliseconds > 10000)
                {
                    throw new CommunicationFailureException("Communications are too slow.");
                }

            };

            sw.Stop();

            // Newtonsoft JSON serializer
            var jsonSerializerSettings = new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore };

            // Don't output the whole thing to screen, just a few items (partialdict = up to 10 items from dict)
            richTextBox1.Text = JsonConvert.SerializeObject(
                                            partialdict,
                                            Newtonsoft.Json.Formatting.Indented,
                                            jsonSerializerSettings);




        
            label1.Text = "db upsert count: "+ dict.Count.ToString() + " elapsed:" +sw.ElapsedMilliseconds.ToString()+
                          " ms, bucket item count= "+writer.getBucketItemCount().ToString();


        }

        private void DeleteDemo()
        {
            var connection = getConnection();
            var trans = connection.BeginTransaction();
            int result = executeCommand(connection, trans, "delete from  AMDQUEUE" ); // returns integer result
            trans.Commit();
            connection.Close();

            
        }

        private void JoinDemo()
        {
            // Query Administrator-level users.
            var connection = getConnection();
            var trans = connection.BeginTransaction();
            var query = executeQueryParam1(connection, trans,
                @"-- active administrative and support account names
                select U.USERNAME,R.rolename,U.status 
                from userlist U 
                left join ROLELIST R on R.ROLEID=U.ROLEID
                where
                (u.USERNAME starts with @1 or
                ROLENAME starts with 'SU')
                and STATUS <> 'INACTIVE' ",  "RAM");

            if (query.HasRows)
            {
                var sb = new StringBuilder();
            
                while (query.Read())
                {
                    sb.AppendLine( "USERNAME:"+ query.GetString(0)+ "  ROLENAME:"+query.GetString(1)+ "  STATUS:"+ query.GetString(2) );



                }

                richTextBox1.Text = sb.ToString();
            }

            trans.Commit();

            connection.Close();


        }

        private void button1_Click(object sender, EventArgs e)
        {
           // DEMO1 - Find any table or field within the Dataset.
           // EnumerateDataSetsAndColumns();

           // DEMO2 - Some simple flattening of SQL complexity, configuration items.
           // GetConfigurationItems();

           // DEMO3 - Gets data from firebird and converts to JSON
           //RunACustomQueryOnApplicationLogAndConvertToJSON();

           // DEMO4 - Like demo3, but also writes to couchbase
           RunACustomQueryOnStudyAccessLogAndConvertToJSON();

           // DEMO5 - Do an SQL Join via a query with one parameter (@1)
           // JoinDemo();
        }
    }
}
