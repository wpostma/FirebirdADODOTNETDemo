using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

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

        private void button1_Click(object sender, EventArgs e)
        {
           // EnumerateDataSetsAndColumns();
            

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
                sb.AppendLine( row.USERNAME +":: "+ row.FIRSTCAT + '.' + row.SECONDCAT + '.' + row.ITEM+ " = " + strValue);
                lines++;

                if (lines > 300) {
                    break;
                }
                

            }
            richTextBox1.Text = sb.ToString();

        }
    }
}
