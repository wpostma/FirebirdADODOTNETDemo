using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

// This tool is used to take an SQL Query against Firebird and convert result set data 
// to documents and store them into Couchbase.

namespace FirebirdTest1
{
    public partial class MainForm : Form
    {

        private SqlToDocumentDbConversion Conversion;

        public void StatusEvent(object sender, StatusEventArgs args)
        {
            toolStripStatusLabel1.Text = args.Message;
            statusStrip1.Refresh();


        }

        public void SampleOutputEvent(object sender, StatusEventArgs args)
        {
            textBox1.AppendText(  args.Message );
            textBox1.Refresh();
        }


        public MainForm()
        {
            InitializeComponent();

            // Create worker class and wire up events
            Conversion = new SqlToDocumentDbConversion();

            
            Conversion.StatusEvent += new EventHandler<StatusEventArgs>(StatusEvent);
            Conversion.SampleOutputEvent += new EventHandler<StatusEventArgs>(SampleOutputEvent);

        }

        private void button1_Click(object sender, EventArgs e)
        {


            if (textBoxUrl.Text == "")
            {
                Conversion.ServerUrl = "http://couchbase1.ramsoft.biz:8091/pools";
            }
            else
            {
                Conversion.ServerUrl = textBoxUrl.Text;
            }

            toolStripStatusLabel1.Text = "Please wait...";
            toolStripStatusLabel1.BackColor = Color.Beige;
            statusStrip1.Refresh(); 

            Conversion.ConvertStudyAccessData(); // run the Study Access Log Data conversion.
            Conversion.ConvertRetrieveStudyLogData();
            Conversion.ConvertRetrieveRequestLogData();



            statusStrip1.Refresh();

        }
    }
}
