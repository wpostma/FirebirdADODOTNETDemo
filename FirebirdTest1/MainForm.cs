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
    public partial class MainForm : Form
    {

        private SqlToDocumentDbConversion Conversion;

        public void StatusEvent(object sender, StatusEventArgs args)
        {
            toolStripStatusLabel1.Text = args.Message;


        }

        public void SampleOutputEvent(object sender, StatusEventArgs args)
        {
            textBox1.Text = args.Message;
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
            toolStripStatusLabel1.Text = "Please wait...";
            Conversion.DoConvert(); // run the conversion.
        }
    }
}
