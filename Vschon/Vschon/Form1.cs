using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;



namespace Vschon
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }

        private void Form1_Load(object sender, EventArgs e) {
        }

        private void button1_Click(object sender, EventArgs e)
        {
        }

        private void label1_Click(object sender, EventArgs e)
        {

        }

        private void textBox1_TextChanged(object sender, EventArgs e)
        {

        }

        private void buttonTestForm_Click(object sender, EventArgs e)
        {
            TestForm testForm = new TestForm();
            testForm.Show();
        }

        private void button1_Click_1(object sender, EventArgs e)
        {
            Form_Update_CFFEX_IF_Quote formManualUpdate_CFFEX_IF_Quote = new Form_Update_CFFEX_IF_Quote();
            formManualUpdate_CFFEX_IF_Quote.Show();
        }

    }
}
