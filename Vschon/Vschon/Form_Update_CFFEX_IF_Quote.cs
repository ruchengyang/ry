using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

using System.IO;

namespace Vschon
{
    public partial class Form_Update_CFFEX_IF_Quote : Form
    {
        private string addr1, addr2, addr3, addr4;
        private string currentMon, nextMon, nextQtr, next2Qtr;
        public Form_Update_CFFEX_IF_Quote()
        {
            InitializeComponent();
        }

        private void dateTimePicker1_ValueChanged(object sender, EventArgs e)
        {
            string[] contracts = DataManager.getActiveContracts(dateTimePicker1.Value);
            currentMon = contracts[0];
            nextMon = contracts[1];
            nextQtr = contracts[2];
            next2Qtr = contracts[3];
            checkBox1.Text = currentMon;
            checkBox2.Text = nextMon;
            checkBox3.Text = nextQtr;
            checkBox4.Text = next2Qtr;
            checkBox1.Checked = true;
            checkBox2.Checked = true;
            checkBox3.Checked = true;
            checkBox4.Checked = true;
            label10.Text = "No Data";
            label11.Text = "No Data";
            label12.Text = "No Data";
            label13.Text = "No Data";

            //检测csv数据是否存在
            string yyyyMM = dateTimePicker1.Value.ToString("yyyyMM");
            string yyyyMMdd = dateTimePicker1.Value.ToString("yyyyMMdd");
            
            string[] addrArray1 = {@"C:\database\raw\CFFEX_IF_Quote\Kensuo\IF_",yyyyMM,@"\",contracts[0],"_",yyyyMMdd,".csv"};
            addr1 = string.Concat(addrArray1);
            label6.Text = addr1;
            if (File.Exists(addr1)) {
                label17.Text = "发现源文件";
            }
            else
            {
                label17.Text = "未检测到源文件 ";
            }

            string[] addrArray2 = { @"C:\database\raw\CFFEX_IF_Quote\Kensuo\IF_", yyyyMM, @"\", contracts[1], "_", yyyyMMdd, ".csv" };
            addr2 = string.Concat(addrArray2);
            label7.Text = addr2;
            if (File.Exists(addr2))
            {
                label16.Text = "发现源文件";
            }
            else
            {
                label16.Text = "未检测到源文件";
            }

            string[] addrArray3 = { @"C:\database\raw\CFFEX_IF_Quote\Kensuo\IF_", yyyyMM, @"\", contracts[2], "_", yyyyMMdd, ".csv" };
            addr3 = string.Concat(addrArray3);
            label8.Text = addr3;
            if (File.Exists(addr3))
            {
                label15.Text = "发现源文件";
            }
            else
            {
                label15.Text = "未检测到源文件 ";
            }

            string[] addrArray4 = { @"C:\database\raw\CFFEX_IF_Quote\Kensuo\IF_", yyyyMM, @"\", contracts[3], "_", yyyyMMdd, ".csv" };
            addr4 = string.Concat(addrArray4);
            label9.Text =  addr4;
            if (File.Exists(addr4))
            {
                label14.Text = "发现源文件";
            }
            else
            {
                label14.Text = "未检测到源文件 ";
            }
            button1.Enabled = true;
        }

        private void button1_Click(object sender, EventArgs e)
        {
            button1.Enabled = false;
            if (checkBox1.Checked == true &&  File.Exists(addr1)) {
                DataManager.Exec_pr_bulk_Insert_IF_Quote_From_KenSuo_NonCTP(addr1, currentMon);
                label10.Text = "完成";
            }
            if (checkBox2.Checked == true && File.Exists(addr2))
            {
                DataManager.Exec_pr_bulk_Insert_IF_Quote_From_KenSuo_NonCTP(addr2, nextMon);
                label11.Text = "完成";
            }
            if (checkBox3.Checked == true && File.Exists(addr3))
            {
                DataManager.Exec_pr_bulk_Insert_IF_Quote_From_KenSuo_NonCTP(addr3, nextQtr);
                label12.Text = "完成";
            }
            if (checkBox4.Checked == true && File.Exists(addr4))
            {
                DataManager.Exec_pr_bulk_Insert_IF_Quote_From_KenSuo_NonCTP(addr4, next2Qtr);
                label13.Text = "完成";
            }
            
        }

        private void label17_Click(object sender, EventArgs e)
        {

        }

        private void label16_Click(object sender, EventArgs e)
        {

        }

        private void label15_Click(object sender, EventArgs e)
        {

        }
    }
}
