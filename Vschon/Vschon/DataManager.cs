using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Data.SqlClient;
using System.Data;
using System.IO;

namespace Vschon
{
    class DataManager
    {
        /// <summary>
        /// 执行CFFEX_IF_Quote数据库的存储过程pr_bulk_Insert_IF_Quote_From_KenSuo_CTP
        /// 批量插入肯索IF Quote数据，数据格式CTP
        /// </summary>
        /// <param name="csvAddr">string, csv地址名</param>
        /// <param name="tableName">string, 插入目标表名，6位secID,例如IF1005</param>
        public static void Exec_pr_bulk_Insert_IF_Quote_From_KenSuo_CTP(string csvAddr, string tableName)
        {   
            string source = "server = (local); database = CFFEX_IF_Quote;uid=sa;pwd=871108";
            SqlConnection conn = new SqlConnection(source);
            conn.Open();
            SqlCommand cmd = new SqlCommand("pr_bulk_Insert_IF_Quote_From_KenSuo_CTP", conn);
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Parameters.AddWithValue("@csvAddr", csvAddr);
            cmd.Parameters.AddWithValue("@tableName", tableName);
            cmd.ExecuteNonQuery();
            conn.Close();
        }

        /// <summary>
        /// 执行CFFEX_IF_Quote数据库的存储过程pr_bulk_Insert_IF_Quote_From_KenSuo_NonCTP
        /// 批量插入肯索IF Quote数据，数据格式非CTP，2015年2月以后的数据采用此格式
        /// </summary>
        /// <param name="csvAddr"></param>
        /// <param name="tableName"></param>
        public static void Exec_pr_bulk_Insert_IF_Quote_From_KenSuo_NonCTP(string csvAddr, string tableName)
        {

            string source = "server = (local); database = CFFEX_IF_Quote;uid=sa;pwd=871108";
            SqlConnection conn = new SqlConnection(source);
            conn.Open();
            SqlCommand cmd = new SqlCommand("pr_bulk_Insert_IF_Quote_From_KenSuo_NonCTP", conn);
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Parameters.AddWithValue("@csvAddr", csvAddr);
            cmd.Parameters.AddWithValue("@tableName", tableName);
            cmd.ExecuteNonQuery();
            conn.Close();
        }

        public static void batchExec_pr_bulk_Insert_IF_Quote_From_KenSuo_NonCTP()
        {
            /*
             * 一次性导入数据2015年2月2日-13日的肯索IF Quote数据
             */
            string csvAddr;
            string tableName;

            string[] list = { "2", "3", "6", "9" };
            foreach (string exp in list)
            {
                 tableName = String.Concat("IF150",exp);
                Console.WriteLine(tableName);
                for (int i = 1; i <= 13; i++)
                {
                    //string
                    csvAddr = String.Concat(@"C:\database\raw\CFFEX_IF_Quote\Kensuo_Daily\", "IF1509_201502", i.ToString("d2"), ".csv");
                    Console.WriteLine("{0}   {1}",csvAddr,File.Exists(csvAddr));
                    if (File.Exists(csvAddr)) {
                        Exec_pr_bulk_Insert_IF_Quote_From_KenSuo_NonCTP(csvAddr, tableName);
                    }
                }
            }
        }

        /// <summary>
        /// 返回指定secID的IF合约的到期交割日
        /// </summary>
        /// <param name="secID">string,6位合约代码，如IF1005</param>
        /// <returns>DateTime, 交割日</returns>
        public static DateTime getSettleDate(string secID)
        {
            DateTime result;
            string source = "server = (local); database = CFFEX_IF_Quote;uid=sa;pwd=871108";
            SqlConnection conn = new SqlConnection(source);
            conn.Open();
            string sql = String.Concat(@"select SettleDate from IF_Settle_Date where SecID = '",secID,"'") ;
            SqlCommand cmd = new SqlCommand(sql,conn);
            object temp = cmd.ExecuteScalar();
            
            //如果temp is null，那么将会被转化为0001年，调用时注意判断
            result = Convert.ToDateTime(temp);

            return result;
        }

        /// <summary>
        /// 返回指定日期的四个活跃IF合约代码
        /// </summary>
        /// <param name="t">DateTime,指定日期</param>
        /// <returns>string array，四个活跃合约代码</returns>
        public static string[] getActiveContracts(DateTime t)
        {
            string source = "server = (local); database = CFFEX_IF_Quote;uid=sa;pwd=871108";
            SqlConnection conn = new SqlConnection(source);
            conn.Open();
            string sql = String.Concat(@"select secID from IF_Settle_Date where SettleDate >= '", t.ToString("yyyy-MM-dd"), "' order by SettleDate");
            SqlCommand cmd = new SqlCommand(sql, conn);
            string secID = (string)cmd.ExecuteScalar();
            string currentDateStr = secID.Substring(2, 4);

            DateTime currentMon = DateTime.ParseExact(currentDateStr, "yyMM",null);
            DateTime nextMon = currentMon.AddMonths(1);
            DateTime nextQtr  = new DateTime();
            DateTime next2Qtr = new DateTime();

            if (nextMon.Month >= 1 && nextMon.Month <= 2)
            {
                nextQtr = new DateTime(nextMon.Year, 3, 1);
                next2Qtr = new DateTime(nextMon.Year, 6, 1);
            }
            else if (nextMon.Month >= 3 && nextMon.Month <= 5)
            {
                nextQtr = new DateTime(nextMon.Year, 6, 1);
                next2Qtr = new DateTime(nextMon.Year, 9, 1);
            }
            else if (nextMon.Month >= 6 && nextMon.Month <= 8)
            {
                nextQtr = new DateTime(nextMon.Year, 9, 1);
                next2Qtr = new DateTime(nextMon.Year, 12, 1);
            }
            else if (nextMon.Month >= 9 && nextMon.Month <= 11)
            {
                nextQtr = new DateTime(nextMon.Year, 12, 1);
                next2Qtr = new DateTime(nextMon.Year+1, 3, 1);
            }
            else if (nextMon.Month == 12)
            {
                nextQtr = new DateTime(nextMon.Year+1, 3, 1);
                next2Qtr = new DateTime(nextMon.Year + 1, 6, 1);
            }

            string nextMonStr = string.Concat("IF",nextMon.ToString("yyMM"));
            string nextQtrStr = string.Concat("IF", nextQtr.ToString("yyMM"));
            string next2QtrStr = string.Concat("IF", next2Qtr.ToString("yyMM"));
            conn.Close();
            string[] result = {secID, nextMonStr, nextQtrStr, next2QtrStr};
            return result;
        }


        public static void iterateIFQuoteTable()
        {
            string source = "server = (local); database = CFFEX_IF_Quote;uid=sa;pwd=871108";
            SqlConnection conn = new SqlConnection(source);
            conn.Open();
            SqlCommand cmd = new SqlCommand("select * from IF1005", conn);
            SqlDataReader reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                //Console.WriteLine("{0} {1} {2} {3}", reader[0], reader[1], reader[2], reader[3]);
            }
            reader.Close();
            conn.Close();
        }
    }
}
