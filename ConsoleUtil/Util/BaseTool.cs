using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Security.Principal;
using System.Text.RegularExpressions;

namespace ConsoleUtil.Util
{
    /// <summary>
    /// 通用工具类
    /// </summary>
    public class BaseTool
    {
        /// <summary>
        /// 从参数化查询获取完整SQL
        /// </summary>
        /// <param name="cmd">命令对象</param>
        /// <returns>完整SQL</returns>
        public static string GetCommandText(SqlCommand cmd)
        {
            foreach (SqlParameter parameter in cmd.Parameters)
            {
                cmd.CommandText = cmd.CommandText.Replace(parameter.ParameterName, parameter.Value is string ? parameter.Value.ToString() : parameter.ParameterName);
            }
            return cmd.CommandText;
        }
        /// <summary>
        /// 加载Excel
        /// </summary>
        /// <param name="filePos">文件路径</param>
        /// <returns>数据表</returns>
        public static DataTable GetExcel(string filePos)//foreach (DataRow dr in dt.Rows)
        {
            if (string.IsNullOrEmpty(filePos) || !File.Exists(filePos)) return null;
            var extension = Path.GetExtension(filePos);
            string connectionString;
            if (extension.Equals(".xls"))
            {
                connectionString = $"Provider=Microsoft.Jet.OLEDB.4.0;Data Source={filePos};Jet OLEDB:Engine Type=35;Extended Properties=Excel 8.0;Persist Security Info=False";
            }
            else if (extension.Equals(".xlsx"))
            {
                //未在本地计算机上注册"Microsoft.ACE.OLEDB.12.0"  下载安装在  http://download.microsoft.com/download/7/0/3/703ffbcb-dc0c-4e19-b0da-1463960fdcdb/AccessDatabaseEngine.exe
                connectionString = $"Provider=Microsoft.ACE.OLEDB.12.0;Data Source={filePos};Jet OLEDB:Engine Type=35;Extended Properties=Excel 12.0;Persist Security Info=False";
            }
            else
            {
                return null;
            }
            var ds = new DataSet();
            string tableName;
            using (var connection = new OleDbConnection(connectionString))
            {
                connection.Open();
                var table = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, null);
                tableName = table?.Rows[0]["Table_Name"].ToString();
                var strExcel = $"select * from [{tableName}]";
                var adapter = new OleDbDataAdapter(strExcel, connectionString);
                adapter.Fill(ds, tableName);
                connection.Close();
            }
            return ds.Tables[tableName];
        }
        /// <summary>
        /// 提取数据库连接字符串
        /// </summary>
        /// <param name="text">数据库连接字符串</param>
        public static string ExtractConnectionString(string text) => Regex.Match(text, @"database=([^;]+)").Groups[1].Value;
        /// <summary>
        /// 判断文件目录是否合法
        /// </summary>
        /// <param name="path">文件目录</param>
        public static bool IsValidPath(string path)
        {
            var pattern = string.Join("", DriveInfo.GetDrives().Select(t => t.Name.First()).ToList());
            pattern = $@"(?i)^[{pattern}]:(((\\[^ /:*?<>\""|\\]+)+\\?)|(\\)?)\s*$";
            var mt = Regex.Match(path, pattern);
            return mt.Success;
        }
        /// <summary>
        /// 程序是否处于管理员身份运行下
        /// </summary>
        /// <returns></returns>
        public static bool IsAdministrator()
        {
            var identity = WindowsIdentity.GetCurrent();//用户            
            var principal = new WindowsPrincipal(identity);//用户组
            return principal.IsInRole(WindowsBuiltInRole.Administrator);
        }

        /// <summary>
        /// 查询
        /// </summary>
        /// <param name="sql">sql字符串</param>
        /// <returns>数据表</returns>
        public DataTable QueryTable(string sql)
        {
            var adapter = new SqlDataAdapter(sql, new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString));
            var ds = new DataSet();
            adapter.Fill(ds);
            return ds.Tables[0];
        }
        /// <summary>
        /// 提取sql参数
        /// </summary>
        /// <param name="sql">sql字符串</param>
        /// <returns>sql参数</returns>
        public static IEnumerable<string> ExtractSqlParameters(string sql)
        {
            var paramReg = new Regex(@"(?<!@)[^\w$#@]@(?!@)[\w$#@]+");
            var matches = paramReg.Matches(sql);
            foreach (Match m in matches)
            {
                yield return m.Groups[0].Value.Substring(m.Groups[0].Value.IndexOf('@'));
            }
        }
    }
}
