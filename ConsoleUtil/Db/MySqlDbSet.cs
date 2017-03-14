using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;

namespace ConsoleUtil.Db
{
    public class MySqlDbSet<T> : MySqlDb<T>
    {
        private MySqlDbSet()
        {
        }
        public MySqlDbSet(MySqlDb<T> dbContext)
        {
            this.DbContext = dbContext;
            WhereString = string.Empty;
            MySqlParameterList = new List<MySqlParameter>();
        }

        public MySqlDb<T> DbContext
        {
            get;
            set;
        }

        public string WhereString { get; set; }
        public List<MySqlParameter> MySqlParameterList { get; set; }

        /// <summary>
        /// 计数
        /// </summary>
        /// <returns>执行成功返回数量</returns>
        public override long Count()
        {
            string sql = "select count(*) from " + TableName + WhereString;
            using (var conn = new MySqlConnection())
            {
                var cmd = new MySqlCommand(sql, conn);
                cmd.Parameters.AddRange(MySqlParameterList.ToArray());
                cmd.ExecuteNonQuery();
                return Convert.ToInt64(cmd.ExecuteScalar());
            }
        }

        /// <summary>
        /// 查询
        /// </summary>
        /// <returns>结果集</returns>
        public override List<T> ToList()
        {
            var list = new List<T>();
            var sql = "select * from " + TableName + WhereString;
            using (var conn = new MySqlConnection())
            {
                var cmd = new MySqlCommand(sql, conn);
                cmd.Parameters.AddRange(MySqlParameterList.ToArray());
                var dr = cmd.ExecuteReader();
                if (dr.HasRows)
                {
                    while (dr.Read())
                    {
                        T entity = Activator.CreateInstance<T>();
                        foreach (PropertyInfo finfos in Type.GetProperties())
                        {
                            finfos.SetValue(entity,
                                Convert.ChangeType(dr[Dict[finfos.Name]],
                                    (Nullable.GetUnderlyingType(finfos.PropertyType) ?? finfos.PropertyType)), null);
                        }
                        list.Add(entity);
                    }
                }
                return list;
            }
        }
        /// <summary>
        /// 修改
        /// </summary>
        /// <param name="entity">实体类</param>
        /// <returns>执行成功返回数量</returns>
        public int Update(T entity)
        {
            var sql = new StringBuilder();
            var list = new List<MySqlParameter>();
            foreach (var finfos in Type.GetProperties())
            {
                var key = finfos.Name;
                var value = Convert.ToString(Type.GetProperty(key).GetValue(entity, null));
                if (!string.IsNullOrEmpty(value))
                {
                    sql.Append(",").Append(Dict[key]).Append("=@").Append(Dict[key]);
                    list.Add(new MySqlParameter("@" + Dict[key], value));
                }
            }
            if (sql.Length > 0)
            {
                list.AddRange(MySqlParameterList);
                sql.Remove(0, 1).Insert(0, " set ").Insert(0, TableName).Insert(0, "update ").Append(WhereString);
                using (var conn = new MySqlConnection())
                {
                    var tran = conn.BeginTransaction();
                    try
                    {
                        var cmd = new MySqlCommand(sql.ToString(), conn);
                        cmd.Parameters.AddRange(list.ToArray());
                        var result = cmd.ExecuteNonQuery();
                        tran.Commit();
                        return result;
                    }
                    catch (Exception ex)
                    {
                        tran.Rollback();
                        throw ex;
                    }
                }
            }
            return 0;
        }
    }
}
