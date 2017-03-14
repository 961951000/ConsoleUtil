using System;
using System.Collections.Generic;
using System.Linq;
using System.ComponentModel.DataAnnotations.Schema;
using System.Reflection;
using System.Text;
using MySql.Data.MySqlClient;

namespace ConsoleUtil.Db
{
    public class MySqlDb<T>
    {
        public MySqlDb()
        {
            Type = typeof(T);
            foreach (var table in Type.GetCustomAttributes(true).OfType<TableAttribute>())
            {
                TableName = table.Name;
            }
            Dict = new Dictionary<string, string>();
            foreach (var finfos in Type.GetProperties())
            {
                foreach (var colum in finfos.GetCustomAttributes().OfType<ColumnAttribute>())
                {
                    Dict.Add(finfos.Name, colum.Name);
                }
            }
        }
        protected Type Type { get; set; }
        protected string TableName { get; set; }
        protected Dictionary<string, string> Dict { get; set; }

        /// <summary>
        /// 筛选条件
        /// </summary>
        /// <param name="conditions">条件</param>
        public MySqlDbSet<T> Where(params string[] conditions)
        {
            var set = new MySqlDbSet<T>(this);
            var whereStringBuilder = new StringBuilder();
            foreach (var kv in Dict)
            {
                foreach (string condition in conditions)
                {
                    var arr = condition.Split('=', '<', '>');
                    arr = arr.Where(x => !string.IsNullOrEmpty(x)).ToArray();
                    if (arr[0] == kv.Key)
                    {
                        string str = condition.Remove(condition.LastIndexOf(arr[1])).Remove(0, kv.Key.Length);
                        string key = kv.Value;
                        string value = arr[1].Trim();
                        whereStringBuilder.Append(" and ").Append(key).Append(str).Append("?").Append(key);
                        set.MySqlParameterList.Add(new MySqlParameter("?" + key, value));
                    }
                }
            }
            if (whereStringBuilder.Length > 0)
            {
                whereStringBuilder.Remove(0, 4).Insert(0, " where ");
                set.WhereString = whereStringBuilder.ToString();
            }
            return set;
        }

        /// <summary>
        /// 筛选条件
        /// </summary>
        /// <param name="entity">条件</param>
        public MySqlDbSet<T> Where(T entity)
        {
            var set = new MySqlDbSet<T>(this);
            var whereStringBuilder = new StringBuilder();
            foreach (var finfos in Type.GetProperties())
            {
                var key = finfos.Name;
                var value = Convert.ToString(Type.GetProperty(key).GetValue(entity, null));
                if (!string.IsNullOrEmpty(value))
                {
                    string columName = Dict[key];
                    whereStringBuilder.Append(" and ").Append(columName).Append("=?").Append(columName);
                    set.MySqlParameterList.Add(new MySqlParameter("?" + columName, value));
                }
            }
            if (whereStringBuilder.Length > 0)
            {
                whereStringBuilder.Remove(0, 4).Insert(0, " where ");
                set.WhereString = whereStringBuilder.ToString();
            }
            return set;
        }

        /// <summary>
        /// 计数
        /// </summary>
        /// <returns>执行成功返回数量</returns>
        public virtual long Count()
        {
            string sql = "select count(*) from " + TableName;
            using (MySqlConnection conn = new MySqlConnection())
            {
                return Convert.ToInt64(new MySqlCommand(sql, conn).ExecuteScalar());
            }
        }

        /// <summary>
        /// 添加
        /// </summary>
        /// <param name="eneiey">实体</param>
        /// <returns>执行成功返回数量</returns>
        public int Add(T eneiey)
        {
            var sql = new StringBuilder();
            var keys = new StringBuilder();
            var values = new StringBuilder();
            var list = new List<MySqlParameter>();
            foreach (var finfos in Type.GetProperties())
            {
                foreach (var column in finfos.GetCustomAttributes(true).OfType<ColumnAttribute>())
                {
                    var value = Convert.ToString(Type.GetProperty(finfos.Name).GetValue(eneiey, null));
                    if (!string.IsNullOrEmpty(value))
                    {
                        string columnName = column.Name;
                        keys.Append(columnName).Append(",");
                        values.Append("@").Append(columnName).Append(",");
                        list.Add(new MySqlParameter("@" + columnName, value));
                    }
                }
            }
            keys.Remove(keys.Length - 1, 1);
            values.Remove(values.Length - 1, 1);
            sql.Append("insert ignore into ")
                .Append(TableName)
                .Append("(")
                .Append(keys)
                .Append(") values(")
                .Append(values)
                .Append(");");
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

        /// <summary>
        /// 批量添加
        /// </summary>
        /// <param name="entities">实体集合</param>
        /// <returns>执行成功返回数量</returns>
        public int AddRange(List<T> entities)
        {

            using (var conn = new MySqlConnection())
            {
                var tran = conn.BeginTransaction();
                try
                {
                    var result = 0;
                    foreach (var entity in entities)
                    {
                        var sql = new StringBuilder();
                        var keys = new StringBuilder();
                        var values = new StringBuilder();
                        var list = new List<MySqlParameter>();
                        foreach (var finfos in Type.GetProperties())
                        {
                            foreach (var column in finfos.GetCustomAttributes().OfType<ColumnAttribute>())
                            {
                                var value = Convert.ToString(Type.GetProperty(finfos.Name).GetValue(entity, null));
                                if (!string.IsNullOrEmpty(value))
                                {
                                    string columNmae = column.Name;
                                    keys.Append(columNmae).Append(",");
                                    values.Append("@").Append(columNmae).Append(",");
                                    list.Add(new MySqlParameter("@" + columNmae, value));
                                }
                            }
                        }
                        if (keys.Length > 0 && values.Length > 0)
                        {
                            keys.Remove(keys.Length - 1, 1);
                            values.Remove(values.Length - 1, 1);
                            sql.Append("insert ignore into ")
                                .Append(TableName)
                                .Append("(")
                                .Append(keys)
                                .Append(") values(")
                                .Append(values)
                                .Append(");");
                            keys = new StringBuilder();
                            values = new StringBuilder();

                        }

                        var cmd = new MySqlCommand(sql.ToString(), conn);
                        cmd.Parameters.AddRange(list.ToArray());
                        result += cmd.ExecuteNonQuery();
                    }
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

        /// <summary>
        /// 查询
        /// </summary>
        /// <returns>结果集</returns>
        public virtual List<T> ToList()
        {
            var list = new List<T>();
            var sql = "select * from " + TableName;
            using (var conn = new MySqlConnection())
            {
                var dr = new MySqlCommand(sql, conn).ExecuteReader();
                if (dr.HasRows)
                {
                    while (dr.Read())
                    {
                        T entity = Activator.CreateInstance<T>();
                        foreach (var finfos in Type.GetProperties())
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

        public int Update(T entity, Func<T, T> func)
        {
            var conditions = func(entity);
            var sql = new StringBuilder();
            var whereStringBuilder = new StringBuilder();
            var list = new List<MySqlParameter>();
            foreach (var finfos in Type.GetProperties())
            {
                var key = finfos.Name;
                var value = Convert.ToString(Type.GetProperty(key).GetValue(entity, null));
                var condition = Convert.ToString(Type.GetProperty(key).GetValue(conditions, null));
                if (!string.IsNullOrEmpty(value))
                {
                    sql.Append(",").Append(Dict[key]).Append("=@").Append(Dict[key]);
                    list.Add(new MySqlParameter("@" + Dict[key], value));
                }
                if (!string.IsNullOrEmpty(condition))
                {
                    var whereKey = Dict[key];
                    var whereValue = condition;
                    whereStringBuilder.Append(" and ").Append(whereKey).Append("=?").Append(whereKey);
                    list.Add(new MySqlParameter("?" + whereKey, whereValue));
                }
            }
            if (sql.Length > 0)
            {
                sql.Remove(0, 1).Insert(0, " set ").Insert(0, TableName).Insert(0, "update ");
                if (whereStringBuilder.Length > 0)
                {
                    whereStringBuilder.Remove(0, 4).Insert(0, " where ");
                    sql.Append(whereStringBuilder);
                }
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
