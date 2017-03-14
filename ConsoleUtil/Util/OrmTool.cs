using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;

namespace ConsoleUtil.Util
{
    public class OrmTool<T>
    {
        public static ORM GetOrm()
        {
                var orm = Activator.CreateInstance<ORM>();
                orm.Type = typeof(T);
                orm.Properties = orm.Type.GetProperties();
                orm.Fields = new Dictionary<string, string>();
                var dict = new Dictionary<string, string>();
                foreach (var finfos in orm.Type.GetCustomAttributes(true).OfType<TableAttribute>())
                {
                    if (string.IsNullOrEmpty(orm.TableName.Key))
                    {
                        dict.Add(orm.Type.Name, finfos.Name);
                    }
                }
                if (dict.Count == 0)
                {
                    dict.Add(orm.Type.Name, string.Empty);
                }
                orm.TableName = dict.First();
                dict.Clear();
                foreach (var finfos in orm.Properties)
                {
                    foreach (var colum in finfos.GetCustomAttributes(true).OfType<ColumnAttribute>())
                    {
                        orm.Fields.Add(finfos.Name, colum.Name);
                    }
                }
                foreach (var finfos in orm.Properties)
                {
                    foreach (var colum in finfos.GetCustomAttributes(true).OfType<KeyAttribute>())
                    {
                        dict.Add(finfos.Name, orm.Fields[finfos.Name]);
                    }
                }
                if (dict.Count == 0)
                {
                    dict.Add(string.Empty, string.Empty);
                }
                orm.PrimaryKey = dict.First();
                return orm;
        }
    }
    public class ORM
    {
        /// <summary>
        /// 类型
        /// </summary>
        public Type Type { get; set; }
        /// <summary>
        /// 属性集
        /// </summary>
        public PropertyInfo[] Properties { get; set; }
        /// <summary>
        /// 表名
        /// </summary>
        public KeyValuePair<string, string> TableName { get; set; }
        /// <summary>
        /// 主键
        /// </summary>
        public KeyValuePair<string, string> PrimaryKey { get; set; }
        /// <summary>
        /// 字段
        /// </summary>
        public Dictionary<string, string> Fields { get; set; }
    }
}
