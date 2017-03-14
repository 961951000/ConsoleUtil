using System;
using System.Collections.Generic;
using System.IO;

namespace ConsoleUtil.Util
{
    public class FileTool
    {
        /// <summary>
        /// 清理路径
        /// </summary>
        /// <param name="path"></param>
        public void DirectoryClear(string path)
        {
            try
            {
                var i = 0;
                var files = GetAll(new DirectoryInfo(path));
                foreach (var file in files)
                {
                    if ((file.Name.Length <= 1 || file.Name.Substring(0, 2) != "~$") && !string.IsNullOrEmpty(file.Extension)) continue;
                    if (file.Attributes.ToString().IndexOf("ReadOnly", StringComparison.Ordinal) > -1)
                    {
                        file.Attributes = file.Attributes & FileAttributes.ReadOnly;
                    }
                    file.Delete();
                    i++;
                    Console.WriteLine(file.FullName);
                }
                Console.WriteLine($"删除文件:    {i}");
            }
            catch (IOException e)
            {
                Console.WriteLine(e.Message);
            }
            Console.ReadLine();
        }
        /// <summary>
        /// 搜索文件夹中的文件
        /// </summary>
        /// <param name="dir"></param>
        /// <returns></returns>
        private IEnumerable<FileInfo> GetAll(DirectoryInfo dir)
        {
            var files = new List<FileInfo>();
            if (IsSystemHidden(dir)) return files;
            files.AddRange(dir.GetFiles());
            var allDir = dir.GetDirectories();
            foreach (var d in allDir)
            {
                files.AddRange(GetAll(d));
            }
            return files;
        }
        /// <summary>
        /// 隐藏文件过滤
        /// </summary>
        /// <param name="dir"></param>
        /// <returns></returns>
        private bool IsSystemHidden(DirectoryInfo dir)
        {
            if (dir.Parent == null)
            {
                return false;
            }
            var attributes = dir.Attributes.ToString();
            return attributes.IndexOf("Hidden", StringComparison.Ordinal) > -1 && attributes.IndexOf("System", StringComparison.Ordinal) > -1;
        }
    }
}
