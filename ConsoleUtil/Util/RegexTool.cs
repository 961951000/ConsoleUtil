using System.IO;
using System.Linq;
using static System.Text.RegularExpressions.Regex;

namespace ConsoleUtil.Util
{
    public class RegexTool
    {
        /// <summary>
        /// 验证用户名和密码
        /// </summary>
        /// <param name="str">字符串</param>
        /// <returns>验证结果</returns>
        public static bool IsUsernameOrPassword(string str)
        {
            return IsMatch(str, @"^[a-zA-Z]\w{5,15}$");//正确格式："[A-Z][a-z]_[0-9]"组成,并且第一个字必须为字母6~16位
        }
        /// <summary>
        /// 验证电话号码
        /// </summary>
        /// <param name="str">字符串</param>
        /// <returns>验证结果</returns>
        public static bool IsTalk(string str)
        {
            return IsMatch(str, @"^(\d{3.4}-)\d{7,8}$");//正确格式：xxx/xxxx-xxxxxxx/xxxxxxxx；
        }
        /// <summary>
        /// 验证座机号码
        /// </summary>
        /// <param name="str">字符串</param>
        /// <returns>验证结果</returns>
        public static bool IsTelephone(string str)
        {
            return IsMatch(str, @"^(\d{3,4}-)?\d{6,8}$");//正确格式：xxx/xxxx-xxxxxx/xxxxxxxx；
        }
        /// <summary>
        /// 验证身份证号
        /// </summary>
        /// <param name="str">字符串</param>
        /// <returns>验证结果</returns>
        public static bool IsIdCard(string str)
        {
            return IsMatch(str, @"^\d{15}|\d{18}$");//15位或18位数字
        }
        /// <summary>
        /// 验证Email地址
        /// </summary>
        /// <param name="str">字符串</param>
        /// <returns>验证结果</returns>
        public static bool IsEmail(string str)
        {
            return IsMatch(str, @"^\w + ([-+.]\w +) *@\w + ([-.]\w +)*\.\w + ([-.]\w +)*$");
        }
        /// <summary>
        /// 只能输入由数字和26个英文字母组成的字符串
        /// </summary>
        /// <param name="str">字符串</param>
        /// <returns>验证结果</returns>
        public static bool IsNumberOrLetter(string str)
        {
            return IsMatch(str, @"^[A-Za-z0-9]+$");
        }
        /// <summary>
        /// 整数或者小数
        /// </summary>
        /// <param name="str">字符串</param>
        /// <returns>验证结果</returns>
        public static bool IsIntegerOrDecimal(string str)
        {
            return IsMatch(str, @"^[0-9]+\.{0,1}[0-9]{0,2}$");
        }

        public static bool IsInteger(string str)
        {
            return IsMatch(str, @"^\d*$");
        }
        /// <summary>
        /// 只能输入数字
        /// </summary>
        /// <param name="str">字符串</param>
        /// <returns>验证结果</returns>
        public static bool IsNumber(string str)
        {
            return IsMatch(str, @"^[0 - 9] * $");
        }
        /// <summary>
        /// 只能输入n位的数字
        /// </summary>
        /// <param name="str">字符串</param>
        /// <param name="n">数字位数</param>
        /// <returns>验证结果</returns>
        public static bool IsNumbers(string str, string n)
        {
            return IsMatch(str, $@"^\d{{{n}}}$");
        }
        /// <summary>
        /// 只能输入至少n位的数字
        /// </summary>
        /// <param name="str">字符串</param>
        /// <param name="n">数字位数</param>
        /// <returns>验证结果</returns>
        public static bool IsGreater(string str, string n)
        {
            return IsMatch(str, $@"^\d{{{n},}}$");
        }
        /// <summary>
        /// 只能输入m~n位的数字
        /// </summary>
        /// <param name="str">字符串</param>
        /// <param name="m">开始位数</param>
        /// <param name="n">结束位数</param>
        /// <returns>验证结果</returns>
        public static bool IsBeginAnd(string str, string m, string n)
        {
            return IsMatch(str, $@"^\d{{{m},{n}}}$");
        }
        /// <summary>
        /// 只能输入零和非零开头的数字
        /// </summary>
        /// <param name="str">字符串</param>
        /// <returns>验证结果</returns>
        public static bool IsStartNumber(string str)
        {
            return IsMatch(str, @"^(0|[1-9][0-9]*)$");
        }
        /// <summary>
        /// 只能输入有两位小数的正实数
        /// </summary>
        /// <param name="str">字符串</param>
        /// <returns>验证结果</returns>
        public static bool IsDouble(string str)
        {
            return IsMatch(str, @"^[0-9]+(.[0-9]{2})?$");
        }
        /// <summary>
        /// 只能输入有1~3位小数的正实数
        /// </summary>
        /// <param name="str">字符串</param>
        /// <returns>验证结果</returns>
        public static bool IsOneToThreePositiveNumber(string str)
        {
            return IsMatch(str, @"^[0-9]+(.[0-9]{1,3})?$");
        }
        /// <summary>
        /// 只能输入非零的正整数
        /// </summary>
        /// <param name="str">字符串</param>
        /// <returns>验证结果</returns>
        public static bool IsPositiveInteger(string str)
        {
            return IsMatch(str, @"^\+?[1-9][0-9]*$");
        }
        /// <summary>
        /// 只能输入非零的负整数
        /// </summary>
        /// <param name="str">字符串</param>
        /// <returns>验证结果</returns>
        public static bool IsNegativeInteger(string str)
        {
            return IsMatch(str, @"^\-[1-9][0-9]*$");
        }
        /// <summary>
        /// 只能输入长度为n的字符
        /// </summary>
        /// <param name="str">字符串</param>
        /// <param name="n">输入长度</param>
        /// <returns>验证结果</returns>
        public static bool IsEmail(string str, string n)
        {
            return IsMatch(str, $@"^.{{{n}}}$");
        }
        /// <summary>
        /// 只能输入由26个英文字母组成的字符串
        /// </summary>
        /// <param name="str">字符串</param>
        /// <returns>验证结果</returns>
        public static bool IsLetter(string str)
        {
            return IsMatch(str, @"^[A-Za-z]+$");
        }
        /// <summary>
        /// 只能输入由26个大写英文字母组成的字符串
        /// </summary>
        /// <param name="str">字符串</param>
        /// <returns>验证结果</returns>
        public static bool IsCapitalLetters(string str)
        {
            return IsMatch(str, @"^[A-Z]+$");
        }
        /// <summary>
        /// 只能输入由26个小写英文字母组成的字符串
        /// </summary>
        /// <param name="str">字符串</param>
        /// <returns>验证结果</returns>
        public static bool IsLowercaseLetters(string str)
        {
            return IsMatch(str, @"^[a-z]+$");
        }
        /// <summary>
        /// 验证是否含有^%&',;=?$\"等字符
        /// </summary>
        /// <param name="str">字符串</param>
        /// <returns>验证结果</returns>
        public static bool IsLegalCharacters(string str)
        {
            return IsMatch(str, @"[^%&',;=?$\x22]+");
        }
        /// <summary>
        /// 只能输入汉字
        /// </summary>
        /// <param name="str">字符串</param>
        /// <returns>验证结果</returns>
        public static bool IsChineseCharacters(string str)
        {
            return IsMatch(str, @"^[\u4e00-\u9fa5]{0,}$");
        }
        /// <summary>
        /// 验证URL
        /// </summary>
        /// <param name="str">字符串</param>
        /// <returns>验证结果</returns>
        public static bool IsUrl(string str)
        {
            return IsMatch(str, @"^http://([\w-]+\.)+[\w-]+(/[\w-./?%&=]*)?$");
        }
        /// <summary>
        /// 验证一年的12个月
        /// </summary>
        /// <param name="str">字符串</param>
        /// <returns>验证结果</returns>
        public static bool IsMonth(string str)
        {
            return IsMatch(str, @"^(0?[1-9]|1[0-2])$");//正确格式为："01"～"09"和"1"～"12"
        }
        /// <summary>
        /// 验证一个月的31天
        /// </summary>
        /// <param name="str">字符串</param>
        /// <returns>验证结果</returns>
        public static bool IsDay(string str)
        {
            return IsMatch(str, @"^((0?[1-9])|((1|2)[0-9])|30|31)$");//正确格式为；"01"～"09"和"1"～"31"
        }
        /// <summary>
        /// 判断文件目录是否合法
        /// </summary>
        /// <param name="path">文件目录</param>
        public static bool IsPath(string path)
        {
            var pattern = string.Join("", DriveInfo.GetDrives().Select(t => t.Name.First()).ToList());
            return IsMatch(path, $@"(?i)^[{pattern}]:(((\\[^ /:*?<>\""|\\]+)+\\?)|(\\)?)\s*$");
        }
    }
}
