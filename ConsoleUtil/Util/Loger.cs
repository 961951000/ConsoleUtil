using log4net;
using System;
using System.Diagnostics;
[assembly: log4net.Config.XmlConfigurator(ConfigFile = "log4net.config", Watch = true)]

namespace ConsoleUtil.Util
{
    /// <summary>
    /// 日志类
    /// </summary>
    public class Loger
    {
        #region MyRegion   
        private static ILog Log => LogManager.GetLogger(new StackTrace().GetFrame(2).GetMethod().ReflectedType);
        #endregion
        /// <summary>
        /// 错误日志
        /// </summary>
        /// <param name="message">日志消息对象</param>
        public static void Fatal(object message)
        {
            if (Log.IsFatalEnabled)
            {
                Log.Fatal(message);
            }
        }
        /// <summary>
        /// 错误日志
        /// </summary>
        /// <param name="message">日志消息对象</param>
        /// <param name="exception">异常</param>
        public static void Fatal(object message, Exception exception)
        {
            if (Log.IsFatalEnabled)
            {
                Log.Fatal(message, exception);
            }
        }
        /// <summary>
        /// 异常日志
        /// </summary>
        /// <param name="message">日志消息对象</param>
        public static void Error(object message)
        {
            if (Log.IsErrorEnabled)
            {
                Log.Error(message);
            }
        }
        /// <summary>
        /// 异常日志
        /// </summary>
        /// <param name="message">日志消息对象</param>
        /// <param name="exception">异常</param>
        public static void Error(object message, Exception exception)
        {
            if (Log.IsErrorEnabled)
            {
                Log.Error(message, exception);
            }
        }
        /// <summary>
        /// 警告日志
        /// </summary>
        /// <param name="message">日志消息对象</param>
        public static void Warn(object message)
        {
            if (Log.IsWarnEnabled)
            {
                Log.Warn(message);
            }
        }
        /// <summary>
        /// 警告日志
        /// </summary>
        /// <param name="message">日志消息对象</param>
        /// <param name="exception">异常</param>
        public static void Warn(object message, Exception exception)
        {
            if (Log.IsWarnEnabled)
            {
                Log.Warn(message, exception);
            }
        }
        /// <summary>
        /// 消息日志
        /// </summary>
        /// <param name="message">日志消息对象</param>
        public static void Info(object message)
        {
            if (Log.IsInfoEnabled)
            {
                Log.Info(message);
            }
        }
        /// <summary>
        /// 消息日志
        /// </summary>
        /// <param name="message">日志消息对象</param>
        /// <param name="exception">异常</param>
        public static void Info(object message, Exception exception)
        {
            if (Log.IsInfoEnabled)
            {
                Log.Info(message, exception);
            }
        }
        /// <summary>
        /// 调试日志
        /// </summary>
        /// <param name="message">日志消息对象</param>
        public static void Debug(string message)
        {
            if (Log.IsDebugEnabled)
            {
                Log.Debug($"{new StackTrace().GetFrame(1).GetMethod()} : {message}");
            }
        }
        /// <summary>
        /// 调试日志
        /// </summary>
        /// <param name="message">日志消息对象</param>
        /// <param name="exception">异常</param>
        public static void Debug(object message, Exception exception)
        {
            if (Log.IsDebugEnabled)
            {
                Log.Debug($"{new StackTrace().GetFrame(1).GetMethod()} : {message}");
            }
        }
    }
}
