using System;
using System.Threading.Tasks;

namespace ConsoleUtil.Test
{
    public class TaskTool
    {
        public static void Await()
        {
            var t1 = Task.Factory.StartNew(() =>
            {
                for (var i = 0; i < 1000; i++)
                {
                    Console.WriteLine($"{{A}}={i}");
                }
            });
            t1.Wait();
            var t2 = Task.Factory.StartNew(() =>
            {
                for (var i = 0; i < 1000; i++)
                {
                    Console.WriteLine($"{{B}}={i}");
                }
            });
        }
        public static void Async()
        {
            var t1 = Task.Factory.StartNew(() =>
            {
                for (var i = 0; i < 1000; i++)
                {
                    Console.WriteLine($"{{A}}={i}");
                }
            });
            var t2 = Task.Factory.StartNew(() =>
            {
                for (var i = 0; i < 1000; i++)
                {
                    Console.WriteLine($"{{B}}={i}");
                }
            });
            Task.WaitAll(t1, t2);
            var t3 = Task.Factory.StartNew(() =>
            {
                for (var i = 0; i < 1000; i++)
                {
                    Console.WriteLine($"{{C}}={i}");
                }
            });
        }
    }
}
