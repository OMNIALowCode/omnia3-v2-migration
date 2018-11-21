using System.Diagnostics;
using System.Threading.Tasks;

namespace OmniaMigrationTool.Extensions
{
    public static class ProcessExtensions
    {
        public static Task<int> StartAsync(this Process process)
        {
            var tcs = new TaskCompletionSource<int>();

            process.Exited += (sender, args) =>
            {
                tcs.SetResult(process.ExitCode);
                process.Dispose();
            };

            process.Start();

            return tcs.Task;
        }
    }
}