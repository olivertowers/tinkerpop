using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Exceptions;
using Gremlin.Net.IntegrationTest.Util;
using Gremlin.Net.Structure.IO.GraphSON;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;

namespace Gremlin.Net.IntegrationTest.Driver
{
    public class MakeConsoleWork
    {
        private readonly ITestOutputHelper _output;
        private readonly TextWriter _originalOut;
        private readonly TextWriter _textWriter;

        public MakeConsoleWork(ITestOutputHelper output)
        {
            _output = output;
            _originalOut = Console.Out;
            _textWriter = new TestOutputHelperWriter(output);
            Console.SetOut(_textWriter);
        }
    }

    public class TestOutputHelperWriter : TextWriter
    {
        public TestOutputHelperWriter(ITestOutputHelper output)
        {
            Output = output;
        }

        public ITestOutputHelper Output { get; }

        public override Encoding Encoding => Encoding.UTF8;

        //
        // Summary:
        //     Writes a subarray of characters to the string.
        //
        // Parameters:
        //   buffer:
        //     The character array to write data from.
        //
        //   index:
        //     The position in the buffer at which to start reading data.
        //
        //   count:
        //     The maximum number of characters to write.
        //
        // Exceptions:
        //   T:System.ArgumentNullException:
        //     buffer is null.
        //
        //   T:System.ArgumentOutOfRangeException:
        //     index or count is negative.
        //
        //   T:System.ArgumentException:
        //     (index + count)> buffer. Length.
        //
        //   T:System.ObjectDisposedException:
        //     The writer is closed.
        public override void Write(char[] buffer, int index, int count)
        {
            Output.WriteLine(new string(buffer).Substring(index, count));
        }
        //
        // Summary:
        //     Writes a string to the current string.
        //
        // Parameters:
        //   value:
        //     The string to write.
        //
        // Exceptions:
        //   T:System.ObjectDisposedException:
        //     The writer is closed.
        public override void Write(string value)
        {
            Output.WriteLine(value);
        }
        //
        // Summary:
        //     Writes a character to the string.
        //
        // Parameters:
        //   value:
        //     The character to write.
        //
        // Exceptions:
        //   T:System.ObjectDisposedException:
        //     The writer is closed.
        public override void Write(char value)
        {
            Output.WriteLine(value.ToString());
        }

        //
        // Summary:
        //     Writes a character to the string asynchronously.
        //
        // Parameters:
        //   value:
        //     The character to write to the string.
        //
        // Returns:
        //     A task that represents the asynchronous write operation.
        //
        // Exceptions:
        //   T:System.ObjectDisposedException:
        //     The string writer is disposed.
        //
        //   T:System.InvalidOperationException:
        //     The string writer is currently in use by a previous write operation.
        public override Task WriteAsync(char value)
        {
            return WriteLineAsync(value);
        }
        //
        // Summary:
        //     Writes a subarray of characters to the string asynchronously.
        //
        // Parameters:
        //   buffer:
        //     The character array to write data from.
        //
        //   index:
        //     The position in the buffer at which to start reading data.
        //
        //   count:
        //     The maximum number of characters to write.
        //
        // Returns:
        //     A task that represents the asynchronous write operation.
        //
        // Exceptions:
        //   T:System.ArgumentNullException:
        //     buffer is null.
        //
        //   T:System.ArgumentException:
        //     The index plus count is greater than the buffer length.
        //
        //   T:System.ArgumentOutOfRangeException:
        //     index or count is negative.
        //
        //   T:System.ObjectDisposedException:
        //     The string writer is disposed.
        //
        //   T:System.InvalidOperationException:
        //     The string writer is currently in use by a previous write operation.
        public override Task WriteAsync(char[] buffer, int index, int count)
        {
            return WriteLineAsync(buffer, index, count);
        }

        //
        // Summary:
        //     Writes a string to the current string asynchronously.
        //
        // Parameters:
        //   value:
        //     The string to write. If value is null, nothing is written to the text stream.
        //
        // Returns:
        //     A task that represents the asynchronous write operation.
        //
        // Exceptions:
        //   T:System.ObjectDisposedException:
        //     The string writer is disposed.
        //
        //   T:System.InvalidOperationException:
        //     The string writer is currently in use by a previous write operation.
        public override Task WriteAsync(string value)
        {
            return WriteLineAsync(value);
        }
        //
        // Summary:
        //     Writes a string followed by a line terminator asynchronously to the current string.
        //
        // Parameters:
        //   value:
        //     The string to write. If the value is null, only a line terminator is written.
        //
        // Returns:
        //     A task that represents the asynchronous write operation.
        //
        // Exceptions:
        //   T:System.ObjectDisposedException:
        //     The string writer is disposed.
        //
        //   T:System.InvalidOperationException:
        //     The string writer is currently in use by a previous write operation.
        public override Task WriteLineAsync(string value)
        {
            Output.WriteLine(value);
            return Task.CompletedTask;
        }
        //
        // Summary:
        //     Writes a character followed by a line terminator asynchronously to the string.
        //
        // Parameters:
        //   value:
        //     The character to write to the string.
        //
        // Returns:
        //     A task that represents the asynchronous write operation.
        //
        // Exceptions:
        //   T:System.ObjectDisposedException:
        //     The string writer is disposed.
        //
        //   T:System.InvalidOperationException:
        //     The string writer is currently in use by a previous write operation.
        public override Task WriteLineAsync(char value)
        {
            Output.WriteLine(value.ToString());
            return Task.CompletedTask;
        }

        //
        // Summary:
        //     Writes a subarray of characters followed by a line terminator asynchronously
        //     to the string.
        //
        // Parameters:
        //   buffer:
        //     The character array to write data from.
        //
        //   index:
        //     The position in the buffer at which to start reading data.
        //
        //   count:
        //     The maximum number of characters to write.
        //
        // Returns:
        //     A task that represents the asynchronous write operation.
        //
        // Exceptions:
        //   T:System.ArgumentNullException:
        //     buffer is null.
        //
        //   T:System.ArgumentException:
        //     The index plus count is greater than the buffer length.
        //
        //   T:System.ArgumentOutOfRangeException:
        //     index or count is negative.
        //
        //   T:System.ObjectDisposedException:
        //     The string writer is disposed.
        //
        //   T:System.InvalidOperationException:
        //     The string writer is currently in use by a previous write operation.
        public override Task WriteLineAsync(char[] buffer, int index, int count)
        {
            Output.WriteLine(new string(buffer).Substring(index, count));
            return Task.CompletedTask;
        }

        public override void WriteLine(string value)
        {
            Output.WriteLine(value);
        }
    }

    public class WebSocketConnectionTests : MakeConsoleWork
    {
        private readonly RequestMessageProvider _requestMessageProvider = new RequestMessageProvider();
        private static readonly string TestHost = ConfigProvider.Configuration["TestServerIpAddress"];
        private static readonly int TestPort = Convert.ToInt32(ConfigProvider.Configuration["TestServerPort"]);
        private static readonly string UserName = ConfigProvider.Configuration["TestUserName"];
        private static readonly string Password = ConfigProvider.Configuration["TestPassword"];

        public WebSocketConnectionTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact()]
        public async Task RunManyRequests()
        {
            Task[] requestTasks = new Task[10000];

            var loggerFactory = new LoggerFactory()
                .AddConsole();

            GremlinServer.GremlinLogger = loggerFactory.CreateLogger("Gremlin.NET");

            var gremlinServer = new GremlinServer(TestHost, TestPort, true, UserName, Password);
            using (var gremlinClient = new GremlinClient(gremlinServer, new GraphSON2Reader(), new GraphSON2Writer(), GremlinClient.GraphSON2MimeType))
            {
                for (int i = 0; i < requestTasks.Length; ++i)
                {
                    int index = i;
                    requestTasks[i] = Task.Run(() => SubmitRequest(gremlinClient, index));
                    //await Task.Delay(1000);
                }

                var timeoutTask = Task.Delay(TimeSpan.FromMinutes(100));
                if (timeoutTask == await Task.WhenAny(timeoutTask, Task.WhenAll(requestTasks)))
                {
                    throw new InvalidOperationException("Timeout waiting for request tasks to complete.");
                }
            }
        }

        private static async Task SubmitRequest(GremlinClient gremlinClient, int index)
        {
            Console.WriteLine($"Executing request {index}");
            try
            {
                ResultSet<dynamic> result = await gremlinClient.SubmitAsync<dynamic>("g.inject(0)");
                Console.WriteLine($"Request {result.RequestId} completed. Current Connection Count: {gremlinClient.NrConnections}");
            }
            catch (ResponseException e)
            {
                Console.WriteLine($"Request {e.RequestId} failed. Current Connection Count: {gremlinClient.NrConnections}. Exception: {e}");
                throw;
            }
            catch (Exception e)
            {
                Console.WriteLine($"Request failed. Current Connection Count: {gremlinClient.NrConnections}. Exception: {e}");
                throw;
            }
            finally
            {
                Console.WriteLine($"Finished executing request {index}.");
            }
        }
    }
}
