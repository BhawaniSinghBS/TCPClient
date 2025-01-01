using Serilog;
using System.Net;
using System.Net.Sockets;
//register serilog in program.cs of main project
namespace TCPClient
{
    public class TCPClient : IDisposable
    {
        private bool disposedValue;
        public TcpClient TCPClientApp { get; private set; }
        public bool TCPClientIsConnected => TCPClientApp?.Connected ?? false;
        public bool TCPClientIsConnecting = false;
        public IPAddress ServerIp { get; }
        public int ServerPort { get; }
        private NetworkStream Stream { get; set; }
        private readonly TimeSpan _reconnectDelay = TimeSpan.FromSeconds(5);
        private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();

        // Event triggered when data is received from the server
        public event Action<byte[]> DataReceived;

        public TCPClient() { }
        public TCPClient(int serverPort = 303, string serverIp = "")
        {
            ServerIp = string.IsNullOrEmpty(serverIp) || !IPAddress.TryParse(serverIp, out var ip)
                ? Dns.GetHostEntry("example.com").AddressList[0]
                : ip;
            ServerPort = serverPort;
            if (!TCPClientIsConnected && !TCPClientIsConnecting)
            {
                // Initialize the connection and start health checks
                Task.Run(() => InitializeConnectionAsync());
                Task.Run(() => MonitorConnectionAsync(_cancellationTokenSource.Token));
            }
        }

        private async Task InitializeConnectionAsync()
        {
            while (!TCPClientIsConnected && !TCPClientIsConnecting)
            {
                TCPClientIsConnecting = true;
                try
                {
                    Thread.SpinWait(1000);
                    TCPClientApp = new TcpClient();
                    await TCPClientApp.ConnectAsync(ServerIp, ServerPort);
                    Thread.SpinWait(1000);
                    if (TCPClientApp?.Connected ?? false)
                    {
                        Stream = TCPClientApp.GetStream();
                        Log.Information("Connected to server at {ServerIp}:{ServerPort}", ServerIp, ServerPort);

                        // Start listening for incoming data
                        _ = ListenForDataAsync(_cancellationTokenSource.Token);
                    }
                }
                catch (Exception ex)
                {
                    Log.Error("Connection error: {Message}. Retrying in {ReconnectDelay}...", ex.Message, _reconnectDelay);
                    await Task.Delay(_reconnectDelay);
                }
                finally
                {
                    TCPClientIsConnecting = false;
                }
            }

        }

        private async Task MonitorConnectionAsync(CancellationToken cancellationToken)
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    // Check if the client is disconnected
                    if (!TCPClientIsConnected)
                    {
                        Log.Warning("Connection lost. Attempting to reconnect...");
                        await InitializeConnectionAsync();
                    }

                    await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken); // Check every 5 seconds
                }
            }
            catch (Exception ex)
            {
                Log.Error($"Error in MonitorConnectionAsync: {ex.Message}", ex.Message);
                Dispose();
                Thread.SpinWait(1000);
                TCPClientIsConnecting = false;
            }
        }
        public static byte TimerTriggerCountTillByteMax = 0;
        public async Task<bool> SendDataIfAlreadyConnectAsync(byte[] data)
        {
            try
            {
                if (data.Length < 10)
                {
                    Log.Information("Invalid data to came to send length is less than 10");
                    return false;
                }
                if (Stream?.CanWrite == true)
                {
                    TimerTriggerCountTillByteMax = TimerTriggerCountTillByteMax <= 0 || TimerTriggerCountTillByteMax == byte.MaxValue ? (byte)1 : (byte)(TimerTriggerCountTillByteMax + 1);

                    data[2] = TimerTriggerCountTillByteMax;
                    await Stream.WriteAsync(data, 0, data.Length);

                    string byteArrayInInt = string.Join(", ", data.Select(b => b.ToString()));
                    Log.Information("Sent data to server " + byteArrayInInt);

                    return true;
                }
                return false;
            }
            catch (Exception ex)
            {
                Log.Error($"Error in MonitorConnectionAsync: {ex.Message}", ex.Message);
                return false;
            }
        }

        private async Task ListenForDataAsync(CancellationToken cancellationToken)
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested && TCPClientIsConnected)
                {
                    if (Stream?.CanRead == true)
                    {
                        // Initialize a temporary buffer for reading data chunks
                        var tempBuffer = new byte[2048]; // Adjust this to a reasonable chunk size

                        // List to accumulate all received data
                        using (var memoryStream = new MemoryStream())
                        {
                            int bytesRead;
                            do
                            {
                                // Read the data in chunks
                                bytesRead = await Stream.ReadAsync(tempBuffer, 0, tempBuffer.Length, cancellationToken);

                                if (bytesRead > 0)
                                {
                                    // Write the read bytes to the MemoryStream
                                    memoryStream.Write(tempBuffer, 0, bytesRead);
                                }
                            }
                            while (Stream.DataAvailable && bytesRead > 0);

                            // Convert accumulated data to a byte array
                            var receivedData = memoryStream.ToArray();

                            // Ensure we have some data to process
                            if (receivedData?.Length > 0)
                            {
                                // Invoke the DataReceived event with the received data
                                DataReceived?.Invoke(receivedData);

                                // Log the received data as integers
                                var byteArrayAsInt = string.Join(", ", receivedData.Select(b => b.ToString()));
                                Log.Information("Bytes received from server: {ByteArray}", byteArrayAsInt);
                            }
                        }
                    }
                    else
                    {
                        await InitializeConnectionAsync(); // Attempt to reconnect if stream is not readable
                    }
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // Log any exceptions and try to reconnect
                Log.Error($"Error in listening for data: {ex.Message}", ex.Message);
                await InitializeConnectionAsync();
            }
        }


        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    Stream?.Dispose();
                    TCPClientApp?.Close();
                    TCPClientApp?.Dispose();
                }
                disposedValue = true;
            }
        }

        public void Dispose()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
