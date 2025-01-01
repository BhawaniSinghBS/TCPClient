using Serilog;

namespace TCPClient
{
    public class InterfaceToUseTCP
    {
        public static TCPClient TCPClient = new TCPClient();
        public async static Task CallTcpAndSendRecivedBytesToSubscribers(byte[] byteFramesToToSendOnTCP)
        {

            // do not wait send only
            try
            {
                if (byteFramesToToSendOnTCP[2] == 0)// message count can not be zero
                {
                    byteFramesToToSendOnTCP[2] = 1;
                }

                if (byteFramesToToSendOnTCP == null || (byteFramesToToSendOnTCP[1] != 0 && byteFramesToToSendOnTCP[2] != 0))
                {
                    if (!TCPClient.TCPClientIsConnected && !TCPClient.TCPClientIsConnecting)
                    {
                        TCPClient = new TCPClient(303, "11.22.33.44");
                    }
                    
                    if (TCPClient.TCPClientIsConnected)
                    {
                        bool isSend = await TCPClient.SendDataIfAlreadyConnectAsync(byteFramesToToSendOnTCP);
                        if (!isSend)
                        {
                            await TCPClient.SendDataIfAlreadyConnectAsync(byteFramesToToSendOnTCP);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex.Message + " " + ex.StackTrace);
            }
        }
    }
}
