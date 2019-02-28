#region License

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#endregion

using System;
using System.IO;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Gremlin.Net.Driver
{
    internal class WebSocketConnection : IDisposable
    {
        private const int ReceiveBufferSize = 1024;
        private const WebSocketMessageType MessageType = WebSocketMessageType.Binary;
        private readonly ClientWebSocket _client;
        private readonly ILogger _logger;
        private bool _isBrokenConnection;

        public WebSocketConnection(Action<ClientWebSocketOptions> webSocketConfiguration)
        {
            _logger = GremlinServer.GremlinLogger;
            _client = new ClientWebSocket();
            webSocketConfiguration?.Invoke(_client.Options);
            Id = Guid.NewGuid();
        }

        public async Task ConnectAsync(Uri uri)
        {
            LogInfo($"Enter ConnectAsync: Uri: {uri}, ");

            try
            {
                await _client.ConnectAsync(uri, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                _isBrokenConnection = true;
                LogWarning($"ConnectAsync: WebSocketState: {_client.State}, Exception: {e}");
                throw new WebSocketException(WebSocketError.Faulted, e);
            }
            finally
            {
                LogInfo($"Exit ConnectAsync");
            }
        }

        private void LogInfo(string message)
        {
            _logger.LogInformation($"{DateTime.UtcNow.ToString("o")} : {Environment.CurrentManagedThreadId} : Connection: {this.Id} : {message}");
        }

        private void LogWarning(string message)
        {
            _logger.LogWarning($"{DateTime.UtcNow.ToString("o")} : {Environment.CurrentManagedThreadId} : Connection: {this.Id} : {message}");
        }

        public async Task CloseAsync()
        {
            LogInfo($"Enter CloseAsync");

            try
            {
                if (CloseAlreadyInitiated)
                {
                    LogInfo($"CloseAsync : Close already initiated.");
                    return;
                }

                try
                {
                    await
                        _client.CloseAsync(WebSocketCloseStatus.NormalClosure, string.Empty, CancellationToken.None)
                            .ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    _isBrokenConnection = true;
                    // Swallow exceptions silently as there is nothing to do when closing fails
                    LogWarning($"CloseAsync : WebSocketState: {_client.State}, Exception: {e}");
                    throw new WebSocketException(WebSocketError.Faulted, e);
                }
            }
            finally
            {
                LogInfo($"Exit CloseAsync");
            }
        }

        private bool CloseAlreadyInitiated => _client.State == WebSocketState.Closed ||
                                            _client.State == WebSocketState.Aborted ||
                                            _client.State == WebSocketState.CloseSent;

        public async Task SendMessageAsync(byte[] message)
        {
            LogInfo($"Enter SendMessageAsync");

            try
            {
                string messageString = Encoding.UTF8.GetString(message);
                LogInfo($"SendMessageAsync: Message: {messageString}");

                await
                    _client.SendAsync(new ArraySegment<byte>(message), MessageType, true, CancellationToken.None)
                        .ConfigureAwait(false);
            }
            catch (Exception e)
            {
                _isBrokenConnection = true;
                LogWarning($"SendMessageAsync : WebSocketState: {_client.State}, Exception: {e}");
                throw new WebSocketException(WebSocketError.Faulted, e);
            }
            finally
            {
                LogInfo($"Exit SendMessageAsync");
            }
        }

        public async Task<byte[]> ReceiveMessageAsync()
        {
            LogInfo($"Enter ReceiveMessageAsync");

            try
            {
                using (var ms = new MemoryStream())
                {
                    WebSocketReceiveResult received;
                    do
                    {
                        var receiveBuffer = new ArraySegment<byte>(new byte[ReceiveBufferSize]);
                        received = await _client.ReceiveAsync(receiveBuffer, CancellationToken.None).ConfigureAwait(false);
                        ms.Write(receiveBuffer.Array, receiveBuffer.Offset, received.Count);
                    } while (!received.EndOfMessage);

                    byte[] response = ms.ToArray();
                    string responseAsString = Encoding.UTF8.GetString(response);
                    int truncatedLength = Math.Min(240, responseAsString.Length);
                    LogInfo($"ReceiveMessageAsync : Content: {responseAsString.Substring(0, truncatedLength)}...");

                    return response;
                }
            }
            catch (Exception e)
            {
                _isBrokenConnection = true;
                LogWarning($"ReceiveMessageAsync : WebSocketState: {_client.State}, Exception: {e}");
                throw new WebSocketException(WebSocketError.Faulted, e);
            }
            finally
            {
                LogInfo($"Exit ReceiveMessageAsync");
            }
        }

        public bool IsOpen => _client.State == WebSocketState.Open && !_isBrokenConnection;

        public Guid Id { get; }

        #region IDisposable Support

        private bool _disposed;

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            LogInfo($"Enter Dispose");

            try
            {
                if (!_disposed)
                {
                    if (disposing)
                        _client?.Dispose();
                    _disposed = true;
                }
            }
            catch (Exception e)
            {
                LogInfo($"Dispose : WebSocketState: {_client.State}, Exception {e}");
                throw;
            }
            finally
            {
                LogInfo($"Exit Dispose");
            }
        }

        #endregion
    }
}