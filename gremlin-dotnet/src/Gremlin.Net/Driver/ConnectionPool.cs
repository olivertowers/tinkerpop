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
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Gremlin.Net.Process;
using Microsoft.Extensions.Logging;

namespace Gremlin.Net.Driver
{
    internal class ConnectionPool : IDisposable
    {
        private readonly ConnectionFactory _connectionFactory;
        private readonly ConcurrentBag<Connection> _connections = new ConcurrentBag<Connection>();
        private readonly object _connectionsLock = new object();

        public ConnectionPool(ConnectionFactory connectionFactory)
        {
            _connectionFactory = connectionFactory;
        }

        public int NrConnections { get; private set; }

        public async Task<IConnection> GetAvailableConnectionAsync()
        {
            if (!TryGetConnectionFromPool(out var connection))
                connection = await CreateNewConnectionAsync().ConfigureAwait(false);

            return new ProxyConnection(connection, AddConnectionIfOpen);
        }

        private bool TryGetConnectionFromPool(out Connection connection)
        {
            while (true)
            {
                connection = null;
                lock (_connectionsLock)
                {
                    GremlinServer.GremlinLogger.LogInformation($"{DateTime.UtcNow.ToString("o")} : {Environment.CurrentManagedThreadId} : ConnectionPool: TryGetConnectionFromPool - PoolSize:: {_connections.Count}");
                    if (_connections.IsEmpty) return false;
                    _connections.TryTake(out connection);
                }

                if (connection.IsOpen)
                {
                    GremlinServer.GremlinLogger.LogWarning($"{DateTime.UtcNow.ToString("o")} : {Environment.CurrentManagedThreadId} : ConnectionPool: TryGetConnectionFromPool - Returning open connection  {connection.Id}");
                    return true;
                }

                connection.Dispose();
            }
        }

        private async Task<Connection> CreateNewConnectionAsync()
        {
            NrConnections++;
            var newConnection = _connectionFactory.CreateConnection();
            await newConnection.ConnectAsync().ConfigureAwait(false);
            return newConnection;
        }

        private void AddConnectionIfOpen(Connection connection)
        {
            if (!connection.IsOpen)
            {
                ConsiderUnavailable();
                connection.Dispose();
                return;
            }
            AddConnection(connection);
        }

        private void AddConnection(Connection connection)
        {
            lock (_connectionsLock)
            {
                _connections.Add(connection);
            }
        }

        private void ConsiderUnavailable()
        {
            CloseAndRemoveAllConnections();
        }

        private void CloseAndRemoveAllConnections()
        {
            lock (_connectionsLock)
            {
                RemoveAllConnections().WaitUnwrap();
            }
        }

        private async Task RemoveAllConnections()
        {
            try
            {
                GremlinServer.GremlinLogger.LogInformation($"{DateTime.UtcNow.ToString("o")} : {Environment.CurrentManagedThreadId} : ConnectionPool : Enter RemoveAllConnections. PoolSize: {_connections.Count}");

                Task[] connectionRemoveTasks = new Task[_connections.Count];
                int i = 0;
                while (_connections.TryTake(out var connection))
                {
                    connectionRemoveTasks[i++] = Task.Run(async () =>
                    {
                        await ForceClose(connection);
                        connection.Dispose();
                    });
                }

                await Task.WhenAll(connectionRemoveTasks);
            }
            catch (Exception e)
            {
                GremlinServer.GremlinLogger.LogWarning($"{DateTime.UtcNow.ToString("o")} : {Environment.CurrentManagedThreadId} : ConnectionPool : RemoveAllConnections : Exception: {e}");
            }
            finally
            {
                GremlinServer.GremlinLogger.LogInformation($"{DateTime.UtcNow.ToString("o")} : {Environment.CurrentManagedThreadId} : ConnectionPool : Exit RemoveAllConnections");
            }
        }

        private static Task ForceClose(Connection c)
        {
            GremlinServer.GremlinLogger.LogInformation($"{DateTime.UtcNow.ToString("o")} : {Environment.CurrentManagedThreadId} : ConnectionPool : ForceClose : ConnectionId: {c.Id}, IsOpen: {c.IsOpen}");
            try
            {
                return c.CloseAsync();
            }
            catch (Exception e)
            {
                GremlinServer.GremlinLogger.LogWarning($"{DateTime.UtcNow.ToString("o")} : {Environment.CurrentManagedThreadId} : ConnectionPool : ForceClose : ConnectionId: {c.Id}, IsOpen: {c.IsOpen}, " +
                    $"Exception: {e}");
            }

            return Task.CompletedTask;
        }

        #region IDisposable Support

        private bool _disposed;

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                    CloseAndRemoveAllConnections();
                _disposed = true;
            }
        }
        #endregion
    }
}