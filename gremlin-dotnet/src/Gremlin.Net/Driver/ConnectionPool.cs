﻿#region License

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
using System.Linq;
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
                    if (_connections.IsEmpty) return false;
                    _connections.TryTake(out connection);
                }

                if (connection.IsOpen) return true;
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
                TeardownAsync().WaitUnwrap();
                RemoveAllConnections();
            }
        }

        private void RemoveAllConnections()
        {
            while (_connections.TryTake(out var connection))
            {
                try
                {
                    connection.Dispose();
                }
                catch (Exception e)
                {
                    GremlinServer.GremlinLogger.LogWarning($"{DateTime.UtcNow.ToString("o")} : {Environment.CurrentManagedThreadId} : ConnectionPool : RemoveAllConnections : ConnectionId: {connection.Id}, IsOpen: {connection.IsOpen}, " +
                        $"Exception: {e}");
                }
            }
        }

        private async Task TeardownAsync()
        {
            await Task.WhenAll(_connections.Select(c => ForceClose(c))).ConfigureAwait(false);
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