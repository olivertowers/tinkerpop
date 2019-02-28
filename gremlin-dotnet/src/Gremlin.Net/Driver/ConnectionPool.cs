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
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Gremlin.Net.Driver.Exceptions;
using Gremlin.Net.Process;

namespace Gremlin.Net.Driver
{
    internal class ConnectionPool : IDisposable
    {
        private readonly ConnectionFactory _connectionFactory;
        private readonly ConcurrentQueue<Connection> _connections = new ConcurrentQueue<Connection>();
        private readonly int _poolSize;
        private readonly int _maxInProcessPerConnection;
        private int _nrConnections;
        private const int PoolEmpty = 0;
        private const int PoolPopulationInProgress = -1;

        private SemaphoreSlim _populatePool;

        public ConnectionPool(ConnectionFactory connectionFactory, ConnectionPoolSettings settings)
        {
            _connectionFactory = connectionFactory;
            _poolSize = settings.PoolSize;
            _maxInProcessPerConnection = settings.MaxInProcessPerConnection;
            _populatePool = new SemaphoreSlim(1, 1);
            PopulatePoolAsync().WaitUnwrap();
        }
        
        public int NrConnections
        {
            get
            {
                var nrConnections = Interlocked.CompareExchange(ref _nrConnections, PoolEmpty, PoolEmpty);
                return nrConnections < 0 ? 0 : nrConnections;
            }
        }
        
        public async Task<IConnection> GetAvailableConnectionAsync()
        {
            await EnsurePoolIsPopulatedAsync().ConfigureAwait(false);
            return ProxiedConnection(await GetConnectionFromPool());
        }

        private async Task EnsurePoolIsPopulatedAsync()
        {
            Stopwatch watch = Stopwatch.StartNew();
            // The pool could have been empty because of connection problems. So, we need to populate it again.
            //  while (true)
            //  {
                // nrOpened = 1
                // var nrOpened = Interlocked.CompareExchange(ref _nrConnections, PoolEmpty, PoolEmpty);
                if (_nrConnections >= _poolSize) return;
                int deficit = _poolSize - _nrConnections;

                //ar curState = Interlocked.CompareExchange(ref _poolState, PoolInDeficit, PoolComplete);
                //if (curState != PoolPopulationInProgress)
                //{
                    await PopulatePoolAsync().ConfigureAwait(false);
                //}
            // }
            Console.WriteLine($"repopulated pool with deficit: {deficit}, duration: {watch.Elapsed.TotalMilliseconds}");
        }

        private async Task PopulatePoolAsync()
        {
            // var curState = Interlocked.CompareExchange(ref _poolState, PoolPopulationInProgress, PoolInDeficit);
            // if (curState == PoolPopulationInProgress || _nrConnections >= _poolSize) return;
            try
            {
                await _populatePool.WaitAsync().ConfigureAwait(false);
                var _nrOpened = _nrConnections;
                if (_nrOpened >= _poolSize) return;

                var poolDeficit = _poolSize - _nrOpened;
                var connectionCreationTasks = new List<Task<Connection>>(poolDeficit);
                for (var i = 0; i < poolDeficit; i++)
                {
                    connectionCreationTasks.Add(CreateNewConnectionAsync());
                }

                var createdConnections = await Task.WhenAll(connectionCreationTasks).ConfigureAwait(false);
                foreach (var c in createdConnections)
                {
                    _connections.Enqueue(c);
                }
            }
            finally
            {
                // We need to remove the PoolPopulationInProgress flag again even if an exception occurred, so we don't block the pool population for ever
                Console.WriteLine($"pool repopulated: {_connections.Count}");
                Interlocked.Exchange(ref _nrConnections, _connections.Count);
                _populatePool.Release();
            }
        }
        
        private async Task<Connection> CreateNewConnectionAsync()
        {
            var newConnection = _connectionFactory.CreateConnection();
            await newConnection.ConnectAsync().ConfigureAwait(false);
            return newConnection;
        }

        private async Task<Connection> GetConnectionFromPool()
        {
            Stopwatch watch = Stopwatch.StartNew();
            while (true)
            {
                Stopwatch usedConnWatch = Stopwatch.StartNew();
                var connection = await SelectLeastUsedConnection();
                Console.WriteLine($"Selected coonncetion duration: {usedConnWatch.Elapsed.TotalMilliseconds}");
                if (connection == null && _nrConnections > 0)
                {
                    Console.WriteLine($"connections unavailable, retrying selction.");
                    continue;
                }

                if (connection == null)
                    throw new ServerUnavailableException();
                if (connection.NrRequestsInFlight >= _maxInProcessPerConnection)
                    throw new ConnectionPoolBusyException(_poolSize, _maxInProcessPerConnection);
                if (connection.IsOpen) {
                    _connections.Enqueue(connection);
                    Console.WriteLine($"Retrieved connection from pool. Duration: {watch.Elapsed.TotalMilliseconds}.");
                    return connection;
                }

                Console.WriteLine($"Bad connection.");
                DefinitelyDestroyConnection(connection);
            }
        }

        private async Task<Connection> SelectLeastUsedConnection()
        {
            if (_connections.IsEmpty) return null;
            var nrMinInFlightConnections = int.MaxValue;
            Connection leastBusy = null;
            Stopwatch watch = Stopwatch.StartNew();
            int repopulation = 0;
            int poolContention = 0;
            while (leastBusy == null)
            {
                int index = 0;
                int nrAvaialable = _connections.Count;
                while (index < nrAvaialable)
                {
                    if (!_connections.TryDequeue(out Connection connection))
                    {
                        break;
                    }

                    if (connection.IsOpen)
                    {
                        var nrInFlight = connection.NrRequestsInFlight;
                        if (nrInFlight >= nrMinInFlightConnections)
                        {
                            // found least used connection. Return last dequeued
                            // conncection break-out.
                            _connections.Enqueue(connection);
                            break;
                        }

                        if (leastBusy != null)
                        {
                            // a less used connection was found. Return
                            // previous candidate.
                            _connections.Enqueue(leastBusy);
                            leastBusy = null;
                        }

                        nrMinInFlightConnections = nrInFlight;
                        leastBusy = connection;
                    }

                    index++;
                }

                if (leastBusy == null)
                {
                    // If we didn't find a connection, the pool is either
                    // seeing high contention (multiple threads trying to borrow a connection)
                    // OR, the pool has reduced due to closed connections.
                    if (NrConnections > 0)
                    {
                        poolContention++;
                    }
                    else
                    {
                        repopulation++;
                        await EnsurePoolIsPopulatedAsync();
                        nrAvaialable = _connections.Count;
                    }
                }
                else
                {
                    _connections.Enqueue(leastBusy);
                    Console.WriteLine($"Selected connection {index}. Repop: {repopulation}, Contention: {poolContention}, Duration; {watch.Elapsed.TotalMilliseconds}");
                }
            }

            return leastBusy;
        }
        
        private IConnection ProxiedConnection(Connection connection)
        {
            return new ProxyConnection(connection, ReturnConnectionIfOpen);
        }

        private void ReturnConnectionIfOpen(Connection connection)
        {
            if (connection.IsOpen) return;
            ConsiderUnavailable();
        }

        private void ConsiderUnavailable()
        {
            CloseAndRemoveAllConnectionsAsync().WaitUnwrap();
        }

        private async Task CloseAndRemoveAllConnectionsAsync()
        {
            while (_connections.TryDequeue(out var connection))
            {
                await connection.CloseAsync().ConfigureAwait(false);
                DefinitelyDestroyConnection(connection);
            }
        }

        private void DefinitelyDestroyConnection(Connection connection)
        {
            connection.Dispose();
            // remove from pool
            Interlocked.Decrement(ref _nrConnections);
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
                    CloseAndRemoveAllConnectionsAsync().WaitUnwrap();
                _disposed = true;
            }
        }

        #endregion
    }
}