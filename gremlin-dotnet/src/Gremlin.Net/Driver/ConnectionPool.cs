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
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Gremlin.Net.Driver.Exceptions;
using Gremlin.Net.Process;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Gremlin.Net.Driver
{
    internal class ConnectionPool : IDisposable
    {
        private readonly ConnectionFactory _connectionFactory;
        private readonly HashSet<Connection> _connections = new HashSet<Connection>(ReferenceEqualityComparer.Default);

        private readonly ConcurrentDictionary<Guid, Connection> _bin = new ConcurrentDictionary<Guid, Connection>();

        private readonly int _poolSize;
        private readonly int _maxInProcessPerConnection;
        private readonly int _maxSimultaneousUsagePerConnection;
        private readonly TimeSpan _reconnectInterval;
        private readonly TimeSpan _maxWaitForConnectionTimeout;
        private readonly ILogger _logger;
        private int _nrConnectionsOpened;
        private int _nrWaitingOnConnection;
        private int _scheduledNewConnections;
        private bool _isClosed;
        private SemaphoreSlim _maxConcurrent;

        public ConnectionPool(ConnectionFactory connectionFactory, ConnectionPoolSettings settings, ILogger logger = null)
        {
            _connectionFactory = connectionFactory;
            _poolSize = settings.PoolSize;
            _maxInProcessPerConnection = settings.MaxInProcessPerConnection;
            _maxSimultaneousUsagePerConnection = settings.MaxSimultaneousUsagePerConnection;
            _maxWaitForConnectionTimeout = settings.MaxWaitForConnection;
            _reconnectInterval = settings.ReconnectInterval;
            _maxConcurrent = new SemaphoreSlim(_maxSimultaneousUsagePerConnection * _poolSize);
            _logger = logger ?? NullLogger.Instance;
            SchedulePopulatePool(_poolSize);
        }
        
        public int NrConnections
        {
            get
            {
                return _nrConnectionsOpened < 0 ? 0 : _nrConnectionsOpened;
            }
        }
        
        public async Task<IConnection> GetAvailableConnectionAsync()
        {
            return ProxiedConnection(await BorrowConnection());
        }

        private async Task<Connection> CreateNewConnectionAsync()
        {
            var newConnection = _connectionFactory.CreateConnection();
            await newConnection.ConnectAsync().ConfigureAwait(false);
            return newConnection;
        }

        public int CurrentSize()
        {
            lock (_connections)
            {
                return _connections.Count;
            }
        }

        private async Task<Connection> BorrowConnection()
        {
            if (_isClosed)
            {
                throw new ServerUnavailableException();
            }

            Stopwatch borrowConnectionWatch = Stopwatch.StartNew();
            Connection leastUsedConn = null;
            try
            {
                Interlocked.Increment(ref _nrWaitingOnConnection);

                // Select the least used connection. This will temporarily borrow the connection 
                // so that a space its taken up for the pending request. If the candidate connection is
                // found to be invalid, then the borrow count on the connection is decremented.
                leastUsedConn = SelectLeastUsedConnection();
                int currentPoolSize = CurrentSize();

                // No connection found and pool is empty. Full re-population required.
                if (leastUsedConn == null && currentPoolSize == 0)
                {
                    _logger.LogDebug($"Empty pool found. Pool size: {currentPoolSize}, Bin size: {_bin.Count}.");
                    SchedulePopulatePool(_poolSize);
                    return await WaitForConnectionAsync(borrowConnectionWatch);
                }

                // We missed getting a connection after it was populated. This could be due to a pool full of dead connections
                // so try to remove them and schedule creation of a new connection.
                if (leastUsedConn == null)
                {
                    _logger.LogDebug($"No connection selected. Pool size: {currentPoolSize}, Bin size: {_bin.Count}.");
                    if (_bin.Count > 0)
                    {
                        _logger.LogDebug($"Attempting to remove all dead connections: Size: {_bin.Count}.");
                        foreach (var deadConn in _bin.Values)
                        {
                            RemoveDeadConnection(deadConn);
                        }
                    }

                    ConsiderNewConnection();
                    return await WaitForConnectionAsync(borrowConnectionWatch);
                }

                if (leastUsedConn.NrBorrowed >= _maxSimultaneousUsagePerConnection && (currentPoolSize - _bin.Count) < _poolSize)
                {
                    _logger.LogDebug($"Least used connection is too busy, borrowed {leastUsedConn.NrBorrowed} >= max {_maxSimultaneousUsagePerConnection}. Pool has capacity: Pool size: {currentPoolSize}, Bin size: {_bin.Count}.");
                    GrowPoolIfCapacityAvailable();
                }

                // finalize borrowing the select connection.
                while (true)
                {
                    int borrowed = leastUsedConn.NrBorrowed;
                    int nrInFlight = leastUsedConn.NrRequestsInFlight;
                    if (borrowed >= _maxSimultaneousUsagePerConnection && nrInFlight >= _maxInProcessPerConnection)
                    {
                        _logger.LogDebug($"Least used connection {leastUsedConn.Id} has borrowed {borrowed} >= max usage {_maxSimultaneousUsagePerConnection} and in-flight {nrInFlight} >= max inflight {_maxInProcessPerConnection}.");
                        leastUsedConn.DecrementBorrowed();
                        return await WaitForConnectionAsync(borrowConnectionWatch);
                    }

                    if (!leastUsedConn.IsOpen)
                    {
                        leastUsedConn.DecrementBorrowed();
                        _logger.LogDebug($"Least used connection no longer open. Id: {leastUsedConn.Id}, Pool size: {currentPoolSize}.");
                        ReplaceDeadConnection(leastUsedConn);
                        return await WaitForConnectionAsync(borrowConnectionWatch);
                    }

                    _logger.LogInformation($"Borrowed connection: {leastUsedConn.Id}. Duration: {borrowConnectionWatch.Elapsed.TotalMilliseconds}ms.");
                    Interlocked.Decrement(ref _nrWaitingOnConnection);
                    return leastUsedConn;
                }
            }
            catch (Exception e)
            {
                _logger.LogDebug($"Borrow connection threw exception after {borrowConnectionWatch.Elapsed.TotalMilliseconds}ms. Exception: {e}.");
                leastUsedConn?.DecrementBorrowed();
                Interlocked.Decrement(ref _nrWaitingOnConnection);
                throw;
            }
        }

        private void GrowPoolIfCapacityAvailable()
        {
            // Remove a dead connection if we need to make room for a new connnection.
            int currentSize = CurrentSize();
            if (currentSize >= _poolSize && currentSize - _bin.Count < _poolSize)
            {
                foreach (var deadConnection in _bin.Values)
                {
                    if (RemoveDeadConnection(deadConnection))
                    {
                        break;
                    }
                }
            }

            // least used connection is too busy and the pool is not at capacity, try to schedule a connection.
            ConsiderNewConnection();
        }

        private void SchedulePopulatePool(int populateSize)
        {
            populateSize = Math.Min(populateSize, _poolSize);
            _logger.LogDebug($"Re-populate pool by size: {populateSize}.");
            for (int i = 0; i < populateSize; ++i)
            {
                if (_scheduledNewConnections < populateSize)
                {
                    Interlocked.Increment(ref _scheduledNewConnections);
                    ScheduleNewConnection();
                }
            }
        }

        private void ConsiderNewConnection()
        {
            while (true)
            {
                int inCreation = _scheduledNewConnections;

                if (inCreation * _maxInProcessPerConnection >=  (1 + _nrWaitingOnConnection))
                {
                    _logger.LogDebug($"Skip scheduling new connection. Sufficient tasks are pending. Scheduled connection tasks: {inCreation + 1}. Waiting requests: {_nrWaitingOnConnection}.");
                    return;
                }

                if (Interlocked.CompareExchange(ref _scheduledNewConnections, inCreation + 1, inCreation) == inCreation)
                {
                    _logger.LogDebug($"Schedule new connection: Scheduled connection tasks: {inCreation + 1}. Waiting request: {_nrWaitingOnConnection}");
                    break;
                }
            }

            ScheduleNewConnection();
        }

        private void ScheduleNewConnection()
        {
            Task.Run(async () => {
                await AddConnectionIfUnderMax();
                Interlocked.Decrement(ref _scheduledNewConnections);
            }).Forget();
        }

        private async Task<Connection> WaitForConnectionAsync(Stopwatch borrowConnectionWatch)
        {
            long start = Stopwatch.GetTimestamp();

            TimeSpan timeout = _maxWaitForConnectionTimeout;
            TimeSpan remaining = _maxWaitForConnectionTimeout;

            do
            {
                if (!(await _maxConcurrent.WaitAsync(remaining)))
                {
                    break;
                }

                Connection leastUsedConn = SelectLeastUsedConnection();
                if (leastUsedConn != null) 
                {
                    while (true)
                    {
                        int borrowed = leastUsedConn.NrBorrowed;
                        if (borrowed >= _maxInProcessPerConnection)
                        {
                            GrowPoolIfCapacityAvailable();
                            leastUsedConn.DecrementBorrowed();
                            _logger.LogDebug($"Wait on connection returned busy connection {leastUsedConn.Id}. Continue wait.");
                            break;
                        }

                        _logger.LogInformation($"Borrowed connection: {leastUsedConn.Id}. Duration: {borrowConnectionWatch.Elapsed.TotalMilliseconds}ms.");
                        Interlocked.Decrement(ref _nrWaitingOnConnection);
                        return leastUsedConn;
                    }
                }

                remaining = timeout - new TimeSpan(Stopwatch.GetTimestamp() - start);
            }
            while (remaining > TimeSpan.Zero);

            _logger.LogDebug($"Timed out waiting for connection: Timeout: {timeout}. Number requests waiting: {_nrWaitingOnConnection}.");
            Interlocked.Decrement(ref _nrWaitingOnConnection);
            throw new ConnectionPoolBusyException(_poolSize, _maxInProcessPerConnection, $"Timed out waiting for connection after {timeout}.");
        }

        private Connection SelectLeastUsedConnection()
        {
            int nrMinInFlightConnections = int.MaxValue;
            long currentTime = Stopwatch.GetTimestamp();
            long maxLastUsedDelta = 0;
            Connection leastBusy = null;

            lock (_connections)
            {
                foreach (Connection connection in _connections)
                {
                    int nrInFlight = connection.NrBorrowed;
                    long lastUsed = connection.LastUsedTimeTicks;
                    if (connection.IsOpen &&
                        (nrInFlight < nrMinInFlightConnections ||
                        (nrInFlight == nrMinInFlightConnections &&
                        currentTime - lastUsed > maxLastUsedDelta)))
                    {
                        nrMinInFlightConnections = nrInFlight;
                        maxLastUsedDelta = currentTime - lastUsed;
                        leastBusy = connection;
                    }

                    if (!connection.IsOpen)
                    {
                        _bin.TryAdd(connection.Id, connection);
                    }
                }

                if (leastBusy != null)
                {
                    int borrowed = leastBusy.NrBorrowed;
                    while (!leastBusy.TryCompareSetBorrow(borrowed, borrowed + 1))
                    {
                        borrowed = leastBusy.NrBorrowed;
                    }
                }
            }

            return leastBusy;
        }

        private async Task<bool> AddConnectionIfUnderMax()
        {
            while (true)
            {
                int currentSize = CurrentSize();
                int nrOpened = _nrConnectionsOpened;
                if (nrOpened >= _poolSize)
                {
                    _logger.LogDebug($"Skip new connection. Pool at capacity. Opened size: {nrOpened}. Actual: {currentSize}.");
                    return false;
                }

                if (Interlocked.CompareExchange(ref _nrConnectionsOpened, nrOpened + 1, nrOpened) == nrOpened)
                {
                    _logger.LogDebug($"Adding new connection. Opened: size {_nrConnectionsOpened}. Actual: {currentSize}.");
                    break;
                }
            }

            // Keep trying to reconnect until the timeout is hit.
            long start = Stopwatch.GetTimestamp();
            TimeSpan timeout = _maxWaitForConnectionTimeout;
            TimeSpan remaining = timeout;

            do
            {
                Connection connection = null;

                try
                {
                    connection = await CreateNewConnectionAsync();
                    if (connection.IsOpen)
                    {
                        _logger.LogDebug($"Created connection {connection.Id}.");
                        lock (_connections)
                        {
                            _connections.Add(connection);
                        }

                        _maxConcurrent.Release(_maxSimultaneousUsagePerConnection);
                        return true;
                    }
                    else
                    {
                        _logger.LogDebug($"Connection created in bad state. Id: {connection.Id}.");
                        connection.Dispose();
                    }
                }
                catch (Exception e)
                {
                    _logger.LogDebug($"Exception on create connection: Exception: {e}.");
                    connection?.Dispose();
                }

                await Task.Delay(_reconnectInterval);
                remaining = timeout - new TimeSpan(Stopwatch.GetTimestamp() - start);
            }
            while (remaining > TimeSpan.Zero);

            Interlocked.Decrement(ref _nrConnectionsOpened);
            _logger.LogDebug($"Timed out creating connection after {timeout.TotalMilliseconds}ms. Pool size {_nrConnectionsOpened}.");
            ConsiderUnavailable();
            return false;
        }

        private IConnection ProxiedConnection(Connection connection)
        {
            return new ProxyConnection(connection, ReturnConnectionIfOpen);
        }

        private void ReturnConnectionIfOpen(Connection connection)
        {
            int borrrowed = connection.DecrementBorrowed();
            if (!_isClosed && connection.IsOpen)
            {
                _logger.LogDebug($"Returning connection {connection.Id} to pool. Current borrowed: {borrrowed}.");
                //_availableConnection.Notify();
                _maxConcurrent.Release(1);
                return;
            }

            ReplaceDeadConnection(connection);
        }

        private void ConsiderUnavailable()
        {
            _isClosed = true;
            CloseAndRemoveAllConnectionsAsync().WaitUnwrap();
        }

        private void ReplaceDeadConnection(Connection connection)
        {
            if (RemoveDeadConnection(connection))
            {
                ConsiderNewConnection();
            }
        }

        private bool RemoveDeadConnection(Connection connection)
        {
            if (connection.IsOpen)
            {
                throw new InvalidOperationException("Invalid pool state. Attempting to remove a connection which is open.");
            }

            if (connection.NrBorrowed > 0)
            {
                // avoid prematurely destroying a connection if it is in-use.
                _logger.LogDebug($"Skip remove of  dead connection {connection.Id}. Current borrowed: {connection.NrBorrowed}.");
                return false;
            }

            try
            {
                DefinitelyDestroyConnection(connection);
            }
            finally
            { 
                lock (_connections)
                {
                    if (_connections.Contains(connection))
                    {
                        _bin.TryRemove(connection.Id, out Connection _);
                        _connections.Remove(connection);
                        Interlocked.Decrement(ref _nrConnectionsOpened);
                        _logger.LogDebug($"Removed dead connection {connection.Id} from pool: Opened: {_nrConnectionsOpened}. Actual: {_connections.Count}.");
                    }
                }
            }

            return true;
        }

        private async Task CloseAndRemoveAllConnectionsAsync()
        {
            _logger.LogDebug($"Removing all connections.");
            HashSet<Connection> connectionsToDestroy = new HashSet<Connection>(ReferenceEqualityComparer.Default);
            lock (_connections)
            {
                // Clear all connections in the pool.
                foreach (var connection in  _connections)
                {
                    if (connection.NrBorrowed == 0)
                    {
                        connectionsToDestroy.Add(connection);
                        _connections.Remove(connection);
                        Interlocked.Decrement(ref _nrConnectionsOpened);
                    }
                }

                // Remove all dead connections in the bin.
                foreach (var connection in _bin.Values)
                {
                    if (connection.NrBorrowed == 0)
                    {
                        connectionsToDestroy.Add(connection);
                        _bin.TryRemove(connection.Id, out Connection _);
                    }
                }
            }

            foreach (var connection in connectionsToDestroy)
            {
                try
                {
                    await connection.CloseAsync().ConfigureAwait(false);
                    DefinitelyDestroyConnection(connection);
                }
                catch
                {
                    // Ignore exceptions here.
                }
            }
        }

        private void DefinitelyDestroyConnection(Connection connection)
        {
            connection.Dispose();
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

        public sealed class ReferenceEqualityComparer : IEqualityComparer, IEqualityComparer<object>
        {
            public static ReferenceEqualityComparer Default { get; } = new ReferenceEqualityComparer();

            public new bool Equals(object x, object y) => ReferenceEquals(x, y);
            public int GetHashCode(object obj) => RuntimeHelpers.GetHashCode(obj);
        }

        // private sealed class EventAsync
        // {
        //     /// <summary>Task tracking pending waiters. Each pending waiter is waiting on this task.</summary>
        //     /// <remarks>If null then there are no pending waiters.</remarks>
        //     private TaskCompletionSource<object> task;

        //     /// <summary>Waits for the event to be signaled.</summary>
        //     /// <returns>Returns a task that completes when the event has been signaled.</returns>
        //     public Task WaitAsync()
        //     {
        //         // If there are already pending waiters then just add to the list, otherwise create a new task.
        //         TaskCompletionSource<object> newTaskCompletionSource = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
        //         newTaskCompletionSource = Interlocked.CompareExchange(ref this.task, newTaskCompletionSource, null) ?? newTaskCompletionSource;
        //         return newTaskCompletionSource.Task;
        //     }

        //     /// <summary>Pulses the event.</summary>
        //     /// <remarks>All pending waiters are enabled, but subsequent waiters will block until the next signal.</remarks>
        //     public void Notify()
        //     {
        //         // If there are no pending waiters then there is nothing to do.
        //         TaskCompletionSource<object> oldTaskCompletionSource = Interlocked.Exchange(ref this.task, null);
        //         oldTaskCompletionSource?.SetResult(null);
        //     }
        // }
    }
}