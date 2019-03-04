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

namespace Gremlin.Net.Driver
{
    internal class ConnectionPool : IDisposable
    {
        private readonly ConnectionFactory _connectionFactory;
        private readonly HashSet<Connection> _connections = new HashSet<Connection>(ReferenceEqualityComparer.Default);
        private readonly int _poolSize;
        private readonly int _maxInProcessPerConnection;
        private readonly EventAsync _availableConnection;
        private int _opened;
        private int _nrWaitingOnConnection;
        private int scheduledConnection;
    
        private TimeSpan waitForConnectionTimeout = TimeSpan.FromMinutes(1);

        // private const int PoolEmpty = 0;
        // private const int PoolPopulationInProgress = -1;

        // private SemaphoreSlim _populatePool;

        public ConnectionPool(ConnectionFactory connectionFactory, ConnectionPoolSettings settings)
        {
            _connectionFactory = connectionFactory;
            _poolSize = settings.PoolSize;
            _maxInProcessPerConnection = settings.MaxInProcessPerConnection;
            _availableConnection = new EventAsync();
            //_populatePool = new SemaphoreSlim(1, 1);
            SchedulePopulatePool2(_poolSize);
        }
        
        public int NrConnections
        {
            get
            {
                return _opened < 0 ? 0 : _opened;
            }
        }
        
        public async Task<IConnection> GetAvailableConnectionAsync()
        {
            // await EnsurePoolIsPopulatedAsync().ConfigureAwait(false);
            //return ProxiedConnection(await GetConnectionFromPool());

            return ProxiedConnection(await BorrowConnection());
        }

        // private async Task EnsurePoolIsPopulatedAsync()
        // {
        //     Stopwatch watch = Stopwatch.StartNew();
        //     // The pool could have been empty because of connection problems. So, we need to populate it again.
        //     //  while (true)
        //     //  {
        //         // nrOpened = 1
        //         // var nrOpened = Interlocked.CompareExchange(ref _nrConnections, PoolEmpty, PoolEmpty);
        //         if (_nrConnections >= _poolSize) return;
        //         int deficit = _poolSize - _nrConnections;

        //         //ar curState = Interlocked.CompareExchange(ref _poolState, PoolInDeficit, PoolComplete);
        //         //if (curState != PoolPopulationInProgress)
        //         //{
        //             await PopulatePoolAsync().ConfigureAwait(false);
        //         //}
        //     // }
        //     Console.WriteLine($"repopulated pool with deficit: {deficit}, duration: {watch.Elapsed.TotalMilliseconds}");
        // }

        // private async Task PopulatePoolAsync()
        // {
        //     // var curState = Interlocked.CompareExchange(ref _poolState, PoolPopulationInProgress, PoolInDeficit);
        //     // if (curState == PoolPopulationInProgress || _nrConnections >= _poolSize) return;
        //     try
        //     {
        //         await _populatePool.WaitAsync().ConfigureAwait(false);
        //         var _nrOpened = _nrConnections;
        //         if (_nrOpened >= _poolSize) return;

        //         var poolDeficit = _poolSize - _nrOpened;
        //         var connectionCreationTasks = new List<Task<Connection>>(poolDeficit);
        //         for (var i = 0; i < poolDeficit; i++)
        //         {
        //             connectionCreationTasks.Add(CreateNewConnectionAsync());
        //         }

        //         var createdConnections = await Task.WhenAll(connectionCreationTasks).ConfigureAwait(false);
        //         foreach (var c in createdConnections)
        //         {
        //             _connections.Enqueue(c);
        //         }
        //     }
        //     finally
        //     {
        //         // We need to remove the PoolPopulationInProgress flag again even if an exception occurred, so we don't block the pool population for ever
        //         Console.WriteLine($"pool repopulated: {_connections.Count}");
        //         Interlocked.Exchange(ref _nrConnections, _connections.Count);
        //         _populatePool.Release();
        //     }
        // }
        
        private async Task<Connection> CreateNewConnectionAsync()
        {
            var newConnection = _connectionFactory.CreateConnection();
            await newConnection.ConnectAsync().ConfigureAwait(false);
            return newConnection;
        }

        // private async Task<Connection> GetConnectionFromPool()
        // {
        //     Stopwatch watch = Stopwatch.StartNew();
        //     while (true)
        //     {
        //         Stopwatch usedConnWatch = Stopwatch.StartNew();
        //         var connection = await SelectLeastUsedConnection();
        //         Console.WriteLine($"Selected coonncetion duration: {usedConnWatch.Elapsed.TotalMilliseconds}");
        //         if (connection == null && _nrConnections > 0)
        //         {
        //             Console.WriteLine($"connections unavailable, retrying selction.");
        //             continue;
        //         }

        //         if (connection == null)
        //             throw new ServerUnavailableException();
        //         if (connection.NrRequestsInFlight >= _maxInProcessPerConnection)
        //             throw new ConnectionPoolBusyException(_poolSize, _maxInProcessPerConnection);
        //         if (connection.IsOpen) {
        //             _connections.Enqueue(connection);
        //             Console.WriteLine($"Retrieved connection from pool. Duration: {watch.Elapsed.TotalMilliseconds}.");
        //             return connection;
        //         }

        //         Console.WriteLine($"Bad connection.");
        //         DefinitelyDestroyConnection(connection);
        //     }
        // }

        public int CurrentSize()
        {
            lock (_connections)
            {
                return _connections.Count;
            }
        }

        private async Task<Connection> BorrowConnection()
        {
            Stopwatch watch = Stopwatch.StartNew();

            try
            {
                Interlocked.Increment(ref _nrWaitingOnConnection);
                var deadConnections = new List<Connection>();
                Connection leastUsedConn = SelectLeastUsedConnection2(deadConnections);

                if (CurrentSize() == 0)
                {
                    // full re-population required.
                    SchedulePopulatePool2(_poolSize);
                    return await WaitForConnectionAsync(waitForConnectionTimeout);
                }

                if (leastUsedConn == null)
                {
                    // We missed getting a connection after it was populated so wait for the next available.
                    // What if the pool is populated since? This won't get notified until a deficit is detected.
                    if (deadConnections.Count > 0)
                    {
                        foreach (var deadConn in deadConnections)
                        {
                            RemoveDeadConnection(deadConn);
                        }

                        Console.WriteLine($"Removed {deadConnections.Count} connections.");
                    }

                    ConsiderNewConnection();
                    return await WaitForConnectionAsync(waitForConnectionTimeout);
                }

                int currentPoolSize = CurrentSize();
                if (leastUsedConn.NrBorrowed >= _maxInProcessPerConnection && (currentPoolSize - deadConnections.Count) < _poolSize)
                {
                    // remove a dead connection to make room for a new connnection.
                    if (currentPoolSize >= _poolSize)
                    {
                        foreach (var deadConnection in deadConnections)
                        {
                            if (RemoveDeadConnection(deadConnection))
                            {
                                break;
                            }
                        }
                    }

                    // least used connection is too busy and the pool is not at capacity, try to schedule
                    // a connection.
                    ConsiderNewConnection();
                }

                // borrow the connection.
                while (true)
                {
                    int borrowed = leastUsedConn.NrBorrowed;
                    if (borrowed >= _maxInProcessPerConnection && leastUsedConn.NrRequestsInFlight >= _maxInProcessPerConnection)
                    {
                        return await WaitForConnectionAsync(waitForConnectionTimeout);
                    }

                    if (!leastUsedConn.IsOpen)
                    {
                        ReplaceDeadConnection(leastUsedConn);
                        return await WaitForConnectionAsync(waitForConnectionTimeout);
                    }

                    if (leastUsedConn.TryCompareSetBorrow(borrowed, borrowed + 1))
                    {
                        Interlocked.Decrement(ref _nrWaitingOnConnection);
                        return leastUsedConn;
                    }
                }
            }
            finally
            {
                Console.WriteLine($"Finished borrow connection. Duration: {watch.Elapsed.TotalMilliseconds}.");
            }
        }

        private void SchedulePopulatePool2(int populateSize)
        {
            populateSize = Math.Min(populateSize, _poolSize);
            for (int i = 0; i < populateSize; ++i)
            {
                if (scheduledConnection < populateSize)
                {
                    Interlocked.Increment(ref scheduledConnection);
                    ScheduleNewConnection();
                }
            }
        }

        private void ConsiderNewConnection()
        {
            while (true)
            {
                int inCreation = scheduledConnection;

                if (inCreation * _maxInProcessPerConnection >=  (1 + _nrWaitingOnConnection))
                {
                    return;
                }

                if (Interlocked.CompareExchange(ref scheduledConnection, inCreation + 1, inCreation) == inCreation)
                {
                    break;
                }
            }

            ScheduleNewConnection();
        }

        private void ScheduleNewConnection()
        {
            Task.Run(async () => {
                await AddConnectionIfUnderMax(waitForConnectionTimeout);
                Interlocked.Decrement(ref scheduledConnection);
            }).Forget();
        }

        private async Task<Connection> WaitForConnectionAsync(TimeSpan timeout)
        {
            long start = Stopwatch.GetTimestamp();
            TimeSpan remaining = timeout;

            do
            {
                Task timeoutTask = Task.Delay(timeout);
                Task waitAvailableTask = _availableConnection.WaitAsync();
                if ((await Task.WhenAny(waitAvailableTask, timeoutTask)) != waitAvailableTask)
                {
                    break;
                }

                Connection leastUsedConn = SelectLeastUsedConnection2();
                if (leastUsedConn != null) 
                {
                    while (true)
                    {
                        int borrowed = leastUsedConn.NrBorrowed;
                        if (borrowed >= _maxInProcessPerConnection)
                        {
                            break;
                        }

                        if (leastUsedConn.TryCompareSetBorrow(borrowed, borrowed + 1))
                        {
                            Interlocked.Decrement(ref _nrWaitingOnConnection);
                            return leastUsedConn;
                        }
                    }
                }

                remaining = timeout - new TimeSpan(Stopwatch.GetTimestamp() - start);
            }
            while (remaining > TimeSpan.Zero);

            Interlocked.Decrement(ref _nrWaitingOnConnection);
            ConsiderUnavailable();
            throw new TimeoutException($"Timed out waiting for connection after {timeout}");
        }

        private Connection SelectLeastUsedConnection2(List<Connection> deadConnections = null)
        {
            var nrMinInFlightConnections = int.MaxValue;
            var maxLastUsedTime = long.MinValue;
            Connection leastBusy = null;

            lock (_connections)
            {
                foreach (Connection connection in _connections)
                {
                    int nrInFlight = connection.NrRequestsInFlight;
                    long lastUsed = connection.LastUsedTimeTicks;
                    if (connection.IsOpen &&
                        nrInFlight < nrMinInFlightConnections ||
                        (nrInFlight == nrMinInFlightConnections &&
                        maxLastUsedTime < lastUsed))
                    {
                        nrMinInFlightConnections = nrInFlight;
                        maxLastUsedTime = lastUsed;
                        leastBusy = connection;
                    }

                    if (!connection.IsOpen && deadConnections != null)
                    {
                        deadConnections.Add(connection);
                    }
                }
            }

            return leastBusy;
        }

        private async Task<bool> AddConnectionIfUnderMax(TimeSpan timeout)
        {
            while (true)
            {
                int nrOpened = _opened;
                if (nrOpened >= _poolSize)
                {
                    Console.WriteLine($"Skip new connection. Pool at capacity. Opened size: {_opened}. Actual: {CurrentSize()} ");
                    return false;
                }

                if (Interlocked.CompareExchange(ref _opened, nrOpened + 1, nrOpened) == nrOpened)
                {
                    Console.WriteLine($"Adding new connection. Opened: size {_opened}. Actual: {CurrentSize()}");
                    break;
                }
            }

            // Aggressively try to connect.
            long start = Stopwatch.GetTimestamp();
            TimeSpan remaining = timeout;

            do
            {
                Connection connection = null;

                try
                {
                    connection = await CreateNewConnectionAsync();
                    if (connection.IsOpen)
                    {
                        lock (_connections)
                        {
                            _connections.Add(connection);
                            _availableConnection.Notify();
                        }

                        return true;
                    }
                    else
                    {
                        connection.Dispose();
                    }
                }
                catch
                {
                    try
                    {
                        // An exception should only happen if the connection failed to open,
                        // but dispose it if it just in case.
                        connection?.Dispose();
                    }
                    catch
                    {
                    }
                }

                remaining = timeout - new TimeSpan(Stopwatch.GetTimestamp() - start);
            }
            while (remaining > TimeSpan.Zero);

            Console.WriteLine($"Timed out creating connection. Pool size {_opened}.");
            Interlocked.Decrement(ref _opened);
            return false;
        }

        private sealed class EventAsync
        {
            /// <summary>Task tracking pending waiters. Each pending waiter is waiting on this task.</summary>
            /// <remarks>If null then there are no pending waiters.</remarks>
            private TaskCompletionSource<object> task;

            /// <summary>Waits for the event to be signaled.</summary>
            /// <returns>Returns a task that completes when the event has been signaled.</returns>
            public Task WaitAsync()
            {
                // If there are already pending waiters then just add to the list, otherwise create a new task.
                TaskCompletionSource<object> newTaskCompletionSource = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
                newTaskCompletionSource = Interlocked.CompareExchange(ref this.task, newTaskCompletionSource, null) ?? newTaskCompletionSource;
                return newTaskCompletionSource.Task;
            }

            /// <summary>Pulses the event.</summary>
            /// <remarks>All pending waiters are enabled, but subsequent waiters will block until the next signal.</remarks>
            public void Notify()
            {
                // If there are no pending waiters then there is nothing to do.
                TaskCompletionSource<object> oldTaskCompletionSource = Interlocked.Exchange(ref this.task, null);
                oldTaskCompletionSource?.SetResult(null);
            }
        }

        // private async Task<Connection> SelectLeastUsedConnection()
        // {
        //     if (_connections.IsEmpty) return null;
        //     var nrMinInFlightConnections = int.MaxValue;
        //     Connection leastBusy = null;
        //     Stopwatch watch = Stopwatch.StartNew();
        //     int repopulation = 0;
        //     int poolContention = 0;
        //     while (leastBusy == null)
        //     {
        //         int index = 0;
        //         int nrAvaialable = _connections.Count;
        //         while (index < nrAvaialable)
        //         {
        //             if (!_connections.TryDequeue(out Connection connection))
        //             {
        //                 break;
        //             }

        //             if (connection.IsOpen)
        //             {
        //                 var nrInFlight = connection.NrRequestsInFlight;
        //                 if (nrInFlight >= nrMinInFlightConnections)
        //                 {
        //                     // found least used connection. Return last dequeued
        //                     // conncection break-out.
        //                     _connections.Enqueue(connection);
        //                     break;
        //                 }

        //                 if (leastBusy != null)
        //                 {
        //                     // a less used connection was found. Return
        //                     // previous candidate.
        //                     _connections.Enqueue(leastBusy);
        //                     leastBusy = null;
        //                 }

        //                 nrMinInFlightConnections = nrInFlight;
        //                 leastBusy = connection;
        //             }

        //             index++;
        //         }

        //         if (leastBusy == null)
        //         {
        //             // If we didn't find a connection, the pool is either
        //             // seeing high contention (multiple threads trying to borrow a connection)
        //             // OR, the pool has reduced due to closed connections.
        //             if (NrConnections > 0)
        //             {
        //                 poolContention++;
        //             }
        //             else
        //             {
        //                 repopulation++;
        //                 await EnsurePoolIsPopulatedAsync();
        //                 nrAvaialable = _connections.Count;
        //             }
        //         }
        //         else
        //         {
        //             _connections.Enqueue(leastBusy);
        //             Console.WriteLine($"Selected connection {index}. Repop: {repopulation}, Contention: {poolContention}, Duration; {watch.Elapsed.TotalMilliseconds}");
        //         }
        //     }

        //     return leastBusy;
        // }
        
        private IConnection ProxiedConnection(Connection connection)
        {
            return new ProxyConnection(connection, ReturnConnectionIfOpen);
        }

        private void ReturnConnectionIfOpen(Connection connection)
        {
            int borrrowed = connection.DecrementBorrowed();
            if (connection.IsOpen)
            {
                _availableConnection.Notify();
                return;
            }

            ReplaceDeadConnection(connection);
        }

        private void ConsiderUnavailable()
        {
            CloseAndRemoveAllConnectionsAsync().WaitUnwrap();
        }

        private void ReplaceDeadConnection(Connection connection)
        {
            if (RemoveDeadConnection(connection))
            {
                ConsiderNewConnection();
            }
        }

        // private async Task RemoveConnection(Connection connection)
        // {
        //     try
        //     {
        //         if (connection.IsOpen)
        //         {
        //             await connection.CloseAsync().ConfigureAwait(false);
        //         }

        //         DefinitelyDestroyConnection(connection);
        //     }
        //     finally
        //     { 
        //         lock (_connections)
        //         {
        //             _connections.Remove(connection);
        //         }
        //     }

        // }

        private bool RemoveDeadConnection(Connection connection)
        {
            if (connection.IsOpen)
            {
                throw new InvalidOperationException("Invalid pool state. Attempting to remove a connection which is open.");
            }

            if (connection.NrBorrowed > 0)
            {
                // avoid prematurely destroying a connection if
                // it is in-use.
                Console.WriteLine($"Dead Connection with requests in flight {connection.NrRequestsInFlight}.");
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
                        _connections.Remove(connection);
                        Interlocked.Decrement(ref _opened);
                        Console.WriteLine($"Removed dead connection: Opened: {_opened}. Actual: {_connections.Count}.");
                    }
                }
            }

            return true;
        }

        private async Task CloseAndRemoveAllConnectionsAsync()
        {
            List<Connection> connectionsToDestroy = new List<Connection>();
            lock (_connections)
            {
                foreach (var connection in  _connections)
                {
                    connectionsToDestroy.Add(connection);
                    _connections.Remove(connection);
                    Interlocked.Decrement(ref _opened);
                    Console.WriteLine($"Removed connection: Opened: {_opened}. Actual: {_connections.Count}.");
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
    }
}