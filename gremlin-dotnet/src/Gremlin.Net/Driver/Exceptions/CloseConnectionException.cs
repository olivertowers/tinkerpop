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
using System.Net.WebSockets;

namespace Gremlin.Net.Driver.Exceptions
{
    /// <summary>
    ///     The exception that is thrown when a websocket close message is received from Gremlin Server.
    /// </summary>
    public class CloseConnectionException : Exception
    {
        /// <summary>
        /// Initialize a new instance of <see cref="CloseConnectionException" /> class.
        /// </summary>
        /// <param name="id">The id of the connection that closed.</param>
        /// <param name="closeStatus">The status of the close message returned by the server.</param>
        /// <param name="closeDescription">The close description returned by the server</param>
        public CloseConnectionException(Guid id, WebSocketCloseStatus? closeStatus, string closeDescription)
        {
            CloseStatus = closeStatus;
            CloseDescription = closeDescription;
            ConnectionId = id;
        }

        /// <summary>
        /// Gets the status of the close message returned.
        /// </summary>
        public WebSocketCloseStatus? CloseStatus { get; }

        /// <summary>
        /// Gets close description returned by the server.
        /// </summary>
        public string CloseDescription { get; }

        /// <summary>
        /// Gets the id of the connection which was closed.
        /// </summary>
        public Guid ConnectionId { get; }
    }
}