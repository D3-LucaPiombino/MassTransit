﻿// Copyright 2007-2015 Chris Patterson, Dru Sellers, Travis Smith, et. al.
//  
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the 
// License at 
// 
//     http://www.apache.org/licenses/LICENSE-2.0 
// 
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR 
// CONDITIONS OF ANY KIND, either express or implied. See the License for the 
// specific language governing permissions and limitations under the License.
namespace MassTransit
{
    using System;
    using RabbitMqTransport.Configuration;


    /// <summary>
    /// Configure a receiving RabbitMQ endpoint
    /// </summary>
    public interface IRabbitMqReceiveEndpointConfigurator :
        IReceiveEndpointConfigurator,
        IQueueConfigurator
    {
        void ConnectManagementEndpoint(IManagementEndpointConfigurator management);

        /// <summary>
        /// Bind an existing exchange to the receive endpoint queue by name
        /// </summary>
        /// <param name="exchangeName">The exchange name</param>
        void Bind(string exchangeName);

        /// <summary>
        /// Bind an exchange to the receive endpoint exchange
        /// </summary>
        /// <param name="exchangeName">The exchange name</param>
        /// <param name="callback">Configure the exchange and binding</param>
        void Bind(string exchangeName, Action<IExchangeBindingConfigurator> callback);
    }
}