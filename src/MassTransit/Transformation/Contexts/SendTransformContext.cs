// Copyright 2007-2015 Chris Patterson, Dru Sellers, Travis Smith, et. al.
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
namespace MassTransit.Transformation.Contexts
{
    using System;
    using System.Threading;
    using Context;
    using Util;


    /// <summary>
    /// Sits in front of the consume context and allows the inbound message to be 
    /// transformed.
    /// </summary>
    /// <typeparam name="TMessage"></typeparam>
    public class SendTransformContext<TMessage> :
        TransformContext<TMessage>
        where TMessage : class
    {
        readonly SendContext<TMessage> _context;
        readonly PayloadCache _payloadCache;

        public SendTransformContext(SendContext<TMessage> context)
        {
            _context = context;
            _payloadCache = new PayloadCache();
        }

        CancellationToken TransformContext.CancellationToken => _context.CancellationToken;
        Guid? TransformContext.MessageId => _context.MessageId;
        Guid? TransformContext.RequestId => _context.RequestId;
        Guid? TransformContext.CorrelationId => _context.CorrelationId;
        Guid? TransformContext.ConversationId => _context.ConversationId;
        Guid? TransformContext.InitiatorId => _context.InitiatorId;
        Uri TransformContext.SourceAddress => _context.SourceAddress;
        Uri TransformContext.DestinationAddress => _context.DestinationAddress;
        Headers TransformContext.Headers => _context.Headers;
        HostInfo TransformContext.Host => HostMetadataCache.Host;

        bool TransformContext.HasPayloadType(Type contextType)
        {
            return _payloadCache.HasPayloadType(contextType) || _context.HasPayloadType(contextType);
        }

        bool TransformContext.TryGetPayload<TPayload>(out TPayload context)
        {
            if (_payloadCache.TryGetPayload(out context))
                return true;

            return _context.TryGetPayload(out context);
        }

        TPayload TransformContext.GetOrAddPayload<TPayload>(PayloadFactory<TPayload> payloadFactory)
        {
            TPayload payload;
            if (_payloadCache.TryGetPayload(out payload))
                return payload;

            if (_context.TryGetPayload(out payload))
                return payload;

            return _payloadCache.GetOrAddPayload(payloadFactory);
        }

        TMessage TransformContext<TMessage>.Input => _context.Message;

        bool TransformContext<TMessage>.HasInput => true;
    }
}