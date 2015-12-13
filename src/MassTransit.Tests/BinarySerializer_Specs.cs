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
namespace MassTransit.Tests
{
    using System.Threading.Tasks;
    using NUnit.Framework;
    using TestFramework;
    using Util;
    using MassTransit.Transports.InMemory;
    using System;
    using System.Threading;
    using MassTransit.Pipeline;
    using System.Collections.Generic;
    using MassTransit.Serialization;
    using Events;
    using System.Linq;
    using Context;
    [TestFixture]
    public class When_using_the_binary_serializer :
        InMemoryTestFixture
    {
        readonly TaskCompletionSource<Fault<A>> _faultTaskTcs = new TaskCompletionSource<Fault<A>>();

        protected override void ConfigureBus(IInMemoryBusFactoryConfigurator configurator)
        {
            configurator.UseBinarySerializer();
            base.ConfigureBus(configurator);
        }
        protected override void ConfigureInputQueueEndpoint(IInMemoryReceiveEndpointConfigurator configurator)
        {
            base.ConfigureInputQueueEndpoint(configurator);

            #pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously

            configurator.Handler<A>(async m =>
            {
                throw new System.Exception("Booom!");
            });

            configurator.Handler<Fault<A>>(async m =>
            {
                _faultTaskTcs.TrySetResult(m.Message);
            });

            #pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously

        }


        [System.Serializable]
        class A
        {
        }



        [Test]
        public async Task Should_be_possibile_to_send_and_consume_faults()
        {
            await Bus.Publish(new A());
            var faultTask = _faultTaskTcs.Task;
            var completedTask = await Task.WhenAny(faultTask, Task.Delay(2000));
            Assert.AreEqual(faultTask, completedTask);
        }
    }


    [TestFixture]
    public class When_sending_messages_using_the_binary_serializer_between_multiple_buses
    {
        [Serializable]
        public class ListNode
        {
            public ListNode Next { get; set; }
            public int Value { get; set; }
        }

        [Serializable]
        public class Base
        {
            public ListNode Head { get; set; }
            public int PropBase { get; set; }
        }

        [Serializable]
        public class Derived : Base
        {
            public int PropDerived { get; set; }
        }

        [Test]
        public async Task Should_be_able_to_consume_messages_polymorphically_if_the_receiving_bus_support_the_binary_serializer()
        {
            var inMemoryTransportCache = new InMemoryTransportCache(Environment.ProcessorCount);
            var consumed = new TaskCompletionSource<Base>();

            var sourceBus = Bus.Factory.CreateUsingInMemory(x =>
            {
                x.SetTransportProvider(inMemoryTransportCache);
                x.UseBinarySerializer();
            });
            var recvBus = Bus.Factory.CreateUsingInMemory(x =>
            {
                x.SetTransportProvider(inMemoryTransportCache);
                x.SupportBinaryMessageDeserializer();
                x.UseJsonSerializer();
                x.ReceiveEndpoint("input_queue", configurator =>
                {
                    #pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
                    configurator.Handler<Base>(async ctx =>
                    {
                        consumed.TrySetResult(ctx.Message);
                    });
                    #pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
                });
            });
            await sourceBus.StartAsync();
            await recvBus.StartAsync();

            // Create a recursive list
            var head = new ListNode { Value = 100 };
            var tail = new ListNode { Next = head, Value = 200 };
            head.Next = tail;

            var messageToSend = new Derived()
            {
                PropBase = 10,
                PropDerived = 20,
                Head = head
            };
            await sourceBus.Publish(messageToSend);

            var completedTask = await Task.WhenAny(consumed.Task, Task.Delay(250));

            Assert.AreEqual(consumed.Task, completedTask, 
                "Timeout while waiting to receive the message sent on the source bus.");

            var message = await consumed.Task;
            Assert.NotNull(message);
            Assert.AreEqual(messageToSend.PropBase, message.PropBase);
            Assert.AreEqual(head.Value, message.Head.Value);
            Assert.AreEqual(tail.Value, message.Head.Next.Value);
            Assert.AreEqual(head.Value, message.Head.Next.Next.Value);

            await sourceBus.StopAsync();
            await recvBus.StopAsync();
        }

        public async Task Should_serialize_faults_correctly_when_the_receiving_bus(
            Action<IBusFactoryConfigurator> configureBusSerializer
        )
        {
            var inMemoryTransportCache = new InMemoryTransportCache(Environment.ProcessorCount);
            var consumedFault = new TaskCompletionSource<Fault<Base>>();
            var receiveObserver = new ReceiveObserver();


            var sourceBus = Bus.Factory.CreateUsingInMemory(x =>
            {
                x.SetTransportProvider(inMemoryTransportCache);
                x.UseBinarySerializer();
            });
            var recvBus = Bus.Factory.CreateUsingInMemory(x =>
            {
                x.SetTransportProvider(inMemoryTransportCache);
                x.SupportBinaryMessageDeserializer();
                x.UseFilter(new FaultSerializerNegotiatorInjectorFilter());
                configureBusSerializer(x);

                x.ReceiveEndpoint("input_queue", configurator =>
                {
                    #pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
                    configurator.Handler<Base>(async ctx =>
                    {
                        throw new Exception("Booom!");
                    });
                    configurator.Handler<Fault<Base>>(async ctx =>
                    {
                        consumedFault.TrySetResult(ctx.Message);
                    });
                    #pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
                });
            });
            recvBus.ConnectReceiveObserver(receiveObserver);
            await sourceBus.StartAsync();
            await recvBus.StartAsync();

            // Create a recursive list
            var head = new ListNode { Value = 100 };
            var tail = new ListNode { Next = head, Value = 200 };
            head.Next = tail;

            var messageToSend = new Derived()
            {
                PropBase = 10,
                PropDerived = 20,
                Head = head
            };
            await sourceBus.Publish(messageToSend);

            var completedTask = await Task.WhenAny(consumedFault.Task, receiveObserver.ReceiveFaulted);

            if (completedTask == receiveObserver.ReceiveFaulted)
                await receiveObserver.ReceiveFaulted;

            var fault = await consumedFault.Task;

            Assert.NotNull(fault);
            Assert.AreEqual(1, fault.Exceptions.Length);
            Assert.AreEqual("Booom!", fault.Exceptions[0].Message);

            var message = fault.Message;

            Assert.NotNull(message);
            Assert.AreEqual(messageToSend.PropBase, message.PropBase);
            Assert.AreEqual(head.Value, message.Head.Value);
            Assert.AreEqual(tail.Value, message.Head.Next.Value);
            Assert.AreEqual(head.Value, message.Head.Next.Next.Value);

            await sourceBus.StopAsync();
            await recvBus.StopAsync();
        }


        [Test]
        public async Task Should_serialize_faults_correctly_when_the_receiving_bus_use_the_binary_serializer()
        {
            await Should_serialize_faults_correctly_when_the_receiving_bus(
                x => x.UseBinarySerializer()
            );
        }

        [Test]
        public async Task Should_serialize_faults_correctly_when_the_receiving_bus_use_the_json_serializer()
        {
            await Should_serialize_faults_correctly_when_the_receiving_bus(
                x => x.UseJsonSerializer()
            );
        }

        private class ReceiveObserver : IReceiveObserver
        {
            private TaskCompletionSource<object> _tcs = new TaskCompletionSource<object>();

            public Task ReceiveFaulted => _tcs.Task;

            public Task ConsumeFault<T>(ConsumeContext<T> context, TimeSpan duration, string consumerType, Exception exception) where T : class
            {
                return Task.FromResult(true);
            }

            public Task PostConsume<T>(ConsumeContext<T> context, TimeSpan duration, string consumerType) where T : class
            {
                return Task.FromResult(true);
            }

            public Task PostReceive(ReceiveContext context)
            {
                return Task.FromResult(true);
            }

            public Task PreReceive(ReceiveContext context)
            {
                return Task.FromResult(true);
            }

            public Task ReceiveFault(ReceiveContext context, Exception exception)
            {
                _tcs.TrySetException(exception);
                return Task.FromResult(true);
            }
        }
    }

    /// <summary>
    /// Inject into the pipeline and wrap the <see cref="ConsumeContext"/> with a
    /// <see cref="FaultSerializerNegotiatorConsumeContextDecorator"/>
    /// </summary>
    public class FaultSerializerNegotiatorInjectorFilter : IFilter<ConsumeContext>
    {
        public async Task Send(ConsumeContext context, IPipe<ConsumeContext> next)
        {
            var decoratedContext = new FaultSerializerNegotiatorConsumeContextDecorator(context);
            await next.Send(decoratedContext);
            await decoratedContext.CompleteTask;
        }
        public void Probe(ProbeContext context)
        {
        }
    }


    /// <summary>
    /// Wrap another <see cref="ConsumeContext"/> instance and reimplement 
    /// the <see cref="ConsumeContext.NotifyFaulted{T}"/> method to correctly 
    /// publish any Fault message.
    /// </summary>
    /// <remarks>
    /// 
    /// Default Behavior:
    /// 
    /// Message(content type: binary) 
    ///     --> Deserialize(BinaryFormatter) 
    ///         --> Consume
    ///             --> Fault!
    ///                 --> To Error Queue: 
    ///                     --> OK
    ///                 --> To Fault{TMessage} exchange: 
    ///                     --> Serialize with the JsonSerializer 
    ///                         --> BREAK because it is not possibile to serialize this with the JsonSerializer
    /// Even if we fix that:
    ///  
    /// Message(content type: binary) 
    ///     --> Deserialize(BinaryFormatter) 
    ///         --> Consume
    ///             --> Fault!
    ///                 --> To Error Queue: 
    ///                     --> OK
    ///                 --> To Fault{TMessage} exchange: 
    ///                     --> Serialize with the correct serializer somehow
    ///                         --> Publish
    ///                             --> BREAK because it is RabbitMQ can't serialize complex types in the message headers
    /// 
    /// Instead:
    /// 
    ///  Message(content type: binary) 
    ///     --> Deserialize(BinaryFormatter) 
    ///         --> Consume
    ///             --> Fault!
    ///                 --> To Error Queue: 
    ///                     --> OK
    ///                 --> To Fault{TMessage} exchange: 
    ///                     --> Inspect ReceiveContext.ContentType 
    ///                         --> Serialize with the same serializer used to deserialize 
    ///                             the message (by inspecting ReceiveContext.ContentType)
    ///                         --> Convert all the headers values to string
    ///                             --> Publish
    ///                                 --> OK
    /// 
    /// 
    /// Note that this is required because the current MassTransit implementation
    /// 
    /// 1) Break by itself during the serialization of the headers in the RabbitMQ.BasicProperties
    /// 2) Do not take into account that when a consumer of a message that has been deserialized 
    ///    using the BinarySerializer (normally because the JsonSerializer does not support the type) 
    ///    the message should probably be serialized again using the same kind of serialization. 
    /// </remarks>
    public class FaultSerializerNegotiatorConsumeContextDecorator : ConsumeContext
    {
        readonly IList<Task> _pendingTasks = new List<Task>();

        private readonly ConsumeContext _context;

        public FaultSerializerNegotiatorConsumeContextDecorator(ConsumeContext context)
        {
            if (context == null)
                throw new ArgumentNullException("context");

            _context = context;
        }

        #region Forward only methods and properties

        public Guid? MessageId => _context.MessageId;
        public Guid? RequestId => _context.RequestId;
        public Guid? CorrelationId => _context.CorrelationId;
        public Guid? ConversationId => _context.ConversationId;
        public Guid? InitiatorId => _context.InitiatorId;
        public DateTime? ExpirationTime => _context.ExpirationTime;
        public Uri SourceAddress => _context.SourceAddress;
        public Uri DestinationAddress => _context.DestinationAddress;
        public Uri ResponseAddress => _context.ResponseAddress;
        public Uri FaultAddress => _context.FaultAddress;
        public Headers Headers => _context.Headers;
        public HostInfo Host => _context.Host;
        public ReceiveContext ReceiveContext => _context.ReceiveContext;
        public Task CompleteTask => Task.WhenAll(_pendingTasks.Concat(new[] { _context.CompleteTask }));
        public IEnumerable<string> SupportedMessageTypes => _context.SupportedMessageTypes;
        public CancellationToken CancellationToken => _context.CancellationToken;

        public bool HasPayloadType(Type contextType)
        {
            return _context.HasPayloadType(contextType);
        }

        public bool TryGetPayload<TPayload>(out TPayload payload) where TPayload : class
        {
            return _context.TryGetPayload(out payload);
        }

        public TPayload GetOrAddPayload<TPayload>(PayloadFactory<TPayload> payloadFactory) where TPayload : class
        {
            return _context.GetOrAddPayload(payloadFactory);
        }

        public Task Publish<T>(T message, CancellationToken cancellationToken = new CancellationToken()) where T : class
        {
            return _context.Publish(message, cancellationToken);
        }

        public Task Publish<T>(T message, IPipe<PublishContext<T>> publishPipe, CancellationToken cancellationToken = new CancellationToken()) where T : class
        {
            return _context.Publish(message, publishPipe, cancellationToken);
        }

        public Task Publish<T>(T message, IPipe<PublishContext> publishPipe, CancellationToken cancellationToken = new CancellationToken()) where T : class
        {
            return _context.Publish(message, publishPipe, cancellationToken);
        }

        public Task Publish(object message, CancellationToken cancellationToken = new CancellationToken())
        {
            return _context.Publish(message, cancellationToken);
        }

        public Task Publish(object message, IPipe<PublishContext> publishPipe, CancellationToken cancellationToken = new CancellationToken())
        {
            return _context.Publish(message, publishPipe, cancellationToken);
        }

        public Task Publish(object message, Type messageType, CancellationToken cancellationToken = new CancellationToken())
        {
            return _context.Publish(message, messageType, cancellationToken);
        }

        public Task Publish(object message, Type messageType, IPipe<PublishContext> publishPipe,
            CancellationToken cancellationToken = new CancellationToken())
        {
            return _context.Publish(message, messageType, publishPipe, cancellationToken);
        }

        public Task Publish<T>(object values, CancellationToken cancellationToken = new CancellationToken()) where T : class
        {
            return _context.Publish<T>(values, cancellationToken);
        }

        public Task Publish<T>(object values, IPipe<PublishContext<T>> publishPipe, CancellationToken cancellationToken = new CancellationToken()) where T : class
        {
            return _context.Publish(values, publishPipe, cancellationToken);
        }

        public Task Publish<T>(object values, IPipe<PublishContext> publishPipe, CancellationToken cancellationToken = new CancellationToken()) where T : class
        {
            return _context.Publish<T>(values, publishPipe, cancellationToken);
        }

        public Task<ISendEndpoint> GetSendEndpoint(Uri address)
        {
            return _context.GetSendEndpoint(address);
        }

        public bool HasMessageType(Type messageType)
        {
            return _context.HasMessageType(messageType);
        }

        public Task RespondAsync<T>(T message) where T : class
        {
            return _context.RespondAsync(message);
        }

        public Task RespondAsync<T>(T message, IPipe<SendContext<T>> sendPipe) where T : class
        {
            return _context.RespondAsync(message, sendPipe);
        }

        public void Respond<T>(T message) where T : class
        {
            _context.Respond(message);
        }

        public Task NotifyConsumed<T>(ConsumeContext<T> context, TimeSpan duration, string consumerType)
            where T : class
        {
            return _context.NotifyConsumed(context, duration, consumerType);
        }

        public Task RespondAsync<T>(T message, IPipe<SendContext> sendPipe) where T : class
        {
            return _context.RespondAsync<T>(message, sendPipe);
        }

        public Task RespondAsync(object message)
        {
            return _context.RespondAsync(message);
        }

        public Task RespondAsync(object message, Type messageType)
        {
            return _context.RespondAsync(message, messageType);
        }

        public Task RespondAsync(object message, IPipe<SendContext> sendPipe)
        {
            return _context.RespondAsync(message, sendPipe);
        }

        public Task RespondAsync(object message, Type messageType, IPipe<SendContext> sendPipe)
        {
            return _context.RespondAsync(message, messageType, sendPipe);
        }

        public Task RespondAsync<T>(object values) where T : class
        {
            return _context.RespondAsync<T>(values);
        }

        public Task RespondAsync<T>(object values, IPipe<SendContext<T>> sendPipe) where T : class
        {
            return _context.RespondAsync<T>(values, sendPipe);
        }

        public Task RespondAsync<T>(object values, IPipe<SendContext> sendPipe) where T : class
        {
            return _context.RespondAsync<T>(values, sendPipe);
        }

        public ConnectHandle ConnectPublishObserver(IPublishObserver observer)
        {
            return _context.ConnectPublishObserver(observer);
        }

        public ConnectHandle ConnectSendObserver(ISendObserver observer)
        {
            return _context.ConnectSendObserver(observer);
        }

        #endregion

        public bool TryGetMessage<T>(out ConsumeContext<T> consumeContext) where T : class
        {
            if (_context.TryGetMessage(out consumeContext))
            {
                // If the context become typed, we need to make sure to also decorate it else
                // this wont work.
                consumeContext = new FaultSerializerNegotiatorConsumeContextDecorator<T>(consumeContext);
                return true;
            }
            return false;
        }

        public Task NotifyFaulted<T>(ConsumeContext<T> context, TimeSpan duration, string consumerType, Exception exception)
            where T : class
        {
            // This is the same
            var faultTask = GenerateFault(context, context.Message, exception);

            _pendingTasks.Add(faultTask);

            var receiveTask = _context.ReceiveContext.NotifyFaulted(context, duration, consumerType, exception);

            _pendingTasks.Add(receiveTask);

            return TaskUtil.Completed;
        }

        private async Task GenerateFault<T>(ConsumeContext<T> context, T message, Exception exception)
            where T : class
        {
            var fault = new FaultEvent<T>(message, context.MessageId, HostMetadataCache.Host, exception);
            var faultPipe = CreateFaultPipe(context);

            if (FaultAddress != null)
            {
                var endpoint = await GetSendEndpoint(FaultAddress);

                await endpoint.Send(fault, faultPipe, CancellationToken);
            }
            else if (ResponseAddress != null)
            {
                var endpoint = await GetSendEndpoint(ResponseAddress);

                await endpoint.Send(fault, faultPipe, CancellationToken);
            }
            else
                await _context.Publish(fault, faultPipe, CancellationToken).ConfigureAwait(false);
        }

        private static IPipe<SendContext<Fault<T>>> CreateFaultPipe<T>(ConsumeContext<T> context) 
            where T : class
        {
            return Pipe.New<SendContext<Fault<T>>>(x => x.UseExecute(sendContext =>
            {
                // We only really need this!
                PrepareFaultSendContext(context, sendContext);
            }));
        }

        private static void PrepareFaultSendContext<T>(ConsumeContext<T> context, SendContext<Fault<T>> sendContext) 
            where T : class
        {
            var receiveContext = context.ReceiveContext;

            sendContext.SourceAddress = receiveContext.InputAddress;
            sendContext.CorrelationId = context.CorrelationId;
            sendContext.RequestId = context.RequestId;

            NegotiateSerializer(receiveContext, sendContext);
            // RabbitMQ break when serializing the headers if we dont do this...:(
            ConvertHeadersToString(context.Headers, sendContext);
        }

        private static void NegotiateSerializer<T>(ReceiveContext receiveContext, SendContext<Fault<T>> sendContext) 
            where T : class
        {
            if (receiveContext.ContentType.Equals(BinaryMessageSerializer.BinaryContentType))
            {
                sendContext.Serializer = new BinaryMessageSerializer();
            }
            else if (receiveContext.ContentType.Equals(JsonMessageSerializer.JsonContentType))
            {
                sendContext.Serializer = new JsonMessageSerializer();
            }
            else if (receiveContext.ContentType.Equals(BsonMessageSerializer.BsonContentType))
            {
                sendContext.Serializer = new BsonMessageSerializer();
            }
            else if (receiveContext.ContentType.Equals(XmlMessageSerializer.XmlContentType))
            {
                sendContext.Serializer = new XmlMessageSerializer();
            }
            else if (receiveContext.ContentType.Equals(EncryptedMessageSerializer.EncryptedContentType))
            {
                // TODO: here even if we use another serializer and the operation succeed, we risk publishing 
                // sensible data. 
                // We dont have the crypto stream provider to create an EncryptedMessageSerializer. 
                // What is the best way to handle this?
                sendContext.Serializer = null;
            }
        }

        private static void ConvertHeadersToString(Headers headers, SendContext sendContext)
        {
            foreach (var header in headers.GetAll())
            {
                if (header.Value is string)
                {
                    sendContext.Headers.Set(header.Key, header.Value);
                }
                else if (header.Value is MessageUrn || header.Value is Uri || header.Value is Guid)
                {
                    sendContext.Headers.Set(header.Key, header.Value.ToString());
                }
                // ignore others
                // TODO: we are discarding data here! log at least something!
            }
        }
    }
    public class FaultSerializerNegotiatorConsumeContextDecorator<T> :
        FaultSerializerNegotiatorConsumeContextDecorator, ConsumeContext<T>
        where T : class
    {
        private readonly ConsumeContext<T> _context;

        public FaultSerializerNegotiatorConsumeContextDecorator(ConsumeContext<T> context) :
            base(context)
        {
            _context = context;
        }

        public Task NotifyConsumed(TimeSpan duration, string consumerType)
        {
            return NotifyConsumed(_context, duration, consumerType);
        }

        public Task NotifyFaulted(TimeSpan duration, string consumerType, Exception exception)
        {
            return NotifyFaulted(_context, duration, consumerType, exception);
        }

        public T Message
        {
            get { return _context.Message; }
        }
    }
}