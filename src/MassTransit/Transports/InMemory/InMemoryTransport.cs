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
namespace MassTransit.Transports.InMemory
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Internals.Extensions;
    using Logging;
    using Monitoring.Introspection;
    using Pipeline;
    using Util;


    /// <summary>
    /// Support in-memory message queue that is not durable, but supports parallel delivery of messages
    /// based on TPL usage.
    /// </summary>
    public class InMemoryTransport_Old :
        IReceiveTransport,
        ISendTransport,
        IDisposable
    {
        static readonly ILog _log = Logger.Get<InMemoryTransport2>();

        readonly BlockingCollection<InMemoryTransportMessage> _collection;
        readonly Uri _inputAddress;
        readonly Connectable<ISendObserver> _observers;
        readonly ReceiveObservable _receiveObservers;
        ConcurrentQueue<InMemoryTransportMessage> _queue;

        public InMemoryTransport2(Uri inputAddress)
        {
            _inputAddress = inputAddress;

            _observers = new Connectable<ISendObserver>();
            _receiveObservers = new ReceiveObservable();

            _queue = new ConcurrentQueue<InMemoryTransportMessage>();
            _collection = new BlockingCollection<InMemoryTransportMessage>(_queue);
        }

        public async Task Probe(ProbeContext context)
        {
            ProbeContext scope = context.CreateScope("transport");
            scope.Set(new
            {
                Address = _inputAddress,
                QueueLength = _queue.Count,
            });
        }

        public void Dispose()
        {
            if (_collection != null)
                _collection.Dispose();
        }

        ReceiveTransportHandle IReceiveTransport.Start(IPipe<ReceiveContext> receivePipe)
        {
            var stopTokenSource = new CancellationTokenSource();

            Task receiveTask = StartReceiveTask(receivePipe, stopTokenSource);

            return new Handle(receiveTask, stopTokenSource);
        }

        public ObserverHandle ConnectReceiveObserver(IReceiveObserver observer)
        {
            return _receiveObservers.Connect(observer);
        }

        async Task ISendTransport.Send<T>(T message, IPipe<SendContext<T>> pipe, CancellationToken cancelSend)
        {
            var context = new InMemorySendContext<T>(message, cancelSend);

            try
            {
                await pipe.Send(context).ConfigureAwait(false);

                Guid messageId = context.MessageId ?? NewId.NextGuid();

                await _observers.ForEach(x => x.PreSend(context)).ConfigureAwait(false);

                var transportMessage = new InMemoryTransportMessage(messageId, context.Body, context.ContentType.MediaType, TypeMetadataCache<T>.ShortName);

                _collection.Add(transportMessage, cancelSend);

                context.DestinationAddress.LogSent(context.MessageId.HasValue ? context.MessageId.Value.ToString("N") : "",
                    TypeMetadataCache<T>.ShortName);

                await _observers.ForEach(x => x.PostSend(context)).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _log.Error(string.Format("SEND FAULT: {0} {1} {2}", _inputAddress, context.MessageId, TypeMetadataCache<T>.ShortName));

                _observers.ForEach(x => x.SendFault(context, ex)).Wait(cancelSend);

                throw;
            }
        }

        async Task ISendTransport.Move(ReceiveContext context, IPipe<SendContext> pipe)
        {
            Guid messageId = GetMessageId(context);

            byte[] body;
            using (Stream bodyStream = context.GetBody())
            {
                body = await GetMessageBody(bodyStream).ConfigureAwait(false);
            }

            string messageType = "Unknown";
            InMemoryTransportMessage receivedMessage;
            if (context.TryGetPayload(out receivedMessage))
                messageType = receivedMessage.MessageType;

            var transportMessage = new InMemoryTransportMessage(messageId, body, context.ContentType.MediaType, messageType);

            _collection.Add(transportMessage, context.CancellationToken);
        }

        public ConnectHandle Connect(ISendObserver observer)
        {
            return _observers.Connect(observer);
        }

        Task StartReceiveTask(IPipe<ReceiveContext> receivePipe, CancellationTokenSource stopTokenSource)
        {
            return Task.Run(() =>
            {
                _log.DebugFormat("Starting InMemory Transport: {0}", _inputAddress);
                using (RegisterShutdown(stopTokenSource.Token))
                {
                    try
                    {
                        Parallel.ForEach(GetConsumingPartitioner(_collection), async message =>
                        {
                            if (stopTokenSource.Token.IsCancellationRequested)
                                return;

                            var context = new InMemoryReceiveContext(_inputAddress, message, _receiveObservers);

                            try
                            {
                                _receiveObservers.NotifyPreReceive(context);

                                await receivePipe.Send(context).ConfigureAwait(false);

                                await context.CompleteTask.ConfigureAwait(false);

                                _receiveObservers.NotifyPostReceive(context);

                                _inputAddress.LogReceived(message.MessageId.ToString("N"), message.MessageType);
                            }
                            catch (Exception ex)
                            {
                                message.DeliveryCount++;
                                _log.Error(string.Format("RCV FAULT: {0}", message.MessageId), ex);

                                _receiveObservers.NotifyReceiveFault(context, ex);
                            }
                        });
                    }
                    catch (OperationCanceledException)
                    {
                    }
                }
            }, stopTokenSource.Token);
        }

        async Task<byte[]> GetMessageBody(Stream body)
        {
            using (var ms = new MemoryStream())
            {
                await body.CopyToAsync(ms).ConfigureAwait(false);

                return ms.ToArray();
            }
        }

        static Guid GetMessageId(ReceiveContext context)
        {
            object messageIdValue;
            return context.TransportHeaders.TryGetHeader("MessageId", out messageIdValue)
                ? new Guid(messageIdValue.ToString())
                : NewId.NextGuid();
        }

        CancellationTokenRegistration RegisterShutdown(CancellationToken cancellationToken)
        {
            return cancellationToken.Register(() =>
            {
                // signal collection that no more messages will be added, ending it
                _collection.CompleteAdding();
            });
        }

        Partitioner<T> GetConsumingPartitioner<T>(BlockingCollection<T> collection)
        {
            return new BlockingCollectionPartitioner<T>(collection);
        }


        class BlockingCollectionPartitioner<T> :
            Partitioner<T>
        {
            readonly BlockingCollection<T> _collection;

            internal BlockingCollectionPartitioner(BlockingCollection<T> collection)
            {
                if (collection == null)
                    throw new ArgumentNullException("collection");
                _collection = collection;
            }

            public override bool SupportsDynamicPartitions
            {
                get { return true; }
            }

            public override IList<IEnumerator<T>> GetPartitions(int partitionCount)
            {
                if (partitionCount < 1)
                    throw new ArgumentOutOfRangeException("partitionCount");

                IEnumerable<T> dynamicPartitioner = GetDynamicPartitions();

                return Enumerable.Range(0, partitionCount).Select(_ => dynamicPartitioner.GetEnumerator()).ToArray();
            }

            public override IEnumerable<T> GetDynamicPartitions()
            {
                return _collection.GetConsumingEnumerable();
            }
        }


        class Handle :
            ReceiveTransportHandle
        {
            readonly Task _receiverTask;
            readonly CancellationTokenSource _stop;

            public Handle(Task receiverTask, CancellationTokenSource cancellationTokenSource)
            {
                _stop = cancellationTokenSource;
                _receiverTask = receiverTask;
            }

            void IDisposable.Dispose()
            {
                _stop.Cancel();
            }

            async Task ReceiveTransportHandle.Stop(CancellationToken cancellationToken)
            {
                _stop.Cancel();

                await _receiverTask.WithCancellation(cancellationToken).ConfigureAwait(false);
            }
        }
    }


    /// <summary>
    /// Support in-memory message queue that is not durable, but supports parallel delivery of messages
    /// based on TPL usage.
    /// </summary>
    public class OptimizedForAsyncInMemoryTransport :
        IReceiveTransport,
        ISendTransport,
        IDisposable
    {
        static readonly ILog _log = Logger.Get<OptimizedForAsyncInMemoryTransport>();

        readonly Uri _inputAddress;
        readonly Connectable<ISendObserver> _observers;
        readonly ReceiveObservable _receiveObservers;
        readonly AsyncQueue<InMemoryTransportMessage> _queue;

        public OptimizedForAsyncInMemoryTransport(Uri inputAddress)
        {
            _inputAddress = inputAddress;

            _observers = new Connectable<ISendObserver>();
            _receiveObservers = new ReceiveObservable();
            _queue = new AsyncQueue<InMemoryTransportMessage>();
        }

        public async Task Probe(ProbeContext context)
        {
            ProbeContext scope = context.CreateScope("transport");
            scope.Set(new
            {
                Address = _inputAddress,
                QueueLength = _queue.Count,
            });
        }

        public void Dispose()
        {
            _queue.Dispose();
        }

        ReceiveTransportHandle IReceiveTransport.Start(IPipe<ReceiveContext> receivePipe)
        {
            return new Handle(_inputAddress, _queue, receivePipe, _receiveObservers);
        }

        public ObserverHandle ConnectReceiveObserver(IReceiveObserver observer)
        {
            return _receiveObservers.Connect(observer);
        }

        async Task ISendTransport.Send<T>(T message, IPipe<SendContext<T>> pipe, CancellationToken cancelSend)
        {
            var context = new InMemorySendContext<T>(message, cancelSend);

            try
            {
                await pipe.Send(context).ConfigureAwait(false);

                Guid messageId = context.MessageId ?? NewId.NextGuid();

                await _observers.ForEach(x => x.PreSend(context)).ConfigureAwait(false);

                var transportMessage = new InMemoryTransportMessage(messageId, context.Body, context.ContentType.MediaType, TypeMetadataCache<T>.ShortName);

                _queue.Enqueue(transportMessage, cancelSend);

                context.DestinationAddress.LogSent(context.MessageId.HasValue ? context.MessageId.Value.ToString("N") : "",
                    TypeMetadataCache<T>.ShortName);

                await _observers.ForEach(x => x.PostSend(context)).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _log.Error(string.Format("SEND FAULT: {0} {1} {2}", _inputAddress, context.MessageId, TypeMetadataCache<T>.ShortName));

                _observers.ForEach(x => x.SendFault(context, ex)).Wait(cancelSend);

                throw;
            }
        }

        async Task ISendTransport.Move(ReceiveContext context, IPipe<SendContext> pipe)
        {
            Guid messageId = GetMessageId(context);

            byte[] body;
            using (Stream bodyStream = context.GetBody())
            {
                body = await GetMessageBody(bodyStream).ConfigureAwait(false);
            }

            string messageType = "Unknown";
            InMemoryTransportMessage receivedMessage;
            if (context.TryGetPayload(out receivedMessage))
                messageType = receivedMessage.MessageType;

            var transportMessage = new InMemoryTransportMessage(messageId, body, context.ContentType.MediaType, messageType);

            _queue.Enqueue(transportMessage, context.CancellationToken);
        }

        public ConnectHandle Connect(ISendObserver observer)
        {
            return _observers.Connect(observer);
        }

        async Task<byte[]> GetMessageBody(Stream body)
        {
            using (var ms = new MemoryStream())
            {
                await body.CopyToAsync(ms).ConfigureAwait(false);

                return ms.ToArray();
            }
        }

        static Guid GetMessageId(ReceiveContext context)
        {
            object messageIdValue;
            return context.TransportHeaders.TryGetHeader("MessageId", out messageIdValue)
                ? new Guid(messageIdValue.ToString())
                : NewId.NextGuid();
        }

        class AsyncQueue<T>
        {
            public struct QueueItem
            {
                public T Item;
                public CancellationToken? CancellationToken;
            }
            readonly SemaphoreSlim _semaphoreSlim;
            readonly ConcurrentQueue<QueueItem> _queue;
            int _disposed;

            public AsyncQueue()
            {
                _semaphoreSlim = new SemaphoreSlim(0);
                _queue = new ConcurrentQueue<QueueItem>();
            }

            public void Enqueue(T item, CancellationToken? cancellationToken = null)
            {
                _queue.Enqueue(new QueueItem { Item = item, CancellationToken = cancellationToken });
                _semaphoreSlim.Release();
            }

            public async Task<QueueItem> DequeueAsync(CancellationToken cancellationToken)
            {
                await _semaphoreSlim.WaitAsync(cancellationToken).ConfigureAwait(false);
                QueueItem queueItem;
                if (!_queue.TryDequeue(out queueItem))
                {
                    throw new Exception("The queue semaphore was signaled but no item was present " +
                        "in the queue. This should never have happened, something smell bad here...");
                }
                return queueItem;
            }

            public int Count
            {
                get { return _queue.Count; }
            }

            public void Dispose()
            {
                if (Interlocked.CompareExchange(ref _disposed, 1, 0) == 0)
                    _semaphoreSlim.Dispose();
            }
        }

        class Handle :
            ReceiveTransportHandle
        {
            readonly AsyncQueue<InMemoryTransportMessage> _queue;
            readonly IPipe<ReceiveContext> _receivePipe;
            readonly Uri _inputAddress;
            readonly ReceiveObservable _receiveObservers;

            readonly CancellationTokenSource _cancellation;
            readonly Task _notifyTask;


            public Handle(
                Uri inputAddress,
                AsyncQueue<InMemoryTransportMessage> queue,
                IPipe<ReceiveContext> receivePipe,
                ReceiveObservable receiveObservers
            )
            {
                _cancellation = new CancellationTokenSource();
                _queue = queue;
                _receivePipe = receivePipe;
                _inputAddress = inputAddress;
                _receiveObservers = receiveObservers;
                _notifyTask = StartNotificationTask();
            }

            public Task Stop(CancellationToken cancellationToken)
            {
                _cancellation.Cancel();
                return _notifyTask.WithCancellation(cancellationToken);
            }

            Task StartNotificationTask()
            {
                return Task.Run(async () =>
                {
                    try
                    {
                        while (true)
                        {
                            var queueItem = await _queue.DequeueAsync(_cancellation.Token);

                            if (_cancellation.Token.IsCancellationRequested)
                                break;

                            var message = queueItem.Item;
                            var context = new InMemoryReceiveContext(_inputAddress, message, _receiveObservers);
                            try
                            {
                                //using (RegisterForSendCancellation(context, queueItem.CancellationToken))
                                {
                                    _receiveObservers.NotifyPreReceive(context);

                                    await _receivePipe.Send(context).ConfigureAwait(false);

                                    await context.CompleteTask.ConfigureAwait(false);

                                    _receiveObservers.NotifyPostReceive(context);

                                    _inputAddress.LogReceived(message.MessageId.ToString("N"), message.MessageType);    
                                }
                            }
                            catch (Exception ex)
                            {
                                message.DeliveryCount++;
                                _log.Error(string.Format("RCV FAULT: {0}", message.MessageId), ex);

                                _receiveObservers.NotifyReceiveFault(context, ex);
                            }
                        }
                    }
                    catch (OperationCanceledException)
                    {
                    }
                }, _cancellation.Token);
            }


            private IDisposable RegisterForSendCancellation(InMemoryReceiveContext context, CancellationToken? queuedItemCancellationToken)
            {
                if (!queuedItemCancellationToken.HasValue)
                    return null;
                return queuedItemCancellationToken.Value.Register(context.Cancel);
            }

            public void Dispose()
            {

            }
        }
    }


    public class InMemoryTransport : OptimizedForAsyncInMemoryTransport
    {
        public InMemoryTransport(Uri inputAddress)
            : base(inputAddress)
        {
        }
    }
}