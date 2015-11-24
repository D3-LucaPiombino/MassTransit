// Copyright 2007-2014 Chris Patterson, Dru Sellers, Travis Smith, et. al.
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
namespace MassTransit.Tests.Transports
{
    namespace InMemoryTransport_Specs
    {
        using System;
        using System.Diagnostics;
        using System.Threading;
        using System.Threading.Tasks;
        using MassTransit.Pipeline;
        using MassTransit.Pipeline.Filters;
        using MassTransit.Serialization;
        using MassTransit.Transports;
        using MassTransit.Transports.InMemory;
        using NUnit.Framework;
        using TestFramework;


        [TestFixture]
        public class Using_the_in_memory_transport :
            AsyncTestFixture
        {
            [Test]
            public async void Should_be_asynchronous()
            {
                var shutdown = new CancellationTokenSource();

                var inputAddress = new Uri("loopback://localhost/input_queue");

                var transport = new InMemoryTransport(inputAddress);

                TaskCompletionSource<int> received = GetTask<int>();

                IPipe<ReceiveContext> receivePipe = Pipe.New<ReceiveContext>(x =>
                {
                    x.UseFilter(new DelegateFilter<ReceiveContext>(context =>
                    {
                        Console.WriteLine("Message: {0}", context.TransportHeaders.Get("MessageId", "N/A"));

                        received.TrySetResult(1);
                    }));
                });

                ReceiveTransportHandle receiveHandle = ((IReceiveTransport)transport).Start(receivePipe);

                var sendEndpoint = new SendEndpoint(transport, new JsonMessageSerializer(), inputAddress,
                    inputAddress);

                await sendEndpoint.Send(new A(), TestCancellationToken);

                await received.Task;

                shutdown.Cancel();

                await receiveHandle.Stop();
            }

            [Test]
            public async Task Should_be_wicked_fast()
            {
                var handle = MassTransit.Bus.Factory.CreateUsingInMemory(x =>
                {

                    for (int i = 0; i < 2; i++)
                    {

                        x.ReceiveEndpoint(string.Format("input_queue_{0}", i), e =>
                        {
                            //e.PrefetchCount = 16;
                            //e.PurgeOnStartup();
                            e.Handler<TestMessage>(ctx =>
                            {
                                return Task.FromResult(true);
                            });
                        });
                    }

                });

                var busControl = handle.Start();
                
                var sw = Stopwatch.StartNew();
                while (true)
                {
                    await Task.Delay(1000);
                    int maxWorker, maxIoCompletion, availableWorkers, availableIoCompletion;

                    ThreadPool.GetAvailableThreads(out availableWorkers, out availableIoCompletion);
                    ThreadPool.GetMaxThreads(out maxWorker, out maxIoCompletion);
                    Console.WriteLine(
                        "[{0}] Pooled Threads(Worker/IO Completion): {1} / {2}",
                        sw.Elapsed,
                        maxWorker - availableWorkers,
                        maxIoCompletion - availableIoCompletion
                    );
                }
                
                await busControl.Stop();

            }
        }

        
        class TestMessage
        {
            
        }
        class A
        {
        }
    }
}