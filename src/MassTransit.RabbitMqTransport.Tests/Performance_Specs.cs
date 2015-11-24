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
namespace MassTransit.RabbitMqTransport.Tests
{
    using System;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using NUnit.Framework;
    using RabbitMQ.Client;
    using TestFramework;
    using TestFramework.Messages;


    [TestFixture, Explicit]
    public class Performance_of_the_RabbitMQ_transport :
        RabbitMqTestFixture
    {
        [Test]
        public async void Should_be_wicked_fast()
        {
            int limit = 10000;
            int count = 0;

            await _requestClient.Request(new PingMessage());

            Stopwatch timer = Stopwatch.StartNew();

            await Task.WhenAll(Enumerable.Range(0, limit).Select(async x =>
            {
                await _requestClient.Request(new PingMessage());

                Interlocked.Increment(ref count);
            }));

            timer.Stop();

            Console.WriteLine("Time to process {0} messages = {1}", count * 2, timer.ElapsedMilliseconds + "ms");
            Console.WriteLine("Messages per second: {0}", count * 2 * 1000 / timer.ElapsedMilliseconds);
        }

        IRequestClient<PingMessage, PongMessage> _requestClient;

        [TestFixtureSetUp]
        public void Setup()
        {
            _requestClient = new MessageRequestClient<PingMessage, PongMessage>(Bus, InputQueueAddress, TestTimeout);
        }

        protected override void ConfigureBus(IRabbitMqBusFactoryConfigurator configurator)
        {
            base.ConfigureBus(configurator);

            configurator.PrefetchCount = 100;
        }

        protected override void ConfigureInputQueueEndpoint(IRabbitMqReceiveEndpointConfigurator configurator)
        {
            configurator.PrefetchCount = 100;

            configurator.Handler<PingMessage>(async context =>
            {
                try
                {
                    await context.RespondAsync(new PongMessage(context.Message.CorrelationId));
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                }
            });
        }
    }


    [TestFixture, Explicit]
    public class Performance_of_the_RabbitMQ_transport2 :
        AsyncTestFixture
    {
        [Test]
        public async Task Should_be_wicked_fast()
        {
            var handle = MassTransit.Bus.Factory.CreateUsingRabbitMq(x =>
            {
                //ConfigureBus(x);

                IRabbitMqHost host = x.Host(new Uri("rabbitmq://localhost/LI-223/"), h =>
                {
                    h.Username("guest");
                    h.Password("guest");
                });

                CleanUpVirtualHost(host);

                for (int i = 0; i < 40; i++)
                {

                    x.ReceiveEndpoint(host, string.Format("input_queue_{0}", i), e =>
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
        void CleanUpVirtualHost(IRabbitMqHost host)
        {

            ConnectionFactory connectionFactory = host.Settings.GetConnectionFactory();
            using (IConnection connection = connectionFactory.CreateConnection())
            using (IModel model = connection.CreateModel())
            {
                for (int i = 0; i < 100; i++)
                {
                    var qname = string.Format("input_queue_{0}", i);
                    model.ExchangeDelete(qname);
                    model.QueueDelete(qname);    
                }
                

                //OnCleanupVirtualHost(model);
            }
        }

        class TestMessage
        {
            
        }

       
    }
}