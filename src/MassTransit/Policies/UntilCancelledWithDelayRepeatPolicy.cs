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
namespace MassTransit.Policies
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    

    public class UntilCancelledWithDelayRepeatPolicy : IRepeatPolicy
    {
        readonly CancellationToken _cancellationToken;


        public class Context : IRepeatContext
        {
            readonly CancellationToken _cancellationToken;

            public Context(CancellationToken cancellationToken)
            {
                _cancellationToken = cancellationToken;
            }

            public void Dispose()
            {
            }

            public CancellationToken CancellationToken
            {
                get { return _cancellationToken; }
            }

            public bool CanRepeat(out TimeSpan delay)
            {
                delay = TimeSpan.FromSeconds(2);
                return !_cancellationToken.IsCancellationRequested;
            }
        }

        public UntilCancelledWithDelayRepeatPolicy(CancellationToken cancellationToken)
        {
            _cancellationToken = cancellationToken;
        }

        public IRepeatContext GetRepeatContext()
        {
            return new Context(_cancellationToken);
        }

        async Task IProbeSite.Probe(ProbeContext context)
        {
            context.CreateScope("untilCancelled");
        }
    }
}