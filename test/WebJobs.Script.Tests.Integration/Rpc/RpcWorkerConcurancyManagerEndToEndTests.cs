﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs.Script.Config;
using Microsoft.Azure.WebJobs.Script.Eventing;
using Microsoft.Azure.WebJobs.Script.Workers.Rpc;
using Microsoft.Extensions.Options;
using Xunit;

namespace Microsoft.Azure.WebJobs.Script.Tests
{
    public class RpcWorkerConcurancyManagerEndToEndTests : IClassFixture<RpcWorkerConcurancyManagerEndToEndTests.TestFixture>
    {
        public RpcWorkerConcurancyManagerEndToEndTests(TestFixture fixture)
        {
            Fixture = fixture;
        }

        public TestFixture Fixture { get; set; }

        [Fact]
        public async Task WorkerStatus_NewWorkersAdded()
        {
            RpcFunctionInvocationDispatcher fd = null;
            IEnumerable<IRpcWorkerChannel> channels = null;
            // Latency > 1s
            TestScriptEventManager.WaitBeforePublish = TimeSpan.FromSeconds(2);
            await TestHelpers.Await(async () =>
            {
                fd = Fixture.JobHost.FunctionDispatcher as RpcFunctionInvocationDispatcher;
                channels = await fd.GetInitializedWorkerChannelsAsync();
                return channels.Count() == 4;
            }, pollingInterval: 1000, timeout: 120 * 1000);
        }

        [Fact]
        public async Task WorkerStatus_NewWorkersNotAdded()
        {
            // Latency < 1s
            TestScriptEventManager.WaitBeforePublish = TimeSpan.FromMilliseconds(100);
            await Task.Delay(1000);
            RpcFunctionInvocationDispatcher fd = Fixture.JobHost.FunctionDispatcher as RpcFunctionInvocationDispatcher;
            var channels = await fd.GetInitializedWorkerChannelsAsync();
            Assert.Equal(channels.Count(), 1);
        }

        public class TestFixture : ScriptHostEndToEndTestFixture
        {
            public TestFixture() : base(@"TestScripts\Node", "node", RpcWorkerConstants.NodeLanguageWorkerName,
                startHost: true, functions: new[] { "HttpTrigger" },
                concurrencyOptions: Options.Create(new RpcWorkerConcurrencyOptions()
                {
                    Enabled = true,
                    MaxWorkerCount = 4,
                    AdjustmentPeriod = TimeSpan.Zero,
                    CheckInterval = TimeSpan.FromMilliseconds(1000)
                }))
            {
            }
        }

        internal class TestScriptEventManager : IScriptEventManager, IDisposable
        {
            private readonly IScriptEventManager _scriptEventManager;


            public TestScriptEventManager()
            {
                _scriptEventManager = new ScriptEventManager();
            }

            public static TimeSpan WaitBeforePublish;

            public async void Publish(ScriptEvent scriptEvent)
            {
                // Emulate long worker status latency
                await Task.Delay(WaitBeforePublish);
                _scriptEventManager.Publish(scriptEvent);
            }

            public IDisposable Subscribe(IObserver<ScriptEvent> observer)
            {
                return _scriptEventManager.Subscribe(observer);
            }

            public void Dispose() => ((IDisposable)_scriptEventManager).Dispose();
        }
    }
}
