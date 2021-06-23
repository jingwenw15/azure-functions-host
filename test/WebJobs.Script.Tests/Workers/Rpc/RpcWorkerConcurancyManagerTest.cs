// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs.Script.Workers;
using Microsoft.Azure.WebJobs.Script.Workers.Rpc;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.WebJobs.Script.Tests;
using Moq;
using Xunit;

namespace Microsoft.Azure.WebJobs.Script.Tests.Workers.Rpc
{
    public class RpcWorkerConcurancyManagerTest
    {
        private TestLoggerProvider _loggerProvider;
        private ILoggerFactory _loggerFactory;

        public RpcWorkerConcurancyManagerTest()
        {
            _loggerProvider = new TestLoggerProvider();
            _loggerFactory = new LoggerFactory();
            _loggerFactory.AddProvider(_loggerProvider);
        }

        public static IEnumerable<object[]> DataForIsOverloaded =>
            new List<object[]>
            {
                new object[]
                {
                    Options.Create(new RpcWorkerConcurrencyOptions()
                    {
                        Enabled = true,
                        HistorySize = 5
                    }),
                    new int[] { 1, 2, 3, 4 },
                    false
                },
                new object[]
                {
                    Options.Create(new RpcWorkerConcurrencyOptions()
                    {
                        Enabled = true,
                        LatencyThreshold = TimeSpan.FromMilliseconds(10),
                        HistorySize = 5
                    }),
                    new int[] { 1, 2, 3, 4, 5 },
                    false
                },
                new object[]
                {
                    Options.Create(new RpcWorkerConcurrencyOptions()
                    {
                        Enabled = true,
                        LatencyThreshold = TimeSpan.FromMilliseconds(10),
                        HistorySize = 5
                    }),
                    new int[] { 11, 12, 13, 14, 15 },
                    true
                },
                new object[]
                {
                    Options.Create(new RpcWorkerConcurrencyOptions()
                    {
                        Enabled = true,
                        LatencyThreshold = TimeSpan.FromMilliseconds(13),
                        HistorySize = 6,
                        HistoryThreshold = 0.5F
                    }),
                    new int[] { 11, 12, 13, 14, 15, 16 },
                    true
                },
                new object[]
                {
                    Options.Create(new RpcWorkerConcurrencyOptions()
                    {
                        Enabled = true,
                        LatencyThreshold = TimeSpan.FromMilliseconds(14),
                        HistorySize = 6,
                        HistoryThreshold = 0.5F
                    }),
                    new int[] { 11, 12, 13, 14, 15, 16 },
                    false
                },
                new object[]
                {
                    Options.Create(new RpcWorkerConcurrencyOptions()
                    {
                        Enabled = true,
                        LatencyThreshold = TimeSpan.FromMilliseconds(14),
                        HistorySize = 5,
                    }),
                    new int[] { },
                    false
                }
            };

        //public void AddWorkerIfNeeded_Returns_Expected(IOptions<RpcWorkerConcurrencyOptions> options,
        //    int[] latencies1, int[] latencies2, bool readyForInvocations1, bool readyForInvocations2,
        //    TimeSpan elapsedFromLastAdding, bool expected)
        public static IEnumerable<object[]> DataForForAddWorkerIfNeeded =>
            new List<object[]>
            {
                new object[]
                {
                    Options.Create(new RpcWorkerConcurrencyOptions()
                    {
                        Enabled = true,
                        HistorySize = 5,
                        LatencyThreshold = TimeSpan.FromMilliseconds(200),
                        AdjustmentPeriod = TimeSpan.FromMilliseconds(1000),
                        MaxWorkerCount = 3,
                    }),
                    new int[] { 100, 100, 100, 100, 100 },
                    new int[] { 150, 150, 150, 150, 150 },
                    true,
                    true,
                    TimeSpan.FromSeconds(2000),
                    false
                },
                new object[]
                {
                    Options.Create(new RpcWorkerConcurrencyOptions()
                    {
                        Enabled = true,
                        HistorySize = 5,
                        LatencyThreshold = TimeSpan.FromMilliseconds(110),
                        AdjustmentPeriod = TimeSpan.FromMilliseconds(1000),
                        MaxWorkerCount = 3,
                    }),
                    new int[] { 100, 100, 100, 100, 100 },
                    new int[] { 150, 150, 150, 150, 150 },
                    true,
                    true,
                    TimeSpan.FromSeconds(2000),
                    true
                },
                new object[]
                {
                    Options.Create(new RpcWorkerConcurrencyOptions()
                    {
                        Enabled = true,
                        HistorySize = 5,
                        LatencyThreshold = TimeSpan.FromMilliseconds(110),
                        AdjustmentPeriod = TimeSpan.FromMilliseconds(1000),
                        MaxWorkerCount = 3,
                    }),
                    new int[] { 100, 100, 100, 100, 100 },
                    new int[] { 150, 150, 150, 150, 150 },
                    true,
                    false,
                    TimeSpan.FromSeconds(2000),
                    false
                },
                new object[]
                {
                    Options.Create(new RpcWorkerConcurrencyOptions()
                    {
                        Enabled = true,
                        HistorySize = 5,
                        LatencyThreshold = TimeSpan.FromMilliseconds(110),
                        AdjustmentPeriod = TimeSpan.FromMilliseconds(1000),
                        MaxWorkerCount = 3,
                    }),
                    new int[] { 100, 100, 100 },
                    new int[] { 150, 150, 150 },
                    true,
                    false,
                    TimeSpan.FromSeconds(2000),
                    false
                },
                new object[]
                {
                    Options.Create(new RpcWorkerConcurrencyOptions()
                    {
                        Enabled = true,
                        HistorySize = 5,
                        LatencyThreshold = TimeSpan.FromMilliseconds(110),
                        AdjustmentPeriod = TimeSpan.FromMilliseconds(1000),
                        MaxWorkerCount = 3,
                    }),
                    new int[] { 100, 100, 100, 100, 100 },
                    new int[] { 150, 150, 150, 150, 150 },
                    true,
                    false,
                    TimeSpan.FromSeconds(500),
                    false
                },
                new object[]
                {
                    Options.Create(new RpcWorkerConcurrencyOptions()
                    {
                        Enabled = true,
                        HistorySize = 5,
                        LatencyThreshold = TimeSpan.FromMilliseconds(110),
                        AdjustmentPeriod = TimeSpan.FromMilliseconds(1000),
                        MaxWorkerCount = 2,
                    }),
                    new int[] { 100, 100, 100, 100, 100 },
                    new int[] { 150, 150, 150, 150, 150 },
                    true,
                    false,
                    TimeSpan.FromSeconds(2000),
                    false
                }
            };

        [Fact]
        public async Task Start_StartsTimer()
        {
            IOptions<RpcWorkerConcurrencyOptions> options = Options.Create(new RpcWorkerConcurrencyOptions()
            {
                Enabled = true
            });
            Mock<IFunctionInvocationDispatcher> functionInvocationDispatcher = new Mock<IFunctionInvocationDispatcher>();
            RpcWorkerConcurrencyManager concurrancyManger = new RpcWorkerConcurrencyManager(functionInvocationDispatcher.Object, options, _loggerFactory);
            concurrancyManger.Start();

            await TestHelpers.Await(() =>
            {
                var sratedLog = _loggerProvider.GetAllLogMessages().FirstOrDefault(x => x.FormattedMessage.StartsWith("Language worker concurancy is enabled."));
                return sratedLog != null;
            }, pollingInterval: 1000, timeout: 10 * 1000);
        }

        [Fact]
        public async Task Start_DoesNot_StartTimer()
        {
            IOptions<RpcWorkerConcurrencyOptions> options = Options.Create(new RpcWorkerConcurrencyOptions()
            {
                Enabled = false
            });
            Mock<IFunctionInvocationDispatcher> functionInvocationDispatcher = new Mock<IFunctionInvocationDispatcher>();
            RpcWorkerConcurrencyManager concurrancyManger = new RpcWorkerConcurrencyManager(functionInvocationDispatcher.Object, options, _loggerFactory);
            concurrancyManger.Start();

            await Task.Delay(1000);
            var sratedLog = _loggerProvider.GetAllLogMessages().FirstOrDefault(x => x.FormattedMessage.StartsWith("Language worker concurancy is enabled."));
            Assert.True(sratedLog == null);
        }

        [Theory]
        [MemberData(nameof(DataForIsOverloaded))]
        public void IsOverloaded_Returns_Expected(IOptions<RpcWorkerConcurrencyOptions> options, int[] latencies, bool expected)
        {
            Mock<IFunctionInvocationDispatcher> functionInvocationDispatcher = new Mock<IFunctionInvocationDispatcher>(MockBehavior.Strict);
            RpcWorkerConcurrencyManager concurrancyManger = new RpcWorkerConcurrencyManager(functionInvocationDispatcher.Object, options, _loggerFactory);
            IEnumerable<TimeSpan> list = latencies.Select(x => TimeSpan.FromMilliseconds(x));

            WorkerStatus status = new WorkerStatus()
            {
                RpcWorkerStats = new RpcWorkerStats()
                {
                    LatencyHistory = latencies.Select(x => TimeSpan.FromMilliseconds(x))
                }
            };

            Assert.Equal(concurrancyManger.IsOverloaded(status), expected);
        }

        [Theory]
        [MemberData(nameof(DataForForAddWorkerIfNeeded))]
        public void AddWorkerIfNeeded_Returns_Expected(IOptions<RpcWorkerConcurrencyOptions> options,
            int[] latencies1, int[] latencies2, bool readyForInvocations1, bool readyForInvocations2,
            TimeSpan elapsedFromLastAdding, bool expected)
        {
            Mock<IFunctionInvocationDispatcher> functionInvocationDispatcher = new Mock<IFunctionInvocationDispatcher>(MockBehavior.Strict);
            RpcWorkerConcurrencyManager concurrancyManger = new RpcWorkerConcurrencyManager(functionInvocationDispatcher.Object, options, _loggerFactory);

            List<IRpcWorkerChannel> list = new List<IRpcWorkerChannel>();

            Mock<IRpcWorkerChannel> channelMock1 = new Mock<IRpcWorkerChannel>(MockBehavior.Strict);
            Mock<IRpcWorkerConcurrencyChannel> concurrencyChannelMock1 = channelMock1.As<IRpcWorkerConcurrencyChannel>();
            channelMock1.Setup(x => x.IsChannelReadyForInvocations()).Returns(readyForInvocations1);
            channelMock1.Setup(x => x.Id).Returns("test2");
            channelMock1.Setup(x => x.FunctionInputBuffers).Returns(() => { return null; });
            concurrencyChannelMock1.Setup(x => x.GetWorkerStatus()).Returns(new WorkerStatus()
            {
                RpcWorkerStats = new RpcWorkerStats()
                {
                    LatencyHistory = latencies1.Select(x => TimeSpan.FromMilliseconds(x))
                }
            });
            list.Add(channelMock1.Object);

            Mock<IRpcWorkerChannel> channelMock2 = new Mock<IRpcWorkerChannel>();
            Mock<IRpcWorkerConcurrencyChannel> concurrencyChannelMock2 = channelMock2.As<IRpcWorkerConcurrencyChannel>();
            channelMock2.Setup(x => x.IsChannelReadyForInvocations()).Returns(readyForInvocations2);
            channelMock2.Setup(x => x.Id).Returns("test1");
            channelMock2.Setup(x => x.FunctionInputBuffers).Returns(() => { return null; });
            concurrencyChannelMock2.Setup(x => x.GetWorkerStatus()).Returns(new WorkerStatus()
            {
                RpcWorkerStats = new RpcWorkerStats()
                {
                    LatencyHistory = latencies2.Select(x => TimeSpan.FromMilliseconds(x))
                }
            });
            list.Add(channelMock2.Object);

            RpcWorkerConcurrencyManager concurrancyManager = new RpcWorkerConcurrencyManager(functionInvocationDispatcher.Object, options, _loggerFactory);

            bool value = concurrancyManager.AddWorkerIfNeeded(list, elapsedFromLastAdding);

            Assert.Equal(value, expected);
        }
    }
}
