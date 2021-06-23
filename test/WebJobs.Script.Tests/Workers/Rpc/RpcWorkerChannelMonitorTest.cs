// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs.Script.Workers;
using Microsoft.Azure.WebJobs.Script.Workers.Rpc;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace Microsoft.Azure.WebJobs.Script.Tests.Workers.Rpc
{
    public class RpcWorkerChannelMonitorTest
    {
        [Fact]
        public async Task GetStats_StartsTimer()
        {
            Mock<IRpcWorkerChannel> channelMock = new Mock<IRpcWorkerChannel>(MockBehavior.Strict);
            Mock<IRpcWorkerConcurrencyChannel> concurrencyChannelMock = channelMock.As<IRpcWorkerConcurrencyChannel>();
            concurrencyChannelMock.Setup(x => x.GetWorkerStatusResponseAsync(TimeSpan.FromMilliseconds(200))).Returns(() =>
            {
                TimeSpan? timespan = TimeSpan.FromMilliseconds(50);
                return Task.FromResult(timespan);
            });
            var options = Options.Create(new RpcWorkerConcurrencyOptions()
            {
                Enabled = true,
                CheckInterval = TimeSpan.FromMilliseconds(100),
                LatencyThreshold = TimeSpan.FromMilliseconds(200)
            });
            var monitor = new RpcWorkerChannelMonitor(channelMock.Object, options);

            RpcWorkerStats stats = null;
            await TestHelpers.Await(() =>
            {
                stats = monitor.GetStats();
                return stats.LatencyHistory.Count() > 0;
            }, pollingInterval: 1000, timeout: 10 * 1000);

            Assert.True(stats.LatencyHistory.All(x => x.TotalMilliseconds == 50));
        }

        [Fact]
        public async Task GetStats_DoesNot_StartTimer()
        {
            Mock<IRpcWorkerChannel> channelMock = new Mock<IRpcWorkerChannel>(MockBehavior.Strict);
            Mock<IRpcWorkerConcurrencyChannel> concurrencyChannelMock = channelMock.As<IRpcWorkerConcurrencyChannel>();
            concurrencyChannelMock.Setup(x => x.GetWorkerStatusResponseAsync(TimeSpan.FromMilliseconds(200))).Returns(() =>
            {
                TimeSpan? timespan = TimeSpan.FromMilliseconds(50);
                return Task.FromResult(timespan);
            });
            var options = Options.Create(new RpcWorkerConcurrencyOptions()
            {
                Enabled = false,
                CheckInterval = TimeSpan.FromMilliseconds(100),
                LatencyThreshold = TimeSpan.FromMilliseconds(200)
            });
            var monitor = new RpcWorkerChannelMonitor(channelMock.Object, options);

            await Task.Delay(1000);
            RpcWorkerStats stats = monitor.GetStats();

            Assert.True(stats.LatencyHistory.Count() == 0);
        }
    }
}
