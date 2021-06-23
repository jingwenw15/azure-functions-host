// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using Microsoft.Azure.WebJobs.Script.Config;
using Microsoft.Azure.WebJobs.Script.Workers.Rpc;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Microsoft.Azure.WebJobs.Script.Tests.Configuration
{
    public class RpcWorkerConcurrencyOptionsSetupTest
    {
        [Theory]
        [InlineData("true", "", "node", "", "", true)]
        [InlineData("true", "1", "node", "", "", false)]
        [InlineData("true", "", "python", "1", "1", false)]
        [InlineData("true", "", "powershell", "1", "1", false)]
        public void Configure_SetsExpectedValues(
            string functionWorkerConcurrencyEnabled,
            string functionsWorkerProcessCount,
            string functionWorkerRuntime,
            string pythonTreadpoolThreadCount,
            string pSWorkerInProcConcurrencyUpperBound,
            bool enabled)
        {
            IConfiguration config = new ConfigurationBuilder().Build();
            TestEnvironment environment = new TestEnvironment();
            environment.SetEnvironmentVariable(RpcWorkerConstants.FunctionWorkerConcurrencyEnabledSettingName, functionWorkerConcurrencyEnabled);
            environment.SetEnvironmentVariable(RpcWorkerConstants.FunctionsWorkerProcessCountSettingName, functionsWorkerProcessCount);
            environment.SetEnvironmentVariable(RpcWorkerConstants.FunctionWorkerRuntimeSettingName, functionWorkerRuntime);
            environment.SetEnvironmentVariable(RpcWorkerConstants.PythonTreadpoolThreadCount, pythonTreadpoolThreadCount);
            environment.SetEnvironmentVariable(RpcWorkerConstants.PSWorkerInProcConcurrencyUpperBound, pSWorkerInProcConcurrencyUpperBound);

            RpcWorkerConcurrencyOptionsSetup setup = new RpcWorkerConcurrencyOptionsSetup(config, environment);
            RpcWorkerConcurrencyOptions options = new RpcWorkerConcurrencyOptions();
            setup.Configure(options);

            Assert.Equal(options.Enabled, enabled);
            if (enabled)
            {
                Assert.Equal(options.MaxWorkerCount, (Environment.ProcessorCount * 2) + 2);
            }
            else
            {
                Assert.Equal(options.MaxWorkerCount, 0);
            }
        }

        [Fact]
        public void Congigure_Binds_Congiguration()
        {
            IConfiguration config = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string>
            {
                [$"{nameof(RpcWorkerConcurrencyOptions)}:MaxWorkerCount"] = "1",
                [$"{nameof(RpcWorkerConcurrencyOptions)}:LatencyThreshold"] = "00:00:03"
            })
            .Build();

            TestEnvironment environment = new TestEnvironment();
            environment.SetEnvironmentVariable(RpcWorkerConstants.FunctionWorkerConcurrencyEnabledSettingName, "true");

            RpcWorkerConcurrencyOptionsSetup setup = new RpcWorkerConcurrencyOptionsSetup(config, environment);
            RpcWorkerConcurrencyOptions options = new RpcWorkerConcurrencyOptions();
            setup.Configure(options);

            Assert.Equal(options.MaxWorkerCount, 1);
            Assert.Equal(options.LatencyThreshold, TimeSpan.FromSeconds(3));
        }
    }
}
