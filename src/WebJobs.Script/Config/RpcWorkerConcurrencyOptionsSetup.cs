// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using Microsoft.Azure.WebJobs.Script.Workers.Rpc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;

namespace Microsoft.Azure.WebJobs.Script.Config
{
    internal class RpcWorkerConcurrencyOptionsSetup : IConfigureOptions<RpcWorkerConcurrencyOptions>
    {
        private readonly IConfiguration _configuration;
        private readonly IEnvironment _environment;

        public RpcWorkerConcurrencyOptionsSetup(IConfiguration configuration, IEnvironment environment)
        {
            _configuration = configuration;
            _environment = environment;
        }

        public void Configure(RpcWorkerConcurrencyOptions options)
        {
            if (bool.TryParse(_environment.GetEnvironmentVariable(RpcWorkerConstants.FunctionWorkerDynamicConcurrencyEnabledSettingName), out bool enabled))
            {
                // Do not enable convurrency if any concurrency settings are defined
                if (enabled && string.IsNullOrEmpty(_environment.GetEnvironmentVariable(RpcWorkerConstants.FunctionsWorkerProcessCountSettingName)))
                {
                    string functionWorkerRuntime = _environment.GetEnvironmentVariable(RpcWorkerConstants.FunctionWorkerRuntimeSettingName);

                    if ((functionWorkerRuntime == RpcWorkerConstants.PythonLanguageWorkerName
                        && !string.IsNullOrEmpty(_environment.GetEnvironmentVariable(RpcWorkerConstants.PythonTreadpoolThreadCount)))
                        || (functionWorkerRuntime == RpcWorkerConstants.PowerShellLanguageWorkerName
                        && !string.IsNullOrEmpty(_environment.GetEnvironmentVariable(RpcWorkerConstants.PSWorkerInProcConcurrencyUpperBound))))
                    {
                        return;
                    }

                    options.Enabled = true;
                    // Configure language worker concurrency options from IConfiguration
                    _configuration.GetSection(nameof(RpcWorkerConcurrencyOptions)).Bind(options);

                    if (options.MaxWorkerCount == 0)
                    {
                        options.MaxWorkerCount = (_environment.GetEffectiveCoresCount() * 2) + 2;
                    }
                }
            }
        }
    }
}
