// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Microsoft.Azure.WebJobs.Script.Workers.Rpc
{
    internal class RpcWorkerConcurrencyManager : IDisposable
    {
        private readonly TimeSpan _logStateInterval = TimeSpan.FromSeconds(60);
        private readonly IOptions<RpcWorkerConcurrencyOptions> _concurrencyOptions;
        private readonly ILogger _logger;

        private RpcFunctionInvocationDispatcher _dispatcher;
        private System.Timers.Timer _timer;
        private Stopwatch _addWorkeStopWatch = Stopwatch.StartNew();
        private Stopwatch _logStateStopWatch = Stopwatch.StartNew();
        private bool _disposed = false;
        private object syncObj = new object();

        public RpcWorkerConcurrencyManager(IFunctionInvocationDispatcher functionInvocationDispatcher,
            IOptions<RpcWorkerConcurrencyOptions> concurrencyOptions, ILoggerFactory loggerFactory)
        {
            _concurrencyOptions = concurrencyOptions ?? throw new ArgumentNullException(nameof(concurrencyOptions));

            _logger = loggerFactory?.CreateLogger<RpcWorkerConcurrencyManager>();
            _dispatcher = functionInvocationDispatcher as RpcFunctionInvocationDispatcher;
        }

        public void Start()
        {
            if (_timer == null && _concurrencyOptions.Value.Enabled)
            {
                lock (syncObj)
                {
                    if (_timer == null)
                    {
                        _logger.LogDebug($"Language worker concurancy is enabled. Options: {_concurrencyOptions.Value.Format()}");
                        _timer = new System.Timers.Timer()
                        {
                            AutoReset = false,
                            Interval = _concurrencyOptions.Value.CheckInterval.TotalMilliseconds,
                        };

                        _timer.Elapsed += OnTimer;
                        _timer.Start();
                    }
                }
            }
        }

        internal async void OnTimer(object sender, System.Timers.ElapsedEventArgs e)
        {
            if (_disposed)
            {
                return;
            }
            try
            {
                IEnumerable<IRpcWorkerChannel> workerChannels = await _dispatcher.GetAllWorkerChannelsAsync();

                if (AddWorkerIfNeeded(workerChannels, _addWorkeStopWatch.Elapsed))
                {
                    await _dispatcher.RestartWorkerChannel(null);
                    _logger.LogDebug("New worker is added.");
                    _addWorkeStopWatch.Restart();
                }
            }
            catch (Exception ex)
            {
                // don't allow background exceptions to escape
                _logger.LogError(ex.ToString());
            }
            _timer.Start();
        }

        internal bool AddWorkerIfNeeded(IEnumerable<IRpcWorkerChannel> workerChannels, TimeSpan elaspsedFromLastAdding)
        {
            if (elaspsedFromLastAdding < _concurrencyOptions.Value.AdjustmentPeriod)
            {
                return false;
            }

            bool result = false;
            IEnumerable<IRpcWorkerChannel> initializedWorkers = workerChannels.Where(ch => ch.IsChannelReadyForInvocations());

            // Check if there are initializing language workers
            int notInitializedWorkersCount = workerChannels.Count() - initializedWorkers.Count();

            if (notInitializedWorkersCount == 0)
            {
                // Check how many channels are oveloaded
                List<WorkerDescription> descriptions = new List<WorkerDescription>();
                foreach (IRpcWorkerChannel channel in initializedWorkers)
                {
                    if (channel is IRpcWorkerConcurrencyChannel concurrencyChannel)
                    {
                        WorkerStatus workerStatus = concurrencyChannel.GetWorkerStatus();
                        bool overloaded = IsOverloaded(workerStatus);
                        descriptions.Add(new WorkerDescription()
                        {
                            Channel = channel,
                            WorkerStatus = workerStatus,
                            Overloaded = overloaded
                        });
                    }
                }

                int overloadedCount = descriptions.Where(x => x.Overloaded == true).Count();
                if (overloadedCount > 0)
                {
                    if (initializedWorkers.Count() < _concurrencyOptions.Value.MaxWorkerCount)
                    {
                        _logger.LogDebug($"Adding a new worker, overloaded workers = {overloadedCount}, initialized workers = {initializedWorkers.Count()} ");
                        result = true;
                    }
                }

                if (result == true || _logStateStopWatch.Elapsed > _logStateInterval)
                {
                    StringBuilder sb = new StringBuilder();
                    foreach (WorkerDescription description in descriptions)
                    {
                        sb.Append(LogWorkerState(description));
                        sb.Append(Environment.NewLine);
                    }
                    _logStateStopWatch.Restart();
                    _logger.LogDebug(sb.ToString());
                }
            }

            return result;
        }

        internal bool IsOverloaded(WorkerStatus status)
        {
            if (status.RpcWorkerStats.LatencyHistory.Count() >= _concurrencyOptions.Value.HistorySize)
            {
                int overloadedCount = status.RpcWorkerStats.LatencyHistory.Where(x => x.TotalMilliseconds > _concurrencyOptions.Value.LatencyThreshold.TotalMilliseconds).Count();
                double proportion = (double)overloadedCount / _concurrencyOptions.Value.HistorySize;

                return proportion >= _concurrencyOptions.Value.HistoryThreshold;
            }
            return false;
        }

        internal string LogWorkerState(WorkerDescription desc)
        {
            string formattedLoadHistory = string.Empty, formattedLatencyHistory = string.Empty;
            double cpuAvg = 0, cpuMax = 0, latencyAvg = 0, latencyMax = 0;
            if (desc.WorkerStatus != null)
            {
                if (desc.WorkerStatus.ProcessStats != null && desc.WorkerStatus.ProcessStats.CpuLoadHistory != null)
                {
                    formattedLatencyHistory = string.Join(",", desc.WorkerStatus.ProcessStats.CpuLoadHistory);
                    cpuMax = desc.WorkerStatus.ProcessStats.CpuLoadHistory.Max();
                    if (desc.WorkerStatus.ProcessStats.CpuLoadHistory.Count() > 1)
                    {
                        cpuAvg = desc.WorkerStatus.ProcessStats.CpuLoadHistory.Average();
                    }
                }
                if (desc.WorkerStatus.RpcWorkerStats != null && desc.WorkerStatus.RpcWorkerStats.LatencyHistory != null)
                {
                    formattedLatencyHistory = string.Join(",", desc.WorkerStatus.RpcWorkerStats.LatencyHistory);
                    latencyMax = desc.WorkerStatus.RpcWorkerStats.LatencyHistory.Select(x => x.TotalMilliseconds).Max();
                    if (desc.WorkerStatus.RpcWorkerStats.LatencyHistory.Count() > 1)
                    {
                        latencyAvg = desc.WorkerStatus.RpcWorkerStats.LatencyHistory.Select(x => x.TotalMilliseconds).Average();
                    }
                }
            }
            string executingFunctionsCount = desc.Channel.FunctionInputBuffers != null ? desc.Channel.FunctionInputBuffers.Sum(p => p.Value.Count).ToString() : string.Empty;

            return $@"Worker process stats: ProcessId={desc.Channel.Id}, Overloaded={desc.Overloaded},ExecutioningFunctions = {executingFunctionsCount}
CpuLoadHistory=({formattedLoadHistory}), c={cpuAvg}, MaxLoad={cpuMax}, 
LatencyHistory=({formattedLatencyHistory}), AvgLatency={latencyAvg}, MaxLatency={latencyMax}";
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    _timer?.Dispose();
                }

                _disposed = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }

        internal class WorkerDescription
        {
            public IRpcWorkerChannel Channel { get; set; }

            public WorkerStatus WorkerStatus { get; set; }

            public bool Overloaded { get; set; }
        }
    }
}
