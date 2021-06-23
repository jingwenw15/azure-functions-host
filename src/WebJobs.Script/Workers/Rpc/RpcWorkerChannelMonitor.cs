// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using Microsoft.Extensions.Options;

namespace Microsoft.Azure.WebJobs.Script.Workers.Rpc
{
    internal class RpcWorkerChannelMonitor : IDisposable
    {
        private readonly List<TimeSpan> _workerStatusLatecyHistory = new List<TimeSpan>();
        private readonly IOptions<RpcWorkerConcurrencyOptions> _concurrencyOptions;

        private IRpcWorkerChannel _channel;
        private object _syncLock = new object();

        private System.Timers.Timer _timer;
        private bool _disposed = false;

        internal RpcWorkerChannelMonitor(IRpcWorkerChannel channel, IOptions<RpcWorkerConcurrencyOptions> concurrencyOptions)
        {
            _channel = channel;
            _concurrencyOptions = concurrencyOptions;
        }

        internal void EnsureTimerStarted()
        {
            if (_timer == null && _concurrencyOptions.Value.Enabled)
            {
                lock (_syncLock)
                {
                    if (_timer == null)
                    {
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

        public RpcWorkerStats GetStats()
        {
            EnsureTimerStarted();

            RpcWorkerStats stats = null;
            lock (_syncLock)
            {
                stats = new RpcWorkerStats()
                {
                    LatencyHistory = _workerStatusLatecyHistory
                };
            }
            return stats;
        }

        internal async void OnTimer(object sender, System.Timers.ElapsedEventArgs e)
        {
            if (_disposed)
            {
                return;
            }

            try
            {
                if (_channel is IRpcWorkerConcurrencyChannel concurrencyChannel)
                {
                    TimeSpan? latency = await concurrencyChannel.GetWorkerStatusResponseAsync(_concurrencyOptions.Value.LatencyThreshold);
                    if (latency != null)
                    {
                        AddSample(_workerStatusLatecyHistory, latency.Value);
                    }
                }
            }
            catch
            {
                // Don't allow backround execptions to escape
                // E.g. when a rpc channel is shutting down we can process exceptions
            }
            _timer.Start();
        }

        private void AddSample<T>(List<T> samples, T sample)
        {
            lock (_syncLock)
            {
                if (samples.Count == _concurrencyOptions.Value.HistorySize)
                {
                    samples.RemoveAt(0);
                }
                samples.Add(sample);
            }
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
    }
}
