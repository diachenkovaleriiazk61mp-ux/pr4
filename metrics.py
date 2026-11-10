import time
import os
from starlette.responses import JSONResponse
from pathlib import Path
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, ProcessCollector, PlatformCollector
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily

BUCKETS = (.005, .010, .025, .050, .100, .250, .500, 1.0)

class CgroupCPU:
    def __init__(self, paths=None):
        self.paths = paths or [Path('/sys/fs/cgroup/cpu.stat'), Path('/sys/fs/cgroup/cpu,cpuacct/cpu.stat')]

    def collect(self):
        values = None
        for path in self.paths:
            try:
                values = dict(line.split() for line in path.read_text().splitlines())
                if 'nr_periods' in values and 'nr_throttled' in values:
                    break
                values = None
            except (OSError, ValueError):
                continue
        yield GaugeMetricFamily('cgroup_cpu_stat_available', 'CPU quota counters available', value=int(values is not None))
        if values is not None:
            for key in ('nr_periods', 'nr_throttled'):
                yield CounterMetricFamily('cgroup_cpu_' + key + '_total', 'CPU cgroup ' + key, value=int(values[key]))

class Metrics:
    def __init__(self, pod):
        self.registry = CollectorRegistry()
        ProcessCollector(registry=self.registry)
        PlatformCollector(registry=self.registry)
        self.registry.register(CgroupCPU())
        labels = ('method', 'path', 'code')
        self.requests = Counter('http_requests_total', 'Completed HTTP responses', labels, registry=self.registry)
        self.duration = Histogram('http_request_duration_seconds', 'Server request processing time', labels,
                                  buckets=BUCKETS, registry=self.registry)
        self.inflight = Gauge('http_requests_in_progress', 'Requests currently being processed', ('path',), registry=self.registry)
        self.info = Gauge('service_info', 'Service instance identity', ('pod',), registry=self.registry)
        self.info.labels(pod).set(1)

        self.requests.labels('GET', '/vaccinations/{id}', '200')
        self.duration.labels('GET', '/vaccinations/{id}', '200')
        self.inflight.labels('/vaccinations/{id}').set(0)

class Instrumentation:
    def __init__(self, app, metrics, pod):
        self.app, self.metrics, self.pod = app, metrics, pod
        self.workload_active=0
        self.workload_limit=int(os.environ.get("MAX_INFLIGHT_REQUESTS", "64"))
        if self.workload_limit<1:raise ValueError("MAX_INFLIGHT_REQUESTS must be positive")

    async def __call__(self, scope, receive, send):
        if scope['type'] != 'http':
            return await self.app(scope, receive, send)
        raw = scope['path']
        if raw.startswith('/vaccinations/'):
            path = '/vaccinations/{id}'
        elif raw in ('/vaccinations', '/healthz', '/livez', '/metrics'):
            path = raw
        else:
            path = 'unmatched'
        observed = path != '/metrics'
        start, status = time.perf_counter(), 500
        if observed:
            self.metrics.inflight.labels(path).inc()
        async def wrapped(message):
            nonlocal status
            if message['type'] == 'http.response.start':
                status = message['status']
                message['headers'] = list(message.get('headers', [])) + [
                    (b'x-pod', self.pod.encode()), (b'x-instance-id', self.pod.encode())]
            await send(message)
        workload=path in ('/vaccinations','/vaccinations/{id}')
        admitted=workload and self.workload_active<self.workload_limit
        if admitted:self.workload_active+=1
        try:
            if workload and not admitted:
                response=JSONResponse({'errors': {'capacity': 'Черга запитів заповнена'}},status_code=503)
                await response(scope,receive,wrapped)
            else:
                await self.app(scope, receive, wrapped)
        finally:
            if admitted:self.workload_active-=1
            if observed:
                self.metrics.inflight.labels(path).dec()
                self.metrics.requests.labels(scope['method'], path, str(status)).inc()
                self.metrics.duration.labels(scope['method'], path, str(status)).observe(time.perf_counter()-start)
