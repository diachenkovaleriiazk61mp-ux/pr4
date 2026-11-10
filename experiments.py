import argparse
import concurrent.futures as cf
import json
import math
import random
import statistics as st
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

K = ['kubectl', '--context', 'kind-pr4', '-n', 'default']
API = 'http://127.0.0.1:8084'
PROM = 'http://127.0.0.1:9094'
OUT = Path('results')


def run(*cmd, stdin=None):
    return subprocess.run(cmd, input=stdin, text=True, encoding='utf-8',
                          check=True, stdout=subprocess.PIPE).stdout


def k(*args):
    return run(*K, *args)


def sql(query):
    return run(*K, 'exec', '-i', 'db-0', '--', 'psql', '-XqAt', '-v', 'ON_ERROR_STOP=1',
               '-U', 'vaccinations', '-d', 'vaccinations', stdin=query + '\n').strip()


def rows(query):
    return json.loads(sql(f"SELECT coalesce(json_agg(q),'[]'::json) FROM ({query}) q;"))


def http(url, data=None, post=False):
    body = json.dumps(data).encode() if data is not None else b'' if post else None
    with urlopen(Request(url, body, {'Content-Type': 'application/json'}), timeout=10) as r:
        return r.status, json.load(r)


def prom(expr, start=None, end=None):
    params = {'query': expr}
    path = 'query'
    if start is not None:
        path = 'query_range'
        params.update(start=start, end=end, step=5)
    return http(PROM+'/api/v1/'+path+'?'+urlencode(params))[1]['data']['result']


def scalar(expr):
    values = prom(expr)
    if not values:
        return None
    value = float(values[0]['value'][1])
    return value if math.isfinite(value) else None


def save(name, data):
    OUT.mkdir(exist_ok=True)
    (OUT/(name+'.json')).write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
    print(name, 'saved', flush=True)


def seed():
    f = OUT/'records.json'
    if f.exists():
        ids = json.loads(f.read_text())
        if rows(f"SELECT count(*) n FROM vaccinations WHERE id IN ({','.join(map(str, ids))})")[0]['n'] == 100:
            return ids
    ids = []
    for n in range(100):
        _, r = http(API+'/vaccinations', dict(patient_code=f'lab4-{n:03}', vaccine='TEST',
            batch='PR4', dose_number=1, administration_date='2024-01-01', facility='PR4'))
        ids.append(r['id'])
    save('records', ids)
    return ids


def pods():
    return json.loads(k('get','pods','-l','app=worker','-o','json'))['items']


def scale(n):
    k('scale','deployment/worker',f'--replicas={n}')
    if n:
        k('rollout','status','deployment/worker','--timeout=240s')
    until = time.monotonic()+240
    while time.monotonic()<until:
        p = pods()
        if len(p)==n and all(not x['metadata'].get('deletionTimestamp') and
            any(c['type']=='Ready' and c['status']=='True' for c in x['status'].get('conditions',[])) for x in p):
            return
        time.sleep(.5)
    raise RuntimeError('Worker replicas not ready')


def prepare(n, mode='safe', poll=10, fail=0):
    scale(0)
    sql('''BEGIN;
      TRUNCATE task_events, task_results, tasks;
      ALTER TABLE task_results DROP CONSTRAINT IF EXISTS task_results_pkey;
      DROP INDEX IF EXISTS task_results_task_idx;
      ''' + ('CREATE INDEX task_results_task_idx ON task_results(task_id);' if mode=='naive'
             else 'ALTER TABLE task_results ADD PRIMARY KEY(task_id);') + 'COMMIT;')
    k('set','env','deployment/worker',f'CLAIM_MODE={mode}',f'T_POLL_MS={poll}',f'FAIL_RATE={fail}')
    if n:
        scale(n)


def offer(ids, rate, count, delays=None):
    def submit(i, target):
        delay = target-time.monotonic()
        if delay>0:
            time.sleep(delay)
        url = f'{API}/vaccinations/{ids[i%len(ids)]}/tasks'
        if delays:
            after = datetime.fromtimestamp(time.time()+delays[i], timezone.utc).isoformat()
            url += '?'+urlencode({'run_after': after})
        try:
            status, row = http(url, post=True)
            return dict(status=status, id=row['id'])
        except HTTPError as e:
            return dict(status=e.code)
        except OSError as e:
            return dict(status=None, error=type(e).__name__)
    start = time.monotonic()
    with cf.ThreadPoolExecutor(max_workers=48) as ex:
        futures = []
        for i in range(count):
            target = start+i/rate
            time.sleep(max(0,target-time.monotonic()-.003))
            futures.append(ex.submit(submit,i,target))
        return [f.result() for f in futures]


def drained(timeout=600):
    until = time.monotonic()+timeout
    while time.monotonic()<until:
        if sql("SELECT count(*) FROM tasks WHERE status IN ('queued','running');")=='0':
            return
        time.sleep(.5)
    raise RuntimeError('Queue did not drain')


def snapshot():
    tasks = rows("""SELECT t.*,
      extract(epoch from started_at-created_at) AS wait_s,
      extract(epoch from started_at-run_after) AS delay_s,
      (SELECT count(*) FROM task_results r WHERE r.task_id=t.id) AS results
      FROM tasks t ORDER BY id""")
    events = rows('SELECT * FROM task_events ORDER BY ts')
    return dict(tasks=tasks,events=events,duplicates=sum(t['results']>1 for t in tasks),
                failed=sum(t['status']=='failed' for t in tasks),
                retried=sum(t['attempts']>1 for t in tasks))


def logs():
    result=[]
    for pod in pods():
        for line in k('logs',pod['metadata']['name']).splitlines():
            try:
                result.append(json.loads(line))
            except ValueError:
                pass
    return result


def claim_p99(window='2m'):
    return scalar(f'histogram_quantile(0.99, sum by (le) (increase(worker_task_duration_seconds_bucket{{step="claim"}}[{window}])))')


def contention(ids):
    for mode, poll in [('naive',10),('naive',1),('skip',1),('blocking',1)]:
        prepare(4, mode, poll)
        locks=[]
        stop=threading.Event()

        def sample():
            while not stop.is_set():
                t=time.monotonic()
                locks.append(int(sql("SELECT count(*) FROM pg_stat_activity WHERE wait_event_type='Lock';")))
                stop.wait(max(0,1-(time.monotonic()-t)))

        time.sleep(6)
        empty_before=scalar('sum(worker_claims_total{result="empty"})')
        buckets_before=prom('sum by (le) (worker_task_duration_seconds_bucket{step="claim"})')
        thread=threading.Thread(target=sample) if mode=='blocking' else None
        if thread:
            thread.start()
        try:
            posts=offer(ids,100,300)
            drained()
        finally:
            stop.set()
            if thread:
                thread.join()
        time.sleep(6)
        result=snapshot()
        journal=logs()
        done={}
        for line in journal:
            if line.get('event')=='done':
                done.setdefault(line['task_id'],set()).add(line['worker_id'])
        after=scalar('sum(worker_claims_total{result="empty"})')
        result.update(posts=posts, D_db=result['duplicates']/300,
                      D_logs=sum(len(w)>1 for w in done.values())/300, journal=journal,
                      locks=locks, max_locks=max(locks,default=None),
                      claim_p99_s=claim_p99(), buckets_before=buckets_before,
                      buckets_after=prom('sum by (le) (worker_task_duration_seconds_bucket{step="claim"})'),
                      empty_claims=after-empty_before if after is not None and empty_before is not None else None)
        save(f'{mode}-{poll}ms',result)
        if len(result['tasks'])!=300:
            raise RuntimeError('Not all 300 tasks were created')
    prepare(1)


def warm(ids):
    prepare(1)
    posts=offer(ids,20,20)
    drained()
    result=snapshot()
    done=[e for e in logs() if e.get('event')=='done']
    result.update(posts=posts,journal=done,t_io=st.mean(e['t_io'] for e in done),
                  t_cpu=st.mean(e['t_cpu'] for e in done))
    save('warm20',result)


def active():
    return rows("SELECT id,worker_id,attempts,token,lease_until FROM tasks WHERE status='running' ORDER BY started_at")


def freeze(ids):
    prepare(2)
    with cf.ThreadPoolExecutor() as ex:
        load=ex.submit(offer,ids,100,100)
        until=time.monotonic()+6
        chosen=None
        while time.monotonic()<until and not chosen:
            live=active()
            if live:
                chosen=live[0]
        if chosen is None:
            save('freeze-missed',dict(posts=load.result()))
            raise RuntimeError('Could not observe an active task; repeat freeze')
        pod=chosen['worker_id']
        target = json.loads(k('get', 'pod', pod, '-o', 'json'))
        node = target['spec']['nodeName']
        cid = target['status']['containerStatuses'][0]['containerID'].split('://')[-1]
        info = json.loads(run('docker', 'exec', node, 'crictl', 'inspect', cid))
        pid = int(info['info']['pid'])
        if pid <= 1:
            raise RuntimeError('Invalid worker PID')
        run('docker', 'exec', node, 'kill', '-STOP', str(pid))
        try:
            held=[r for r in active() if r['worker_id']==pod]
            time.sleep(20)
        finally:
            run('docker', 'exec', node, 'kill', '-CONT', str(pid))
        posts=load.result()
    drained()
    result=snapshot()
    result.update(posts=posts,before_stop=chosen,held_after_stop=held,journal=logs())
    save('freeze',result)
    if not any(e.get('event')=='lost' and e.get('task_id') in {r['id'] for r in held}
               for e in result['journal']):
        print('No verified stale completion: repeat freeze if needed.',flush=True)


def capacity(ids):
    results={}
    for n in (1,2,4):
        prepare(0)
        posts=offer(ids,100,400)
        if any(p['status']!=202 for p in posts):
            raise RuntimeError('Expected 400 queued tasks')
        launched=time.time()
        scale(n)
        drained()
        finished=time.time()
        data=snapshot()
        timing=rows("""SELECT extract(epoch FROM min(started_at)) AS first,
                    extract(epoch FROM max(finished_at)) AS last FROM tasks""")[0]
        stop=math.floor(timing['last'])
        start=max(math.ceil(timing['first']), stop-30)
        counts={s:0 for s in range(start,stop)}
        for t in data['tasks']:
            if t['status']=='done':
                s=int(epoch(t['finished_at']))
                if s in counts:
                    counts[s]+=1
        seconds=list(counts.values())
        data.update(posts=posts, workers=n, X=st.mean(seconds) if seconds else None,
                    std=st.stdev(seconds) if len(seconds)>1 else None,
                    window_seconds=len(seconds), second_counts=seconds,
                    claim_p99_s=claim_p99(), prometheus={key:prom(expr,launched,finished)
                        for key,expr in {
                            'db_tps':'sum(rate(pg_stat_database_xact_commit{datname="vaccinations"}[30s])) + sum(rate(pg_stat_database_xact_rollback{datname="vaccinations"}[30s]))',
                            'db_cpu':'sum(rate(container_cpu_usage_seconds_total{pod="db-0",container="postgres"}[30s]))'
                        }.items()})
        save(f'capacity-{n}',data)
        results[str(n)]={key:data[key] for key in ('X','std','window_seconds')}
    save('capacity',results)


def xr():
    return json.loads((OUT/'capacity.json').read_text())['4']['X']


def epoch(value):
    import re
    value = re.sub(r'(\.\d{1,6})(?=[+-]|$)',
                   lambda m: m[1].ljust(7, '0'), value)
    return datetime.fromisoformat(value).timestamp()


def percentile(values,p):
    return sorted(values)[max(0,math.ceil(len(values)*p)-1)] if values else None


def flow(ids, name, multiplier, seconds, event=None, fail=0):
    prepare(4,fail=fail)
    rate=multiplier*xr()
    start=time.time()
    event_data={}
    with cf.ThreadPoolExecutor() as ex:
        pending=ex.submit(offer,ids,rate,math.ceil(seconds*rate))
        if event:
            time.sleep(seconds/2)
            event_data=event()
        time.sleep(max(0,start+seconds-time.time()))
        tail=rows("SELECT count(*) AS queued FROM tasks WHERE status='queued'")[0]
        posts=pending.result()
    end=start+seconds
    time.sleep(6)
    series={s:prom(f'max(tasks_{s})',start,end) for s in ('queued','running')}
    restarts=[{'pod':p['metadata']['name'],'statuses':p['status'].get('containerStatuses',[])} for p in pods()]
    if name == 'f1':
        save('f1-window', dict(start=start, end=end, event=event_data,
             posts=posts, series=series, queue_at_end=tail, restarts=restarts,
             snapshot=snapshot()))
        k('set', 'env', 'deployment/worker', 'FAIL_RATE=0')
        k('rollout', 'status', 'deployment/worker', '--timeout=240s')
    drained()
    data=snapshot()
    data.update(start=start,end=end,event=event_data,posts=posts,series=series,queue_at_end=tail,
                restarts=restarts, completed_per_second=[0]*seconds,
                attempts={n:sum(t['attempts']==n for t in data['tasks']) for n in range(1,4)})
    for t in data['tasks']:
        if t['status']=='done' and start<=epoch(t['finished_at'])<end:
            data['completed_per_second'][int(epoch(t['finished_at'])-start)]+=1
    data['X']=sum(data['completed_per_second'])/seconds
    if name=='little':
        a=end-120
        window=[t for t in data['tasks'] if a<=epoch(t['created_at'])<end]
        gauge=[float(v) for r in series['queued'] for ts,v in r['values'] if ts>=a]
        lam=len(window)/120
        wait=st.mean(t['wait_s'] for t in window)
        lq=st.mean(gauge) if gauge else None
        data.update(lam=lam, Wq=wait, Lq=lq, predicted=lam*wait,
                    error_pct=100*abs(lam*wait-lq)/lq if lq else None)
    if name=='overload':
        tasks=data['tasks']
        data.update(first100_s=st.mean(t['wait_s'] for t in tasks[:100]),
                    last100_s=st.mean(t['wait_s'] for t in tasks[-100:]))
    if name=='f1':
        total=len(data['tasks'])
        data.update(expected_retry=.15,expected_failed=.15**3,
                    retry_fraction=data['retried']/total,failed_fraction=data['failed']/total)
    save(name,data)


def rollout():
    before = pods()
    exits = []

    def watch(pod, node, ident):
        deadline = time.monotonic() + 240
        while time.monotonic() < deadline:
            r = subprocess.run(
                ['docker', 'exec', node, 'crictl', 'inspect', ident],
                capture_output=True, text=True, timeout=10
            )
            if r.returncode != 0:
                break
            status = json.loads(r.stdout)['status']
            if status['state'] == 'CONTAINER_EXITED':
                return dict(pod=pod, exit_code=status.get('exitCode'),
                            state=status['state'])
            time.sleep(.1)
        return dict(pod=pod, exit_code=None, state='not_observed')

    with cf.ThreadPoolExecutor(max_workers=4) as pool:
        checks = []
        for p in before:
            pod = p['metadata']['name']
            statuses = p['status'].get('containerStatuses', [])
            if not statuses or not statuses[0].get('containerID'):
                exits.append(dict(pod=pod, exit_code=None, state='not_observed'))
                continue
            ident = statuses[0]['containerID'].split('://')[-1]
            checks.append((pod, pool.submit(watch, pod, p['spec']['nodeName'], ident)))
        start = time.time()
        k('rollout', 'restart', 'deployment/worker')
        k('rollout', 'status', 'deployment/worker', '--timeout=240s')
        for pod, check in checks:
            try:
                exits.append(check.result())
            except (subprocess.SubprocessError, KeyError, ValueError):
                exits.append(dict(pod=pod, exit_code=None, state='not_observed'))

    print(json.dumps(exits, indent=2))
    return dict(at=start, exits=exits)


def force_delete():
    until = time.monotonic() + 45
    while time.monotonic() < until:
        for target in pods():
            pod = target['metadata']['name']
            node = target['spec']['nodeName']
            statuses = target['status'].get('containerStatuses', [])
            if not statuses or 'running' not in statuses[0].get('state', {}):
                continue
            cid = statuses[0]['containerID'].split('://')[-1]
            try:
                info = json.loads(run('docker', 'exec', node, 'crictl', 'inspect', cid))
                pid = int(info['info']['pid'])
                if pid <= 1:
                    continue
                run('docker', 'exec', node, 'kill', '-STOP', str(pid))
            except (subprocess.CalledProcessError, KeyError, ValueError):
                continue

            deleted = False
            try:
                row = next((r for r in active()
                            if r['worker_id'] == pod
                            and epoch(r['lease_until']) > time.time() + 2), None)
                if row is None:
                    continue
                at = time.time()
                k('delete', 'pod', pod, '--grace-period=0', '--force', '--wait=false')
                deleted = True
                return dict(at=at, candidate=row, paused_before_delete=True)
            finally:
                if not deleted:
                    try:
                        run('docker', 'exec', node, 'kill', '-CONT', str(pid))
                    except subprocess.CalledProcessError:
                        pass
        time.sleep(.25)
    return dict(error='No worker with an unexpired lease was caught')


def delayed(ids):
    prepare(4)
    rng=random.Random(7)
    delays=[rng.uniform(10,60) for _ in range(100)]
    posts=offer(ids,25,100,delays=delays)
    drained()
    data=snapshot()
    values=[t['delay_s'] for t in data['tasks'] if t['delay_s'] is not None]
    data.update(posts=posts,p50_s=percentile(values,.5),p99_s=percentile(values,.99),
                early=sum(x<0 for x in values))
    save('d2',data)


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('stage',choices=['warm','locks','freeze','capacity','little','overload','rollout','f1','d2'])
    stage=parser.parse_args().stage
    ids=seed()
    commands=dict(warm=warm,locks=contention,freeze=freeze,capacity=capacity,d2=delayed)
    if stage in commands:
        commands[stage](ids)
    else:
        multiplier,seconds,event,fail={
            'little':(.7,180,None,0),'overload':(1.3,120,None,0),
            'rollout':(.7,180,rollout,0),'f1':(.7,300,force_delete,.15)
        }[stage]
        try:
            flow(ids,stage,multiplier,seconds,event,fail)
        finally:
            if fail:
                k('set','env','deployment/worker','FAIL_RATE=0')
                k('rollout','status','deployment/worker','--timeout=240s')


if __name__=='__main__':
    main()
