import json
import os
import random
import signal
import threading
import time
from urllib.request import urlopen

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from slice_processing import process

DSN = os.environ['DATABASE_URL']
WEB = os.getenv('WEB_URL', 'http://web:8000')
WORKER = os.getenv('WORKER_ID', 'worker-local')
MODE = os.getenv('CLAIM_MODE', 'safe')
POLL = float(os.getenv('T_POLL_MS', '10')) / 1000
LEASE = float(os.getenv('LEASE_SECONDS', '10'))
FAIL = float(os.getenv('FAIL_RATE', '0'))
STOP = threading.Event()
DEADLINE = float('inf')
TASKS = Counter('worker_tasks_total', 'Task outcomes', ['result'])
CLAIMS = Counter('worker_claims_total', 'Claim outcomes', ['result'])
DURATION = Histogram('worker_task_duration_seconds', 'Step duration', ['step'],
                     buckets=(.0001,.00025,.0005,.001,.0025,.005,.01,.025,.05,.1,.25,.5,1,2,5,10))
FLIGHT = Gauge('worker_tasks_in_flight', 'Tasks in progress')
for label in ('done', 'failed', 'lost'):
    TASKS.labels(label)
for label in ('task', 'empty'):
    CLAIMS.labels(label)
for label in ('claim', 'fetch', 'process', 'store'):
    DURATION.labels(label)


def log(event, **fields):
    print(json.dumps(dict(event=event, worker_id=WORKER, **fields)), flush=True)


def connect():
    return psycopg.connect(DSN, autocommit=True, row_factory=dict_row, connect_timeout=3,
                           application_name=WORKER, options='-c statement_timeout=3000')


def stop(*_):
    global DEADLINE
    DEADLINE = min(DEADLINE, time.monotonic()+25)
    STOP.set()


def record(c, t, kind):
    c.execute('INSERT INTO task_events(task_id,token,worker_id,kind) VALUES (%s,%s,%s,%s)',
              (t['id'], t['token'], WORKER, kind))


def reap(c):
    for row in c.execute("""
        WITH expired AS (
            SELECT id,worker_id FROM tasks WHERE status='running' AND lease_until <= clock_timestamp()
            FOR UPDATE SKIP LOCKED
        ), moved AS (
            UPDATE tasks t SET status=CASE WHEN attempts >= max_attempts THEN 'failed' ELSE 'queued' END,
              finished_at=CASE WHEN attempts >= max_attempts THEN clock_timestamp() END,
              worker_id=NULL, lease_until=NULL
            FROM expired e WHERE t.id=e.id RETURNING t.id,t.token,t.status,e.worker_id
        )
        INSERT INTO task_events(task_id,token,worker_id,kind)
        SELECT id,token,worker_id,CASE WHEN status='failed' THEN 'failed' ELSE 'expired' END FROM moved
        RETURNING kind
    """):
        if row['kind'] == 'failed':
            TASKS.labels('failed').inc()


def claim(c):
    eligible = "status='queued' AND (run_after IS NULL OR run_after <= clock_timestamp())"
    set_values = """status='running',worker_id=%s,token=token+1,attempts=attempts+1,
                    started_at=clock_timestamp(),lease_until=clock_timestamp()+%s*interval '1 second'"""
    if MODE == 'naive':
        row = c.execute(f'SELECT id FROM tasks WHERE {eligible} ORDER BY created_at,id LIMIT 1').fetchone()
        if row is None or STOP.is_set():
            return None
        return c.execute(f'UPDATE tasks SET {set_values} WHERE id=%s RETURNING *',
                         (WORKER, LEASE, row['id'])).fetchone()
    lock = 'FOR UPDATE' if MODE == 'blocking' else 'FOR UPDATE SKIP LOCKED'
    return c.execute(f"""UPDATE tasks SET {set_values}
        WHERE id=(SELECT id FROM tasks WHERE {eligible} ORDER BY created_at,id LIMIT 1 {lock})
        RETURNING *""", (WORKER, LEASE)).fetchone()


def heartbeat(t, ended):
    if ended.wait(LEASE / 3):
        return
    try:
        with connect() as c:
            while not ended.is_set():
                row = c.execute("""UPDATE tasks SET lease_until=clock_timestamp()+%s*interval '1 second'
                    WHERE id=%s AND token=%s AND status='running' AND lease_until>clock_timestamp()""",
                    (LEASE,t['id'],t['token']))
                if row.rowcount == 0:
                    log('heartbeat_zero_rows', task_id=t['id'], token=t['token'])
                    return
                if ended.wait(LEASE / 3):
                    return
    except psycopg.Error as e:
        log('heartbeat_error', task_id=t['id'], error=type(e).__name__)


def return_task(c, t):
    with c.transaction():
        row = c.execute("""UPDATE tasks SET status='queued',worker_id=NULL,lease_until=NULL
            WHERE id=%s AND token=%s AND status='running' AND lease_until>clock_timestamp()""",
            (t['id'], t['token']))
        if row.rowcount:
            record(c,t,'returned')


def store(c, t, result):
    with c.transaction():
        if MODE == 'naive':
            c.execute("UPDATE tasks SET status='done',finished_at=clock_timestamp(),lease_until=NULL WHERE id=%s",
                      (t['id'],))
            suffix = ''
        else:
            row = c.execute("""UPDATE tasks SET status='done',finished_at=clock_timestamp(),lease_until=NULL
                WHERE id=%s AND token=%s AND status='running' AND lease_until>clock_timestamp()""",
                (t['id'], t['token']))
            if row.rowcount == 0:
                record(c,t,'lost')
                return False
            suffix = ' ON CONFLICT (task_id) DO NOTHING'
        c.execute('INSERT INTO task_results(task_id,token,worker_id,result) VALUES (%s,%s,%s,%s)' + suffix,
                  (t['id'],t['token'],WORKER,Jsonb(result)))
        record(c,t,'done')
    return True


def execute(c, t):
    ended = threading.Event()
    thread = None
    if MODE == 'safe':
        thread = threading.Thread(target=heartbeat,args=(t,ended),daemon=True)
        thread.start()
    FLIGHT.set(1)
    log('claimed', task_id=t['id'], token=t['token'], attempts=t['attempts'])
    try:
        start = time.perf_counter()
        with urlopen(f"{WEB}/vaccinations/{t['record_id']}", timeout=3) as response:
            response.read()
        fetched = time.perf_counter()
        DURATION.labels('fetch').observe(fetched-start)
        result = process(t['seed'])
        computed = time.perf_counter()
        DURATION.labels('process').observe(computed-fetched)
        if random.random() < FAIL:
            log('crash',task_id=t['id'],token=t['token'])
            os._exit(1)
        if time.monotonic() >= DEADLINE:
            return_task(c,t)
            return
        success = store(c,t,result)
        stored = time.perf_counter()
        DURATION.labels('store').observe(stored-computed)
        outcome = 'done' if success else 'lost'
        TASKS.labels(outcome).inc()
        log(outcome, task_id=t['id'],token=t['token'],
            updated_rows=int(success),t_io=(fetched-start)+(stored-computed),t_cpu=computed-fetched)
    finally:
        ended.set()
        if thread:
            thread.join(timeout=4)
        FLIGHT.set(0)


def main():
    if MODE not in ('safe','naive','blocking','skip') or not (0<=FAIL<=1 and POLL>0 and LEASE>0):
        raise ValueError('Invalid worker configuration')
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    start_http_server(9100)
    while not STOP.is_set():
        try:
            with connect() as c:
                next_reap = 0
                while not STOP.is_set():
                    if MODE == 'safe' and time.monotonic() >= next_reap:
                        reap(c)
                        next_reap = time.monotonic()+.25
                    if STOP.is_set():
                        break
                    with DURATION.labels('claim').time():
                        t = claim(c)
                    CLAIMS.labels('task' if t else 'empty').inc()
                    if not t:
                        STOP.wait(POLL)
                        continue
                    if STOP.is_set():
                        return_task(c,t)
                    else:
                        execute(c,t)
        except (OSError, psycopg.Error) as e:
            log('retry_connection', error=type(e).__name__)
            STOP.wait(.5)
    log('shutdown',exit_code=0)


if __name__ == '__main__':
    main()
