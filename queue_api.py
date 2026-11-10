import asyncio
import secrets
from datetime import datetime
from prometheus_client import Gauge
from starlette.responses import JSONResponse
from starlette.routing import Route


def routes(pool, registry, normalize):
    gauges = {s: Gauge('tasks_' + s, 'Tasks in state ' + s, registry=registry)
              for s in ('queued', 'running')}

    async def refresh():
        while True:
            try:
                async with pool.connection() as c:
                    rows = await (await c.execute(
                        "SELECT status,count(*) AS n FROM tasks WHERE status IN ('queued','running') GROUP BY status"
                    )).fetchall()
                counts = {r['status']: r['n'] for r in rows}
                for s, g in gauges.items():
                    g.set(counts.get(s, 0))
            except Exception:
                pass
            await asyncio.sleep(5)

    async def create(request):
        raw = request.query_params.get('run_after')
        try:
            after = datetime.fromisoformat(raw.replace('Z', '+00:00')) if raw else None
            if after and after.utcoffset() is None:
                raise ValueError()
        except ValueError:
            return JSONResponse({'error': 'run_after: ISO 8601 з часовим поясом'}, status_code=400)
        async with pool.connection() as c:
            row = await (await c.execute(
                "INSERT INTO tasks(record_id,seed,run_after) "
                "SELECT id,%s,%s FROM vaccinations WHERE id=%s FOR KEY SHARE RETURNING *",
                (secrets.randbits(31), after, request.path_params['record_id'])
            )).fetchone()
        return JSONResponse(normalize(row), status_code=202) if row else JSONResponse({}, status_code=404)

    async def detail(request):
        async with pool.connection() as c:
            row = await (await c.execute(
                "SELECT t.*, (SELECT r.result FROM task_results r WHERE r.task_id=t.id "
                "ORDER BY r.created_at LIMIT 1) AS result FROM tasks t WHERE t.id=%s",
                (request.path_params['task_id'],)
            )).fetchone()
        return JSONResponse(normalize(row)) if row else JSONResponse({}, status_code=404)

    async def listing(request):
        status = request.query_params.get('status')
        try:
            limit = int(request.query_params.get('limit', 20))
            offset = int(request.query_params.get('offset', 0))
            if not (1 <= limit <= 100 and 0 <= offset <= 2147483647):
                raise ValueError()
            if status not in (None, 'queued', 'running', 'done', 'failed'):
                raise ValueError()
        except ValueError:
            return JSONResponse({'error': 'Некоректний фільтр або пагінація'}, status_code=400)
        async with pool.connection() as c:
            rows = await (await c.execute(
                "SELECT * FROM tasks WHERE (%s::text IS NULL OR status=%s) ORDER BY id LIMIT %s OFFSET %s",
                (status, status, limit, offset)
            )).fetchall()
        return JSONResponse({'items': [normalize(r) for r in rows], 'limit': limit, 'offset': offset})

    return [
        Route('/vaccinations/{record_id:int}/tasks', create, methods=['POST']),
        Route('/tasks/{task_id:int}', detail),
        Route('/tasks', listing),
    ], refresh
