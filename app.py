import asyncio
import json
import logging
import os
import socket
from contextlib import asynccontextmanager, suppress
from datetime import date
from psycopg import DataError, IntegrityError, OperationalError, InterfaceError
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool, PoolTimeout
from starlette.applications import Starlette
from starlette.responses import JSONResponse, Response
from starlette.routing import Route
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from metrics import Metrics, Instrumentation
from queue_api import routes
from validation import FIELDS, validate


def normalized(row):
    return {k: v.isoformat() if isinstance(v, date) else v for k, v in row.items()}


def create_app(pool=None):
    pod = os.environ.get('POD_NAME') or socket.gethostname()
    pool_size = int(os.environ.get('DB_POOL_SIZE', '15'))
    if pool_size < 1:
        raise ValueError('DB_POOL_SIZE must be positive')
    owned_pool = pool is None
    if pool is None:
        pool = AsyncConnectionPool(os.environ['DATABASE_URL'], min_size=1, max_size=pool_size,
                                   kwargs={'row_factory': dict_row}, timeout=3, open=False)
    metrics = Metrics(pod)
    queue_routes, refresh = routes(pool, metrics.registry, normalized)

    @asynccontextmanager
    async def lifespan(app):
        if owned_pool:
            await pool.open()
            await pool.wait(timeout=60)
        task = asyncio.create_task(refresh())
        try:
            yield
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
            if owned_pool:
                await pool.close(timeout=10)
                logging.getLogger('uvicorn.error').info('Database pool closed; shutdown complete')

    async def live(request):
        return JSONResponse({'status': 'alive'})

    async def health(request):
        try:
            async with pool.connection() as conn:
                await conn.execute('SELECT 1')
            return JSONResponse({'status': 'ready'})
        except (OperationalError, InterfaceError, PoolTimeout):
            return JSONResponse({'status': 'not_ready'}, status_code=503)

    async def export(request):
        return Response(generate_latest(metrics.registry), headers={'Content-Type': CONTENT_TYPE_LATEST})

    async def get_body(request):
        if 'application/json' not in request.headers.get('content-type', ''):
            return None, {'body': 'Очікується application/json'}
        pieces, length = [], 0
        async for chunk in request.stream():
            length += len(chunk)
            if length > 16384:
                return None, {'body': 'Тіло запиту перевищує 16 КіБ'}
            pieces.append(chunk)
        try:
            data = json.loads(b''.join(pieces))
        except (ValueError, UnicodeDecodeError):
            return None, {'body': 'Некоректний JSON'}
        return data, validate(data)

    async def collection(request):
        if request.method == 'POST':
            data, errors = await get_body(request)
            if errors:
                return JSONResponse({'errors': errors}, status_code=400)
            async with pool.connection() as conn:
                cursor = await conn.execute('INSERT INTO vaccinations ('+','.join(FIELDS)+') VALUES ('+
                                            ','.join(['%s']*len(FIELDS))+') RETURNING *', tuple(data[k] for k in FIELDS))
                row = await cursor.fetchone()
            return JSONResponse(normalized(row), status_code=201)
        errors, pagination = {}, {}
        for key, default, lo, hi in [('limit',20,1,100), ('offset',0,0,2147483647)]:
            try:
                pagination[key] = int(request.query_params.get(key, default))
                if not lo <= pagination[key] <= hi:
                    raise ValueError()
            except (ValueError, TypeError):
                errors[key] = f'Ціле число від {lo} до {hi}'
        vaccine = request.query_params.get('vaccine')
        if vaccine is not None and ('\x00' in vaccine or len(vaccine)>200):
            errors['vaccine'] = 'Некоректний фільтр'
        if errors:
            return JSONResponse({'errors': errors}, status_code=400)
        clause, args = (' WHERE vaccine=%s', (vaccine,)) if vaccine is not None else ('', ())
        async with pool.connection() as conn:
            await conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            cursor = await conn.execute('SELECT count(*) AS total FROM vaccinations'+clause, args)
            total = (await cursor.fetchone())['total']
            cursor = await conn.execute('SELECT * FROM vaccinations'+clause+' ORDER BY id LIMIT %s OFFSET %s',
                                        args+(pagination['limit'],pagination['offset']))
            rows = await cursor.fetchall()
        return JSONResponse(dict(items=[normalized(r) for r in rows], total=total, **pagination))

    async def record(request):
        ident = request.path_params['record_id']
        if request.method == 'PUT':
            data, errors = await get_body(request)
            if errors:
                return JSONResponse({'errors': errors}, status_code=400)
        async with pool.connection() as conn:
            if request.method == 'GET':
                cursor = await conn.execute('SELECT * FROM vaccinations WHERE id=%s', (ident,))
            elif request.method == 'PUT':
                cursor = await conn.execute('UPDATE vaccinations SET '+','.join(k+'=%s' for k in FIELDS)+
                                            ' WHERE id=%s RETURNING *', tuple(data[k] for k in FIELDS)+(ident,))
            else:
                cursor = await conn.execute('DELETE FROM vaccinations WHERE id=%s RETURNING id', (ident,))
            row = await cursor.fetchone()
        if row is None:
            return JSONResponse({'errors': {'id': 'Запис не знайдено'}}, status_code=404)
        return Response(status_code=204) if request.method == 'DELETE' else JSONResponse(normalized(row))

    async def database_error(request, exc):
        return JSONResponse({'errors': {'database': 'База тимчасово недоступна'}}, status_code=503)

    async def data_error(request, exc):
        return JSONResponse({'errors': {'body': 'Дані порушують обмеження таблиці'}}, status_code=400)

    inner = Starlette(routes=[Route('/livez', live), Route('/healthz', health), Route('/metrics', export),
                              Route('/vaccinations', collection, methods=['POST','GET']),
                              Route('/vaccinations/{record_id:int}', record, methods=['GET','PUT','DELETE']), *queue_routes],
                       lifespan=lifespan, exception_handlers={OperationalError: database_error,
                       InterfaceError: database_error, PoolTimeout: database_error,
                       DataError: data_error, IntegrityError: data_error})
    app = Instrumentation(inner, metrics, pod)
    app.metrics, app.pool = metrics, pool
    return app
