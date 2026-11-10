import json
import math
from datetime import datetime
from pathlib import Path
from statistics import mean

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

ROOT = Path('results')


def read(name):
    p = ROOT / (name+'.json')
    return json.loads(p.read_text(encoding='utf-8')) if p.exists() else None


def fmt(x):
    if x is None:
        return 'н/д'
    return f'{x:.5g}'.replace('.',',') if isinstance(x,float) else str(x)


def table(head, rows):
    return ['| '+' | '.join(head)+' |','| '+' | '.join(['---']*len(head))+' |',
            *['| '+' | '.join(map(fmt,row))+' |' for row in rows],'']


from experiments import epoch as ts


def fit(x):
    y2, y4 = (w*x[0]/v-1 for w,v in zip((2,4),x[1:]))
    s,k=2*y2-y4/3,(y4-3*y2)/6
    if s < 0:
        s,k=0,max(0,(2*y2+12*y4)/148)
    elif k < 0:
        s,k=max(0,(y2+3*y4)/10),0
    return s,k,math.sqrt((1-s)/k) if k>0 and s<1 else None


def main():
    figures=Path('figures')
    figures.mkdir(exist_ok=True)
    report=['# Лабораторна робота 4. Синхронізація пула воркерів','',
        'Варіант 7. Дьяченко Валерія, група ЗК-61мп.','',
        'Сервіс записів про вакцинацію доповнено чергою PostgreSQL та окремим Deployment воркерів. '
        'Зріз 256 × 256 генерується з seed; яскравість перетворюється за квантилями 0,02 і 0,98. '
        'Для чотирьох воркерів використано опитування 10 мс, lease 10 с і не більше трьох захоплень '
        'після відмов. У захищеному режимі захоплення використовує SKIP LOCKED, heartbeat перевіряє '
        'токен і чинність lease, а завершення і запис результату виконуються в одній транзакції.','']
    warm=read('warm20')
    if warm:
        report+=['## 20 задач на одному воркері','',
                 f"Середній час HTTP і запису: {fmt(warm['t_io'])} с; обробка зрізу: {fmt(warm['t_cpu'])} с.",'']
    lockrows=[]
    for name in ('naive-10ms','naive-1ms','skip-1ms','blocking-1ms'):
        d=read(name)
        if d:
            lockrows.append([name,len(d['tasks']),d['duplicates'],d['D_db'],d['D_logs'],
                             d['claim_p99_s'],d['empty_claims'],d['max_locks']])
    if lockrows:
        report+=['## Захоплення задач','']+table(
            ['Режим','Задач','Дублі','D за БД','D за журналом','p99, с','Порожні захоплення','Макс. Lock'],lockrows)
        report+=['Лічильники Prometheus прив’язані до моментів збору; короткі очікування блокування могли не потрапити у вибірку.','']
    freeze=read('freeze')
    if freeze:
        held={r['id']:r for r in freeze['held_after_stop']}
        lost=[e for e in freeze['journal'] if e.get('event')=='lost' and e.get('task_id') in held]
        report+=['## SIGSTOP на 20 секунд','']
        for e in lost:
            t=next(r for r in freeze['tasks'] if r['id']==e['task_id'])
            prev=held[t['id']]
            report += [f"Задача {t['id']}: attempts {prev['attempts']} → {t['attempts']}, "
                       f"token {prev['token']} → {t['token']}. Завершив {t['worker_id']}. "
                       f"Результатів: {t['results']}. Рядок журналу:",'', '```json',
                       json.dumps(e,ensure_ascii=False),'```','']
        if not lost:
            report+=['Застаріле завершення з нулем оновлених рядків не зафіксовано. Прогін треба повторити.','']
    cap=[read('capacity-'+str(w)) for w in (1,2,4)]
    if all(cap):
        x=[d['X'] for d in cap]
        std=[d['std'] or 0 for d in cap]
        report+=['## Пропускна здатність','']
        def average(d,key):
            v=[float(v) for series in d['prometheus'][key] for _,v in series['values'] if math.isfinite(float(v))]
            return mean(v) if v else None
        report+=table(['Воркери','X, задач/с','Розкид','Вікно, с','p99 захоплення, с','Транзакцій/с','CPU БД, ядер','C'],
            [[d['workers'],d['X'],d['std'],d['window_seconds'],d['claim_p99_s'],
              average(d,'db_tps'),average(d,'db_cpu'),d['X']/x[0] if x[0] and d['X'] is not None else None] for d in cap])
        if min(d['window_seconds'] for d in cap)<30:
            report+=['У частині прогонів 400 задач завершилися швидше ніж за 30 с. '
                     'Використано лише повні секунди між початком роботи і спорожненням черги; ці оцінки є наближеними.','']
        if warm:
            estimate=1/(warm['t_io']+warm['t_cpu']+.005)
            report += [f"Оцінка 1/(t_io + t_cpu + T_poll/2): {fmt(estimate)} задач/с; виміряне X(1): {fmt(x[0])}.",'']
        if all(v is not None and v>0 for v in x):
            s,k,peak=fit(x)
            report += [f'σ = {fmt(s)}, κ = {fmt(k)}, W* = {fmt(peak) if peak is not None else "скінченного максимуму немає"}.','']
            w=[1+i*.03 for i in range(101)]
            fig,ax=plt.subplots()
            ax.plot(w,[x[0]*n/(1+s*(n-1)+k*n*(n-1)) for n in w],label='USL')
            ax.errorbar([1,2,4],x,yerr=std,fmt='o',capsize=4,label='Вимірювання')
            ax.set(xlabel='Кількість воркерів',ylabel='Задач/с')
            ax.legend()
            fig.savefig(figures/'capacity.png',dpi=150,bbox_inches='tight')
            plt.close(fig)
            report+=['![Пропускна здатність](figures/capacity.png)','']
    little=read('little')
    if little:
        report+=['## Закон Літтла','']+table(['λ','Wq, с','Lq','λ × Wq','Розбіжність, %'],
            [[little[k] for k in ('lam','Wq','Lq','predicted','error_pct')]])
        if little['error_pct'] is None:
            report+=['Відносна розбіжність не визначена: виміряне Lq дорівнює нулю або відсутнє.','']
    overload=read('overload')
    if overload:
        report+=['## Перевантаження','']+table(['Черга наприкінці','Wq перших 100, с','Wq останніх 100, с'],
            [[overload['queue_at_end']['queued'],overload['first100_s'],overload['last100_s']]])
    rollout=read('rollout')
    if rollout:
        report+=['## Коректна зупинка','']+table(['Pod','Код завершення','Стан'],
            [[e['pod'],e['exit_code'],e.get('state')] for e in rollout['event'].get('exits',[])])
        report+=table(['Повернуто','Failed','Дублі','Attempts > 1'],[[
            sum(e['kind']=='returned' for e in rollout['events']),rollout['failed'],
            rollout['duplicates'],rollout['retried']]])
    f1=read('f1')
    if f1:
        report+=['## Ф1. Видалення пода і випадкові відмови','']
        event=f1['event']
        report+=table(['Перезапуски контейнерів наявних подів','Attempts > 1','Очікувана частка','Failed',
                       'Очікувана частка failed','Дублі','X, задач/с','X(4) без падінь'],
            [[sum(c.get('restartCount',0) for p in f1['restarts'] for c in p['statuses']),
              f1['retry_fraction'],.15,f1['failed_fraction'],.15**3,f1['duplicates'],f1['X'],cap[-1]['X'] if cap[-1] else None]])
        report+=table(['Attempts','Кількість'],f1['attempts'].items())
        if 'candidate' in event:
            t=event['candidate']
            expired=[e for e in f1['events'] if e['kind']=='expired'
                     and e['worker_id']==t['worker_id'] and 0<=ts(e['ts'])-event['at']<=12]
            if expired:
                task_ids={e['task_id'] for e in expired}
                report += table(['Задача','До повернення, с','Attempts','Завершив','Результатів'],
                    [[end['id'],ts(e['ts'])-event['at'],end['attempts'],end['worker_id'],end['results']]
                     for e in expired for end in f1['tasks'] if end['id']==e['task_id'] and end['id'] in task_ids])
                report+=['До таблиці потрапили задачі видаленого пода, lease яких сплив після команди видалення. '
                         'Випадкові відмови також увімкнено, тому причину окремого спливу треба звіряти з журналом.','']
            else:
                report+=['Для задачі, поміченої перед видаленням, сплив lease не зафіксовано. '
                         'Вона могла завершитися до фактичної зупинки пода; Ф1 треба повторити.','']
        else:
            report+=['Не вдалося підтвердити видалення пода під час задачі.','']
        fig,ax=plt.subplots()
        for state,series in f1['series'].items():
            for line in series:
                ax.step([t-f1['start'] for t,v in line['values']],[float(v) for t,v in line['values']],
                        where='post',label=state)
        ax.plot(f1['completed_per_second'],label='Завершено за секунду',alpha=.7)
        if 'at' in event:
            ax.axvline(event['at']-f1['start'],color='black',linestyle='--',label='Видалення пода')
        ax.set(xlabel='Час, с',ylabel='Кількість задач')
        ax.legend()
        fig.savefig(figures/'f1.png',dpi=150,bbox_inches='tight')
        plt.close(fig)
        report+=['![Прогін Ф1](figures/f1.png)','']
    d2=read('d2')
    if d2:
        report+=['## Д2. Відкладений запуск','']+table(['Задачі','p50, с','p99, с','Запущено раніше'],
            [[len(d2['tasks']),d2['p50_s'],d2['p99_s'],d2['early']]])
    Path('REPORT.md').write_text('\n'.join(report),encoding='utf-8')


if __name__=='__main__':
    main()
