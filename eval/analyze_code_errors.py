"""Analyze code-level errors in V1 results (<=500 rows, SQL followed QA)"""
import json
from pathlib import Path

dirs = {
    'schools': ('california_schools', Path('eval/results/test_data_profile_schools')),
    'financial': ('financial', Path('eval/results/test_data_profile_financial')),
    'debit': ('debit_card_specializing', Path('eval/results/test_data_profile_debit')),
}

with open('eval/dev.json') as f:
    all_q = {q['question_id']: q for q in json.load(f)}

cases = []
for db_name, (prefix, d) in dirs.items():
    for f in sorted(d.glob('*.json')):
        data = json.loads(f.read_text())
        if data.get('judge_correct', False):
            continue
        rows = data.get('sql_row_count', 0)
        if rows == 0 or rows > 500:
            continue

        qid = data['question_id']
        gold = all_q.get(qid, {})

        cases.append({
            'qid': qid, 'db': db_name, 'diff': data['difficulty'],
            'q': data['question'][:80],
            'hint': gold.get('evidence', '')[:80],
            'rows': rows,
            'got': data.get('final_answer', '')[:60],
            'want': json.dumps(data.get('expected_result', [])[:1], ensure_ascii=False, default=str)[:70],
            'code': data.get('code', '')[:120],
            'gold_sql': gold.get('SQL', '')[:100],
            'pipe_sql': data.get('pipeline_sql', '')[:100],
            'qa_task': json.loads(data['task_plan']).get('sql_task', '')[:100] if data.get('task_plan') else '',
        })

print(f'Code-level errors (<=500 rows): {len(cases)}')
print()
for c in cases:
    print(f'#{c["qid"]} ({c["db"]}, {c["diff"]}, {c["rows"]} rows)')
    print(f'  Q: {c["q"]}')
    if c['hint']:
        print(f'  Hint: {c["hint"]}')
    print(f'  QA task: {c["qa_task"]}')
    print(f'  Gold SQL: {c["gold_sql"]}')
    print(f'  Pipe SQL: {c["pipe_sql"]}')
    print(f'  Code: {c["code"]}')
    print(f'  Got:  {c["got"]}')
    print(f'  Want: {c["want"]}')
    print()
