"""Compare result_validation results vs baseline"""
import json
from pathlib import Path

new_dirs = {
    'schools': Path('eval/results/test_result_validation_schools'),
    'financial': Path('eval/results/test_result_validation_financial'),
    'debit': Path('eval/results/test_result_validation_debit'),
}

baseline_dirs = {
    'schools': Path('eval/results/test_clean_schools'),
    'financial': Path('eval/results/test_clean_financial'),
    'debit': Path('eval/results/test_clean_debit'),
}

total_new = 0
total_new_correct = 0
total_base = 0
total_base_correct = 0
total_improvements = []
total_regressions = []

for db_name in ['schools', 'financial', 'debit']:
    new_dir = new_dirs[db_name]
    base_dir = baseline_dirs[db_name]

    if not new_dir.exists():
        print(f'{db_name}: new results not found')
        continue

    new_files = sorted(new_dir.glob('*.json'))
    n_total = len(new_files)
    n_correct = sum(1 for f in new_files if json.loads(f.read_text()).get('judge_correct', False))

    b_total = 0
    b_correct = 0
    improvements = []
    regressions = []

    if base_dir.exists():
        base_files = sorted(base_dir.glob('*.json'))
        b_total = len(base_files)
        b_correct = sum(1 for f in base_files if json.loads(f.read_text()).get('judge_correct', False))

        for f in new_files:
            bf = base_dir / f.name
            if not bf.exists():
                continue
            new_data = json.loads(f.read_text())
            old_data = json.loads(bf.read_text())
            new_c = new_data.get('judge_correct', False)
            old_c = old_data.get('judge_correct', False)
            if old_c and not new_c:
                regressions.append((new_data['question_id'], new_data['question'][:50], new_data.get('difficulty', '')))
            elif not old_c and new_c:
                improvements.append((new_data['question_id'], new_data['question'][:50], new_data.get('difficulty', '')))

    total_new += n_total
    total_new_correct += n_correct
    total_base += b_total
    total_base_correct += b_correct
    total_improvements.extend(improvements)
    total_regressions.extend(regressions)

    b_pct = f"{b_correct/b_total*100:.1f}%" if b_total else "N/A"
    print(f'{db_name:12s}: {n_correct}/{n_total} ({n_correct/n_total*100:.1f}%)  baseline: {b_correct}/{b_total} ({b_pct})  +{len(improvements)}/-{len(regressions)}')

print()
combined_label = "COMBINED"
b_pct_total = f"{total_base_correct/total_base*100:.1f}%" if total_base else "N/A"
print(f'{combined_label:12s}: {total_new_correct}/{total_new} ({total_new_correct/total_new*100:.1f}%)  baseline: {total_base_correct}/{total_base} ({b_pct_total})')
net = len(total_improvements) - len(total_regressions)
print(f'Net: +{len(total_improvements)}/-{len(total_regressions)} = {net:+d}')
print()
print('Improvements:')
for qid, q, diff in total_improvements:
    print(f'  + #{qid} ({diff}): {q}')
print()
print('Regressions:')
for qid, q, diff in total_regressions:
    print(f'  - #{qid} ({diff}): {q}')
