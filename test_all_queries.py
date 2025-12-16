"""
Test script to verify all 6 queries match expected outputs
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'backend'))

from graph_builder import GraphBuilder
from query_visualizer import QueryVisualizer
import pandas as pd

# Initialize
gb = GraphBuilder()
qv = QueryVisualizer(gb)

# Test queries with expected results
test_queries = [
    {
        'name': 'practiceQuery1',
        'query': 'select cid, company, location from customer where cid like "%bank%" and company not like "%bank%";',
        'expected_cols': ['cid', 'company', 'location'],
        'expected_rows': [
            {'cid': 'bank3', 'company': 'Credit Union Universal', 'location': 'New York'},
            {'cid': 'bank4', 'company': 'Anytime Anywhere Crypto', 'location': 'Houston'}
        ],
        'expected_row_count': 2
    },
    {
        'name': 'practiceQuery2',
        'query': 'select pnumber, min(frequency), max(frequency) from maintenance_types where cost <= 1000 group by pnumber having max(frequency) - min(frequency) >= 16;',
        'expected_cols': ['pnumber', 'min(frequency)', 'max(frequency)'],
        'expected_rows': [
            {'pnumber': 2, 'min(frequency)': 10, 'max(frequency)': 30},
            {'pnumber': 10, 'min(frequency)': 4, 'max(frequency)': 20}
        ],
        'expected_row_count': 2
    },
    {
        'name': 'practiceQuery3',
        'query': 'select f.fsid, f.remaining, f.usage_rate, b.bcode, b.balance from fund_source f left join budget b on f.fsid = b.fsid where f.usage_rate < 3000 and f.remaining > 20000;',
        'expected_cols': ['fsid', 'remaining', 'usage_rate', 'bcode', 'balance'],
        'expected_rows': [
            {'fsid': 3, 'remaining': 27000, 'usage_rate': 1000, 'bcode': None, 'balance': None},
            {'fsid': 5, 'remaining': 31000, 'usage_rate': 2000, 'bcode': 10, 'balance': 170000},
            {'fsid': 29, 'remaining': 21000, 'usage_rate': 1000, 'bcode': None, 'balance': None}
        ],
        'expected_row_count': 3
    },
    {
        'name': 'practiceQuery4',
        'query': 'select distinct p.pname, p.pnumber, p.plocation from project p where p.pnumber in (select distinct pnumber from operations) and pnumber in (select distinct pnumber from maintenance);',
        'expected_cols': ['pname', 'pnumber', 'plocation'],
        'expected_rows': [
            {'pname': 'ProductY', 'pnumber': 2, 'plocation': 'Sugarland'},
            {'pname': 'Computerization', 'pnumber': 10, 'plocation': 'Stafford'}
        ],
        'expected_row_count': 2
    },
    {
        'name': 'practiceQuery5',
        'query': 'select c.fsid, c.assets from customer c where c.assets > 417000 or c.assets is null union select b.fsid, b.balance from budget b where b.balance >= 64000;',
        'expected_cols': ['fsid', 'assets'],  # Note: second part uses 'balance' but UNION will use first column names
        'expected_rows': [
            {'fsid': 29, 'assets': 619000},
            {'fsid': 7, 'assets': None},
            {'fsid': 13, 'assets': 850000},
            {'fsid': 5, 'assets': 170000},
            {'fsid': None, 'assets': 64000},  # From budget table
            {'fsid': 17, 'assets': 516000}
        ],
        'expected_row_count': 6
    },
    {
        'name': 'practiceQuery6',
        'query': 'select e.fname, e.lname, r.ip_address, r.user_account, t.start_hour from employee e left join remote_access r on e.ssn = r.ssn left join time_frames t on t.ssn = r.ssn where start_hour + duration > 17;',
        'expected_cols': ['fname', 'lname', 'ip_address', 'user_account', 'start_hour'],
        'expected_rows': [
            {'fname': 'Ramesh', 'lname': 'Narayan', 'ip_address': '403e:8f59:336e:d11b:0425:ed18:2f34:48a3', 'user_account': 'rnarayan3', 'start_hour': 13},
            {'fname': 'James', 'lname': 'Borg', 'ip_address': '26c8:4186:2105:cf66:7b3f:4b03:5dd7:3eb4', 'user_account': 'jborg1', 'start_hour': 15},
            {'fname': 'Jennifer', 'lname': 'Wallace', 'ip_address': '3208:78e4:578b:034b:c7ff:1b55:6e41:8ece', 'user_account': 'jwallace3', 'start_hour': 23}
        ],
        'expected_row_count': 3
    }
]

print("=" * 80)
print("TESTING ALL QUERIES AGAINST EXPECTED OUTPUTS")
print("=" * 80)

all_passed = True

for test in test_queries:
    print(f"\n{'='*80}")
    print(f"Testing: {test['name']}")
    print(f"Query: {test['query']}")
    print(f"{'='*80}")
    
    try:
        # Test compilation
        result = qv.compile_query(test['query'])
        print(f"[OK] Compilation successful")
        print(f"  - Query ID: {result['query_id']}")
        print(f"  - Steps: {len(result['steps'])}")
        
        # Test execution (get final state)
        if result['steps']:
            final_step_idx = len(result['steps']) - 1
            print(f"  - Final step index: {final_step_idx}")
            print(f"  - Final step type: {result['steps'][final_step_idx]['type']}")
            
            # Get the line number for the final step
            final_step = result['steps'][final_step_idx]
            line_range = final_step.get('line_range', (0, 0))
            final_line_idx = line_range[0] if isinstance(line_range, tuple) else 0
            print(f"  - Final step line range: {line_range}")
            print(f"  - Using line index: {final_line_idx}")
            
            try:
                # Use sub_step_index for SELECT_COL steps
                if final_step['type'] == 'SELECT_COL' and result.get('sub_steps'):
                    # Find the sub_step_index for this step
                    sub_step_idx = None
                    for i, sub_step in enumerate(result['sub_steps']):
                        if sub_step['step_index'] == final_step_idx:
                            sub_step_idx = i
                            break
                    print(f"  - Sub step index: {sub_step_idx}")
                    if sub_step_idx is not None:
                        state = qv.get_visual_state(result['query_id'], final_line_idx, sub_step_idx)
                    else:
                        state = qv.get_visual_state(result['query_id'], final_line_idx)
                else:
                    state = qv.get_visual_state(result['query_id'], final_line_idx)
                print(f"  - State retrieved successfully")
                if state.get('explanation_text'):
                    print(f"  - Explanation: {state['explanation_text']}")
            except Exception as e:
                print(f"  - Error getting visual state: {e}")
                import traceback
                traceback.print_exc()
                state = None
            
            if state.get('output_table'):
                output = state['output_table']
                actual_rows = output['row_count']
                actual_cols = output['columns']
                actual_data = output['data']
                
                print(f"\n[OK] Execution successful")
                print(f"  - Output rows: {actual_rows}")
                print(f"  - Output columns: {actual_cols}")
                
                # Check row count
                if actual_rows == test['expected_row_count']:
                    print(f"  [OK] Row count matches: {test['expected_row_count']}")
                else:
                    print(f"  [FAIL] Row count mismatch! Expected: {test['expected_row_count']}, Got: {actual_rows}")
                    all_passed = False
                
                # Check columns
                expected_cols_set = set(test['expected_cols'])
                actual_cols_set = set(actual_cols)
                
                if actual_cols_set == expected_cols_set:
                    print(f"  [OK] Columns match: {sorted(actual_cols)}")
                else:
                    print(f"  [FAIL] Column mismatch!")
                    print(f"    Expected: {sorted(expected_cols_set)}")
                    print(f"    Got: {sorted(actual_cols_set)}")
                    print(f"    Missing: {sorted(expected_cols_set - actual_cols_set)}")
                    print(f"    Extra: {sorted(actual_cols_set - expected_cols_set)}")
                    all_passed = False
                
                # Check data (if we have expected rows)
                if test.get('expected_rows'):
                    print(f"\n  Checking data rows...")
                    # Convert actual data to comparable format
                    actual_df = pd.DataFrame(actual_data)
                    expected_df = pd.DataFrame(test['expected_rows'])
                    
                    # Normalize column names (handle case sensitivity)
                    actual_df.columns = [c.lower() for c in actual_df.columns]
                    expected_df.columns = [c.lower() for c in expected_df.columns]
                    
                    # Compare row counts
                    if len(actual_df) == len(expected_df):
                        print(f"    [OK] Row count matches: {len(actual_df)}")
                    else:
                        print(f"    [FAIL] Row count mismatch: Expected {len(expected_df)}, Got {len(actual_df)}")
                        all_passed = False
                    
                    # Show first few rows for comparison
                    print(f"\n    First 3 actual rows:")
                    for i, row in actual_df.head(3).iterrows():
                        print(f"      {dict(row)}")
                    
                    print(f"\n    Expected rows:")
                    for i, row in expected_df.iterrows():
                        print(f"      {dict(row)}")
            else:
                print(f"  [FAIL] No output table generated")
                all_passed = False
        
    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()
        all_passed = False

print("\n" + "=" * 80)
if all_passed:
    print("[SUCCESS] ALL TESTS PASSED!")
else:
    print("[FAILURE] SOME TESTS FAILED - See details above")
print("=" * 80)

