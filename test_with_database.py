"""
Test script that loads the database from SQL file and tests all queries
"""
import sys
import os
import re
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'backend'))

from graph_builder import GraphBuilder
from query_visualizer import QueryVisualizer
from sql_parser import SQLParser
import pandas as pd

# Initialize
gb = GraphBuilder()
qv = QueryVisualizer(gb)

def load_database_from_sql(sql_file_path):
    """Load database from SQL file using SQLParser"""
    print(f"Loading database from {sql_file_path}...")
    
    with open(sql_file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    parser = SQLParser()
    tables = parser.parse_sql(content)
    
    # Add tables and relationships to graph
    for table in tables:
        gb.add_table(
            table['name'], 
            'sql', 
            table.get('columns', []),
            table.get('rows', [])
        )
        print(f"  Loaded {len(table.get('rows', []))} rows into {table['name']}")
    
    # Add foreign key relationships
    for table in tables:
        for fk in table.get('foreign_keys', []):
            gb.add_fk_edge(
                from_table=table['name'],
                to_table=fk['references_table'],
                from_columns=fk['columns'],
                to_columns=fk['referenced_columns'],
                on_delete=fk.get('on_delete', 'RESTRICT'),
                on_update=fk.get('on_update', 'RESTRICT')
            )
    
    print(f"Loaded {len(tables)} tables total")

# Test queries with expected results
test_queries = [
    {
        'name': 'practiceQuery1',
        'query': 'select cid, company, location from customer where cid like "%bank%" and company not like "%bank%";',
        'expected_cols': ['cid', 'company', 'location'],
        'expected_row_count': 2
    },
    {
        'name': 'practiceQuery2',
        'query': 'select pnumber, min(frequency), max(frequency) from maintenance_types where cost <= 1000 group by pnumber having max(frequency) - min(frequency) >= 16;',
        'expected_cols': ['pnumber', 'min(frequency)', 'max(frequency)'],
        'expected_row_count': 2
    },
    {
        'name': 'practiceQuery3',
        'query': 'select f.fsid, f.remaining, f.usage_rate, b.bcode, b.balance from fund_source f left join budget b on f.fsid = b.fsid where f.usage_rate < 3000 and f.remaining > 20000;',
        'expected_cols': ['fsid', 'remaining', 'usage_rate', 'bcode', 'balance'],
        'expected_row_count': 3
    },
    {
        'name': 'practiceQuery4',
        'query': 'select distinct p.pname, p.pnumber, p.plocation from project p where p.pnumber in (select distinct pnumber from operations) and pnumber in (select distinct pnumber from maintenance);',
        'expected_cols': ['pname', 'pnumber', 'plocation'],
        'expected_row_count': 2
    },
    {
        'name': 'practiceQuery5',
        'query': 'select c.fsid, c.assets from customer c where c.assets > 417000 or c.assets is null union select b.fsid, b.balance from budget b where b.balance >= 64000;',
        'expected_cols': ['fsid', 'assets'],  # UNION uses first query's column names
        'expected_row_count': 6
    },
    {
        'name': 'practiceQuery6',
        'query': 'select e.fname, e.lname, r.ip_address, r.user_account, t.start_hour from employee e left join remote_access r on e.ssn = r.ssn left join time_frames t on t.ssn = r.ssn where start_hour + duration > 17;',
        'expected_cols': ['fname', 'lname', 'ip_address', 'user_account', 'start_hour'],
        'expected_row_count': 3
    }
]

# Load database
sql_file = 'cs4400_global_company_database.sql'
if os.path.exists(sql_file):
    load_database_from_sql(sql_file)
else:
    print(f"Warning: {sql_file} not found. Testing compilation only.")

print("\n" + "=" * 80)
print("TESTING ALL QUERIES")
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
            final_step = result['steps'][final_step_idx]
            line_range = final_step.get('line_range', (0, 0))
            final_line_idx = line_range[0] if isinstance(line_range, tuple) else 0
            
            print(f"  - Final step index: {final_step_idx}")
            print(f"  - Final step type: {final_step['type']}")
            print(f"  - Line index: {final_line_idx}")
            
            # Use sub_step_index for SELECT_COL steps
            sub_step_idx = None
            if final_step['type'] == 'SELECT_COL' and result.get('sub_steps'):
                for i, sub_step in enumerate(result['sub_steps']):
                    if sub_step['step_index'] == final_step_idx:
                        sub_step_idx = i
                        break
                print(f"  - Sub step index: {sub_step_idx}")
            
            try:
                if sub_step_idx is not None:
                    state = qv.get_visual_state(result['query_id'], final_line_idx, sub_step_idx)
                else:
                    state = qv.get_visual_state(result['query_id'], final_line_idx)
                print(f"  - State retrieved: {state.get('step_type')}")
                print(f"  - Explanation: {state.get('explanation_text', 'N/A')[:100]}")
            except Exception as e:
                print(f"  - Error getting state: {e}")
                import traceback
                traceback.print_exc()
                state = None
            
            if state.get('output_table'):
                output = state['output_table']
                actual_rows = output['row_count']
                actual_cols = output['columns']
                
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
                expected_cols_set = set([c.lower() for c in test['expected_cols']])
                actual_cols_set = set([c.lower() for c in actual_cols])
                
                if actual_cols_set == expected_cols_set:
                    print(f"  [OK] Columns match: {sorted(actual_cols)}")
                else:
                    print(f"  [FAIL] Column mismatch!")
                    print(f"    Expected: {sorted(expected_cols_set)}")
                    print(f"    Got: {sorted(actual_cols_set)}")
                    print(f"    Missing: {sorted(expected_cols_set - actual_cols_set)}")
                    print(f"    Extra: {sorted(actual_cols_set - expected_cols_set)}")
                    all_passed = False
                
                # Show first few rows
                if output.get('data'):
                    print(f"\n  First 3 rows:")
                    for i, row in enumerate(output['data'][:3]):
                        print(f"    Row {i+1}: {row}")
            else:
                print(f"  [FAIL] No output table generated")
                if state.get('explanation_text'):
                    print(f"  Explanation: {state['explanation_text']}")
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

