"""
Test script to verify query compilation and execution
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

# Test queries from user
test_queries = [
    {
        'name': 'practiceQuery1',
        'query': 'select cid, company, location from customer where cid LIKE "%bank%" and company NOT LIKE "%bank%";',
        'expected_cols': ['cid', 'company', 'location'],
        'expected_rows': 2
    },
    {
        'name': 'practiceQuery2',
        'query': 'select pnumber, min(frequency), max(frequency) from maintenance_types where cost <= 1000 group by pnumber having max(frequency) - min(frequency) >= 16;',
        'expected_cols': ['pnumber', 'min(frequency)', 'max(frequency)'],
        'expected_rows': 2
    },
    {
        'name': 'practiceQuery3',
        'query': 'select f.fsid, f.remaining, f.usage_rate, b.bcode, b.balance from fund_source f left join budget b on f.fsid = b.fsid where f.usage_rate < 3000 and f.remaining > 20000;',
        'expected_cols': ['fsid', 'remaining', 'usage_rate', 'bcode', 'balance'],
        'expected_rows': 4
    }
]

print("=" * 80)
print("TESTING QUERY COMPILATION")
print("=" * 80)

for test in test_queries:
    print(f"\n{'='*80}")
    print(f"Testing: {test['name']}")
    print(f"Query: {test['query']}")
    print(f"{'='*80}")
    
    try:
        # Test compilation
        result = qv.compile_query(test['query'])
        print(f"✓ Compilation successful")
        print(f"  - Query ID: {result['query_id']}")
        print(f"  - Steps: {len(result['steps'])}")
        print(f"  - Line count: {result['line_count']}")
        
        # Print steps
        for i, step in enumerate(result['steps']):
            print(f"    Step {i}: {step['type']} - {step.get('description', 'N/A')}")
        
        # Test execution (get final state)
        if result['steps']:
            final_step_idx = len(result['steps']) - 1
            state = qv.get_visual_state(result['query_id'], final_step_idx)
            
            if state.get('output_table'):
                output = state['output_table']
                print(f"\n✓ Execution successful")
                print(f"  - Output rows: {output['row_count']}")
                print(f"  - Output columns: {output['columns']}")
                
                # Check expected results
                if output['row_count'] == test['expected_rows']:
                    print(f"  ✓ Row count matches expected: {test['expected_rows']}")
                else:
                    print(f"  ✗ Row count mismatch! Expected: {test['expected_rows']}, Got: {output['row_count']}")
                
                # Check columns
                actual_cols = set(output['columns'])
                expected_cols = set(test['expected_cols'])
                if actual_cols == expected_cols:
                    print(f"  ✓ Columns match expected")
                else:
                    print(f"  ✗ Column mismatch!")
                    print(f"    Expected: {expected_cols}")
                    print(f"    Got: {actual_cols}")
                    print(f"    Missing: {expected_cols - actual_cols}")
                    print(f"    Extra: {actual_cols - expected_cols}")
            else:
                print(f"  ✗ No output table generated")
        
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()

print("\n" + "=" * 80)
print("TESTING COMPLETE")
print("=" * 80)

