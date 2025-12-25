from backend.query_visualizer import QueryVisualizer
from backend.graph_builder import GraphBuilder
import sqlparse

q = "select * from employee e join works_on w\non e.ssn = w.essn;"
print("RAW QUERY:\n", repr(q))
parsed = sqlparse.parse(q)[0]
qv = QueryVisualizer(GraphBuilder())
steps = qv._extract_steps(parsed, q)
print("STEPS:")
for i, s in enumerate(steps):
    print(i, s.get('type'), s.get('table'), 'cond=', s.get('condition'))
