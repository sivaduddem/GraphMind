"""
GraphMind FastAPI Backend
Main application entry point
"""
from fastapi import FastAPI, UploadFile, File, HTTPException, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from typing import Optional, List, Dict
import uvicorn
from pathlib import Path

from backend.graph_builder import GraphBuilder
from backend.sql_parser import SQLParser
from backend.csv_analyzer import CSVAnalyzer
from backend.constraint_simulator import ConstraintSimulator
from backend.query_visualizer import QueryVisualizer

app = FastAPI(title="GraphMind API", version="1.0.0")

# CORS middleware for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global graph builder instance
graph_builder = GraphBuilder()

# Global query visualizer instance
query_visualizer = QueryVisualizer(graph_builder)


@app.get("/")
async def root():
    """Serve the frontend index.html"""
    frontend_path = Path(__file__).parent.parent / 'frontend' / 'index.html'
    if frontend_path.exists():
        return FileResponse(str(frontend_path))
    return {"message": "GraphMind API", "version": "1.0.0"}


@app.get("/api/")
async def api_root():
    return {"message": "GraphMind API", "version": "1.0.0"}


@app.post("/api/upload/sql")
async def upload_sql(file: UploadFile = File(...)):
    """Upload and parse a MySQL .sql schema file"""
    try:
        content = await file.read()
        sql_content = content.decode('utf-8')
        
        # Clear existing graph before parsing new SQL file
        graph_builder.clear()
        
        parser = SQLParser()
        tables = parser.parse_sql(sql_content)
        
        # Add tables and relationships to graph
        for table in tables:
            graph_builder.add_table(
                table['name'], 
                'sql', 
                table.get('columns', []),
                table.get('rows', [])
            )
        
        # Add foreign key relationships
        for table in tables:
            for fk in table.get('foreign_keys', []):
                graph_builder.add_fk_edge(
                    from_table=table['name'],
                    to_table=fk['references_table'],
                    from_columns=fk['columns'],
                    to_columns=fk['referenced_columns'],
                    on_delete=fk.get('on_delete', 'RESTRICT'),
                    on_update=fk.get('on_update', 'RESTRICT')
                )
        
        return {
            "status": "success",
            "tables_parsed": len(tables),
            "message": f"Successfully parsed {len(tables)} tables"
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/upload/csv")
async def upload_csv(file: UploadFile = File(...), table_name: Optional[str] = None):
    """Upload and analyze a CSV file"""
    try:
        import pandas as pd
        import io
        
        content = await file.read()
        df = pd.read_csv(io.BytesIO(content))
        
        # Use filename as table name if not provided
        if not table_name:
            table_name = file.filename.replace('.csv', '').replace('.CSV', '')
        
        analyzer = CSVAnalyzer()
        profile = analyzer.profile_csv(df, table_name)
        
        # Convert DataFrame to list of dictionaries for row storage
        rows = df.to_dict('records')
        
        # Add table to graph
        graph_builder.add_table(table_name, 'csv', profile['columns'], rows)
        
        # Infer relationships with existing tables
        inferred_edges = analyzer.infer_relationships(df, table_name, graph_builder)
        
        # Add inferred edges to graph
        for edge in inferred_edges:
            graph_builder.add_inferred_edge(
                from_table=edge['from_table'],
                to_table=edge['to_table'],
                from_column=edge['from_column'],
                to_column=edge['to_column'],
                confidence=edge['confidence'],
                stats=edge['stats']
            )
        
        return {
            "status": "success",
            "table_name": table_name,
            "rows": len(df),
            "columns": len(df.columns),
            "inferred_relationships": len(inferred_edges),
            "profile": profile
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/graph")
async def get_graph(min_confidence: float = 0.0):
    """Get the full graph or filtered by confidence"""
    graph_data = graph_builder.to_json(min_confidence=min_confidence)
    return graph_data


@app.get("/api/schema")
async def get_schema():
    """Get a concise schema representation for the schema/ERD view"""
    try:
        return graph_builder.get_schema()
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/table/{table_name}")
async def get_table_details(table_name: str):
    """Get detailed information about a specific table"""
    details = graph_builder.get_table_details(table_name)
    if not details:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    return details


@app.get("/api/edge/{from_table}/{to_table}")
async def get_edge_details(from_table: str, to_table: str):
    """Get detailed information about a specific edge"""
    details = graph_builder.get_edge_details(from_table, to_table)
    if not details:
        raise HTTPException(status_code=404, detail=f"Edge from '{from_table}' to '{to_table}' not found")
    return details


@app.get("/api/subgraph")
async def get_subgraph(
    tables: str = Query(..., description="Comma-separated table names"),
    depth: int = Query(1, description="Depth of neighbors to include")
):
    """Get a subgraph containing specified tables and their neighbors"""
    table_names = [t.strip() for t in tables.split(',')]
    subgraph_data = graph_builder.get_subgraph(table_names, depth)
    return subgraph_data


@app.delete("/api/graph")
async def clear_graph():
    """Clear the entire graph"""
    graph_builder.clear()
    return {"status": "success", "message": "Graph cleared"}


@app.post("/api/simulate/delete")
async def simulate_delete(request: Dict = Body(...)):
    """Simulate a DELETE operation on a table"""
    try:
        table_name = request.get('table')
        row_identifiers = request.get('row_identifiers', None)  # List of primary key values or row indices
        
        if not table_name:
            raise HTTPException(status_code=400, detail="Table name is required")
        
        simulator = ConstraintSimulator(graph_builder)
        result = simulator.simulate_delete(table_name, row_identifiers)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/simulate/update")
async def simulate_update(request: Dict = Body(...)):
    """Simulate an UPDATE operation on a table column"""
    try:
        table_name = request.get('table')
        column = request.get('column')
        row_identifiers = request.get('row_identifiers', None)  # List of primary key values or row indices
        new_value = request.get('new_value', None)  # New value for the column
        
        if not table_name:
            raise HTTPException(status_code=400, detail="Table name is required")
        
        simulator = ConstraintSimulator(graph_builder)
        result = simulator.simulate_update(table_name, column, row_identifiers, new_value)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/table/{table_name}/delete-risk")
async def get_delete_risk(table_name: str):
    """Get delete risk score for a table"""
    try:
        simulator = ConstraintSimulator(graph_builder)
        risk = simulator.get_delete_risk_score(table_name)
        return risk
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/table/{table_name}/data")
async def get_table_data(table_name: str):
    """Get row data for a specific table"""
    try:
        rows = graph_builder.get_table_rows(table_name)
        table_details = graph_builder.get_table_details(table_name)
        if not table_details:
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
        
        return {
            "table_name": table_name,
            "rows": rows,
            "columns": table_details.get('columns', [])
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/table/{table_name}/impact")
async def get_table_impact(table_name: str, depth: int = Query(3, ge=1, le=10)):
    """Get downstream impact analysis for a table"""
    try:
        impact = graph_builder.get_downstream_impact(table_name, max_depth=depth)
        return impact
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/graph/critical-tables")
async def get_critical_tables():
    """Get critical tables analysis"""
    try:
        critical = graph_builder.get_critical_tables()
        return critical
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/path/{from_table}/{to_table}")
async def find_join_path(
    from_table: str,
    to_table: str,
    max_depth: int = Query(5, ge=1, le=10)
):
    """Find join paths between two tables"""
    try:
        paths = graph_builder.find_join_paths(from_table, to_table, max_depth=max_depth)
        return paths
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/query/compile")
async def compile_query(request: Dict = Body(...)):
    """
    Execute a SQL query and return its output.
    
    Supports two modes:
    - mode="final" (default): Returns only final result (existing behavior)
    - mode="steps": Returns step-by-step visualization data + final result
    """
    try:
        query_text = request.get("query")
        mode = request.get("mode", "final")  # Default to "final" to preserve existing behavior

        if not query_text:
            raise HTTPException(status_code=400, detail="Query text is required")

        if mode == "steps":
            # Return step-by-step data
            result = query_visualizer.execute_query_steps(query_text)
            return result
        else:
            # Return only final result (existing behavior - DO NOT CHANGE)
            result = query_visualizer.execute_query(query_text)
            # `result` is already JSON-serializable (columns, rows, row_count)
            return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/query/state")
async def get_query_state(request: Dict = Body(...)):
    """Get visual state for a specific line index"""
    try:
        query_id = request.get('query_id')
        line_index = request.get('line_index', 0)
        sub_step_index = request.get('sub_step_index')  # Optional granular step index
        
        if not query_id:
            raise HTTPException(status_code=400, detail="Query ID is required")
        
        if not isinstance(line_index, int) or line_index < 0:
            raise HTTPException(status_code=400, detail="Line index must be a non-negative integer")
        
        try:
            state = query_visualizer.get_visual_state(query_id, line_index, sub_step_index)
        except Exception as e:
            import traceback
            traceback.print_exc()
            raise HTTPException(status_code=500, detail=f"Error getting visual state: {str(e)}")
        
        # Ensure explanation_text is never empty
        explanation_text = state.get('explanation_text', '')
        if not explanation_text or explanation_text.strip() == '':
            explanation_text = 'Processing query step...'
        
        # Ensure all data is JSON-serializable (clean NaN values)
        def clean_for_json(obj):
            """Recursively clean NaN, inf, and -inf values from data structures"""
            import math
            import pandas as pd
            import numpy as np
            
            if isinstance(obj, dict):
                return {k: clean_for_json(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [clean_for_json(item) for item in obj]
            elif isinstance(obj, (float, np.floating)):
                if pd.isna(obj) or math.isnan(obj):
                    return None
                elif math.isinf(obj):
                    return None
                else:
                    return obj
            else:
                return obj
        
        serializable_state = {
            'input_tables': clean_for_json(state.get('input_tables', [])),
            'output_table': clean_for_json(state.get('output_table')),
            'highlighted_cols': state.get('highlighted_cols', []),
            'dimmed_rows': state.get('dimmed_rows', []),
            'annotations': clean_for_json(state.get('annotations', {})),
            'explanation_text': explanation_text,
            'step_type': state.get('step_type', ''),
            'before_row_count': state.get('before_row_count', 0),
            'after_row_count': state.get('after_row_count', 0),
            'join_condition': clean_for_json(state.get('join_condition'))
        }
        
        return serializable_state
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        error_detail = str(e)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Internal server error: {error_detail}")


@app.get("/api/query/datasets")
async def get_available_datasets():
    """Get list of available tables/datasets"""
    try:
        tables = []
        for table_name in graph_builder.table_rows.keys():
            table_details = graph_builder.get_table_details(table_name)
            if table_details:
                row_count = len(graph_builder.get_table_rows(table_name))
                tables.append({
                    'name': table_name,
                    'source': table_details.get('source', 'unknown'),
                    'column_count': len(table_details.get('columns', [])),
                    'row_count': row_count
                })
        return {'tables': tables}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# Serve frontend static files
@app.get("/{full_path:path}", include_in_schema=False)
async def serve_frontend(full_path: str):
    """Serve frontend files, but only for non-API routes"""
    if full_path.startswith("api/"):
        raise HTTPException(status_code=404, detail="API endpoint not found")
    
    try:
        frontend_path = Path(__file__).parent.parent / 'frontend'
        
        # If no path or path is empty, serve index.html
        if not full_path or full_path == "":
            index_path = frontend_path / "index.html"
            if index_path.exists():
                return FileResponse(str(index_path))
        
        # Try to serve the requested file
        file_path = frontend_path / full_path
        if file_path.exists() and file_path.is_file():
            return FileResponse(str(file_path))
        
        # Fallback to index.html for SPA routing
        index_path = frontend_path / "index.html"
        if index_path.exists():
            return FileResponse(str(index_path))
        
        return {"message": "Frontend not found"}
    except Exception:
        return {"message": "Frontend not available. Please serve frontend/index.html separately."}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

