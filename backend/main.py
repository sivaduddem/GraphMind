"""
GraphMind FastAPI Backend
Main application entry point
"""
from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from typing import Optional, List
import uvicorn
from pathlib import Path

from backend.graph_builder import GraphBuilder
from backend.sql_parser import SQLParser
from backend.csv_analyzer import CSVAnalyzer

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
        
        parser = SQLParser()
        tables = parser.parse_sql(sql_content)
        
        # Add tables and relationships to graph
        for table in tables:
            graph_builder.add_table(table['name'], 'sql', table.get('columns', []))
        
        # Add foreign key relationships
        for table in tables:
            for fk in table.get('foreign_keys', []):
                graph_builder.add_fk_edge(
                    from_table=table['name'],
                    to_table=fk['references_table'],
                    from_columns=fk['columns'],
                    to_columns=fk['referenced_columns']
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
        
        # Add table to graph
        graph_builder.add_table(table_name, 'csv', profile['columns'])
        
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

