# GraphMind

**GraphMind** is a data-to-graph explorer that visualizes relationships from MySQL `.sql` schema files and CSV data. It converts structured data into an interactive graph where nodes represent tables and edges represent relationships (either explicit foreign keys or inferred from data patterns).

## Features

- **SQL Schema Import**: Parse MySQL DDL files to extract tables and foreign key relationships
- **CSV Data Analysis**: Upload CSV files and automatically infer relationships with existing tables
- **Interactive Graph Visualization**: D3.js-powered graph with zoom, pan, drag, and search
- **Relationship Types**: 
  - **FK edges**: Explicit foreign keys from SQL schemas
  - **Inferred edges**: Relationships detected through data pattern analysis
- **Confidence Scoring**: Inferred relationships include confidence scores (0-1) with detailed statistics
- **Graph Exploration**: Click nodes to see table details, click edges to see relationship explanations

## Architecture

### Backend (FastAPI)
- **SQL Parser** (`backend/sql_parser.py`): Parses MySQL CREATE TABLE and ALTER TABLE statements
- **CSV Analyzer** (`backend/csv_analyzer.py`): Profiles CSV columns and infers relationships
- **Graph Builder** (`backend/graph_builder.py`): Manages NetworkX DiGraph with caching and optimization
- **API Endpoints**: Upload files, retrieve graph data, get table/edge details

### Frontend (D3.js)
- **Interactive Visualization**: Force-directed graph layout with D3.js
- **Controls**: Toggle FK/inferred edges, confidence slider, search functionality
- **Details Panel**: Shows table columns, relationships, and edge explanations

## Installation

1. **Install Python dependencies**:
```bash
pip install -r requirements.txt
```

2. **Start the backend server**:
```bash
python run.py
```

The API will be available at `http://localhost:8000`

3. **Open the frontend**:
   
   The backend serves the frontend automatically. Just open:
   ```
   http://localhost:8000
   ```

## Usage

### Upload SQL Schema

1. Click "Choose SQL File" or drag and drop a `.sql` file
2. The parser will extract:
   - All CREATE TABLE statements
   - Foreign keys defined inline in CREATE TABLE
   - Foreign keys defined via ALTER TABLE ... ADD CONSTRAINT

### Upload CSV Data

1. Click "Choose CSV File" or drag and drop a `.csv` file
2. The analyzer will:
   - Profile each column (distinct count, nulls, data type)
   - Infer relationships with existing tables
   - Assign confidence scores based on:
     - Column name similarity
     - Profile compatibility (key-like vs FK-like)
     - Data type matching

### Explore the Graph

- **Zoom/Pan**: Use mouse wheel to zoom, drag background to pan
- **Drag Nodes**: Click and drag nodes to reposition
- **Click Node**: View table details, columns, and relationships
- **Click Edge**: View relationship details and inference explanation
- **Search**: Type in the search box to highlight matching tables
- **Filter**: Use confidence slider and checkboxes to filter edges

## Graph Model

### Nodes (Tables)
- `id`: Table name
- `source`: 'sql' or 'csv'
- `column_count`: Number of columns

### Edges (Relationships)
- `source`: Source table name
- `target`: Target table name
- `kind`: 'fk' or 'inferred'
- `from_columns`: List of source columns
- `to_columns`: List of target columns
- `confidence`: Confidence score (0-1, only for inferred edges)
- `stats`: Additional statistics (only for inferred edges)

## API Endpoints

- `POST /api/upload/sql` - Upload and parse SQL schema file
- `POST /api/upload/csv` - Upload and analyze CSV file
- `GET /api/graph?min_confidence=0.0` - Get graph data
- `GET /api/table/{table_name}` - Get table details
- `GET /api/edge/{from_table}/{to_table}` - Get edge details
- `GET /api/subgraph?tables=table1,table2&depth=1` - Get subgraph
- `DELETE /api/graph` - Clear the graph

## Example Files

The `examples/` directory contains sample files:
- `sample_schema.sql` - Sample MySQL schema with multiple tables and foreign keys
- `departments.csv` - Department data
- `employees.csv` - Employee data with department relationships
- `projects.csv` - Project data
- `sample_data.csv` - Simple sample data

## Limitations

- SQL parser supports common MySQL DDL syntax but not all dialects
- CSV relationship inference uses heuristics (name similarity, profile matching) rather than actual data overlap
- No real-time database synchronization
- No query-level lineage tracking

## Project Structure

```
GraphMind/
├── backend/
│   ├── __init__.py
│   ├── main.py           # FastAPI application
│   ├── sql_parser.py     # MySQL DDL parser
│   ├── csv_analyzer.py   # CSV relationship inference
│   └── graph_builder.py  # NetworkX graph management
├── frontend/
│   ├── index.html        # Main UI
│   ├── graph.js          # D3.js visualization
│   └── style.css         # Styling
├── examples/
│   ├── sample_schema.sql # Example SQL file
│   └── *.csv             # Example CSV files
├── requirements.txt      # Python dependencies
├── run.py               # Startup script
├── README.md            # This file
└── QUICKSTART.md        # Quick start guide
```

## License

See LICENSE file for details.
