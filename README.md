# GraphMind

**GraphMind** is an advanced data-to-graph explorer that visualizes relationships from MySQL `.sql` schema files and CSV data. It converts structured data into an interactive graph where nodes represent tables and edges represent relationships (either explicit foreign keys or inferred from data patterns). GraphMind goes beyond traditional ERDs by providing graph analytics, impact analysis, and intelligent relationship discovery.

## 🚀 Key Features

### Core Functionality
- **SQL Schema Import**: Parse MySQL DDL files to extract tables, columns, foreign keys, and row data
- **CSV Data Analysis**: Upload CSV files and automatically infer relationships with existing tables
- **Interactive Graph Visualization**: D3.js-powered force-directed graph with zoom, pan, drag, and search
- **Relationship Types**: 
  - **FK edges**: Explicit foreign keys from SQL schemas (blue)
  - **Inferred edges**: Relationships detected through data pattern analysis (red, dashed)
- **Confidence Scoring**: Inferred relationships include confidence scores (0-1) with detailed statistics

### 🔥 Advanced Graph Analytics

#### A. Impact / Blast Radius Analysis
- **Downstream Impact Traversal**: Click any table to see all downstream tables that would be affected by changes
- **Depth-Limited Analysis**: Configurable traversal depth (1-10 hops)
- **Visual Highlighting**: Impacted tables highlighted in orange, source table in red
- **Path Visualization**: Shows impact paths with hop counts
- **Use Cases**: "If employee changes, what breaks?" - Perfect for schema migration planning

#### B. Critical Table Detection
- **Graph Theory Analytics**: Automatically identifies critical tables using:
  - **In-degree Centrality**: Tables with many dependents
  - **Betweenness Centrality**: Bottleneck nodes in the graph
  - **Articulation Points**: Tables whose removal would disconnect the graph
- **Visual Indicators**: Critical tables highlighted with colored borders (red = high, orange = medium)
- **Toggle Control**: Enable/disable critical table highlighting via sidebar

#### C. Enhanced Edge Explainability
- **Confidence Breakdown**: For inferred edges, see detailed explanation:
  - Name similarity percentage (50% weight)
  - Profile match percentage (40% weight)
  - Type match percentage (10% weight)
  - Direction reasoning (uniqueness analysis)
- **Transparency**: Understand why relationships were inferred, not just that they exist

#### D. Join Path Finder
- **Shortest Path Discovery**: Find the optimal join path between any two tables
- **Multiple Paths**: Shows alternative join paths with detailed column mappings
- **Visual Highlighting**: Path nodes and edges highlighted in blue
- **Join Details**: See exactly which columns to join on at each step
- **Use Cases**: "How do I join customer to project?" - Perfect for query planning

### 💾 Data Management

- **Table Data Viewing**: View actual row data for any table
- **Row Selection**: Select specific rows for simulation operations
- **Data Storage**: SQL INSERT statements and CSV data are parsed and stored

### 🧪 Constraint Simulation

- **DELETE Simulation**: Test what happens when deleting rows from a table
- **UPDATE Simulation**: Test what happens when updating specific columns
- **Row-Level Operations**: Select specific rows to simulate operations on
- **Risk Analysis**: See which tables would block operations and why
- **Cascade Detection**: Identify tables that would be CASCADE deleted/updated

## Architecture

### Backend (FastAPI)
- **SQL Parser** (`backend/sql_parser.py`): Parses MySQL CREATE TABLE, ALTER TABLE, and INSERT statements
- **CSV Analyzer** (`backend/csv_analyzer.py`): Profiles CSV columns and infers relationships
- **Graph Builder** (`backend/graph_builder.py`): Manages NetworkX DiGraph with graph analytics:
  - Impact analysis (BFS traversal)
  - Critical table detection (centrality metrics, articulation points)
  - Join path finding (shortest path algorithms)
- **Constraint Simulator** (`backend/constraint_simulator.py`): Simulates DELETE/UPDATE operations
- **API Endpoints**: Upload files, retrieve graph data, analytics, and simulations

### Frontend (D3.js)
- **Interactive Visualization**: Force-directed graph layout with D3.js
- **Controls**: Toggle FK/inferred edges, confidence slider, critical tables toggle, search functionality
- **Details Panel**: Shows table columns, relationships, data, and analytics:
  - Impact analysis with depth control
  - Join path finder
  - Constraint simulation
  - Enhanced edge explainability
- **Visual Highlighting**: Dynamic highlighting for impacts, paths, and critical tables
- **Data Table**: View and select rows for simulation operations

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
- **Click Node**: View table details, columns, relationships, and data
- **Click Edge**: View relationship details and inference explanation
- **Search**: Type in the search box to highlight matching tables
- **Filter**: Use confidence slider and checkboxes to filter edges

### Advanced Features

#### Impact Analysis
1. Click any table node to open the details panel
2. Click "Show Downstream Impact" button
3. Adjust depth slider (1-10 hops)
4. See all affected tables highlighted in the graph
5. View impact paths and statistics

#### Critical Table Detection
1. Toggle "Highlight Critical Tables" in the sidebar
2. Critical tables automatically highlighted:
   - **Red border**: High criticality (articulation points, many dependents)
   - **Orange border**: Medium criticality (bottleneck nodes)

#### Join Path Finder
1. Click a source table node
2. In the details panel, select a target table from the dropdown
3. Click "Find Path"
4. See the shortest join path with column mappings
5. Path automatically highlighted in the graph

#### Constraint Simulation
1. Click a table node to view its data
2. Select specific rows (or leave unselected for all rows)
3. Click "Simulate DELETE" or "Simulate UPDATE"
4. For UPDATE: Select column and enter new value
5. See detailed results showing:
   - Success/failure status
   - Blocking tables (if operation would fail)
   - Cascade effects
   - Inferred relationship risks

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

### Core Endpoints
- `POST /api/upload/sql` - Upload and parse SQL schema file
- `POST /api/upload/csv` - Upload and analyze CSV file
- `GET /api/graph?min_confidence=0.0` - Get graph data
- `GET /api/table/{table_name}` - Get table details
- `GET /api/edge/{from_table}/{to_table}` - Get edge details
- `GET /api/subgraph?tables=table1,table2&depth=1` - Get subgraph
- `DELETE /api/graph` - Clear the graph

### Advanced Analytics Endpoints
- `GET /api/table/{table_name}/impact?depth={n}` - Get downstream impact analysis
- `GET /api/graph/critical-tables` - Get critical table analysis
- `GET /api/path/{from_table}/{to_table}?max_depth={n}` - Find join paths between tables
- `GET /api/table/{table_name}/data` - Get table row data
- `GET /api/table/{table_name}/delete-risk` - Get delete risk score

### Simulation Endpoints
- `POST /api/simulate/delete` - Simulate DELETE operation
  - Body: `{"table": "table_name", "row_identifiers": [...]}`
- `POST /api/simulate/update` - Simulate UPDATE operation
  - Body: `{"table": "table_name", "column": "col_name", "row_identifiers": [...], "new_value": "value"}`

## Example Files

The `examples/` directory contains sample files:
- `sample_schema.sql` - Sample MySQL schema with multiple tables and foreign keys
- `departments.csv` - Department data
- `employees.csv` - Employee data with department relationships
- `projects.csv` - Project data
- `sample_data.csv` - Simple sample data

## Technical Highlights

### Graph Algorithms
- **BFS Traversal**: For impact analysis and dependency discovery
- **Shortest Path**: NetworkX algorithms for join path finding
- **Centrality Metrics**: Degree, betweenness centrality for critical table detection
- **Articulation Points**: Identify graph disconnection risks

### Resume-Worthy Features
- ✅ Implemented dependency blast-radius analysis using graph traversal
- ✅ Applied graph theory (centrality metrics, articulation points) to identify critical database tables
- ✅ Built join path finder using shortest path algorithms
- ✅ Enhanced ML inference explainability with detailed confidence breakdowns
- ✅ Developed constraint simulation system for database operation testing

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
