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

### 🔍 SQL Query Visualizer
- **Step-by-Step Query Execution**: Visualize SQL queries step-by-step with intermediate table states
- **Supported SQL Operations**:
  - **FROM**: Load tables and display initial data
  - **JOIN**: Visualize table joins (INNER, LEFT, RIGHT, FULL) with input/output tables
  - **WHERE**: Filter rows with conditions including:
    - LIKE / NOT LIKE pattern matching (`%` and `_` wildcards)
    - Comparison operators (`=`, `!=`, `<`, `>`, `<=`, `>=`)
    - IS NULL / IS NOT NULL
    - AND / OR logical operators
    - IN subqueries
  - **SELECT**: Column projection with incremental column selection visualization
  - **GROUP BY**: Group rows by columns
  - **HAVING**: Filter groups with aggregate conditions
  - **UNION**: Combine results from multiple queries
- **Interactive Navigation**:
  - Step forward/backward through query execution
  - Play/pause automatic playback
  - Adjustable playback speed (0.5x to 3x)
  - Reset to beginning
- **Visual Features**:
  - **Input/Output Tables**: See table state before and after each operation
  - **Row Count Tracking**: Before/after row counts for each step
  - **Column Highlighting**: Highlighted columns relevant to current operation
  - **Dimmed Rows**: Visual indication of filtered-out rows
  - **Inspector Panel**: Detailed explanation of each step, step type, and statistics
  - **Code Editor**: Syntax-highlighted SQL editor with active line highlighting
- **Granular Stepping**: Break down SELECT clauses into individual column selections for detailed visualization
- **Real-time Execution**: Execute queries against uploaded data and see actual results

### 🔥 Advanced Graph Analytics

#### A. Impact / Blast Radius Analysis
- **Tree View Visualization**: Shows impact as a clean hierarchical tree (parent → child)
- **Downstream Impact Traversal**: Click any table to see all downstream tables that would be affected by changes
- **Depth-Limited Analysis**: Configurable traversal depth (1-10 hops)
- **Focused View**: Impact tree view hides unrelated nodes for clarity
- **Parent-Child Relationships**: Clear visualization showing source table (parent) impacting dependent tables (children)
- **Full Graph Restoration**: "Show Full Graph" button to return to the complete graph view
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
- **Query Visualizer** (`backend/query_visualizer.py`): 
  - Parses SQL queries into semantic steps
  - Executes queries step-by-step using pandas
  - Handles complex WHERE conditions (LIKE, NOT LIKE, AND, OR, IN subqueries)
  - Supports JOINs, GROUP BY, HAVING, UNION operations
  - Generates visual states for each execution step
- **API Endpoints**: Upload files, retrieve graph data, analytics, simulations, and query visualization

### Frontend (D3.js + CodeMirror)
- **Interactive Visualization**: Force-directed graph layout with D3.js
- **Controls**: Toggle FK/inferred edges, critical tables toggle, search functionality
- **Tree Layout**: Hierarchical tree visualization for impact analysis using D3 tree layout
- **Details Panel**: Shows table columns, relationships, data, and analytics:
  - Impact analysis with depth control and tree view
  - Join path finder
  - Constraint simulation
  - Enhanced edge explainability
- **Visual Highlighting**: Dynamic highlighting for impacts, paths, and critical tables
- **Data Table**: View and select rows for simulation operations
- **View Management**: Seamless switching between full graph and focused impact tree views
- **Query Visualizer**: 
  - CodeMirror-based SQL editor with syntax highlighting
  - Step-by-step query execution visualization
  - Interactive table displays with transformation tracking
  - Inspector panel with detailed step information

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
- **Filter**: Use checkboxes to toggle FK edges and inferred edges visibility

### Advanced Features

#### Impact Analysis
1. Click any table node to open the details panel
2. Click "Show Downstream Impact" button
3. Adjust depth slider (1-10 hops)
4. The graph switches to a focused tree view showing only the impact subgraph
5. Tree layout shows parent (source table) → children (impacted tables) with clear hierarchy
6. View impact statistics and affected tables list
7. Click "Show Full Graph" to return to the complete graph view
8. Clicking a different node automatically restores the full graph

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

#### SQL Query Visualizer
1. Switch to the "Query Visualizer" tab
2. Upload your SQL schema and/or CSV data first (required for query execution)
3. Enter a SQL query in the editor (e.g., `SELECT cid, company FROM customer WHERE cid LIKE "%bank%"`)
4. Click "Compile Query" to parse and prepare the query
5. Navigate through steps:
   - Use "Next Line" / "Prev Line" buttons to step through manually
   - Use "Play" button for automatic playback
   - Adjust speed slider for playback speed
   - Use "Reset" to return to the beginning
6. View transformations:
   - See input tables before each operation
   - See output tables after each operation
   - Check row counts (before → after) in the Inspector panel
   - View highlighted columns relevant to the current step
   - Read explanations of what each step does
7. Supported query features:
   - **Simple SELECT**: `SELECT col1, col2 FROM table`
   - **WHERE filtering**: `SELECT * FROM table WHERE col LIKE "%pattern%" AND col2 > 100`
   - **JOINs**: `SELECT * FROM table1 JOIN table2 ON table1.id = table2.id`
   - **GROUP BY / HAVING**: `SELECT col, COUNT(*) FROM table GROUP BY col HAVING COUNT(*) > 5`
   - **UNION**: `SELECT col FROM table1 UNION SELECT col FROM table2`
   - **Subqueries**: `SELECT * FROM table WHERE col IN (SELECT col FROM other_table)`

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
- `GET /api/graph` - Get graph data (all edges included)
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

### Query Visualizer Endpoints
- `POST /api/query/compile` - Compile SQL query into semantic steps
  - Body: `{"query": "SELECT ... FROM ... WHERE ..."}`
  - Returns: Query ID, steps, line mappings, and sub-steps
- `POST /api/query/state` - Get visual state for a specific query step
  - Body: `{"query_id": "...", "line_index": 0, "sub_step_index": 0}`
  - Returns: Input tables, output table, highlighted columns, explanations, row counts
- `GET /api/query/datasets` - Get list of available tables/datasets
  - Returns: List of tables with row counts and column information

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

## Limitations

- SQL parser supports common MySQL DDL syntax but not all dialects
- CSV relationship inference uses heuristics (name similarity, profile matching) rather than actual data overlap
- No real-time database synchronization
- Query Visualizer supports common SQL operations but may not handle all edge cases:
  - Complex nested subqueries
  - Window functions
  - CTEs (Common Table Expressions)
  - Some database-specific functions
- LIKE pattern matching supports `%` (any sequence) and `_` (single character) wildcards
- Query execution uses pandas DataFrames, so very large datasets may have performance limitations

## Project Structure

```
GraphMind/
├── backend/
│   ├── __init__.py
│   ├── main.py              # FastAPI application
│   ├── sql_parser.py        # MySQL DDL parser
│   ├── csv_analyzer.py      # CSV relationship inference
│   ├── graph_builder.py    # NetworkX graph management
│   ├── constraint_simulator.py  # DELETE/UPDATE simulation
│   └── query_visualizer.py # SQL query step-by-step execution
├── frontend/
│   ├── index.html           # Main UI
│   ├── graph.js             # D3.js graph visualization
│   ├── query_visualizer.js  # Query visualizer frontend logic
│   └── style.css            # Styling
├── examples/
│   ├── sample_schema.sql    # Example SQL file
│   └── *.csv                # Example CSV files
├── requirements.txt         # Python dependencies
├── run.py                  # Startup script
├── README.md               # This file
└── QUICKSTART.md           # Quick start guide
```

## License

See LICENSE file for details.
