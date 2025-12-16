# GraphMind Quick Start Guide

## Quick Setup (5 minutes)

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Start the Server

```bash
python run.py
```

Or directly:
```bash
cd backend
python main.py
```

The server will start at `http://localhost:8000`

### 3. Open the Application

Open your browser and navigate to:
```
http://localhost:8000
```

The frontend will be automatically served by the backend.

## Try It Out

### Step 1: Upload a SQL Schema

1. Click "Choose SQL File" in the sidebar
2. Select `examples/sample_schema.sql` (or any MySQL .sql file)
3. Wait for the success message
4. You should see tables appear in the graph!

### Step 2: Upload CSV Data

1. Click "Choose CSV File" in the sidebar
2. Select `examples/departments.csv` or `examples/employees.csv` (or any CSV file)
3. The system will analyze the CSV and infer relationships
4. New edges (dashed red lines) will appear showing inferred relationships

### Step 3: Explore the Graph

- **Zoom**: Use mouse wheel
- **Pan**: Click and drag the background
- **Move Nodes**: Click and drag nodes
- **View Details**: Click on any node or edge
- **Search**: Type in the search box to highlight tables
- **Filter**: Adjust confidence slider to show/hide low-confidence relationships

## Understanding the Visualization

- **Blue Nodes**: Tables from SQL schemas
- **Red Nodes**: Tables from CSV files
- **Blue Solid Lines**: Explicit foreign key relationships
- **Red Dashed Lines**: Inferred relationships (with confidence scores)

## Troubleshooting

### Frontend not loading?
- Make sure the backend is running on port 8000
- Check browser console for errors
- Try accessing `http://localhost:8000/api/` to verify the API is working

### CORS errors?
- The backend includes CORS middleware, so this shouldn't happen
- If you're serving the frontend separately, make sure it's on the same origin or update CORS settings

### No relationships inferred?
- CSV inference uses heuristics (column names, profiles)
- Try using column names like `user_id`, `product_id` that match table names
- Lower the confidence threshold slider to see more relationships

## Next Steps

- Upload your own SQL schemas
- Upload multiple CSV files to build a complete graph
- Use the search feature to find specific tables
- Click nodes/edges to explore detailed information

## API Testing

You can test the API directly:

```bash
# Get graph data
curl http://localhost:8000/api/graph

# Get table details
curl http://localhost:8000/api/table/employees

# Clear graph
curl -X DELETE http://localhost:8000/api/graph
```

