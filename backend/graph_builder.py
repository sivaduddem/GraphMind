"""
Graph Builder using NetworkX
Manages the graph model with nodes (tables) and edges (relationships)
"""
import networkx as nx
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict


class GraphBuilder:
    """Builds and manages a directed graph of table relationships"""
    
    def __init__(self):
        self.graph = nx.DiGraph()
        self.table_data = {}  # Store table metadata
        self.table_rows = {}  # Store table row data
        self._json_cache = None  # Cache for JSON output
        self._cache_confidence = None  # Confidence threshold used for cache
    
    def add_table(self, table_name: str, source: str, columns: List[Dict[str, Any]], rows: Optional[List[Dict[str, Any]]] = None):
        """Add a table node to the graph"""
        if not self.graph.has_node(table_name):
            self.graph.add_node(table_name)
            self.table_data[table_name] = {
                'name': table_name,
                'source': source,
                'columns': columns
            }
            self.table_rows[table_name] = rows or []
        else:
            # Update existing table
            self.table_data[table_name]['columns'] = columns
            if rows is not None:
                self.table_rows[table_name] = rows
        
        # Invalidate cache
        self._invalidate_cache()
    
    def get_table_rows(self, table_name: str) -> List[Dict[str, Any]]:
        """Get all rows for a table"""
        return self.table_rows.get(table_name, [])
    
    def add_fk_edge(
        self,
        from_table: str,
        to_table: str,
        from_columns: List[str],
        to_columns: List[str],
        on_delete: str = 'RESTRICT',
        on_update: str = 'RESTRICT'
    ):
        """Add a foreign key edge to the graph with constraint metadata"""
        if not self.graph.has_node(from_table):
            self.add_table(from_table, 'sql', [])
        if not self.graph.has_node(to_table):
            self.add_table(to_table, 'sql', [])
        
        # Store edge data with constraint information
        edge_data = {
            'kind': 'fk',
            'from_columns': from_columns,
            'to_columns': to_columns,
            'confidence': 1.0,
            'on_delete': on_delete.upper(),
            'on_update': on_update.upper()
        }
        
        # NetworkX supports edge attributes
        if self.graph.has_edge(from_table, to_table):
            # Multiple edges between same tables - store as list
            existing_data = self.graph[from_table][to_table]
            if 'edges' not in existing_data:
                existing_data['edges'] = [existing_data.copy()]
                # Remove individual attributes
                for key in list(existing_data.keys()):
                    if key != 'edges':
                        del existing_data[key]
            existing_data['edges'].append(edge_data)
        else:
            self.graph.add_edge(from_table, to_table, **edge_data)
        
        # Invalidate cache
        self._invalidate_cache()
    
    def add_inferred_edge(
        self,
        from_table: str,
        to_table: str,
        from_column: str,
        to_column: str,
        confidence: float,
        stats: Dict[str, Any]
    ):
        """Add an inferred edge to the graph"""
        if not self.graph.has_node(from_table):
            self.add_table(from_table, 'csv', [])
        if not self.graph.has_node(to_table):
            self.add_table(to_table, 'csv', [])
        
        edge_data = {
            'kind': 'inferred',
            'from_columns': [from_column],
            'to_columns': [to_column],
            'confidence': confidence,
            'stats': stats
        }
        
        if self.graph.has_edge(from_table, to_table):
            existing_data = self.graph[from_table][to_table]
            if 'edges' not in existing_data:
                existing_data['edges'] = [existing_data.copy()]
                for key in list(existing_data.keys()):
                    if key != 'edges':
                        del existing_data[key]
            existing_data['edges'].append(edge_data)
        else:
            self.graph.add_edge(from_table, to_table, **edge_data)
        
        # Invalidate cache
        self._invalidate_cache()
    
    def _invalidate_cache(self):
        """Invalidate the JSON cache"""
        self._json_cache = None
        self._cache_confidence = None
    
    def get_all_tables(self) -> List[Dict[str, Any]]:
        """Get all tables in the graph"""
        return list(self.table_data.values())
    
    def get_table_details(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about a table"""
        if table_name not in self.table_data:
            return None
        
        details = self.table_data[table_name].copy()
        
        # Add incoming and outgoing edges
        details['outgoing_edges'] = []
        details['incoming_edges'] = []
        
        for source, target, data in self.graph.edges(data=True):
            # Handle multiple edges between same nodes
            if 'edges' in data:
                edge_list = data['edges']
            else:
                edge_list = [data]
            
            for edge_data in edge_list:
                if source == table_name:
                    edge_info = {
                        'target': target,
                        'kind': edge_data.get('kind', 'unknown'),
                        'from_columns': edge_data.get('from_columns', []),
                        'to_columns': edge_data.get('to_columns', []),
                        'confidence': edge_data.get('confidence', 1.0)
                    }
                    if edge_data.get('on_delete'):
                        edge_info['on_delete'] = edge_data.get('on_delete')
                    if edge_data.get('on_update'):
                        edge_info['on_update'] = edge_data.get('on_update')
                    details['outgoing_edges'].append(edge_info)
                if target == table_name:
                    edge_info = {
                        'source': source,
                        'kind': edge_data.get('kind', 'unknown'),
                        'from_columns': edge_data.get('from_columns', []),
                        'to_columns': edge_data.get('to_columns', []),
                        'confidence': edge_data.get('confidence', 1.0)
                    }
                    if edge_data.get('on_delete'):
                        edge_info['on_delete'] = edge_data.get('on_delete')
                    if edge_data.get('on_update'):
                        edge_info['on_update'] = edge_data.get('on_update')
                    details['incoming_edges'].append(edge_info)
        
        return details
    
    def get_edge_details(self, from_table: str, to_table: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about an edge"""
        if not self.graph.has_edge(from_table, to_table):
            return None
        
        edge_data = self.graph[from_table][to_table].copy()
        
        # If multiple edges, return all
        if 'edges' in edge_data:
            return {
                'from_table': from_table,
                'to_table': to_table,
                'edges': edge_data['edges']
            }
        else:
            return {
                'from_table': from_table,
                'to_table': to_table,
                'kind': edge_data.get('kind', 'unknown'),
                'from_columns': edge_data.get('from_columns', []),
                'to_columns': edge_data.get('to_columns', []),
                'confidence': edge_data.get('confidence', 1.0),
                'stats': edge_data.get('stats', {})
            }
    
    def get_schema(self) -> Dict[str, Any]:
        """
        Build a concise schema representation for frontend schema/ERD view.

        Returns:
            {
                "tables": [
                    {
                        "name": str,
                        "source": str,
                        "columns": [{"name": str, "type": str}],
                        "column_count": int,
                        "primary_key": Optional[List[str]]
                    },
                    ...
                ],
                "relationships": [
                    {
                        "from_table": str,
                        "to_table": str,
                        "from_columns": List[str],
                        "to_columns": List[str],
                        "kind": "fk" | "inferred" | "unknown",
                        "on_delete": Optional[str],
                        "on_update": Optional[str],
                        "confidence": float
                    },
                    ...
                ]
            }
        """
        tables: List[Dict[str, Any]] = []
        relationships: List[Dict[str, Any]] = []

        # Build table list
        for table_name, info in self.table_data.items():
            columns = info.get('columns', []) or []

            # Best-effort primary key inference: look for "id" column
            pk_cols: List[str] = []
            for col in columns:
                col_name = str(col.get('name', '')).lower()
                if col_name == 'id':
                    pk_cols = [col.get('name')]
                    break

            tables.append(
                {
                    "name": table_name,
                    "source": info.get("source", "unknown"),
                    "columns": [
                        {
                            "name": col.get("name"),
                            "type": col.get("type", "unknown"),
                        }
                        for col in columns
                    ],
                    "column_count": len(columns),
                    "primary_key": pk_cols or None,
                }
            )

        # Build relationships list from graph edges
        for source, target, data in self.graph.edges(data=True):
            # Handle multiple edges between same nodes
            edge_list = data["edges"] if "edges" in data else [data]

            for edge in edge_list:
                relationships.append(
                    {
                        "from_table": source,
                        "to_table": target,
                        "from_columns": edge.get("from_columns", []),
                        "to_columns": edge.get("to_columns", []),
                        "kind": edge.get("kind", "unknown"),
                        "on_delete": edge.get("on_delete"),
                        "on_update": edge.get("on_update"),
                        "confidence": edge.get("confidence", 1.0),
                    }
                )

        return {"tables": tables, "relationships": relationships}
    
    def to_json(self, min_confidence: float = 0.0) -> Dict[str, Any]:
        """Convert graph to JSON format for frontend (with caching)"""
        # Return cached result if available and confidence matches
        if self._json_cache and self._cache_confidence == min_confidence:
            return self._json_cache
        
        nodes = []
        edges = []
        
        # Add all nodes
        for table_name in self.graph.nodes():
            table_info = self.table_data.get(table_name, {})
            nodes.append({
                'id': table_name,
                'source': table_info.get('source', 'unknown'),
                'column_count': len(table_info.get('columns', []))
            })
        
        # Add all edges
        for source, target, data in self.graph.edges(data=True):
            # Handle multiple edges between same nodes
            if 'edges' in data:
                edge_list = data['edges']
            else:
                edge_list = [data]
            
            for edge in edge_list:
                confidence = edge.get('confidence', 1.0)
                if confidence >= min_confidence:
                    edges.append({
                        'source': source,
                        'target': target,
                        'kind': edge.get('kind', 'unknown'),
                        'from_columns': edge.get('from_columns', []),
                        'to_columns': edge.get('to_columns', []),
                        'confidence': confidence,
                        'stats': edge.get('stats', {})
                    })
        
        result = {
            'nodes': nodes,
            'edges': edges
        }
        
        # Cache the result
        self._json_cache = result
        self._cache_confidence = min_confidence
        
        return result
    
    def clear(self):
        """Clear the entire graph"""
        self.graph.clear()
        self.table_data.clear()
        self.table_rows.clear()
        self._invalidate_cache()
    
    def get_subgraph(self, table_names: List[str], depth: int = 1) -> Dict[str, Any]:
        """
        Get a subgraph containing specified tables and their neighbors
        
        Args:
            table_names: List of table names to include
            depth: How many hops to include (1 = direct neighbors only)
        
        Returns:
            JSON representation of the subgraph
        """
        if not table_names:
            return {'nodes': [], 'edges': []}
        
        # Find all nodes within depth
        subgraph_nodes = set(table_names)
        current_level = set(table_names)
        
        for _ in range(depth):
            next_level = set()
            for node in current_level:
                if node in self.graph:
                    # Add neighbors
                    next_level.update(self.graph.predecessors(node))
                    next_level.update(self.graph.successors(node))
            subgraph_nodes.update(next_level)
            current_level = next_level
        
        # Build subgraph
        subgraph = self.graph.subgraph(subgraph_nodes)
        
        # Convert to JSON format
        nodes = []
        edges = []
        
        for table_name in subgraph.nodes():
            table_info = self.table_data.get(table_name, {})
            nodes.append({
                'id': table_name,
                'source': table_info.get('source', 'unknown'),
                'column_count': len(table_info.get('columns', []))
            })
        
        for source, target, data in subgraph.edges(data=True):
            if 'edges' in data:
                edge_list = data['edges']
            else:
                edge_list = [data]
            
            for edge in edge_list:
                confidence = edge.get('confidence', 1.0)
                if confidence >= 0.0:  # No confidence filter for subgraphs
                    edges.append({
                        'source': source,
                        'target': target,
                        'kind': edge.get('kind', 'unknown'),
                        'from_columns': edge.get('from_columns', []),
                        'to_columns': edge.get('to_columns', []),
                        'confidence': confidence,
                        'stats': edge.get('stats', {})
                    })
        
        return {'nodes': nodes, 'edges': edges}
    
    def get_downstream_impact(self, table_name: str, max_depth: int = 3) -> Dict[str, Any]:
        """
        Get all downstream tables that would be impacted by changes to this table
        
        "Downstream" means tables that DEPEND ON this table (have foreign keys pointing to it).
        If you delete/modify this table, these downstream tables would be affected.
        
        Example: If employee is deleted, works_on (which references employee) would be affected.
        
        Args:
            table_name: Name of the source table
            max_depth: Maximum number of hops to traverse (default: 3)
        
        Returns:
            Dictionary with impacted_tables, paths, and statistics
        """
        if not self.graph.has_node(table_name):
            return {"impacted_tables": [], "paths": [], "error": "Table not found"}
        
        impacted = set()
        paths = []
        
        # BFS traversal to find all tables that depend on this table
        # We use PREDECESSORS because we want tables that point TO this table (depend on it)
        visited = {table_name}
        queue = [(table_name, 0, [table_name])]  # (current_node, depth, path)
        
        while queue:
            current, depth, path = queue.pop(0)
            
            if depth >= max_depth:
                continue
            
            # Find all tables that reference this table (predecessors = tables pointing TO current)
            # These are the tables that would be affected if current table changes
            for dependent_table in self.graph.predecessors(current):
                if dependent_table not in visited:
                    visited.add(dependent_table)
                    impacted.add(dependent_table)
                    new_path = path + [dependent_table]
                    paths.append({
                        'from': table_name,
                        'to': dependent_table,
                        'path': new_path,
                        'hops': len(new_path) - 1
                    })
                    queue.append((dependent_table, depth + 1, new_path))
        
        return {
            "source_table": table_name,
            "impacted_tables": sorted(list(impacted)),
            "impact_count": len(impacted),
            "paths": paths,
            "max_depth": max_depth
        }
    
    def get_critical_tables(self) -> Dict[str, Any]:
        """
        Detect critical tables using graph analytics:
        - High in-degree (many dependents)
        - Articulation points (removing breaks graph)
        - High betweenness centrality (bottleneck nodes)
        
        Returns:
            Dictionary with critical tables and their metrics
        """
        import networkx as nx
        
        # Convert to undirected for articulation points
        undirected = self.graph.to_undirected()
        
        # Calculate metrics
        in_degree = dict(self.graph.in_degree())
        out_degree = dict(self.graph.out_degree())
        betweenness = nx.betweenness_centrality(self.graph)
        
        # Find articulation points (nodes whose removal disconnects graph)
        articulation_points = set(nx.articulation_points(undirected))
        
        # Calculate criticality score for each table
        critical_tables = {}
        max_in_degree = max(in_degree.values()) if in_degree.values() else 1
        max_betweenness = max(betweenness.values()) if betweenness.values() else 1
        
        for node in self.graph.nodes():
            in_deg = in_degree.get(node, 0)
            out_deg = out_degree.get(node, 0)
            betw = betweenness.get(node, 0)
            is_articulation = node in articulation_points
            
            # Normalized scores (0-1)
            in_degree_score = in_deg / max_in_degree if max_in_degree > 0 else 0
            betweenness_score = betw / max_betweenness if max_betweenness > 0 else 0
            
            # Combined criticality score
            criticality = (
                in_degree_score * 0.4 +  # High dependency score
                betweenness_score * 0.4 +  # Bottleneck score
                (1.0 if is_articulation else 0.0) * 0.2  # Articulation point bonus
            )
            
            critical_tables[node] = {
                'in_degree': in_deg,
                'out_degree': out_deg,
                'betweenness_centrality': betw,
                'is_articulation_point': is_articulation,
                'criticality_score': criticality
            }
        
        # Sort by criticality
        sorted_critical = sorted(
            critical_tables.items(),
            key=lambda x: x[1]['criticality_score'],
            reverse=True
        )
        
        return {
            "critical_tables": dict(sorted_critical),
            "top_critical": [name for name, _ in sorted_critical[:10]]
        }
    
    def find_join_paths(self, from_table: str, to_table: str, max_depth: int = 5) -> Dict[str, Any]:
        """
        Find join paths between two tables
        
        Args:
            from_table: Source table name
            to_table: Target table name
            max_depth: Maximum path length to search
        
        Returns:
            Dictionary with paths, shortest path, and join details
        """
        if not self.graph.has_node(from_table) or not self.graph.has_node(to_table):
            return {
                "error": "One or both tables not found",
                "from_table": from_table,
                "to_table": to_table
            }
        
        try:
            import networkx as nx
            
            # Find shortest path
            try:
                shortest_path = nx.shortest_path(self.graph, from_table, to_table)
            except nx.NetworkXNoPath:
                shortest_path = None
            
            # Find all simple paths (up to max_depth)
            all_paths = []
            try:
                for path in nx.all_simple_paths(self.graph, from_table, to_table, cutoff=max_depth):
                    all_paths.append(path)
            except nx.NetworkXNoPath:
                pass
            
            # Build path details with join information
            path_details = []
            for path in all_paths[:10]:  # Limit to top 10 paths
                edges_in_path = []
                for i in range(len(path) - 1):
                    source = path[i]
                    target = path[i + 1]
                    edge_data = self.get_edge_details(source, target)
                    if edge_data:
                        if 'edges' in edge_data:
                            # Use first edge
                            edge = edge_data['edges'][0]
                        else:
                            edge = edge_data
                        
                        edges_in_path.append({
                            'from_table': source,
                            'to_table': target,
                            'from_columns': edge.get('from_columns', []),
                            'to_columns': edge.get('to_columns', []),
                            'kind': edge.get('kind', 'unknown'),
                            'confidence': edge.get('confidence', 1.0)
                        })
                
                path_details.append({
                    'path': path,
                    'hops': len(path) - 1,
                    'edges': edges_in_path,
                    'is_shortest': path == shortest_path if shortest_path else False
                })
            
            # Sort by path length
            path_details.sort(key=lambda x: x['hops'])
            
            return {
                "from_table": from_table,
                "to_table": to_table,
                "shortest_path": shortest_path,
                "shortest_path_length": len(shortest_path) - 1 if shortest_path else None,
                "total_paths_found": len(all_paths),
                "paths": path_details[:5],  # Return top 5 paths
                "max_depth_searched": max_depth
            }
        except Exception as e:
            return {
                "error": str(e),
                "from_table": from_table,
                "to_table": to_table
            }

