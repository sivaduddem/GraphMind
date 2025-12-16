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
        self._json_cache = None  # Cache for JSON output
        self._cache_confidence = None  # Confidence threshold used for cache
    
    def add_table(self, table_name: str, source: str, columns: List[Dict[str, Any]]):
        """Add a table node to the graph"""
        if not self.graph.has_node(table_name):
            self.graph.add_node(table_name)
            self.table_data[table_name] = {
                'name': table_name,
                'source': source,
                'columns': columns
            }
        else:
            # Update existing table
            self.table_data[table_name]['columns'] = columns
        
        # Invalidate cache
        self._invalidate_cache()
    
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
            if source == table_name:
                edge_info = {
                    'target': target,
                    'kind': data.get('kind', 'unknown'),
                    'from_columns': data.get('from_columns', []),
                    'to_columns': data.get('to_columns', []),
                    'confidence': data.get('confidence', 1.0)
                }
                if data.get('on_delete'):
                    edge_info['on_delete'] = data.get('on_delete')
                if data.get('on_update'):
                    edge_info['on_update'] = data.get('on_update')
                details['outgoing_edges'].append(edge_info)
            if target == table_name:
                edge_info = {
                    'source': source,
                    'kind': data.get('kind', 'unknown'),
                    'from_columns': data.get('from_columns', []),
                    'to_columns': data.get('to_columns', []),
                    'confidence': data.get('confidence', 1.0)
                }
                if data.get('on_delete'):
                    edge_info['on_delete'] = data.get('on_delete')
                if data.get('on_update'):
                    edge_info['on_update'] = data.get('on_update')
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

