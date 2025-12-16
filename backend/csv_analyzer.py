"""
CSV Analyzer for Relationship Inference
Profiles CSV columns and infers relationships with existing tables
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
from backend.graph_builder import GraphBuilder


class CSVAnalyzer:
    """Analyzer for CSV files to infer relationships"""
    
    def __init__(self):
        self.min_confidence = 0.3
        self.min_overlap_ratio = 0.1  # At least 10% overlap
        self.max_categorical_values = 20  # Exclude high-cardinality categoricals
    
    def profile_csv(self, df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
        """
        Profile a CSV DataFrame and return column statistics
        
        Returns:
            Dictionary with table name, columns, and their profiles
        """
        profile = {
            'table_name': table_name,
            'columns': []
        }
        
        for col in df.columns:
            col_data = df[col]
            
            # Basic statistics
            distinct_count = col_data.nunique()
            null_count = col_data.isna().sum()
            total_count = len(col_data)
            
            # Data type
            dtype = str(col_data.dtype)
            
            # Uniqueness ratio
            uniqueness = distinct_count / total_count if total_count > 0 else 0
            
            # Check if it's key-like (high uniqueness)
            is_key_like = uniqueness > 0.9 and null_count == 0
            
            # Check if it's foreign-key-like (high overlap potential)
            is_fk_like = 0.3 < uniqueness < 0.95
            
            # Exclude low-signal categoricals
            is_categorical = distinct_count <= self.max_categorical_values and uniqueness < 0.1
            
            col_profile = {
                'name': col,
                'type': dtype,
                'distinct_count': int(distinct_count),
                'null_count': int(null_count),
                'total_count': int(total_count),
                'uniqueness': float(uniqueness),
                'is_key_like': is_key_like,
                'is_fk_like': is_fk_like,
                'is_categorical': is_categorical
            }
            
            profile['columns'].append(col_profile)
        
        return profile
    
    def infer_relationships(
        self,
        df: pd.DataFrame,
        table_name: str,
        graph_builder: GraphBuilder
    ) -> List[Dict[str, Any]]:
        """
        Infer relationships between the CSV table and existing tables in the graph
        
        Returns:
            List of inferred edge dictionaries
        """
        inferred_edges = []
        
        # Get all existing tables from the graph
        existing_tables = graph_builder.get_all_tables()
        
        for existing_table in existing_tables:
            if existing_table['name'] == table_name:
                continue  # Skip self
            
            # Get table details to check columns
            table_details = graph_builder.get_table_details(existing_table['name'])
            if not table_details:
                continue
            
            # Try to find relationships between columns
            for csv_col in df.columns:
                csv_data = df[csv_col].dropna()
                
                # Skip if column is not suitable
                csv_profile = self._get_column_profile(df, csv_col)
                if csv_profile['is_categorical']:
                    continue
                
                # Check against each column in the existing table
                for existing_col_info in table_details.get('columns', []):
                    # Handle both dict and string column info
                    if isinstance(existing_col_info, dict):
                        existing_col = existing_col_info.get('name', '')
                        col_info = existing_col_info
                    else:
                        existing_col = str(existing_col_info)
                        col_info = {'name': existing_col}
                    
                    if not existing_col:
                        continue
                    
                    # We need sample data from existing table to compare
                    # For now, we'll use a heuristic based on column names and profiles
                    relationship = self._infer_column_relationship(
                        csv_col, csv_data, csv_profile,
                        existing_col, col_info,
                        table_name, existing_table['name']
                    )
                    
                    if relationship:
                        inferred_edges.append(relationship)
        
        # Remove duplicates and sort by confidence
        inferred_edges = self._deduplicate_edges(inferred_edges)
        inferred_edges.sort(key=lambda x: x['confidence'], reverse=True)
        
        return inferred_edges
    
    def _get_column_profile(self, df: pd.DataFrame, col: str) -> Dict[str, Any]:
        """Get profile for a single column"""
        col_data = df[col]
        distinct_count = col_data.nunique()
        total_count = len(col_data)
        uniqueness = distinct_count / total_count if total_count > 0 else 0
        
        return {
            'name': col,
            'distinct_count': distinct_count,
            'total_count': total_count,
            'uniqueness': uniqueness,
            'is_key_like': uniqueness > 0.9,
            'is_fk_like': 0.3 < uniqueness < 0.95,
            'is_categorical': distinct_count <= self.max_categorical_values and uniqueness < 0.1
        }
    
    def _infer_column_relationship(
        self,
        csv_col: str,
        csv_data: pd.Series,
        csv_profile: Dict[str, Any],
        existing_col: str,
        existing_col_info: Dict[str, Any],
        csv_table: str,
        existing_table: str
    ) -> Optional[Dict[str, Any]]:
        """
        Infer relationship between two columns
        
        This is a simplified version. In a real implementation, you'd need
        access to the actual data from existing tables to compute overlap.
        For now, we use heuristics based on column names and profiles.
        """
        # Heuristic 1: Column name similarity (e.g., user_id -> users.id)
        name_similarity = self._compute_name_similarity(csv_col, existing_col, existing_table)
        
        # Heuristic 2: Profile compatibility
        # If CSV column is FK-like and existing column is key-like, high confidence
        profile_match = 0.0
        if csv_profile['is_fk_like'] and existing_col_info.get('is_key_like', False):
            profile_match = 0.6
        elif csv_profile['is_key_like'] and existing_col_info.get('is_fk_like', False):
            # Reverse relationship
            profile_match = 0.4
        
        # Heuristic 3: Data type compatibility
        type_match = 0.0
        if csv_profile.get('type') == existing_col_info.get('type'):
            type_match = 0.2
        
        # Combined confidence
        confidence = name_similarity * 0.5 + profile_match * 0.4 + type_match * 0.1
        
        if confidence < self.min_confidence:
            return None
        
        # Determine direction: FK -> PK
        # If CSV column is FK-like, CSV -> Existing
        # If existing column is FK-like, Existing -> CSV
        if csv_profile['is_fk_like'] and existing_col_info.get('is_key_like', False):
            from_table = csv_table
            to_table = existing_table
            from_column = csv_col
            to_column = existing_col
        elif csv_profile['is_key_like'] and existing_col_info.get('is_fk_like', False):
            from_table = existing_table
            to_table = csv_table
            from_column = existing_col
            to_column = csv_col
        else:
            # Default: assume CSV -> Existing based on name similarity
            from_table = csv_table
            to_table = existing_table
            from_column = csv_col
            to_column = existing_col
        
        return {
            'from_table': from_table,
            'to_table': to_table,
            'from_column': from_column,
            'to_column': to_column,
            'confidence': confidence,
            'stats': {
                'name_similarity': name_similarity,
                'profile_match': profile_match,
                'type_match': type_match,
                'csv_uniqueness': csv_profile['uniqueness'],
                'existing_uniqueness': existing_col_info.get('uniqueness', 0)
            }
        }
    
    def _compute_name_similarity(self, col1: str, col2: str, table_name: str) -> float:
        """Compute similarity between column names"""
        col1_lower = col1.lower()
        col2_lower = col2.lower()
        table_lower = table_name.lower()
        
        # Exact match
        if col1_lower == col2_lower:
            return 1.0
        
        # Pattern: table_id matches id in table
        # e.g., user_id in CSV matches id in users table
        if col1_lower.endswith('_id') and col2_lower == 'id':
            prefix = col1_lower[:-3]  # Remove '_id'
            if prefix in table_lower or table_lower in prefix:
                return 0.8
        
        # Pattern: id matches table_id
        if col2_lower.endswith('_id') and col1_lower == 'id':
            prefix = col2_lower[:-3]
            if prefix in table_lower or table_lower in prefix:
                return 0.8
        
        # Contains relationship
        if col1_lower in col2_lower or col2_lower in col1_lower:
            return 0.5
        
        # Common prefix/suffix
        if col1_lower.startswith(col2_lower[:3]) or col2_lower.startswith(col1_lower[:3]):
            return 0.3
        
        return 0.0
    
    def _deduplicate_edges(self, edges: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove duplicate edges, keeping the one with highest confidence"""
        edge_map = {}
        
        for edge in edges:
            key = (edge['from_table'], edge['to_table'], edge['from_column'], edge['to_column'])
            if key not in edge_map or edge['confidence'] > edge_map[key]['confidence']:
                edge_map[key] = edge
        
        return list(edge_map.values())

