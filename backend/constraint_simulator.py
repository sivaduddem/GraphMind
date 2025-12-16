"""
Constraint Simulator for GraphMind
Simulates DELETE and UPDATE operations to predict database errors
"""
from typing import Dict, List, Any, Optional
from backend.graph_builder import GraphBuilder


class ConstraintSimulator:
    """Simulates database operations and predicts constraint violations"""
    
    def __init__(self, graph_builder: GraphBuilder):
        self.graph_builder = graph_builder
    
    def simulate_delete(self, table_name: str) -> Dict[str, Any]:
        """
        Simulate a DELETE operation on a table
        
        Returns:
            Dictionary with result, error_type, blocked_by, and explanation
        """
        if not self.graph_builder.graph.has_node(table_name):
            return {
                "result": "error",
                "error_type": "table_not_found",
                "message": f"Table '{table_name}' not found in graph"
            }
        
        # Find all incoming edges (tables that reference this table)
        incoming_edges = []
        for source, target, data in self.graph_builder.graph.edges(data=True):
            if target == table_name:
                # Handle multiple edges between same nodes
                if 'edges' in data:
                    incoming_edges.extend(data['edges'])
                else:
                    incoming_edges.append(data)
        
        # Get actual source table names from graph
        blocking_table_names = []
        cascade_table_names = []
        inferred_table_names = []
        
        for source, target, data in self.graph_builder.graph.edges(data=True):
            if target == table_name:
                if 'edges' in data:
                    edge_list = data['edges']
                else:
                    edge_list = [data]
                
                for edge in edge_list:
                    if edge.get('kind') == 'fk':
                        on_delete = edge.get('on_delete', 'RESTRICT')
                        if on_delete in ['RESTRICT', 'NO ACTION', None]:
                            if source not in blocking_table_names:
                                blocking_table_names.append(source)
                        elif on_delete == 'CASCADE':
                            if source not in cascade_table_names:
                                cascade_table_names.append(source)
                    elif edge.get('kind') == 'inferred':
                        if source not in inferred_table_names:
                            inferred_table_names.append(source)
        
        # Determine result
        if blocking_table_names:
            # Build explanation
            explanations = []
            for source in blocking_table_names:
                # Get edge details
                edge_details = self.graph_builder.get_edge_details(source, table_name)
                if edge_details:
                    if 'edges' in edge_details:
                        for e in edge_details['edges']:
                            if e.get('kind') == 'fk':
                                from_cols = ', '.join(e.get('from_columns', []))
                                to_cols = ', '.join(e.get('to_columns', []))
                                explanations.append(
                                    f"{source}.{from_cols} references {table_name}.{to_cols}"
                                )
                    else:
                        from_cols = ', '.join(edge_details.get('from_columns', []))
                        to_cols = ', '.join(edge_details.get('to_columns', []))
                        explanations.append(
                            f"{source}.{from_cols} references {table_name}.{to_cols}"
                        )
            
            return {
                "result": "failure",
                "error_type": "referential_integrity",
                "blocked_by": blocking_table_names,
                "cascade_tables": cascade_table_names,
                "inferred_risks": inferred_table_names,
                "explanation": f"DELETE blocked: {table_name} is referenced by {', '.join(blocking_table_names)}. " + 
                             f"Foreign key constraints prevent deletion.",
                "detailed_explanations": explanations
            }
        else:
            # Success, but check for inferred risks
            warnings = []
            if inferred_table_names:
                warnings.append(
                    f"Warning: {len(inferred_table_names)} inferred relationship(s) may break: {', '.join(inferred_table_names)}"
                )
            
            return {
                "result": "success",
                "error_type": None,
                "blocked_by": [],
                "cascade_tables": cascade_table_names,
                "inferred_risks": inferred_table_names,
                "explanation": f"DELETE would succeed. " + 
                             (f"{len(cascade_table_names)} table(s) would be CASCADE deleted. " if cascade_table_names else "") +
                             (" ".join(warnings) if warnings else ""),
                "warnings": warnings
            }
    
    def simulate_update(self, table_name: str, column: Optional[str] = None) -> Dict[str, Any]:
        """
        Simulate an UPDATE operation on a table column
        
        Args:
            table_name: Name of the table
            column: Name of the column being updated (if None, assumes PK update)
        
        Returns:
            Dictionary with result, error_type, blocked_by, and explanation
        """
        if not self.graph_builder.graph.has_node(table_name):
            return {
                "result": "error",
                "error_type": "table_not_found",
                "message": f"Table '{table_name}' not found in graph"
            }
        
        # Get table details to check if column is a primary key
        table_details = self.graph_builder.get_table_details(table_name)
        if not table_details:
            return {
                "result": "error",
                "error_type": "table_not_found",
                "message": f"Could not retrieve details for table '{table_name}'"
            }
        
        # If column is specified, check if it's part of PK or referenced
        # For now, we'll assume updating any column that's referenced causes issues
        # In a real implementation, we'd check if the column is actually a PK
        
        # Find all incoming edges that reference this table
        blocking_tables = []
        cascade_tables = []
        inferred_risks = []
        
        for source, target, data in self.graph_builder.graph.edges(data=True):
            if target == table_name:
                if 'edges' in data:
                    edge_list = data['edges']
                else:
                    edge_list = [data]
                
                for edge in edge_list:
                    if edge.get('kind') == 'fk':
                        # Check if the column being updated is referenced
                        referenced_cols = edge.get('to_columns', [])
                        if not column or column in referenced_cols:
                            on_update = edge.get('on_update', 'RESTRICT')
                            if on_update in ['RESTRICT', 'NO ACTION', None]:
                                blocking_tables.append(source)
                            elif on_update == 'CASCADE':
                                cascade_tables.append(source)
                    elif edge.get('kind') == 'inferred':
                        if not column:
                            inferred_risks.append(source)
        
        if blocking_tables:
            explanations = []
            for source in blocking_tables:
                edge_details = self.graph_builder.get_edge_details(source, table_name)
                if edge_details:
                    if 'edges' in edge_details:
                        for e in edge_details['edges']:
                            if e.get('kind') == 'fk':
                                from_cols = ', '.join(e.get('from_columns', []))
                                to_cols = ', '.join(e.get('to_columns', []))
                                explanations.append(
                                    f"{source}.{from_cols} references {table_name}.{to_cols}"
                                )
                    else:
                        from_cols = ', '.join(edge_details.get('from_columns', []))
                        to_cols = ', '.join(edge_details.get('to_columns', []))
                        explanations.append(
                            f"{source}.{from_cols} references {table_name}.{to_cols}"
                        )
            
            return {
                "result": "failure",
                "error_type": "referential_integrity",
                "blocked_by": blocking_tables,
                "cascade_tables": cascade_tables,
                "inferred_risks": inferred_risks,
                "explanation": f"UPDATE blocked: {table_name}.{column if column else 'PK'} is referenced by {', '.join(blocking_tables)}. " +
                             f"Foreign key constraints prevent update.",
                "detailed_explanations": explanations
            }
        else:
            warnings = []
            if inferred_risks:
                warnings.append(
                    f"Warning: {len(inferred_risks)} inferred relationship(s) may break: {', '.join(inferred_risks)}"
                )
            
            return {
                "result": "success",
                "error_type": None,
                "blocked_by": [],
                "cascade_tables": cascade_tables,
                "inferred_risks": inferred_risks,
                "explanation": f"UPDATE would succeed. " +
                             (f"{len(cascade_tables)} table(s) would be CASCADE updated. " if cascade_tables else ""),
                "warnings": warnings
            }
    
    def get_delete_risk_score(self, table_name: str) -> Dict[str, Any]:
        """
        Calculate delete risk score for a table
        
        Returns:
            Dictionary with risk_score, risk_level, and details
        """
        if not self.graph_builder.graph.has_node(table_name):
            return {"risk_score": 0, "risk_level": "none", "message": "Table not found"}
        
        # Count incoming FK edges
        incoming_fk_count = 0
        restrict_count = 0
        cascade_count = 0
        
        for source, target, data in self.graph_builder.graph.edges(data=True):
            if target == table_name:
                if 'edges' in data:
                    edge_list = data['edges']
                else:
                    edge_list = [data]
                
                for edge in edge_list:
                    if edge.get('kind') == 'fk':
                        incoming_fk_count += 1
                        on_delete = edge.get('on_delete', 'RESTRICT')
                        if on_delete in ['RESTRICT', 'NO ACTION', None]:
                            restrict_count += 1
                        elif on_delete == 'CASCADE':
                            cascade_count += 1
        
        # Calculate risk score (0-100)
        # Base score: number of incoming FKs
        risk_score = incoming_fk_count * 10
        
        # Higher risk if more RESTRICT constraints
        risk_score += restrict_count * 5
        
        # Determine risk level
        if risk_score >= 50:
            risk_level = "high"
        elif risk_score >= 25:
            risk_level = "medium"
        elif risk_score > 0:
            risk_level = "low"
        else:
            risk_level = "none"
        
        return {
            "risk_score": min(risk_score, 100),
            "risk_level": risk_level,
            "incoming_fk_count": incoming_fk_count,
            "restrict_count": restrict_count,
            "cascade_count": cascade_count
        }

