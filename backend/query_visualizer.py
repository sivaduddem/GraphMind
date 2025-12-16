"""
SQL Query Visualizer
Parses SQL queries, extracts semantic steps, and executes them step-by-step
"""
import re
import sqlparse
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict
import pandas as pd
import numpy as np
import math

# Try to import duckdb, but make it optional
try:
    import duckdb
    HAS_DUCKDB = True
except ImportError:
    HAS_DUCKDB = False
    duckdb = None


class QueryVisualizer:
    """Parses SQL queries and generates step-by-step visualization states"""
    
    def __init__(self, graph_builder):
        self.graph_builder = graph_builder
        self.compiled_queries = {}  # Cache compiled queries by query_id
    
    def _clean_for_json(self, obj):
        """Recursively clean NaN, inf, and -inf values from data structures for JSON serialization"""
        if isinstance(obj, dict):
            return {k: self._clean_for_json(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._clean_for_json(item) for item in obj]
        elif isinstance(obj, (float, np.floating)):
            if pd.isna(obj) or math.isnan(obj):
                return None
            elif math.isinf(obj):
                return None
            else:
                return obj
        elif isinstance(obj, pd.Series):
            return [self._clean_for_json(item) for item in obj]
        elif isinstance(obj, pd.DataFrame):
            # Convert DataFrame to dict and clean
            return self._clean_for_json(obj.to_dict('records'))
        else:
            return obj
        
    def compile_query(self, query_text: str, query_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Compile a SQL query into semantic steps and line mappings
        
        Returns:
            {
                'steps': List[Step],
                'line_to_step': Dict[int, int],  # line number -> step index
                'line_count': int,
                'query_id': str
            }
        """
        # Store original query for reference
        original_query = query_text
        
        # Remove "CREATE OR REPLACE VIEW viewname AS" prefix if present
        # Extract only the SELECT statement
        query_lower = query_text.lower().strip()
        if 'create' in query_lower and 'view' in query_lower:
            # Find the SELECT keyword after CREATE VIEW
            select_idx = query_lower.find('select')
            if select_idx >= 0:
                query_text = query_text[select_idx:].strip()
        
        # Note: We keep the trailing semicolon during parsing so sqlparse can handle it correctly
        # Semicolons in the middle (like "employee;") will be stripped from table names during extraction
        
        # Check for UNION queries
        has_union = ' union ' in query_lower
        if has_union:
            # Split by UNION
            union_parts = re.split(r'\s+union\s+', query_text, flags=re.IGNORECASE)
            if len(union_parts) == 2:
                # Process as UNION query
                steps = self._extract_union_steps(union_parts[0], union_parts[1], query_text)
                query_lines = query_text.split('\n')
                line_count = len(query_lines)
            else:
                raise ValueError("UNION queries with more than 2 parts are not yet supported")
        else:
            # Parse SQL into AST
            parsed = sqlparse.parse(query_text)
            if not parsed or not parsed[0]:
                raise ValueError("Invalid SQL query")
            
            ast = parsed[0]
            query_lines = query_text.split('\n')
            line_count = len(query_lines)
            
            # Extract semantic steps
            steps = self._extract_steps(ast, query_text)
        
        # Map SQL lines to step indices
        line_to_step = self._map_lines_to_steps(query_text, steps, line_count)
        
        query_id = query_id or f"query_{hash(query_text)}"
        
        # Create sub_steps list for granular stepping (one per SELECT_COL step)
        sub_steps = []
        for i, step in enumerate(steps):
            if step['type'] == 'SELECT_COL':
                sub_steps.append({
                    'step_index': i,
                    'sub_step_index': len(sub_steps),
                    'type': 'SELECT_COL',
                    'column': step.get('column', ''),
                    'description': step.get('description', '')
                })
            else:
                sub_steps.append({
                    'step_index': i,
                    'sub_step_index': len(sub_steps),
                    'type': step['type'],
                    'description': step.get('description', '')
                })
        
        result = {
            'steps': steps,
            'sub_steps': sub_steps,  # Granular steps for frontend
            'line_to_step': line_to_step,
            'line_count': line_count,
            'total_steps': len(steps),  # Total semantic steps
            'total_sub_steps': len(sub_steps),  # Total granular steps
            'query_id': query_id
        }
        
        # Cache compiled query
        self.compiled_queries[query_id] = result
        
        return result
    
    def get_visual_state(self, query_id: str, line_index: int, sub_step_index: Optional[int] = None) -> Dict[str, Any]:
        """
        Get the visual state for a specific line index
        
        Returns:
            {
                'input_tables': List[Dict],  # Tables to display as input
                'output_table': Dict,  # Result table after this step
                'highlighted_cols': List[str],
                'dimmed_rows': List[int],  # Row indices to dim
                'annotations': Dict,
                'explanation_text': str,
                'step_type': str,
                'before_row_count': int,
                'after_row_count': int,
                'join_condition': Optional[Dict]
            }
        """
        if query_id not in self.compiled_queries:
            raise ValueError(f"Query {query_id} not found. Compile query first.")
        
        compiled = self.compiled_queries[query_id]
        
        # If sub_step_index is provided, use granular stepping
        if sub_step_index is not None and 'sub_steps' in compiled:
            sub_steps = compiled['sub_steps']
            if sub_step_index < 0:
                sub_step_index = 0
            if sub_step_index >= len(sub_steps):
                sub_step_index = len(sub_steps) - 1
            
            sub_step_info = sub_steps[sub_step_index]
            step_index = sub_step_info['step_index']
            steps = compiled['steps']
        else:
            # Legacy line-based stepping
            line_to_step = compiled['line_to_step']
            step_index = line_to_step.get(line_index, -1)
            if step_index < 0:
                return {
                    'input_tables': [],
                    'output_table': None,
                    'highlighted_cols': [],
                    'dimmed_rows': [],
                    'annotations': {},
                    'explanation_text': 'Ready to execute query',
                    'step_type': 'initial',
                    'before_row_count': 0,
                    'after_row_count': 0,
                    'join_condition': None
                }
            
            steps = compiled['steps']
            if step_index >= len(steps):
                step_index = len(steps) - 1
        
        # Execute query up to this step
        try:
            visual_state = self._execute_step(step_index, steps, query_id)
        except Exception as e:
            # Fallback if execution fails
            current_step = steps[step_index] if step_index < len(steps) else None
            visual_state = {
                'input_tables': [],
                'output_table': None,
                'highlighted_cols': [],
                'dimmed_rows': [],
                'annotations': {'error': str(e)},
                'explanation_text': current_step.get('description', f'Error executing step: {str(e)}') if current_step else f'Error: {str(e)}',
                'step_type': current_step.get('type', 'error') if current_step else 'error',
                'before_row_count': 0,
                'after_row_count': 0,
                'join_condition': None
            }
        
        # Ensure explanation is always set - use step description as fallback
        if not visual_state.get('explanation_text') or visual_state.get('explanation_text') in ['Processing query step...', 'Processing query step', 'No explanation available']:
            current_step = steps[step_index] if step_index < len(steps) else None
            if current_step:
                # Use the step's description field which should always be set during compilation
                visual_state['explanation_text'] = current_step.get('description', f'Executing {current_step.get("type", "step")}')
            else:
                visual_state['explanation_text'] = 'Ready to execute query'
        
        return visual_state
    
    def _extract_steps(self, ast, query_text: str) -> List[Dict[str, Any]]:
        """Extract semantic steps from SQL AST"""
        steps = []
        
        # Tokenize and identify clauses
        tokens = list(ast.flatten())
        token_strs = [str(t).strip() for t in tokens if str(t).strip()]
        query_lower = query_text.lower()
        
        # Find FROM clause - use string search as more reliable
        from_idx = query_lower.find(' from ')
        from_table = None
        if from_idx >= 0:
            # Find the table name after FROM
            from_start = from_idx + 6  # Skip " from "
            # Find where the table name ends (before JOIN, WHERE, or end of line)
            from_end = query_lower.find(' join ', from_start)
            if from_end < 0:
                from_end = query_lower.find(' where ', from_start)
            if from_end < 0:
                from_end = query_lower.find(' group ', from_start)
            if from_end < 0:
                from_end = len(query_text)
            
            from_clause = query_text[from_start:from_end].strip()
            # Extract table name (remove alias if present)
            # Handle "table alias" or "table AS alias"
            parts = from_clause.split()
            if parts:
                from_table = parts[0].strip('"\'`').rstrip(';')  # Remove semicolon if present in middle
                # Skip alias if present
                if len(parts) > 1 and parts[1].upper() not in ['AS', 'ON', 'USING', 'INNER', 'LEFT', 'RIGHT', 'FULL', 'OUTER', 'CROSS', 'JOIN']:
                    # parts[1] might be an alias, but we already got the table name
                    pass
        
        if from_table:
            steps.append({
                'type': 'FROM',
                'table': from_table,
                'line_range': self._find_line_range_for_text(query_text, from_idx),
                'description': f'Load table {from_table}'
            })
        
        # Find JOIN clauses - use string search for reliability
        join_positions = []
        search_start = 0
        while True:
            join_idx = query_lower.find(' join ', search_start)
            if join_idx < 0:
                break
            
            # Determine join type
            before_join = query_lower[max(0, join_idx-10):join_idx].strip()
            join_type = 'INNER JOIN'
            if 'left' in before_join:
                join_type = 'LEFT JOIN'
            elif 'right' in before_join:
                join_type = 'RIGHT JOIN'
            elif 'full' in before_join:
                join_type = 'FULL JOIN'
            
            # Find table name after JOIN
            join_start = join_idx + 6  # Skip " join "
            # Find where table name ends (before ON, WHERE, etc.)
            table_end = query_lower.find(' on ', join_start)
            if table_end < 0:
                table_end = query_lower.find(' where ', join_start)
            if table_end < 0:
                table_end = query_lower.find(' group ', join_start)
            if table_end < 0:
                table_end = len(query_text)
            
            join_table_clause = query_text[join_start:table_end].strip()
            parts = join_table_clause.split()
            join_table = parts[0].strip('"\'`').rstrip(';') if parts else None  # Remove semicolon if present in middle
            
            # Find ON condition
            join_condition = None
            on_idx = query_lower.find(' on ', join_start)
            if on_idx >= 0:
                on_start = on_idx + 4  # Skip " on "
                condition_end = query_lower.find(' where ', on_start)
                if condition_end < 0:
                    condition_end = query_lower.find(' group ', on_start)
                if condition_end < 0:
                    condition_end = len(query_text)
                join_condition = query_text[on_start:condition_end].strip().rstrip(';')
            
            if join_table:
                steps.append({
                    'type': 'JOIN',
                    'join_type': join_type,
                    'table': join_table,
                    'condition': join_condition,
                    'line_range': self._find_line_range_for_text(query_text, join_idx),
                    'description': f'{join_type} with {join_table}'
                })
            
            search_start = join_idx + 1
        
        # Find WHERE clause
        where_idx = query_lower.find(' where ')
        if where_idx >= 0:
            where_end = query_lower.find(' group ', where_idx)
            if where_end < 0:
                where_end = query_lower.find(' having ', where_idx)
            if where_end < 0:
                where_end = query_lower.find(' order ', where_idx)
            if where_end < 0:
                where_end = len(query_text)
            
            where_clause = query_text[where_idx:where_end].strip()
            # Remove "where" keyword from the beginning if present
            if where_clause.lower().startswith('where'):
                where_clause = where_clause[5:].strip()
            # Remove trailing semicolon if present
            if where_clause.endswith(';'):
                where_clause = where_clause[:-1].strip()
            # Extract column names from WHERE clause
            where_cols = self._extract_column_names(where_clause)
            
            steps.append({
                'type': 'WHERE',
                'condition': where_clause,
                'columns': where_cols,
                'line_range': self._find_line_range_for_text(query_text, where_idx),
                'description': f'Filter rows: {where_clause}'
            })
        
        # Find GROUP BY clause
        group_idx = query_lower.find(' group by ')
        if group_idx >= 0:
            group_end = query_lower.find(' having ', group_idx)
            if group_end < 0:
                group_end = query_lower.find(' order ', group_idx)
            if group_end < 0:
                group_end = len(query_text)
            
            group_clause = query_text[group_idx:group_end].strip()
            group_cols = self._extract_column_names(group_clause)
            
            steps.append({
                'type': 'GROUP_BY',
                'columns': group_cols,
                'line_range': self._find_line_range_for_text(query_text, group_idx),
                'description': f'Group by: {", ".join(group_cols)}'
            })
        
        # Find HAVING clause
        having_idx = query_lower.find(' having ')
        if having_idx >= 0:
            having_end = query_lower.find(' order ', having_idx)
            if having_end < 0:
                having_end = len(query_text)
            
            having_clause = query_text[having_idx:having_end].strip()
            having_cols = self._extract_column_names(having_clause)
            
            steps.append({
                'type': 'HAVING',
                'condition': having_clause,
                'columns': having_cols,
                'line_range': self._find_line_range_for_text(query_text, having_idx),
                'description': f'Filter groups: {having_clause}'
            })
        
        # Find SELECT clause (projection) - break into individual column steps
        select_idx = query_lower.find('select ')
        if select_idx >= 0:
            select_end = query_lower.find(' from ', select_idx)
            if select_end < 0:
                select_end = len(query_text)
            
            select_clause = query_text[select_idx:select_end].strip()
            select_cols = self._extract_column_names(select_clause)
            
            # Break SELECT into individual column selection steps for granular visualization
            # Each column gets its own step so we can show them one at a time
            for i, col in enumerate(select_cols):
                select_step = {
                    'type': 'SELECT_COL',
                    'column': col,
                    'column_index': i,
                    'all_columns': select_cols,
                    'selected_so_far': select_cols[:i+1],  # Columns selected up to this point
                    'line_range': self._find_line_range_for_text(query_text, select_idx),
                    'description': f'Select column: {col.split(".")[-1] if "." in col else col}'
                }
                steps.append(select_step)
        
        # Sort steps by line number (SELECT_COL steps go at the end)
        non_select_steps = [s for s in steps if s.get('type') != 'SELECT_COL']
        non_select_steps.sort(key=lambda s: s['line_range'][0] if s.get('line_range') and isinstance(s['line_range'], tuple) else 999)
        
        # Add SELECT_COL steps at the end, sorted by column_index
        select_col_steps = [s for s in steps if s.get('type') == 'SELECT_COL']
        select_col_steps.sort(key=lambda s: s.get('column_index', 0))
        
        # Rebuild steps
        steps = non_select_steps + select_col_steps
        
        return steps
    
    def _extract_column_names(self, text: str) -> List[str]:
        """Extract column names from SQL text, preserving table prefixes"""
        # Handle aggregate functions like min(frequency), max(frequency)
        # Handle DISTINCT keyword
        text = re.sub(r'\bdistinct\b', '', text, flags=re.IGNORECASE)
        
        # Simple regex to find column-like patterns
        # Match patterns like table.col, col, aggregate_func(col), or "col"
        # First, extract aggregate functions
        agg_pattern = r'(min|max|sum|avg|count)\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\)'
        agg_matches = re.findall(agg_pattern, text, re.IGNORECASE)
        cols = []
        for func, col_name in agg_matches:
            # Store as "func(col)" for aggregate functions
            cols.append(f"{func}({col_name})")
        
        # Remove aggregate functions from text to avoid double extraction
        text_no_agg = re.sub(agg_pattern, '', text, flags=re.IGNORECASE)
        
        # Now extract regular columns
        pattern = r'([a-zA-Z_][a-zA-Z0-9_]*\.)?([a-zA-Z_][a-zA-Z0-9_]*)'
        matches = re.findall(pattern, text_no_agg)
        for match in matches:
            table_prefix = match[0]  # e.g., "f." or "b."
            col_name = match[1]  # e.g., "fsid"
            # Filter out SQL keywords
            keywords = {'select', 'from', 'where', 'group', 'by', 'having', 'order', 'limit', 
                       'join', 'on', 'inner', 'left', 'right', 'full', 'outer', 'and', 'or', 'not',
                       'as', 'count', 'sum', 'avg', 'max', 'min', 'distinct', 'in', 'union',
                       'null', 'is', 'like', 'not'}
            if col_name.lower() not in keywords and len(col_name) > 0:
                # Preserve table prefix if present (e.g., "f.fsid" or just "fsid")
                if table_prefix:
                    cols.append(table_prefix.rstrip('.') + '.' + col_name)
                else:
                    cols.append(col_name)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_cols = []
        for col in cols:
            if col not in seen:
                unique_cols.append(col)
                seen.add(col)
        
        return unique_cols
    
    def _find_line_range(self, query_text: str, token_idx: int, tokens) -> Tuple[int, int]:
        """Find line range for a token"""
        # Simplified: find position in text and convert to line
        query_lines = query_text.split('\n')
        return (0, len(query_lines) - 1)  # Placeholder
    
    def _find_line_range_for_text(self, query_text: str, char_pos: int) -> Tuple[int, int]:
        """Find line range for a character position"""
        if char_pos < 0 or char_pos >= len(query_text):
            return (0, 0)
        lines_before = query_text[:char_pos].split('\n')
        line_num = len(lines_before) - 1
        return (max(0, line_num), max(0, line_num))
    
    def _map_lines_to_steps(self, query_text: str, steps: List[Dict], line_count: int) -> Dict[int, int]:
        """Map SQL line numbers to step indices"""
        line_to_step = {}
        query_lines = query_text.split('\n')
        
        # If no steps, return empty mapping
        if not steps:
            return line_to_step
        
        # Build a map of which steps start on which lines
        steps_by_start_line = {}
        for step_idx, step in enumerate(steps):
            line_range = step.get('line_range', (0, line_count - 1))
            start_line = line_range[0] if isinstance(line_range, tuple) and len(line_range) >= 2 else 0
            start_line = max(0, min(start_line, line_count - 1))
            
            if start_line not in steps_by_start_line:
                steps_by_start_line[start_line] = []
            steps_by_start_line[start_line].append(step_idx)
        
        # For each line, find the step that should be active
        # Strategy: show the result after executing all steps up to the step that starts on this line
        # If multiple steps start on the same line, use the one with the lowest step index (first in execution order)
        for line_idx in range(line_count):
            # First, try to find a step that starts exactly on this line
            if line_idx in steps_by_start_line:
                # If multiple steps start on this line, use the one with lowest index (first in execution order)
                step_idx = min(steps_by_start_line[line_idx])
                line_to_step[line_idx] = step_idx
            else:
                # Find the step that starts on the closest previous line
                best_step_idx = -1
                best_start_line = -1
                for start_line, step_indices in steps_by_start_line.items():
                    if start_line < line_idx and start_line > best_start_line:
                        best_start_line = start_line
                        # Use the step with highest index from previous lines (most complete)
                        best_step_idx = max(step_indices)
                
                if best_step_idx >= 0:
                    line_to_step[line_idx] = best_step_idx
                elif steps:
                    # Default to first step if no step starts before this line
                    line_to_step[line_idx] = 0
        
        return line_to_step
    
    def _execute_step(self, step_index: int, steps: List[Dict], query_id: str) -> Dict[str, Any]:
        """Execute query up to a specific step and return visual state"""
        # Get tables from graph builder
        available_tables = {}
        for table_name in self.graph_builder.table_rows.keys():
            rows = self.graph_builder.get_table_rows(table_name)
            if rows:
                try:
                    df = pd.DataFrame(rows)
                    # Store with both original name and lowercase for case-insensitive matching
                    # Store with original name (case-sensitive)
                    available_tables[table_name] = df
                    # Also store lowercase version for case-insensitive matching
                    if table_name.lower() != table_name:
                        available_tables[table_name.lower()] = df
                except Exception as e:
                    print(f"Warning: Could not create DataFrame for table {table_name}: {e}")
                    continue
        
        if not available_tables:
            return {
                'input_tables': [],
                'output_table': None,
                'highlighted_cols': [],
                'dimmed_rows': [],
                'annotations': {},
                'explanation_text': 'No tables available. Please upload data first.',
                'step_type': 'error',
                'before_row_count': 0,
                'after_row_count': 0,
                'join_condition': None
            }
        
        # Build partial query up to this step
        current_step = steps[step_index]
        step_type = current_step['type']
        
        # Execute steps incrementally, building up the result
        # First, execute all steps up to (but not including) the current step
        # Then execute the current step
        result_df = None
        input_tables = []
        highlighted_cols = []
        dimmed_rows = []
        explanation = current_step.get('description', f'Executing {step_type}')
        join_condition = None
        
        # Execute all steps up to and including the current step
        try:
            # For SELECT_COL, we need to get the result from the last non-SELECT_COL step
            if step_type == 'SELECT_COL':
                # Find the last non-SELECT_COL step
                prev_step_index = step_index - 1
                while prev_step_index >= 0 and steps[prev_step_index].get('type') == 'SELECT_COL':
                    prev_step_index -= 1
                result_df = self._execute_steps_up_to(steps, prev_step_index, available_tables)
            else:
                # For other steps, execute up to the previous step
                result_df = self._execute_steps_up_to(steps, step_index - 1, available_tables)
            
            # Now handle the current step
            if step_type == 'FROM':
                table_name = current_step.get('table')
                if not table_name:
                    raise ValueError("FROM step missing table name")
                
                # Try case-insensitive matching
                table_name_lower = table_name.lower()
                matched_table = None
                # First try exact match
                if table_name in available_tables:
                    matched_table = table_name
                elif table_name_lower in available_tables:
                    matched_table = table_name_lower
                else:
                    # Try case-insensitive match
                    for available_table in available_tables.keys():
                        if available_table.lower() == table_name_lower:
                            matched_table = available_table
                            break
                
                if matched_table and matched_table in available_tables:
                    result_df = available_tables[matched_table].copy()
                    input_tables = [{
                        'name': matched_table,
                        'data': self._clean_for_json(result_df.head(50).to_dict('records')),
                        'columns': list(result_df.columns),
                        'row_count': len(result_df)
                    }]
                    explanation = f'Loaded table {matched_table} with {len(result_df)} rows'
                else:
                    available_table_names = ', '.join(list(available_tables.keys())[:5])
                    raise ValueError(f"Table '{table_name}' not found. Available tables: {available_table_names}")
            
            elif step_type == 'JOIN':
                # result_df already contains the result from previous steps
                if result_df is None:
                    raise ValueError("No previous result to join with")
                
                prev_result = result_df.copy()  # Save previous result for display
                
                join_table_name = current_step.get('table')
                if not join_table_name:
                    raise ValueError("JOIN step missing table name")
                
                # Case-insensitive matching for join table
                join_table_name_lower = join_table_name.lower()
                matched_join_table = None
                for available_table in available_tables.keys():
                    if available_table.lower() == join_table_name_lower:
                        matched_join_table = available_table
                        break
                
                if matched_join_table and matched_join_table in available_tables:
                    join_table = available_tables[matched_join_table]
                    
                    # Show input tables
                    from_step = next((s for s in steps if s.get('type') == 'FROM'), None)
                    from_table_name = from_step['table'] if from_step and from_step.get('table') else 'base_table'
                    input_tables = [
                        {
                            'name': from_table_name,
                            'data': self._clean_for_json(prev_result.head(50).to_dict('records')),
                            'columns': list(prev_result.columns),
                            'row_count': len(prev_result)
                        },
                        {
                            'name': matched_join_table,
                            'data': self._clean_for_json(join_table.head(50).to_dict('records')),
                            'columns': list(join_table.columns),
                            'row_count': len(join_table)
                        }
                    ]
                    
                    # Perform join
                    condition = current_step.get('condition', '')
                    join_type = current_step.get('join_type', 'INNER JOIN')
                    
                    try:
                        join_keys = self._parse_join_condition(condition, prev_result.columns, join_table.columns)
                        if join_keys:
                            left_key, right_key = join_keys
                            how = 'left' if 'LEFT' in join_type.upper() else 'inner'
                            result_df = prev_result.merge(join_table, left_on=left_key, right_on=right_key, how=how)
                        else:
                            # Fallback: try common column names
                            common_cols = set(prev_result.columns) & set(join_table.columns)
                            if common_cols:
                                join_key = list(common_cols)[0]
                                how = 'left' if 'LEFT' in join_type.upper() else 'inner'
                                result_df = prev_result.merge(join_table, on=join_key, how=how)
                            else:
                                how = 'left' if 'LEFT' in join_type.upper() else 'inner'
                                result_df = prev_result.merge(join_table, how=how, suffixes=('_left', '_right'))
                    except Exception as e:
                        # Fallback to simple merge
                        common_cols = set(prev_result.columns) & set(join_table.columns)
                        if common_cols:
                            join_key = list(common_cols)[0]
                            how = 'left' if 'LEFT' in join_type.upper() else 'inner'
                            result_df = prev_result.merge(join_table, on=join_key, how=how)
                        else:
                            raise ValueError(f"Could not perform join: {str(e)}")
                    
                    join_condition = {'condition': condition, 'columns': self._extract_column_names(condition)}
                    highlighted_cols = join_condition['columns']
                    explanation = f'Joined {from_table_name} with {matched_join_table} ({len(result_df)} rows)'
                else:
                    available_table_names = ', '.join(list(available_tables.keys())[:5])
                    raise ValueError(f"Join table '{join_table_name}' not found. Available tables: {available_table_names}")
            
            elif step_type == 'WHERE':
                # result_df already contains the result from previous steps (FROM + JOIN)
                if result_df is not None:
                    before_count = len(result_df)
                    
                    # Show input table
                    input_tables = [{
                        'name': 'Before filter',
                        'data': self._clean_for_json(result_df.head(50).to_dict('records')),
                        'columns': list(result_df.columns),
                        'row_count': len(result_df)
                    }]
                    
                    # Try to actually filter - parse the WHERE condition
                    condition = current_step.get('condition', '')
                    highlighted_cols = current_step.get('columns', [])
                    
                    # Check for IN (SELECT ...) subqueries or multiple AND conditions with IN
                    condition_lower = condition.lower()
                    has_in_subquery = ' in (' in condition_lower and 'select' in condition_lower
                    
                    # Simple filtering - try to evaluate condition
                    try:
                        if has_in_subquery:
                            # Handle IN subquery - may have multiple AND conditions
                            # Split by AND to handle multiple conditions
                            and_parts = condition.split(' and ')
                            filtered_df = result_df.copy()
                            
                            for and_part in and_parts:
                                and_part = and_part.strip()
                                if ' in (' in and_part.lower() and 'select' in and_part.lower():
                                    filtered_df = self._apply_where_filter_with_subquery(filtered_df, and_part, available_tables)
                                else:
                                    # Regular condition
                                    filtered_df = self._apply_where_filter(filtered_df, and_part)
                            
                            result_df = filtered_df
                            after_count = len(result_df)
                            explanation = f'Filtered {before_count} rows to {after_count} rows using IN subquery'
                        else:
                            # For conditions with AND - split and apply each part
                            # _apply_where_filter already handles AND splitting, but we need to ensure it works
                            # Remove "where" keyword and semicolon if present in condition
                            filter_condition = condition.strip()
                            if filter_condition.lower().startswith('where'):
                                filter_condition = filter_condition[5:].strip()
                            if filter_condition.endswith(';'):
                                filter_condition = filter_condition[:-1].strip()
                            filtered_df = self._apply_where_filter(result_df, filter_condition)
                            result_df = filtered_df
                            after_count = len(result_df)
                            dimmed_count = before_count - after_count
                            explanation = f'Filtered {before_count} rows to {after_count} rows where {filter_condition}'
                    except Exception as e:
                        # If filtering fails, show error in explanation
                        import traceback
                        error_details = traceback.format_exc()
                        print(f"WHERE filter error: {error_details}")
                        explanation = f'Filtering rows where {condition}. Error: {str(e)}'
                        dimmed_rows = []
                        # Keep original result_df if filtering fails
                        result_df = result_df if result_df is not None else None
                else:
                    explanation = 'No data to filter'
            
            elif step_type == 'SELECT_COL':
                # Get result from all previous non-SELECT_COL steps
                if result_df is None:
                    # Find the last non-SELECT_COL step
                    prev_step_index = step_index - 1
                    while prev_step_index >= 0 and steps[prev_step_index].get('type') == 'SELECT_COL':
                        prev_step_index -= 1
                    if prev_step_index >= 0:
                        result_df = self._execute_steps_up_to(steps, prev_step_index, available_tables)
                
                if result_df is not None and len(result_df) > 0:
                    # Show input table (before column selection)
                    input_tables = [{
                        'name': 'Before projection',
                        'data': self._clean_for_json(result_df.head(50).to_dict('records')),
                        'columns': list(result_df.columns),
                        'row_count': len(result_df)
                    }]
                    
                    # Apply column selection incrementally
                    selected_so_far = current_step.get('selected_so_far', [])
                    if selected_so_far:
                        final_cols = []
                        for col in selected_so_far:
                            col_name = col.split('.')[-1] if '.' in col else col
                            
                            if col_name in result_df.columns:
                                final_cols.append(col_name)
                            else:
                                found = False
                                for df_col in result_df.columns:
                                    if df_col.lower() == col_name.lower():
                                        final_cols.append(df_col)
                                        found = True
                                        break
                                
                                if not found:
                                    for df_col in result_df.columns:
                                        df_col_base = df_col.split('_')[0] if '_' in df_col else df_col
                                        if df_col_base.lower() == col_name.lower():
                                            final_cols.append(df_col)
                                            found = True
                                            break
                        
                        seen = set()
                        unique_cols = []
                        for col in final_cols:
                            if col in result_df.columns and col not in seen:
                                unique_cols.append(col)
                                seen.add(col)
                        
                        if unique_cols:
                            result_df = result_df[unique_cols]
                            highlighted_cols = unique_cols
                            column_name = current_step.get('column', '')
                            explanation = f'Selected column: {column_name} ({len(result_df)} rows, {len(unique_cols)} columns)'
                        else:
                            explanation = f'Warning: Could not find selected columns {selected_so_far}'
                    else:
                        explanation = 'No columns to select'
                elif result_df is not None and len(result_df) == 0:
                    # Empty result after filtering - still show structure
                    input_tables = [{
                        'name': 'Before projection',
                        'data': [],
                        'columns': list(result_df.columns) if hasattr(result_df, 'columns') else [],
                        'row_count': 0
                    }]
                    explanation = 'No rows match the filter conditions'
                else:
                    explanation = 'No data to select from'
            
            elif step_type == 'GROUP_BY':
                # Get result from previous steps (FROM, JOIN, WHERE)
                prev_result = self._execute_steps_up_to(steps, step_index - 1, available_tables)
                if prev_result is not None:
                    result_df = prev_result.copy()
                    group_cols = current_step.get('columns', [])
                    
                    # Perform GROUP BY
                    if group_cols:
                        # Find actual column names
                        actual_group_cols = []
                        for col in group_cols:
                            col_name = col.split('.')[-1] if '.' in col else col
                            if col_name in result_df.columns:
                                actual_group_cols.append(col_name)
                            else:
                                for df_col in result_df.columns:
                                    if df_col.lower() == col_name.lower():
                                        actual_group_cols.append(df_col)
                                        break
                        
                        if actual_group_cols:
                            # Group by columns - aggregations will be in SELECT
                            result_df = result_df.groupby(actual_group_cols).first().reset_index()
                    
                    highlighted_cols = current_step.get('columns', [])
                    explanation = f'Grouping by {", ".join(group_cols)}'
                else:
                    explanation = 'No data to group'
            
            elif step_type == 'HAVING':
                # Get result from previous steps (FROM, JOIN, WHERE, GROUP BY)
                prev_result = self._execute_steps_up_to(steps, step_index - 1, available_tables)
                if prev_result is not None:
                    result_df = prev_result.copy()
                    condition = current_step.get('condition', '')
                    
                    # Apply HAVING filter
                    try:
                        filtered_df = self._apply_having_filter(result_df, condition)
                        result_df = filtered_df
                        explanation = f'Filtering groups where {condition}'
                    except Exception as e:
                        explanation = f'Filtering groups where {condition}. Error: {str(e)}'
                    
                    highlighted_cols = current_step.get('columns', [])
                else:
                    explanation = 'No data to filter'
            
            elif step_type == 'SELECT':
                # Get result from all previous steps (FROM, JOIN, WHERE, etc.)
                prev_result = self._execute_steps_up_to(steps, step_index - 1, available_tables)
                if prev_result is not None:
                    result_df = prev_result.copy()
                    select_cols = current_step.get('columns', [])
                    final_cols = []
                    if select_cols:
                        # Handle table prefixes (e.g., "f.fsid" -> "fsid", "b.bcode" -> "bcode")
                        actual_cols = []
                        for col in select_cols:
                            # Remove table prefix if present (e.g., "f.fsid" -> "fsid")
                            col_name = col.split('.')[-1] if '.' in col else col
                            
                            # Try exact match first
                            if col_name in result_df.columns:
                                actual_cols.append(col_name)
                            else:
                                # Try case-insensitive match
                                found = False
                                for df_col in result_df.columns:
                                    if df_col.lower() == col_name.lower():
                                        actual_cols.append(df_col)
                                        found = True
                                        break
                                
                                # If still not found, try partial match (for cases where join added suffixes)
                                if not found:
                                    for df_col in result_df.columns:
                                        # Check if column name matches (ignoring suffixes like _x, _y)
                                        df_col_base = df_col.split('_')[0] if '_' in df_col else df_col
                                        if df_col_base.lower() == col_name.lower():
                                            actual_cols.append(df_col)
                                            found = True
                                            break
                        
                        # Only select columns that exist - remove duplicates while preserving order
                        seen = set()
                        for col in actual_cols:
                            if col in result_df.columns and col not in seen:
                                final_cols.append(col)
                                seen.add(col)
                        
                        if final_cols:
                            result_df = result_df[final_cols]
                        else:
                            # If no matches found, this is an error - keep all columns for debugging
                            explanation = f'Warning: Could not find selected columns {select_cols} in result. Showing all columns.'
                    
                    highlighted_cols = current_step.get('columns', [])
                    # Remove table prefixes from highlighted cols for display
                    highlighted_cols = [c.split('.')[-1] if '.' in c else c for c in highlighted_cols]
                    explanation = f'Selected {len(final_cols) if final_cols else len(select_cols)} columns from result ({len(result_df)} rows)'
                else:
                    explanation = 'No data to select from'
            
            elif step_type == 'UNION':
                # Execute both parts of the UNION
                query1 = current_step.get('query1', '')
                query2 = current_step.get('query2', '')
                
                try:
                    # Execute first query
                    parsed1 = sqlparse.parse(query1)
                    result1 = None
                    if parsed1 and parsed1[0]:
                        steps1 = self._extract_steps(parsed1[0], query1)
                        for s in steps1:
                            if s['type'] == 'FROM':
                                table_name = s.get('table')
                                table_name_lower = table_name.lower()
                                matched_table = None
                                for available_table in available_tables.keys():
                                    if available_table.lower() == table_name_lower:
                                        matched_table = available_table
                                        break
                                if matched_table and matched_table in available_tables:
                                    result1 = available_tables[matched_table].copy()
                            elif s['type'] == 'WHERE' and result1 is not None:
                                condition = s.get('condition', '')
                                if ' in (' in condition.lower() and 'select' in condition.lower():
                                    result1 = self._apply_where_filter_with_subquery(result1, condition, available_tables)
                                else:
                                    result1 = self._apply_where_filter(result1, condition)
                            elif s['type'] == 'SELECT_COL' and result1 is not None:
                                # Apply column selection
                                selected_so_far = s.get('selected_so_far', [])
                                if selected_so_far:
                                    final_cols = []
                                    for col in selected_so_far:
                                        col_name = col.split('.')[-1] if '.' in col else col
                                        if col_name in result1.columns:
                                            final_cols.append(col_name)
                                    if final_cols:
                                        result1 = result1[final_cols]
                    
                    # Execute second query
                    parsed2 = sqlparse.parse(query2)
                    result2 = None
                    if parsed2 and parsed2[0]:
                        steps2 = self._extract_steps(parsed2[0], query2)
                        for s in steps2:
                            if s['type'] == 'FROM':
                                table_name = s.get('table')
                                table_name_lower = table_name.lower()
                                matched_table = None
                                for available_table in available_tables.keys():
                                    if available_table.lower() == table_name_lower:
                                        matched_table = available_table
                                        break
                                if matched_table and matched_table in available_tables:
                                    result2 = available_tables[matched_table].copy()
                            elif s['type'] == 'WHERE' and result2 is not None:
                                condition = s.get('condition', '')
                                result2 = self._apply_where_filter(result2, condition)
                            elif s['type'] == 'SELECT_COL' and result2 is not None:
                                # Apply column selection - need to map to first query's columns
                                selected_so_far = s.get('selected_so_far', [])
                                if selected_so_far:
                                    final_cols = []
                                    for col in selected_so_far:
                                        col_name = col.split('.')[-1] if '.' in col else col
                                        if col_name in result2.columns:
                                            final_cols.append(col_name)
                                    if final_cols:
                                        result2 = result2[final_cols]
                    
                    # Union the results
                    if result1 is not None and result2 is not None:
                        # Align column names for UNION
                        # For UNION, both queries should have same number of columns
                        # Map second query columns to first query column names
                        if len(result1.columns) == len(result2.columns):
                            result2.columns = result1.columns
                            result_df = pd.concat([result1, result2], ignore_index=True).drop_duplicates()
                            explanation = f'Unioned {len(result1)} rows with {len(result2)} rows, result: {len(result_df)} rows'
                            
                            # Show both input tables
                            input_tables = [
                                {
                                    'name': 'Query 1 result',
                                    'data': self._clean_for_json(result1.head(50).to_dict('records')),
                                    'columns': list(result1.columns),
                                    'row_count': len(result1)
                                },
                                {
                                    'name': 'Query 2 result',
                                    'data': self._clean_for_json(result2.head(50).to_dict('records')),
                                    'columns': list(result2.columns),
                                    'row_count': len(result2)
                                }
                            ]
                        else:
                            explanation = f'UNION error: Column count mismatch ({len(result1.columns)} vs {len(result2.columns)})'
                    elif result1 is not None:
                        result_df = result1
                        explanation = 'Union: Using first query result only'
                    elif result2 is not None:
                        result_df = result2
                        explanation = 'Union: Using second query result only'
                    else:
                        explanation = 'Union: No results from either query'
                except Exception as e:
                    explanation = f'Error executing UNION: {str(e)}'
                    import traceback
                    traceback.print_exc()
            
            else:
                # Unknown step type - use description from step
                explanation = current_step.get('description', f'Processing {step_type} step')
                if not result_df:
                    # Try to get result from previous steps
                    prev_result = self._execute_steps_up_to(steps, step_index - 1, available_tables)
                    if prev_result is not None:
                        result_df = prev_result.copy()
            
            # Prepare output table - always show the result
            output_table = None
            if result_df is not None and len(result_df) > 0:
                output_table = {
                    'data': self._clean_for_json(result_df.head(50).to_dict('records')),
                    'columns': list(result_df.columns),
                    'row_count': len(result_df)
                }
            elif result_df is not None and len(result_df) == 0:
                # Empty result - still show the structure
                output_table = {
                    'data': [],
                    'columns': list(result_df.columns) if hasattr(result_df, 'columns') else [],
                    'row_count': 0
                }
            
            # Calculate before/after counts
            before_count = 0
            if input_tables and len(input_tables) > 0:
                before_count = input_tables[0].get('row_count', 0)
            after_count = output_table['row_count'] if output_table else 0
            
            # Ensure explanation is always set - use step description as fallback
            if not explanation or explanation == f'Executing {step_type}':
                # Fallback to step description which should always be set
                explanation = current_step.get('description', f'Processing {step_type} step')
            
            # Final fallback - should never reach here if steps are compiled correctly
            if not explanation:
                explanation = f'Executing {step_type} step'
            
            return {
                'input_tables': input_tables,
                'output_table': output_table,
                'highlighted_cols': highlighted_cols,
                'dimmed_rows': dimmed_rows,
                'annotations': {},
                'explanation_text': explanation,
                'step_type': step_type,
                'before_row_count': before_count,
                'after_row_count': after_count,
                'join_condition': join_condition
            }
        
        except KeyError as e:
            error_msg = f"Missing required field in step: {str(e)}"
            # Try to preserve explanation if it was set
            error_explanation = f'Error executing step: {error_msg}'
            if 'explanation' in locals() and explanation:
                error_explanation = f'{explanation}. Error: {error_msg}'
            return {
                'input_tables': [],
                'output_table': None,
                'highlighted_cols': [],
                'dimmed_rows': [],
                'annotations': {'error': error_msg},
                'explanation_text': error_explanation,
                'step_type': step_type if 'step_type' in locals() else 'error',
                'before_row_count': 0,
                'after_row_count': 0,
                'join_condition': None
            }
        except Exception as e:
            import traceback
            error_details = str(e)
            # Try to preserve explanation if it was set
            error_explanation = f'Error executing step: {error_details}'
            if 'explanation' in locals() and explanation:
                error_explanation = f'{explanation}. Error: {error_details}'
            elif 'current_step' in locals():
                step_desc = current_step.get('description', f'Error in {step_type} step') if 'step_type' in locals() else 'Error executing step'
                error_explanation = f'{step_desc}. Error: {error_details}'
            return {
                'input_tables': [],
                'output_table': None,
                'highlighted_cols': [],
                'dimmed_rows': [],
                'annotations': {'error': error_details},
                'explanation_text': error_explanation,
                'step_type': step_type if 'step_type' in locals() else 'error',
                'before_row_count': 0,
                'after_row_count': 0,
                'join_condition': None
            }
        finally:
            if 'conn' in locals() and conn:
                try:
                    conn.close()
                except:
                    pass
    
    def _execute_steps_up_to(self, steps: List[Dict], max_step_index: int, available_tables: Dict) -> Optional[pd.DataFrame]:
        """Execute steps up to a given index and return the result DataFrame"""
        result_df = None
        
        for i in range(max_step_index + 1):
            if i >= len(steps):
                break
            step = steps[i]
            step_type = step['type']
            
            # Skip SELECT_COL steps in _execute_steps_up_to - they're handled separately
            if step_type == 'SELECT_COL':
                continue
            
            if step_type == 'FROM':
                table_name = step.get('table')
                if table_name:
                    # Case-insensitive matching
                    table_name_lower = table_name.lower()
                    matched_table = None
                    for available_table in available_tables.keys():
                        if available_table.lower() == table_name_lower:
                            matched_table = available_table
                            break
                    if matched_table and matched_table in available_tables:
                        result_df = available_tables[matched_table].copy()
            
            elif step_type == 'JOIN' and result_df is not None:
                join_table_name = step.get('table')
                if join_table_name:
                    # Case-insensitive matching
                    join_table_name_lower = join_table_name.lower()
                    matched_join_table = None
                    for available_table in available_tables.keys():
                        if available_table.lower() == join_table_name_lower:
                            matched_join_table = available_table
                            break
                    if matched_join_table and matched_join_table in available_tables:
                        join_table = available_tables[matched_join_table]
                        condition = step.get('condition', '')
                        join_type = step.get('join_type', 'INNER JOIN')
                        
                        # Parse join condition to find join keys
                        try:
                            join_keys = self._parse_join_condition(condition, result_df.columns, join_table.columns)
                            if join_keys:
                                left_key, right_key = join_keys
                                how = 'left' if 'LEFT' in join_type.upper() else 'inner'
                                result_df = result_df.merge(join_table, left_on=left_key, right_on=right_key, how=how)
                            else:
                                # Fallback: try common column names
                                common_cols = set(result_df.columns) & set(join_table.columns)
                                if common_cols:
                                    join_key = list(common_cols)[0]
                                    how = 'left' if 'LEFT' in join_type.upper() else 'inner'
                                    result_df = result_df.merge(join_table, on=join_key, how=how)
                                else:
                                    how = 'left' if 'LEFT' in join_type.upper() else 'inner'
                                    result_df = result_df.merge(join_table, how=how, suffixes=('_left', '_right'))
                        except Exception:
                            # Fallback to simple merge
                            common_cols = set(result_df.columns) & set(join_table.columns)
                            if common_cols:
                                join_key = list(common_cols)[0]
                                how = 'left' if 'LEFT' in join_type.upper() else 'inner'
                                result_df = result_df.merge(join_table, on=join_key, how=how)
            
            elif step_type == 'WHERE' and result_df is not None:
                condition = step.get('condition', '')
                try:
                    result_df = self._apply_where_filter(result_df, condition)
                except Exception as e:
                    print(f"Warning: WHERE filter failed in _execute_steps_up_to: {e}")
                    pass  # If filtering fails, keep original
            
            elif step_type == 'GROUP_BY' and result_df is not None:
                group_cols = step.get('columns', [])
                if group_cols:
                    actual_group_cols = []
                    for col in group_cols:
                        col_name = col.split('.')[-1] if '.' in col else col
                        if col_name in result_df.columns:
                            actual_group_cols.append(col_name)
                        else:
                            for df_col in result_df.columns:
                                if df_col.lower() == col_name.lower():
                                    actual_group_cols.append(df_col)
                                    break
                    
                    if actual_group_cols:
                        # Group by columns - aggregations will be in SELECT
                        result_df = result_df.groupby(actual_group_cols).first().reset_index()
            
            elif step_type == 'HAVING' and result_df is not None:
                condition = step.get('condition', '')
                try:
                    result_df = self._apply_having_filter(result_df, condition)
                except Exception as e:
                    print(f"Warning: HAVING filter failed in _execute_steps_up_to: {e}")
                    pass  # If filtering fails, keep original
            
            elif step_type == 'SELECT_COL' and result_df is not None:
                # For SELECT_COL steps, incrementally add columns
                selected_so_far = step.get('selected_so_far', [])
                if selected_so_far:
                    final_cols = []
                    for col in selected_so_far:
                        col_name = col.split('.')[-1] if '.' in col else col
                        
                        if col_name in result_df.columns:
                            final_cols.append(col_name)
                        else:
                            found = False
                            for df_col in result_df.columns:
                                if df_col.lower() == col_name.lower():
                                    final_cols.append(df_col)
                                    found = True
                                    break
                            
                            if not found:
                                for df_col in result_df.columns:
                                    df_col_base = df_col.split('_')[0] if '_' in df_col else df_col
                                    if df_col_base.lower() == col_name.lower():
                                        final_cols.append(df_col)
                                        found = True
                                        break
                    
                    seen = set()
                    unique_cols = []
                    for col in final_cols:
                        if col in result_df.columns and col not in seen:
                            unique_cols.append(col)
                            seen.add(col)
                    
                    if unique_cols:
                        result_df = result_df[unique_cols]
        
        return result_df
    
    def _parse_join_condition(self, condition: str, left_cols: List[str], right_cols: List[str]) -> Optional[Tuple[str, str]]:
        """Parse join condition like 'f.fsid = b.fsid' and return (left_key, right_key)"""
        if not condition:
            return None
        
        # Simple pattern matching for "table.col = table.col"
        pattern = r'(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)'
        match = re.search(pattern, condition)
        if match:
            left_table, left_col = match.group(1), match.group(2)
            right_table, right_col = match.group(3), match.group(4)
            
            # Find matching columns (remove table prefix)
            if left_col in left_cols:
                left_key = left_col
            else:
                left_key = next((c for c in left_cols if c.endswith('.' + left_col) or c == left_col), None)
            
            if right_col in right_cols:
                right_key = right_col
            else:
                right_key = next((c for c in right_cols if c.endswith('.' + right_col) or c == right_col), None)
            
            if left_key and right_key:
                return (left_key, right_key)
        
        return None
    
    def _apply_where_filter(self, df: pd.DataFrame, condition: str) -> pd.DataFrame:
        """Apply a WHERE filter condition to a DataFrame"""
        if not condition:
            return df
        
        # Remove "where" keyword if present
        condition = condition.strip()
        if condition.upper().startswith('WHERE'):
            condition = condition[5:].strip()
        # Remove trailing semicolon if present
        if condition.endswith(';'):
            condition = condition[:-1].strip()
        
        print(f"DEBUG WHERE: Full condition after strip: '{condition}'")
        print(f"DEBUG WHERE: Input dataframe has {len(df)} rows")
        
        # Split by ' and ' first to handle AND conditions
        # Then handle OR within each AND group
        # Use case-insensitive split to handle "AND" or "and"
        and_parts = re.split(r'\s+and\s+', condition, flags=re.IGNORECASE)
        print(f"DEBUG WHERE: Split into {len(and_parts)} AND parts: {and_parts}")
        filtered_df = df.copy()
        
        for and_part in and_parts:
            and_part = and_part.strip()
            if not and_part:
                continue
            print(f"DEBUG WHERE: Processing AND part: '{and_part}'")
            # Check for OR conditions within this AND part
            if ' or ' in and_part.lower():
                # Handle OR - at least one condition must be true
                or_parts = re.split(r'\s+or\s+', and_part, flags=re.IGNORECASE)
                or_mask = pd.Series([False] * len(filtered_df))
                for or_cond in or_parts:
                    or_cond = or_cond.strip()
                    if or_cond:
                        or_mask = or_mask | self._evaluate_condition(filtered_df, or_cond)
                filtered_df = filtered_df[or_mask]
                print(f"DEBUG WHERE: After OR, {len(filtered_df)} rows remain")
            else:
                # Handle single AND condition - apply filter sequentially
                before_count = len(filtered_df)
                try:
                    filtered_df = self._evaluate_condition(filtered_df, and_part, return_df=True)
                    after_count = len(filtered_df)
                    print(f"DEBUG WHERE: After '{and_part}', {before_count} -> {after_count} rows")
                except Exception as e:
                    print(f"DEBUG WHERE: Error evaluating condition '{and_part}': {e}")
                    import traceback
                    traceback.print_exc()
                    # Return empty dataframe on error
                    filtered_df = filtered_df.iloc[0:0]
                    after_count = 0
                    print(f"DEBUG WHERE: After error, {before_count} -> {after_count} rows")
        
        return filtered_df
    
    def _evaluate_condition(self, df: pd.DataFrame, condition: str, return_df: bool = False) -> pd.DataFrame:
        """Evaluate a single condition and return filtered DataFrame or boolean Series"""
        cond = condition.strip()
        
        # Handle IS NULL / IS NOT NULL
        if ' is null' in cond.lower() or ' is not null' in cond.lower():
            pattern = r'(\w+\.)?(\w+)\s+is\s+(not\s+)?null'
            match = re.search(pattern, cond, re.IGNORECASE)
            if match:
                col_name = match.group(2)
                is_not = match.group(3) is not None
                
                # Find column
                col = None
                if col_name in df.columns:
                    col = df[col_name]
                else:
                    for df_col in df.columns:
                        if df_col.lower() == col_name.lower():
                            col = df[df_col]
                            break
                
                if col is not None:
                    mask = col.isna() if not is_not else col.notna()
                    return df[mask] if return_df else mask
        
        # Handle LIKE / NOT LIKE
        if ' like ' in cond.lower() or ' not like ' in cond.lower():
            # Pattern to match: column [NOT] LIKE "pattern" or 'pattern'
            # Examples: cid like "%bank%", company not like "%bank%"
            # Try with quotes first (most common case)
            pattern = r'(\w+\.)?(\w+)\s+(not\s+)?like\s+(["\'])(.*?)\4'
            match = re.search(pattern, cond, re.IGNORECASE)
            
            if match:
                col_name = match.group(2)
                is_not = match.group(3) is not None
                pattern_str = match.group(5)  # Pattern without quotes
            else:
                # Try without quotes (unquoted pattern like: cid like %bank%)
                pattern = r'(\w+\.)?(\w+)\s+(not\s+)?like\s+([^\s;,\)]+)'
                match = re.search(pattern, cond, re.IGNORECASE)
                if match:
                    col_name = match.group(2)
                    is_not = match.group(3) is not None
                    pattern_str = match.group(4).strip('"\'')
                else:
                    # Pattern didn't match - log and return empty (shouldn't happen for valid SQL)
                    print(f"DEBUG LIKE: Pattern did not match condition: '{cond}'")
                    print(f"DEBUG LIKE: Tried patterns: quoted and unquoted")
                    # Return empty result if pattern doesn't match (safer than returning all rows)
                    return df.iloc[0:0] if return_df else pd.Series([False] * len(df))
            
            # Find column in dataframe
            col = None
            if col_name in df.columns:
                col = df[col_name]
            else:
                # Case-insensitive search
                for df_col in df.columns:
                    if df_col.lower() == col_name.lower():
                        col = df[df_col]
                        break
            
            if col is not None:
                # Convert SQL LIKE pattern to regex
                # % matches any sequence (0 or more chars), _ matches single character
                # Example: "%bank%" -> ".*bank.*"
                # Strategy: Use simple placeholders that won't appear in patterns
                placeholder_percent = 'XXXPERCENTXXX'
                placeholder_underscore = 'XXXUNDERSCOREXXX'
                
                # Replace % and _ with placeholders BEFORE escaping
                temp_pattern = pattern_str.replace('%', placeholder_percent).replace('_', placeholder_underscore)
                # Escape all regex special characters
                escaped_pattern = re.escape(temp_pattern)
                # Restore % and _ as regex patterns AFTER escaping
                regex_pattern = escaped_pattern.replace(placeholder_percent, '.*').replace(placeholder_underscore, '.')
                
                # Debug output
                print(f"DEBUG LIKE: col_name={col_name}, pattern_str={pattern_str}, regex_pattern={regex_pattern}, is_not={is_not}")
                print(f"DEBUG LIKE: Sample values: {col.head(10).tolist()}")
                
                # Apply the pattern
                mask = col.astype(str).str.contains(regex_pattern, case=False, na=False, regex=True)
                print(f"DEBUG LIKE: Mask matches: {mask.sum()} out of {len(mask)}")
                if is_not:
                    mask = ~mask
                    print(f"DEBUG LIKE: After NOT, matches: {mask.sum()} out of {len(mask)}")
                return df[mask] if return_df else mask
            else:
                # Column not found - log and return empty
                print(f"DEBUG LIKE: Column '{col_name}' not found in dataframe. Available columns: {list(df.columns)}")
                return df.iloc[0:0] if return_df else pd.Series([False] * len(df))
        
        # Handle arithmetic expressions (e.g., "start_hour + duration > 17")
        if '+' in cond or '-' in cond or '*' in cond or '/' in cond:
            # Try to parse arithmetic expression
            # Pattern: col1 + col2 > value or col1 - col2 < value
            pattern = r'(\w+\.)?(\w+)\s*([+\-*/])\s*(\w+\.)?(\w+)\s*(<|>|<=|>=|=|!=)\s*([\d\w\'"]+)'
            match = re.search(pattern, cond)
            if match:
                col1_name = match.group(2)
                operator = match.group(3)
                col2_name = match.group(5)
                comparison = match.group(6)
                value_str = match.group(7).strip('\'"')
                
                # Find columns
                col1 = None
                col2 = None
                for df_col in df.columns:
                    if df_col.lower() == col1_name.lower():
                        col1 = df[df_col]
                    if df_col.lower() == col2_name.lower():
                        col2 = df[df_col]
                
                if col1 is not None and col2 is not None:
                    # Perform arithmetic
                    if operator == '+':
                        result = col1 + col2
                    elif operator == '-':
                        result = col1 - col2
                    elif operator == '*':
                        result = col1 * col2
                    elif operator == '/':
                        result = col1 / col2
                    else:
                        return df if return_df else pd.Series([True] * len(df))
                    
                    # Convert value
                    try:
                        value = float(value_str) if '.' in value_str else int(value_str)
                    except:
                        value = value_str
                    
                    # Apply comparison
                    if comparison == '<':
                        mask = result < value
                    elif comparison == '>':
                        mask = result > value
                    elif comparison == '<=':
                        mask = result <= value
                    elif comparison == '>=':
                        mask = result >= value
                    elif comparison == '=':
                        mask = result == value
                    elif comparison == '!=':
                        mask = result != value
                    else:
                        return df if return_df else pd.Series([True] * len(df))
                    
                    return df[mask] if return_df else mask
        
        # Handle simple comparisons: col < value, col > value, etc.
        pattern = r'(\w+\.)?(\w+)\s*(<|>|<=|>=|=|!=)\s*([\d\w\'"]+)'
        match = re.search(pattern, cond)
        if match:
            col_name = match.group(2)
            operator = match.group(3)
            value_str = match.group(4).strip('\'"')
            
            # Find the column in dataframe
            col = None
            if col_name in df.columns:
                col = df[col_name]
            else:
                # Try case-insensitive match
                for df_col in df.columns:
                    if df_col.lower() == col_name.lower():
                        col = df[df_col]
                        break
            
            if col is None:
                # Column not found - return original dataframe
                return df if return_df else pd.Series([True] * len(df))
            
            # Try to convert value to appropriate type
            try:
                if value_str.replace('.', '').replace('-', '').isdigit():
                    value = float(value_str) if '.' in value_str else int(value_str)
                else:
                    value = value_str
            except:
                value = value_str
            
            # Apply filter
            if operator == '<':
                mask = col < value
            elif operator == '>':
                mask = col > value
            elif operator == '<=':
                mask = col <= value
            elif operator == '>=':
                mask = col >= value
            elif operator == '=':
                mask = col == value
            elif operator == '!=':
                mask = col != value
            else:
                return df if return_df else pd.Series([True] * len(df))
            
            return df[mask] if return_df else mask
        
        # If no pattern matched, return original dataframe
        return df if return_df else pd.Series([True] * len(df))
    
    def _extract_union_steps(self, query1: str, query2: str, full_query: str) -> List[Dict[str, Any]]:
        """Extract steps for a UNION query"""
        steps = []
        
        # Parse first query
        parsed1 = sqlparse.parse(query1)
        if parsed1 and parsed1[0]:
            steps1 = self._extract_steps(parsed1[0], query1)
            steps.extend(steps1)
        
        # Add UNION step
        union_idx = full_query.lower().find(' union ')
        steps.append({
            'type': 'UNION',
            'query1': query1,
            'query2': query2,
            'line_range': self._find_line_range_for_text(full_query, union_idx),
            'description': 'Union with second query'
        })
        
        # Parse second query
        parsed2 = sqlparse.parse(query2)
        if parsed2 and parsed2[0]:
            steps2 = self._extract_steps(parsed2[0], query2)
            # Adjust step indices for second query
            for step in steps2:
                step['union_part'] = 2  # Mark as second part of union
            steps.extend(steps2)
        
        return steps
    
    def _apply_where_filter_with_subquery(self, df: pd.DataFrame, condition: str, available_tables: Dict) -> pd.DataFrame:
        """Apply WHERE filter with IN (SELECT ...) subquery"""
        if not condition:
            return df
        
        # Remove "where" keyword if present
        condition = condition.strip()
        if condition.upper().startswith('WHERE'):
            condition = condition[5:].strip()
        
        # Handle IN (SELECT ...) subqueries
        # Pattern: col IN (SELECT column FROM table) or pnumber IN (SELECT pnumber FROM operations)
        pattern = r'(\w+\.)?(\w+)\s+in\s*\(([^)]+)\)'
        match = re.search(pattern, condition, re.IGNORECASE)
        if match:
            col_name = match.group(2)
            subquery = match.group(3).strip()
            
            # Find column in dataframe
            col = None
            if col_name in df.columns:
                col = df[col_name]
            else:
                for df_col in df.columns:
                    if df_col.lower() == col_name.lower():
                        col = df[df_col]
                        break
            
            if col is not None and subquery.upper().startswith('SELECT'):
                # Parse subquery: SELECT [DISTINCT] column FROM table
                subquery_lower = subquery.lower()
                select_idx = subquery_lower.find('select')
                from_idx = subquery_lower.find(' from ')
                
                if from_idx > 0:
                    # Extract column name
                    select_part = subquery[select_idx+6:from_idx].strip()
                    if 'distinct' in select_part.lower():
                        select_part = re.sub(r'distinct\s+', '', select_part, flags=re.IGNORECASE).strip()
                    subquery_col = select_part.strip()
                    
                    # Extract table name
                    from_part = subquery[from_idx+6:].strip()
                    table_parts = from_part.split()
                    subquery_table = table_parts[0].strip('"\'`') if table_parts else None
                    
                    if subquery_table:
                        # Get values from the subquery table
                        table_name_lower = subquery_table.lower()
                        matched_table = None
                        for available_table in available_tables.keys():
                            if available_table.lower() == table_name_lower:
                                matched_table = available_table
                                break
                        
                        if matched_table and matched_table in available_tables:
                            subquery_df = available_tables[matched_table]
                            
                            # Get distinct values from the subquery column
                            if subquery_col in subquery_df.columns:
                                subquery_values = subquery_df[subquery_col].dropna().unique()
                            else:
                                # Try case-insensitive match
                                for df_col in subquery_df.columns:
                                    if df_col.lower() == subquery_col.lower():
                                        subquery_values = subquery_df[df_col].dropna().unique()
                                        break
                                else:
                                    return df  # Column not found, return original
                            
                            # Filter dataframe where column value is IN subquery values
                            mask = col.isin(subquery_values)
                            return df[mask]
        
        # If no IN subquery found, try regular filtering
        return self._apply_where_filter(df, condition)
    
    def _apply_having_filter(self, df: pd.DataFrame, condition: str) -> pd.DataFrame:
        """Apply a HAVING filter condition to a grouped DataFrame"""
        if not condition:
            return df
        
        # Remove "having" keyword if present
        condition = condition.strip()
        if condition.upper().startswith('HAVING'):
            condition = condition[6:].strip()
        
        # HAVING can contain aggregate functions like MAX, MIN, SUM, etc.
        # Handle expressions like "max(frequency) - min(frequency) >= 16"
        try:
            # Try to parse aggregate expressions
            # Pattern: aggregate_func(col) operator aggregate_func(col) comparison value
            pattern = r'(max|min|sum|avg|count)\((\w+)\)\s*([+\-*/])\s*(max|min|sum|avg|count)?\(?(\w+)?\)?\s*(<|>|<=|>=|=|!=)\s*([\d\w\'"]+)'
            match = re.search(pattern, condition, re.IGNORECASE)
            if match:
                func1 = match.group(1).lower()
                col1 = match.group(2)
                operator = match.group(3)
                func2 = match.group(4)
                col2 = match.group(5)
                comparison = match.group(6)
                value_str = match.group(7).strip('\'"')
                
                # For grouped data, compute aggregates per group
                # Find grouping columns (non-aggregate columns)
                group_cols = [c for c in df.columns if c.lower() not in [col1.lower(), col2.lower() if col2 else '']]
                
                if group_cols and col1 in df.columns:
                    # Group by the grouping columns and compute aggregates
                    grouped = df.groupby(group_cols)
                    
                    if func1 == 'max':
                        agg1 = grouped[col1].transform('max')
                    elif func1 == 'min':
                        agg1 = grouped[col1].transform('min')
                    elif func1 == 'sum':
                        agg1 = grouped[col1].transform('sum')
                    elif func1 == 'avg':
                        agg1 = grouped[col1].transform('mean')
                    else:
                        agg1 = df[col1]
                    
                    if col2 and col2 in df.columns:
                        if func2 and func2.lower() == 'max':
                            agg2 = grouped[col2].transform('max')
                        elif func2 and func2.lower() == 'min':
                            agg2 = grouped[col2].transform('min')
                        elif func2 and func2.lower() == 'sum':
                            agg2 = grouped[col2].transform('sum')
                        elif func2 and func2.lower() == 'avg':
                            agg2 = grouped[col2].transform('mean')
                        else:
                            agg2 = df[col2]
                    else:
                        agg2 = None
                    
                    # Perform arithmetic if needed
                    if operator and agg2 is not None:
                        if operator == '-':
                            result = agg1 - agg2
                        elif operator == '+':
                            result = agg1 + agg2
                        elif operator == '*':
                            result = agg1 * agg2
                        elif operator == '/':
                            result = agg1 / agg2
                        else:
                            result = agg1
                    else:
                        result = agg1
                    
                    # Convert value
                    try:
                        value = float(value_str) if '.' in value_str else int(value_str)
                    except:
                        value = value_str
                    
                    # Apply comparison
                    if comparison == '>=':
                        mask = result >= value
                    elif comparison == '<=':
                        mask = result <= value
                    elif comparison == '>':
                        mask = result > value
                    elif comparison == '<':
                        mask = result < value
                    elif comparison == '=':
                        mask = result == value
                    else:
                        mask = pd.Series([True] * len(df))
                    
                    return df[mask]
            
            # Fallback: try simple WHERE-style filtering
            return self._apply_where_filter(df, condition)
        except Exception as e:
            # If HAVING filter fails, return original
            print(f"Warning: HAVING filter failed: {e}")
            return df
    
    def _apply_where_filter_with_subquery(self, df: pd.DataFrame, condition: str, available_tables: Dict) -> pd.DataFrame:
        """Apply WHERE filter with IN (SELECT ...) subquery"""
        if not condition:
            return df
        
        # Remove "where" keyword if present
        condition = condition.strip()
        if condition.upper().startswith('WHERE'):
            condition = condition[5:].strip()
        
        # Handle IN (SELECT ...) subqueries
        # Pattern: col IN (SELECT column FROM table) or pnumber IN (SELECT pnumber FROM operations)
        pattern = r'(\w+\.)?(\w+)\s+in\s*\(([^)]+)\)'
        match = re.search(pattern, condition, re.IGNORECASE)
        if match:
            col_name = match.group(2)
            subquery = match.group(3).strip()
            
            # Find column in dataframe
            col = None
            if col_name in df.columns:
                col = df[col_name]
            else:
                for df_col in df.columns:
                    if df_col.lower() == col_name.lower():
                        col = df[df_col]
                        break
            
            if col is not None and subquery.upper().startswith('SELECT'):
                # Parse subquery: SELECT [DISTINCT] column FROM table
                subquery_lower = subquery.lower()
                select_idx = subquery_lower.find('select')
                from_idx = subquery_lower.find(' from ')
                
                if from_idx > 0:
                    # Extract column name
                    select_part = subquery[select_idx+6:from_idx].strip()
                    if 'distinct' in select_part.lower():
                        select_part = re.sub(r'distinct\s+', '', select_part, flags=re.IGNORECASE).strip()
                    subquery_col = select_part.strip()
                    
                    # Extract table name
                    from_part = subquery[from_idx+6:].strip()
                    table_parts = from_part.split()
                    subquery_table = table_parts[0].strip('"\'`') if table_parts else None
                    
                    if subquery_table:
                        # Get values from the subquery table
                        table_name_lower = subquery_table.lower()
                        matched_table = None
                        for available_table in available_tables.keys():
                            if available_table.lower() == table_name_lower:
                                matched_table = available_table
                                break
                        
                        if matched_table and matched_table in available_tables:
                            subquery_df = available_tables[matched_table]
                            
                            # Get distinct values from the subquery column
                            if subquery_col in subquery_df.columns:
                                subquery_values = subquery_df[subquery_col].dropna().unique()
                            else:
                                # Try case-insensitive match
                                for df_col in subquery_df.columns:
                                    if df_col.lower() == subquery_col.lower():
                                        subquery_values = subquery_df[df_col].dropna().unique()
                                        break
                                else:
                                    return df  # Column not found, return original
                            
                            # Filter dataframe where column value is IN subquery values
                            mask = col.isin(subquery_values)
                            return df[mask]
        
        # If no IN subquery found, try regular filtering
        return self._apply_where_filter(df, condition)

