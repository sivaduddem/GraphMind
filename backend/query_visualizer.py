"""
Simplified SQL Query Executor

Original `QueryVisualizer` was a very large class responsible for:
- Parsing SQL into "steps"
- Producing per-line / per-step visual states
- Manually emulating parts of SQL with pandas

For this project we only want **one responsibility**:
Given a SQL query string, execute it against the data that has been
loaded into `GraphBuilder` and **return the result rows**.
"""

from typing import Dict, Any, Optional, List, Tuple
from contextlib import contextmanager
import re
import sys
import traceback
import math
import numpy as np
import pandas as pd

# Try to import duckdb, but make it optional
try:  # pragma: no cover - environment dependent
    import duckdb
    HAS_DUCKDB = True
except ImportError:  # pragma: no cover - duckdb not installed
    HAS_DUCKDB = False
    duckdb = None

# Try to import sqlparse for SQL parsing
try:
    import sqlparse
    from sqlparse.tokens import Keyword, DML, Name
    HAS_SQLPARSE = True
except ImportError:
    HAS_SQLPARSE = False
    sqlparse = None


# ============================================================================
# Constants
# ============================================================================

# Step types
STEP_FROM = 'FROM'
STEP_JOIN = 'JOIN'
STEP_WHERE = 'WHERE'
STEP_GROUP_BY = 'GROUP_BY'
STEP_HAVING = 'HAVING'
STEP_SELECT = 'SELECT'
STEP_ORDER_BY = 'ORDER_BY'
STEP_FINAL_RESULT = 'FINAL_RESULT'
STEP_UNION_INPUT = 'UNION_INPUT'
STEP_UNION = 'UNION'

# SQL keywords
KEYWORD_WHERE = 'WHERE'
KEYWORD_GROUP = 'GROUP'
KEYWORD_HAVING = 'HAVING'
KEYWORD_ORDER = 'ORDER'
KEYWORD_BY = 'BY'
KEYWORD_SELECT = 'SELECT'
KEYWORD_FROM = 'FROM'
KEYWORD_JOIN = 'JOIN'
KEYWORD_ON = 'ON'

# Table name constants
TABLE_LEFT = 'left_table'
TABLE_INPUT = 'input_table'
TABLE_JOINED_RESULT = 'joined_result'
TABLE_FILTERED_RESULT = 'filtered_result'
TABLE_GROUPED_RESULT = 'grouped_result'
TABLE_FILTERED_GROUPS = 'filtered_groups'
TABLE_PROJECTED_RESULT = 'projected_result'
TABLE_SORTED_RESULT = 'sorted_result'

# Column extraction skip keywords
SKIP_KEYWORDS_BASE = {'AND', 'OR', 'NOT', 'IN', 'LIKE', 'IS', 'NULL', 'TRUE', 'FALSE', 'BETWEEN', 'AS', 'ON'}
SKIP_KEYWORDS_AGGREGATION = {'SUM', 'COUNT', 'AVG', 'MAX', 'MIN'}

# Compiled regex patterns for better performance
_RE_COL_WITH_OPERATORS = re.compile(r'(?:^\w+\.)?(\w+)(?=\s*(?:=|!=|<>|<|>|<=|>=|LIKE|NOT|IN|IS))')
_RE_TABLE_COL = re.compile(r'\b\w+\.(\w+)\b')
_RE_STANDALONE_COL = re.compile(r'(?<![.\w])(\w+)(?=\s*(?:=|!=|<>|<|>|<=|>=))')
_RE_LIKE_PATTERN = re.compile(r'(\w+)\.(\w+)\s+LIKE', re.IGNORECASE)
_RE_IN_CLAUSE = re.compile(r'\b(\w+)\s+IN\s*\(', re.IGNORECASE)
_RE_BETWEEN_CLAUSE = re.compile(r'\b(\w+)\s+BETWEEN\s+', re.IGNORECASE)
_RE_AGG_FUNC_WITH_PREFIX = re.compile(r'(?:SUM|COUNT|AVG|MAX|MIN)\s*\(\s*(\w+)\.(\w+)\s*\)', re.IGNORECASE)
_RE_AGG_FUNC_WITHOUT_PREFIX = re.compile(r'(?:SUM|COUNT|AVG|MAX|MIN)\s*\(\s*(?:DISTINCT\s+)?(\w+)\s*\)', re.IGNORECASE)
_RE_AGG_WITH_ALIAS = re.compile(r'(?i)(SUM|COUNT|AVG|MAX|MIN)\s*\(\s*(?:DISTINCT\s+)?(?:\w+\.)?(\w+)\s*\)')
_RE_AGG_COL_WITH_PREFIX = re.compile(r'(?:SUM|COUNT|AVG|MAX|MIN)\s*\(\s*(?:DISTINCT\s+)?(?:\w+\.)?(\w+)\s*\)', re.IGNORECASE)
_RE_AGG_COL_TABLE_PREFIX = re.compile(r'(?:SUM|COUNT|AVG|MAX|MIN)\s*\(\s*\w+\.(\w+)\s*\)', re.IGNORECASE)
_RE_NORMALIZE_SPACES = re.compile(r'\s+')
_RE_NORMALIZE_FUNC_WITH_TABLE = re.compile(r'(\w+)\s*\(\s*(\w+)\s*\.\s*(\w+)\s*\)', re.IGNORECASE)
_RE_NORMALIZE_FUNC_WITHOUT_TABLE = re.compile(r'(\w+)\s*\(\s*(\w+)\s*\)', re.IGNORECASE)
_RE_NORMALIZE_TABLE_COL = re.compile(r'(\w+)\s*\.\s*(\w+)')
_RE_STRIP_TABLE_ALIAS = re.compile(r'\b\w+\.(\w+)\b')
_RE_ALIAS_COL_PATTERN = re.compile(r'\b(\w+)\.(\w+)\b')  # For alias.column pattern matching
# Note: _RE_AGG_REPLACE is created dynamically in _execute_having_query, not here
_RE_GROUP_BY_FALLBACK = re.compile(
    rf'{re.escape(KEYWORD_GROUP)}\s+{re.escape(KEYWORD_BY)}\s+(.+?)(?:\s+{re.escape(KEYWORD_HAVING)}\b|\s+{re.escape(KEYWORD_ORDER)}\s+{re.escape(KEYWORD_BY)}\b|;|$)',
    re.IGNORECASE | re.DOTALL
)
_RE_ORDER_BY_FALLBACK = re.compile(
    rf'{re.escape(KEYWORD_ORDER)}\s+{re.escape(KEYWORD_BY)}\s+(.+?)(?:;|$)',
    re.IGNORECASE | re.DOTALL
)
_RE_UNION_SPLIT = re.compile(r'\bUNION\b', re.IGNORECASE)


# ============================================================================
# Debug Logging Utility
# ============================================================================

class DebugLogger:
    """Centralized debug logging utility."""
    
    _enabled = True  # Set to False to disable all debug output
    
    @classmethod
    def log(cls, message: str, *args):
        """Log a debug message."""
        if cls._enabled:
            formatted = message.format(*args) if args else message
            print(f"DEBUG: {formatted}", file=sys.stderr)
    
    @classmethod
    def disable(cls):
        """Disable debug logging."""
        cls._enabled = False
    
    @classmethod
    def enable(cls):
        """Enable debug logging."""
        cls._enabled = True


# ============================================================================
# SQL Query Parser
# ============================================================================

class SQLQueryParser:
    """Parser for SQL queries to extract execution steps."""
    
    def __init__(self):
        self.steps = []
    
    def parse(self, query_text: str) -> List[Dict[str, Any]]:
        """Parse SQL query and extract steps in execution order."""
        if not HAS_SQLPARSE:
            raise RuntimeError("sqlparse library is required for step-by-step visualization")
        
        self.steps = []

        # Handle simple top-level UNION between two SELECT statements up-front.
        # For now we intentionally *skip* UNION ALL / INTERSECT / EXCEPT.
        upper_sql = query_text.upper()
        if 'UNION' in upper_sql and 'UNION ALL' not in upper_sql:
            union_steps = self._parse_union(query_text)
            if union_steps:
                return union_steps
        
        parsed = sqlparse.parse(query_text)[0]
        
        # Debug: check parsed structure
        DebugLogger.log("Parsed SQL structure: {}", type(parsed))
        DebugLogger.log("Parsed SQL tokens (first level): {} tokens", len(list(parsed.tokens)) if hasattr(parsed, 'tokens') else 0)
        
        # Extract clauses in execution order
        from_tables, from_table_aliases = self._extract_from_tables(parsed)
        joins = self._extract_joins(parsed)
        DebugLogger.log("Found {} JOIN clauses", len(joins))
        
        # Add FROM step only if no JOINs (JOIN step handles FROM)
        if from_tables and len(joins) == 0:
            self._add_step(STEP_FROM, {'tables': from_tables})
        
        # Add JOIN steps
        for join in joins:
            left_table_name = from_tables[0] if from_tables else None
            left_table_alias = from_table_aliases.get(left_table_name) if left_table_name else None
            step_info = {
                'join_type': join['type'],
                'left_table': left_table_name,
                'left_table_alias': left_table_alias,
                'from_tables': from_tables,
                'right_table': join['right_table'],
                'right_table_alias': join.get('right_table_alias'),
                'join_condition': join['condition'],
                'join_columns': join['columns'],
            }
            self._add_step(STEP_JOIN, step_info)
            DebugLogger.log("Added JOIN step: {}", step_info)
        
        # Extract and add other clauses
        self._extract_and_add_where(parsed)
        self._extract_and_add_group_by(parsed)
        self._extract_and_add_having(parsed)
        self._extract_and_add_select(parsed)
        self._extract_and_add_order_by(parsed)
        
        return self.steps
    
    def _add_step(self, step_type: str, step_data: Dict[str, Any]):
        """Add a step to the steps list."""
        step_data['step_type'] = step_type
        step_data['step_number'] = len(self.steps) + 1
        self.steps.append(step_data)

    def _parse_union(self, query_text: str) -> List[Dict[str, Any]]:
        """Detect and register steps for a basic UNION between two SELECTs.

        We parse each SELECT statement to extract its internal steps (FROM, JOIN, WHERE, etc.),
        then add a UNION step that combines their results.

        This is intentionally conservative and only supports a single
        top-level UNION (no UNION ALL, INTERSECT, or EXCEPT, and no nested
        set operations).
        """
        # Split on the first occurrence of UNION (case-insensitive).
        parts = _RE_UNION_SPLIT.split(query_text, maxsplit=1)
        if len(parts) < 2:
            return []
        
        left_sql = parts[0].strip().rstrip(';')
        right_sql = parts[1].strip().rstrip(';')
        
        # Basic sanity check: both sides should look like SELECT statements.
        if not left_sql.upper().lstrip().startswith('SELECT'):
            return []
        if not right_sql.upper().lstrip().startswith('SELECT'):
            return []
        
        # Parse the left SELECT statement to get its steps
        left_parser = SQLQueryParser()
        left_steps = left_parser.parse(left_sql)
        
        # Add left side steps with union_side indicator
        for step in left_steps:
            step['union_side'] = 'left'
            step['step_type'] = step.get('step_type', 'UNKNOWN')
            step['step_number'] = len(self.steps) + 1
            self.steps.append(step)
        
        # Parse the right SELECT statement to get its steps
        right_parser = SQLQueryParser()
        right_steps = right_parser.parse(right_sql)
        
        # Add right side steps with union_side indicator
        for step in right_steps:
            step['union_side'] = 'right'
            step['step_type'] = step.get('step_type', 'UNKNOWN')
            step['step_number'] = len(self.steps) + 1
            self.steps.append(step)
        
        # Register the UNION combination step.
        self._add_step(STEP_UNION, {
            'left_query': left_sql,
            'right_query': right_sql,
        })
        
        return self.steps
    
    def _extract_from_tables(self, parsed) -> Tuple[List[str], Dict[str, str]]:
        """Extract table names and aliases from FROM clause.
        
        Returns:
            tuple: (list of table names, dict mapping table_name -> alias)
        """
        tables = []
        table_aliases = {}  # Maps table_name -> alias
        from_seen = False
        tokens = list(parsed.flatten())
        stop_keywords = ['JOIN', 'WHERE', 'GROUP', 'ORDER', 'HAVING']
        
        i = 0
        while i < len(tokens):
            token = tokens[i]
            if token.ttype is Keyword and token.value.upper() == 'FROM':
                from_seen = True
                i += 1
                continue
            
            if from_seen:
                if token.ttype is Keyword and token.value.upper() in stop_keywords:
                    break
                if token.is_whitespace or token.value.strip() in [',', '.', '(', ')']:
                    i += 1
                    continue
                
                if (token.ttype is None or token.ttype is Name) and token.value.strip():
                    value = self._clean_identifier(token.value)
                    if value and value.upper() not in ['INNER', 'LEFT', 'RIGHT', 'FULL', 'OUTER', 'JOIN', 'ON', 'WHERE', 'GROUP', 'ORDER', 'HAVING', 'AS']:
                        # Check if this is a table name (longer than 1 char) or potential alias
                        if len(value) > 1:
                            # Check if next non-whitespace token is a single-letter alias
                            j = i + 1
                            while j < len(tokens) and tokens[j].is_whitespace:
                                j += 1
                            
                            if j < len(tokens):
                                next_token = tokens[j]
                                if ((next_token.ttype is None or next_token.ttype is Name) and
                                    len(next_token.value.strip()) == 1 and 
                                    next_token.value.strip().isalpha()):
                                    # This is a table name followed by an alias
                                    alias = next_token.value.strip()
                                    if value not in tables:
                                        tables.append(value)
                                    table_aliases[value] = alias
                                    i = j + 1  # Skip table, whitespace, and alias
                                    continue
                            
                            # No alias found, just table name
                            if value not in tables:
                                tables.append(value)
                        elif len(value) == 1 and value.isalpha():
                            # Single letter - might be an alias, but we already handled it above
                            pass
            i += 1
        
        return tables, table_aliases
    
    def _extract_joins(self, parsed) -> List[Dict[str, Any]]:
        """Extract JOIN clauses."""
        joins = []
        tokens = list(parsed.flatten())
        
        DebugLogger.log("_extract_joins: scanning {} tokens", len(tokens))
        
        # Debug: log tokens around FROM and expected JOIN locations - show more context
        from_idx = None
        for idx, tok in enumerate(tokens):
            if tok.value.upper() == 'FROM':
                from_idx = idx
                break
        
        if from_idx is not None:
            # Log tokens from FROM onwards (about 30 tokens to see JOIN structure)
            end_idx = min(from_idx + 30, len(tokens))
            DebugLogger.log("_extract_joins: tokens from FROM (index {}) to {}:", from_idx, end_idx)
            for idx in range(from_idx, end_idx):
                tok = tokens[idx]
                DebugLogger.log("  token {}: value={}, ttype={}, is_whitespace={}", 
                              idx, repr(tok.value), tok.ttype, tok.is_whitespace)
        else:
            # Fallback: log tokens with keywords
            for idx, tok in enumerate(tokens):
                tok_val_upper = tok.value.upper()
                if tok_val_upper in ['FROM', 'LEFT', 'RIGHT', 'JOIN', 'ON'] or (tok_val_upper and any(kw in tok_val_upper for kw in ['LEFT', 'JOIN'])):
                    DebugLogger.log("_extract_joins: token {}: value='{}', ttype={}, is_whitespace={}", 
                                  idx, repr(tok.value), tok.ttype, tok.is_whitespace)
        
        i = 0
        while i < len(tokens):
            token = tokens[i]
            token_value = token.value.upper()
            
            # Check if this token could be part of a JOIN (LEFT, RIGHT, INNER, FULL, or JOIN itself)
            # sqlparse may not always mark these as Keyword tokens, so check the value too
            # Also check for compound tokens like "LEFT JOIN" in a single token
            is_keyword = token.ttype is Keyword
            is_join_related = (token_value in ['LEFT', 'RIGHT', 'INNER', 'FULL', 'OUTER', 'JOIN'] or
                              any(jt in token_value for jt in ['LEFT', 'RIGHT', 'INNER', 'FULL']) and 'JOIN' in token_value)
            
            if is_join_related:
                DebugLogger.log("_extract_joins: found join-related token at {}: value='{}', ttype={}, is_keyword={}", 
                              i, token.value, token.ttype, is_keyword)
            
            if not is_keyword and not is_join_related:
                i += 1
                continue
            
            # Try to parse join type from this position
            join_type = self._parse_join_type(tokens, i)
            if join_type is None:
                if is_join_related:
                    DebugLogger.log("_extract_joins: join-related token '{}' at {} did not parse as join type", token.value, i)
                i += 1
                continue
            
            DebugLogger.log("_extract_joins: found join_type={} at token {}: {}", join_type, i, token.value)
            
            # Extract table name, alias, and ON condition
            table_name, table_alias, on_condition, join_columns, next_idx = self._extract_join_details(tokens, i)
            
            if table_name:
                joins.append({
                    'type': join_type,
                    'right_table': table_name,
                    'right_table_alias': table_alias,
                    'condition': on_condition or '',
                    'columns': list(set(join_columns)) if join_columns else []
                })
                DebugLogger.log("Found JOIN - type={}, table={}, condition={}", join_type, table_name, on_condition)
                i = next_idx
            else:
                DebugLogger.log("_extract_joins: join_type found but no table_name extracted at token {}", i)
                i += 1
        
        return joins
    
    def _parse_join_type(self, tokens: List, start_idx: int) -> Optional[str]:
        """Parse join type from tokens starting at start_idx."""
        token = tokens[start_idx]
        value = token.value.upper()
        
        # Check if value contains both join type and JOIN (e.g., "LEFT JOIN" as single token)
        if 'LEFT' in value and 'JOIN' in value:
            return 'LEFT'
        if 'RIGHT' in value and 'JOIN' in value:
            return 'RIGHT'
        if 'INNER' in value and 'JOIN' in value:
            return 'INNER'
        if 'FULL' in value and 'JOIN' in value:
            return 'FULL'
        
        if value in ['INNER', 'LEFT', 'RIGHT', 'FULL']:
            join_type = value
            # Skip whitespace tokens to find JOIN keyword
            next_idx = start_idx + 1
            while next_idx < len(tokens) and tokens[next_idx].is_whitespace:
                next_idx += 1
            
            if next_idx < len(tokens):
                next_token = tokens[next_idx]
                next_token_val = next_token.value.upper()
                DebugLogger.log("_parse_join_type: found '{}' at {}, next token at {}: value='{}', ttype={}", 
                              value, start_idx, next_idx, repr(next_token.value), next_token.ttype)
                if next_token_val == 'JOIN' or 'JOIN' in next_token_val:
                    DebugLogger.log("_parse_join_type: returning join_type='{}'", join_type)
                    return join_type
                elif next_token_val == 'OUTER':
                    # Skip whitespace after OUTER
                    outer_next_idx = next_idx + 1
                    while outer_next_idx < len(tokens) and tokens[outer_next_idx].is_whitespace:
                        outer_next_idx += 1
                    if outer_next_idx < len(tokens) and tokens[outer_next_idx].value.upper() == 'JOIN':
                        return f"{join_type} OUTER"
        elif value == 'JOIN' or 'JOIN' in value:
            # Check backwards for LEFT/RIGHT/FULL
            prev_idx = start_idx - 1
            while prev_idx >= 0 and tokens[prev_idx].is_whitespace:
                prev_idx -= 1
            if prev_idx >= 0:
                prev_val = tokens[prev_idx].value.upper()
                if prev_val in ['LEFT', 'RIGHT', 'FULL', 'INNER']:
                    return prev_val
            return 'INNER'
        elif value == 'OUTER':
            # Check backwards for LEFT/RIGHT/FULL, forwards for JOIN
            prev_idx = start_idx - 1
            while prev_idx >= 0 and tokens[prev_idx].is_whitespace:
                prev_idx -= 1
            if prev_idx >= 0 and tokens[prev_idx].value.upper() in ['LEFT', 'RIGHT', 'FULL']:
                # Check forwards for JOIN
                next_idx = start_idx + 1
                while next_idx < len(tokens) and tokens[next_idx].is_whitespace:
                    next_idx += 1
                if next_idx < len(tokens) and tokens[next_idx].value.upper() == 'JOIN':
                    return f"{tokens[prev_idx].value.upper()} OUTER"
        
        return None
    
    def _extract_join_details(self, tokens: List, start_idx: int) -> tuple:
        """Extract table name, alias, ON condition, and columns from JOIN tokens."""
        table_name = None
        table_alias = None
        on_condition = None
        join_columns = []
        
        # Find table name and alias
        j = start_idx + 1
        while j < len(tokens):
            token_j = tokens[j]
            if token_j.is_whitespace:
                j += 1
                continue
            # Stop at ON or other SQL keywords, or if we hit another JOIN
            if token_j.ttype is Keyword:
                token_val_upper = token_j.value.upper()
                if token_val_upper in ['ON', 'WHERE', 'GROUP', 'ORDER', 'HAVING']:
                    break
                # Also stop if we hit another JOIN keyword
                if ('JOIN' in token_val_upper or 
                    token_val_upper in ['LEFT', 'RIGHT', 'INNER', 'FULL']):
                    break
            if (token_j.ttype is None or token_j.ttype is Name) and not token_j.is_whitespace:
                value = self._clean_identifier(token_j.value)
                if not table_name:
                    table_name = value
                    # Check if next non-whitespace token is a single-letter alias
                    k = j + 1
                    while k < len(tokens) and tokens[k].is_whitespace:
                        k += 1
                    
                    if k < len(tokens):
                        next_token = tokens[k]
                        if ((next_token.ttype is None or next_token.ttype is Name) and
                            len(next_token.value.strip()) == 1 and 
                            next_token.value.strip().isalpha()):
                            table_alias = next_token.value.strip()
                            j = k  # Skip to alias token position
                elif len(value) == 1 and value.isalpha():
                    break
                else:
                    break
            j += 1
        
        # Find ON condition
        j = start_idx + 1
        on_seen = False
        condition_parts = []
        stop_keywords = ['GROUP', 'WHERE', 'ORDER', 'HAVING', 'INNER', 'LEFT', 'RIGHT', 'FULL']
        
        while j < len(tokens):
            token_j = tokens[j]
            token_value_upper = token_j.value.upper() if not token_j.is_whitespace else ''
            
            # Stop if we hit another JOIN (LEFT JOIN, RIGHT JOIN, INNER JOIN, etc.)
            # Check for compound JOIN tokens like "left join" or standalone JOIN keywords
            if (token_j.ttype is Keyword and 
                (token_value_upper == 'JOIN' or 
                 'JOIN' in token_value_upper or
                 token_value_upper in ['LEFT', 'RIGHT', 'INNER', 'FULL', 'OUTER'])):
                # Check if this is a JOIN keyword (not part of current join's ON condition)
                if on_seen:
                    # We've seen ON, so this is the start of the next JOIN
                    break
                # If we haven't seen ON yet, this might be a standalone JOIN keyword, skip it for now
                if token_value_upper == 'JOIN' and not on_seen:
                    break
            
            if token_j.ttype is Keyword and token_j.value.upper() == 'ON':
                on_seen = True
                j += 1
                continue
            
            if on_seen:
                # Stop if we hit another JOIN (LEFT JOIN, RIGHT JOIN, INNER JOIN, etc.)
                # Check for compound JOIN tokens like "left join" or standalone JOIN keywords
                if token_j.ttype is Keyword:
                    # Check if this token contains or is a JOIN keyword
                    is_join_keyword = (token_value_upper in ['JOIN', 'LEFT', 'RIGHT', 'INNER', 'FULL', 'OUTER'] or 
                                       'JOIN' in token_value_upper or
                                       (any(kw in token_value_upper for kw in ['LEFT', 'RIGHT', 'INNER', 'FULL']) and 'JOIN' in token_value_upper))
                    if is_join_keyword:
                        # This is the start of the next JOIN, stop here
                        DebugLogger.log("_extract_join_details: stopping ON condition extraction at token {} (next JOIN detected: '{}')", j, token_j.value)
                        break
                    
                    # Stop at other SQL keywords
                    if token_value_upper == 'GROUP' or token_value_upper.startswith('GROUP'):
                        break
                    if token_value_upper in stop_keywords:
                        break
                
                if 'GROUP' in token_value_upper or 'BY' in token_value_upper:
                    if token_j.ttype is Keyword:
                        break
                
                if token_j.is_whitespace:
                    condition_parts.append(' ')
                else:
                    condition_parts.append(token_j.value)
            j += 1
        
        if condition_parts:
            on_condition = re.sub(r'\s+', ' ', ''.join(condition_parts).strip())
            # Extract columns from the full condition string for comprehensive coverage
            if on_condition:
                extracted_cols = self._extract_columns_from_join_condition(on_condition)
                for col in extracted_cols:
                    if col not in join_columns:
                        join_columns.append(col)
        
        return table_name, table_alias, on_condition, join_columns, j
    
    def _extract_where(self, parsed) -> Optional[Dict[str, Any]]:
        """Extract WHERE clause."""
        where_seen = False
        condition_parts = []
        condition_str = ''
        
        for token in parsed.flatten():
            if token.ttype is Keyword and token.value.upper() == KEYWORD_WHERE:
                where_seen = True
                continue
            if where_seen:
                token_upper = token.value.upper()
                # Stop when we hit the start of GROUP BY / HAVING / ORDER BY clauses,
                # even if sqlparse doesn't mark them as separate Keyword tokens.
                if (
                    (token.ttype is Keyword and token_upper in [KEYWORD_GROUP, KEYWORD_ORDER, KEYWORD_HAVING])
                    or f'{KEYWORD_GROUP} {KEYWORD_BY}' in token_upper
                    or KEYWORD_HAVING in token_upper
                    or f'{KEYWORD_ORDER} {KEYWORD_BY}' in token_upper
                ):
                    break
                condition_parts.append(token.value)
        
        if condition_parts:
            condition_str = ' '.join(condition_parts)
            DebugLogger.log("WHERE condition extracted: {}", condition_str)
            # Extract columns from the full condition string for comprehensive coverage
            where_columns = self._extract_columns_from_condition(condition_str, is_having=False)
            DebugLogger.log("WHERE columns extracted: {}", where_columns)
            return {
                'condition': condition_str,
                'columns': where_columns
            }
        return None
    
    def _extract_group_by(self, parsed) -> Optional[List[str]]:
        """Extract GROUP BY columns."""
        group_by_seen = False
        by_seen = False
        columns: List[str] = []
        tokens_list = list(parsed.flatten())
        
        i = 0
        while i < len(tokens_list):
            token = tokens_list[i]
            token_value_upper = token.value.upper()
            
            if token.ttype is Keyword:
                if token_value_upper == 'GROUP' or token_value_upper.startswith('GROUP'):
                    group_by_seen = True
                    if i + 1 < len(tokens_list):
                        next_token = tokens_list[i + 1]
                        if next_token.value.upper() == 'BY':
                            by_seen = True
                            i += 2
                            continue
                    elif 'BY' in token_value_upper:
                        by_seen = True
                        i += 1
                        continue
                    else:
                        i += 1
                        continue
            
            if group_by_seen and by_seen:
                if token.ttype is Keyword and token_value_upper in [KEYWORD_HAVING, KEYWORD_ORDER]:
                    break
                # Accept both bare and named identifiers (sqlparse may tag them as Name)
                if token.ttype in (None, Name) and token.value.strip() and token_value_upper not in [KEYWORD_HAVING, KEYWORD_ORDER, KEYWORD_BY]:
                    col = self._clean_identifier(token.value.strip().strip(','))
                    if col and col.upper() != KEYWORD_BY:
                        if '.' in col:
                            col = col.split('.')[-1]
                        if col not in columns:
                            columns.append(col)
            
            i += 1
        
        if columns:
            return columns
        
        # Fallback: text-based extraction using the full SQL string
        sql_text = str(parsed)
        match = _RE_GROUP_BY_FALLBACK.search(sql_text)
        if not match:
            return None
        
        group_body = match.group(1)
        fallback_cols: List[str] = []
        for part in group_body.split(','):
            col = part.strip().strip('`\" ')
            if not col:
                continue
            # Remove alias and sort direction if present
            col = col.split()[0]
            # Remove table/alias prefix if present (e.g., "e.ssn" -> "ssn")
            if '.' in col:
                col = col.split('.')[-1]
            if col.upper() not in ['ASC', 'DESC'] and col not in fallback_cols:
                fallback_cols.append(col)
        
        DebugLogger.log("GROUP BY fallback extraction from text: {}", fallback_cols)
        return fallback_cols if fallback_cols else None
    
    def _extract_having(self, parsed) -> Optional[Dict[str, Any]]:
        """Extract HAVING clause."""
        DebugLogger.log("_extract_having called")
        having_seen = False
        condition_parts = []
        having_columns = []
        tokens_list = list(parsed.flatten())
        
        DebugLogger.log("Total tokens: {}", len(tokens_list))
        
        for i, token in enumerate(tokens_list):
            token_value_upper = token.value.upper()
            
            if token.ttype is Keyword and token_value_upper == KEYWORD_HAVING:
                DebugLogger.log("Found HAVING keyword at token {}: {}", i, token.value)
                having_seen = True
                continue
            
            if having_seen:
                # Stop at ORDER BY (may appear as separate tokens or a single combined token)
                if (token.ttype is Keyword and token_value_upper == KEYWORD_ORDER) or f'{KEYWORD_ORDER} {KEYWORD_BY}' in token_value_upper:
                    break
                if token.value == ';':
                    break
                
                if not token.is_whitespace:
                    condition_parts.append(token.value)
                
                if (token.ttype is None or token.ttype is Name) and not token.is_whitespace:
                    having_columns.extend(self._extract_columns_from_token(token.value))
        
        if condition_parts:
            condition_str = ' '.join(condition_parts).strip()
            DebugLogger.log("HAVING condition extracted: {}", condition_str)
            DebugLogger.log("HAVING columns extracted: {}", having_columns)
            aggregates = self._extract_aggregates_with_aliases(condition_str)
            DebugLogger.log("HAVING aggregates extracted: {}", aggregates)
            return {
                'condition': condition_str,
                'columns': having_columns,
                'aggregates': aggregates,
            }
        else:
            DebugLogger.log("HAVING extraction returned None - having_seen={}, condition_parts length={}", 
                          having_seen, len(condition_parts))
            if not having_seen:
                DebugLogger.log("HAVING keyword was never found in tokens")
        return None
    
    def _extract_select_columns(self, parsed) -> Optional[List[str]]:
        """Extract SELECT columns."""
        select_seen = False
        columns = []
        
        for token in parsed.flatten():
            if token.ttype is DML and token.value.upper() == KEYWORD_SELECT:
                select_seen = True
                continue
            if select_seen:
                if token.ttype is Keyword and token.value.upper() == KEYWORD_FROM:
                    break
                if token.ttype is None and token.value.strip() and token.value != '*':
                    col = self._clean_identifier(token.value.strip().strip(','))
                    if col and col.upper() != 'FROM':
                        if ' AS ' in col.upper():
                            col = col.split(' AS ')[0].strip()
                        elif ' ' in col and not col.startswith('('):
                            parts = col.split()
                            col = parts[0] if len(parts) > 1 else col
                        columns.append(col)
        
        return columns if columns else None
    
    def _extract_order_by(self, parsed) -> Optional[List[str]]:
        """Extract ORDER BY columns."""
        order_by_seen = False
        columns: List[str] = []
        
        for token in parsed.flatten():
            if token.ttype is Keyword and token.value.upper() == KEYWORD_ORDER:
                order_by_seen = True
                continue
            if order_by_seen:
                if token.value.upper() == KEYWORD_BY:
                    continue
                # Stop at semicolon or other major keywords
                if token.value == ';' or (token.ttype is Keyword and token.value.upper() in ['LIMIT', 'OFFSET']):
                    break
                # Accept both bare and named identifiers (sqlparse may tag them as Name)
                if token.ttype in (None, Name) and token.value.strip():
                    col = self._clean_identifier(token.value.strip().strip(','))
                    if col and col.upper() not in ['ASC', 'DESC']:
                        # Remove table/alias prefix if present (e.g., "e.ssn" -> "ssn")
                        if '.' in col:
                            col = col.split('.')[-1]
                        if col not in columns:
                            columns.append(col)
        
        if columns:
            return columns
        
        # Fallback: text-based extraction using the full SQL string
        sql_text = str(parsed)
        match = _RE_ORDER_BY_FALLBACK.search(sql_text)
        if not match:
            return None
        
        order_body = match.group(1)
        fallback_cols: List[str] = []
        for part in order_body.split(','):
            col = part.strip().strip('`\" ')
            if not col:
                continue
            # Remove direction / aliases
            col = col.split()[0]
            if '.' in col:
                col = col.split('.')[-1]
            if col.upper() not in ['ASC', 'DESC'] and col not in fallback_cols:
                fallback_cols.append(col)
        
        DebugLogger.log("ORDER BY fallback extraction from text: {}", fallback_cols)
        return fallback_cols if fallback_cols else None
    
    def _extract_and_add_where(self, parsed):
        """Extract WHERE clause and add as step."""
        where_clause = self._extract_where(parsed)
        if where_clause:
            self._add_step(STEP_WHERE, {
                'condition': where_clause['condition'],
                'columns': where_clause.get('columns', [])
            })
    
    def _extract_and_add_group_by(self, parsed):
        """Extract GROUP BY and add as step."""
        group_by = self._extract_group_by(parsed)
        DebugLogger.log("GROUP BY extraction result: {}", group_by)
        if group_by:
            self._add_step(STEP_GROUP_BY, {'columns': group_by})
            DebugLogger.log("Added GROUP BY step with columns: {}", group_by)
    
    def _extract_and_add_having(self, parsed):
        """Extract HAVING and add as step."""
        having = self._extract_having(parsed)
        DebugLogger.log("HAVING extraction result: {}", having)
        if having:
            condition = having['condition']
            columns = having.get('columns', [])
            aggregates = having.get('aggregates', [])
            self._add_step(
                STEP_HAVING,
                {
                    'condition': condition,
                    'columns': columns,
                    'aggregates': aggregates,
                },
            )
            DebugLogger.log("Added HAVING step with condition: {}", condition)

            # Attach aggregates to the most recent GROUP BY step so it can
            # compute the necessary aggregate columns for HAVING.
            if aggregates:
                for step in reversed(self.steps):
                    if step.get('step_type') == STEP_GROUP_BY:
                        step['aggregates'] = aggregates
                        DebugLogger.log(
                            "Attached HAVING aggregates to GROUP BY step: {}",
                            aggregates,
                        )
                        break
    
    def _extract_and_add_select(self, parsed):
        """Extract SELECT and add as step."""
        select_cols = self._extract_select_columns(parsed)
        if select_cols:
            self._add_step(STEP_SELECT, {'columns': select_cols})
    
    def _extract_and_add_order_by(self, parsed):
        """Extract ORDER BY and add as step."""
        order_by = self._extract_order_by(parsed)
        if order_by:
            self._add_step(STEP_ORDER_BY, {'columns': order_by})
    
    def _clean_identifier(self, value: str) -> str:
        """Clean an identifier by removing quotes and whitespace."""
        # More efficient: strip quotes in one pass using lstrip/rstrip with all quote types
        cleaned = value.strip()
        # Remove surrounding quotes if present
        if cleaned:
            quote_chars = '`"\''
            while cleaned and cleaned[0] in quote_chars and cleaned[-1] in quote_chars:
                if cleaned[0] == cleaned[-1]:
                    cleaned = cleaned[1:-1]
                else:
                    break
        return cleaned
    
    def _extract_columns_from_token(self, token_value: str) -> List[str]:
        """Extract column names from a token value (for HAVING conditions)."""
        columns = []
        skip_keywords = SKIP_KEYWORDS_BASE | SKIP_KEYWORDS_AGGREGATION | {'BETWEEN'}
        
        # Extract column names with operators
        col_matches = _RE_COL_WITH_OPERATORS.findall(token_value)
        for col_name in col_matches:
            if col_name.upper() not in skip_keywords and col_name not in columns:
                columns.append(col_name)
        
        # Handle aggregation functions with table prefix
        agg_matches = _RE_AGG_FUNC_WITH_PREFIX.findall(token_value)
        for _, col in agg_matches:
            if col not in columns:
                columns.append(col)
        
        # Handle aggregation without table prefix
        agg_matches2 = _RE_AGG_FUNC_WITHOUT_PREFIX.findall(token_value)
        for col in agg_matches2:
            if col.upper() not in SKIP_KEYWORDS_AGGREGATION and col not in columns:
                columns.append(col)
        
        return columns
    
    def _extract_columns_from_join_condition(self, condition: str) -> List[str]:
        """Extract column names from a JOIN ON condition."""
        columns = []
        skip_keywords = SKIP_KEYWORDS_BASE | {'ON'}
        
        # Extract from patterns like "e.ssn" or "w.essn" (table.column)
        table_col_matches = _RE_TABLE_COL.findall(condition)
        for col in table_col_matches:
            if col.upper() not in skip_keywords and col not in columns:
                columns.append(col)
        
        # Extract standalone column names in comparisons (e.g., "column = value")
        standalone_cols = _RE_STANDALONE_COL.findall(condition)
        for col in standalone_cols:
            if col.upper() not in skip_keywords and col not in columns:
                columns.append(col)
        
        return columns
    
    def _extract_columns_from_condition(self, condition: str, is_having: bool = False) -> List[str]:
        """Extract column names from a WHERE or HAVING condition."""
        if not condition:
            return []
        
        skip_keywords = SKIP_KEYWORDS_BASE | (SKIP_KEYWORDS_AGGREGATION if is_having else set())
        
        # Normalize condition: collapse multiple spaces and handle spaced-out tokens
        # e.g., "sum ( w . hours )" -> "sum(w.hours)"
        normalized_condition = self._normalize_condition_string(condition)
        DebugLogger.log("Normalized condition for extraction: {}", normalized_condition)
        
        # Extract columns with various patterns
        columns = []
        columns.extend(self._extract_columns_with_operators(normalized_condition, skip_keywords))
        columns.extend(self._extract_table_column_patterns(normalized_condition, skip_keywords))
        columns.extend(self._extract_like_patterns(normalized_condition))
        columns.extend(self._extract_in_clauses(normalized_condition, skip_keywords))
        columns.extend(self._extract_between_clauses(normalized_condition, skip_keywords))
        
        if is_having:
            columns.extend(self._extract_aggregation_columns(normalized_condition))
        
        # Remove duplicates while preserving order
        seen = set()
        unique_columns = []
        for col in columns:
            if col not in seen:
                seen.add(col)
                unique_columns.append(col)
        
        return unique_columns
    
    def _normalize_condition_string(self, condition: str) -> str:
        """Normalize condition string by removing extraneous spaces."""
        if not condition:
            return condition
        normalized = _RE_NORMALIZE_SPACES.sub(' ', condition.strip())
        # Handle spaced-out function calls: "sum ( w . hours )" -> "sum(w.hours)"
        normalized = _RE_NORMALIZE_FUNC_WITH_TABLE.sub(r'\1(\2.\3)', normalized)
        normalized = _RE_NORMALIZE_FUNC_WITHOUT_TABLE.sub(r'\1(\2)', normalized)
        # Handle spaced-out table.column: "e . ssn" -> "e.ssn"
        normalized = _RE_NORMALIZE_TABLE_COL.sub(r'\1.\2', normalized)
        return normalized
    
    def _extract_columns_with_operators(self, condition: str, skip_keywords: set) -> List[str]:
        """Extract columns with operators (e.g., "column = value")."""
        columns = []
        col_matches = _RE_COL_WITH_OPERATORS.findall(condition)
        for col in col_matches:
            if col.upper() not in skip_keywords and col not in columns:
                columns.append(col)
        return columns
    
    def _extract_table_column_patterns(self, condition: str, skip_keywords: set) -> List[str]:
        """Extract table.column patterns."""
        columns = []
        table_col_matches = _RE_TABLE_COL.findall(condition)
        for col in table_col_matches:
            if col.upper() not in skip_keywords and col not in columns:
                columns.append(col)
        return columns
    
    def _extract_like_patterns(self, condition: str) -> List[str]:
        """Extract columns from LIKE patterns."""
        columns = []
        like_matches = _RE_LIKE_PATTERN.findall(condition)
        for _, col in like_matches:
            if col not in columns:
                columns.append(col)
        return columns
    
    def _extract_in_clauses(self, condition: str, skip_keywords: set) -> List[str]:
        """Extract columns from IN clauses."""
        columns = []
        in_matches = _RE_IN_CLAUSE.findall(condition)
        for col in in_matches:
            if col.upper() not in skip_keywords and col not in columns:
                columns.append(col)
        return columns
    
    def _extract_between_clauses(self, condition: str, skip_keywords: set) -> List[str]:
        """Extract columns from BETWEEN clauses."""
        columns = []
        between_matches = _RE_BETWEEN_CLAUSE.findall(condition)
        for col in between_matches:
            if col.upper() not in skip_keywords and col not in columns:
                columns.append(col)
        return columns
    
    def _extract_aggregation_columns(self, condition: str) -> List[str]:
        """Extract columns from aggregation functions."""
        columns = []
        # Pattern: SUM(column) or SUM(table.column)
        agg_matches = _RE_AGG_COL_WITH_PREFIX.findall(condition)
        for col in agg_matches:
            if col.upper() not in SKIP_KEYWORDS_AGGREGATION and col not in columns:
                columns.append(col)
        # Also handle table.column in aggregations: SUM(table.column)
        agg_table_col_matches = _RE_AGG_COL_TABLE_PREFIX.findall(condition)
        for col in agg_table_col_matches:
            if col.upper() not in SKIP_KEYWORDS_AGGREGATION and col not in columns:
                columns.append(col)
        return columns

    def _extract_aggregates_with_aliases(self, condition: str) -> List[Dict[str, str]]:
        """Extract aggregate functions and assign stable aliases for GROUP BY/HAVING."""
        aggregates: List[Dict[str, str]] = []
        if not condition:
            return aggregates
        # Normalize first so spaced-out expressions like "sum ( w . hours )"
        # become "sum(w.hours)" and can be matched reliably.
        normalized = self._normalize_condition_string(condition)
        
        # Also normalize DISTINCT: "count ( distinct remote_access )" -> "count(distinct remote_access)"
        normalized = re.sub(r'\bdistinct\s+', 'distinct ', normalized, flags=re.IGNORECASE)

        matches = _RE_AGG_WITH_ALIAS.findall(normalized)
        used_aliases = set()
        for func, col in matches:
            base_col = col
            base_alias = f"{func.lower()}_{base_col}"
            alias = base_alias
            idx = 2
            while alias in used_aliases:
                alias = f"{base_alias}_{idx}"
                idx += 1
            used_aliases.add(alias)
            
            # Check if DISTINCT was present for this specific aggregate
            # Look for pattern: func(distinct col) or func(DISTINCT col)
            # Use a more efficient check: look for the pattern around the matched position
            # Find the position of this aggregate in the normalized string
            agg_pattern = rf'{re.escape(func)}\s*\(\s*(?:distinct\s+)?{re.escape(base_col)}\s*\)'
            match_obj = re.search(agg_pattern, normalized, re.IGNORECASE)
            if match_obj:
                # Check if 'distinct' appears between func and base_col
                func_end = match_obj.start() + len(func)
                col_start = match_obj.end() - len(base_col) - 1  # -1 for closing paren
                between_text = normalized[func_end:col_start].lower()
                has_distinct = 'distinct' in between_text
            else:
                has_distinct = False
            
            aggregates.append(
                {
                    'func': func.upper(),
                    'column': base_col,
                    'alias': alias,
                    'distinct': has_distinct,
                }
            )

        DebugLogger.log("Extracted aggregates from condition '{}': {}", condition, aggregates)
        return aggregates


# ============================================================================
# Query Visualizer
# ============================================================================

class QueryVisualizer:
    """Extremely simplified query executor.

    - Uses tables already loaded into `graph_builder` (from SQL / CSV uploads)
    - Registers those tables as DuckDB views
    - Executes the full SQL query and returns the output table
    """

    def __init__(self, graph_builder):
        """Store the `graph_builder` that holds in-memory tables."""
        self.graph_builder = graph_builder
    
    @contextmanager
    def _db_connection(self, tables: Optional[Dict[str, pd.DataFrame]] = None):
        """Context manager for DuckDB connections with automatic cleanup."""
        if not HAS_DUCKDB:
            raise RuntimeError("DuckDB is not installed.")
        
        con = duckdb.connect(database=":memory:")
        try:
            if tables:
                for name, df in tables.items():
                    con.register(name, df)
            yield con
        finally:
            con.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    
    def _clean_for_json(self, obj: Any):
        """Recursively clean NaN/inf values and convert numpy types to Python types for JSON serialization."""
        if isinstance(obj, dict):
            return {k: self._clean_for_json(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._clean_for_json(item) for item in obj]
        if isinstance(obj, pd.DataFrame):
            # More efficient: convert to dict records directly
            return self._clean_for_json(obj.to_dict("records"))
        if isinstance(obj, pd.Series):
            return [self._clean_for_json(item) for item in obj]
        
        # Handle numpy types more efficiently
        obj_type = type(obj)
        obj_module = getattr(obj_type, '__module__', None)
        
        # Handle numpy integer types
        if isinstance(obj, np.integer):
            return int(obj)
        
        # Handle numpy float types
        if isinstance(obj, np.floating):
            try:
                if pd.isna(obj) or math.isnan(obj) or math.isinf(obj):
                    return None
                return float(obj)
            except (TypeError, ValueError, AttributeError):
                return None
        
        # Handle regular float
        if isinstance(obj, float):
            try:
                if pd.isna(obj) or math.isnan(obj) or math.isinf(obj):
                    return None
                return obj
            except (TypeError, ValueError):
                return None
        
        # Handle numpy bool and string types
        if obj_module == 'numpy':
            type_name = obj_type.__name__
            if type_name in ('bool_', 'bool8'):
                return bool(obj)
            if type_name in ('str_', 'string_', 'unicode_'):
                return str(obj)
        
        return obj

    def _get_tables_as_dataframes(self) -> Dict[str, pd.DataFrame]:
        """Convert all tables known to `graph_builder` into DataFrames."""
        tables: Dict[str, pd.DataFrame] = {}
        table_rows = getattr(self.graph_builder, "table_rows", {}) or {}
        
        for table_name in table_rows.keys():
            rows = self.graph_builder.get_table_rows(table_name)
            if not rows:
                continue
            try:
                tables[table_name] = pd.DataFrame(rows)
            except Exception as exc:  # pragma: no cover - defensive
                print(f"Warning: could not create DataFrame for table {table_name}: {exc}")
        return tables

    def _dataframe_to_table_dict(self, df: pd.DataFrame, name: str) -> Dict[str, Any]:
        """Convert DataFrame to table dictionary for JSON serialization."""
        if df is None or df.empty:
            return {
                'name': name,
                'columns': [],
                'data': [],
                'row_count': 0
            }
        
        rows = self._clean_for_json(df)
        return {
            'name': name,
            'columns': list(df.columns),
            'data': rows,
            'row_count': len(df)
        }

    def _strip_table_aliases(self, expression: str) -> str:
        """Remove table/alias prefixes from column references (e.g., e.ssn -> ssn).

        This is used for intermediate steps (WHERE / HAVING) that operate on a
        single in-memory table where columns no longer carry SQL aliases.
        """
        if not expression:
            return expression
        # Normalize spaced-out table.column first, then strip the prefix.
        normalized = _RE_NORMALIZE_TABLE_COL.sub(r'\1.\2', expression)
        return _RE_STRIP_TABLE_ALIAS.sub(r'\1', normalized)
    
    def _create_error_step(self, step_type: str, step_number: int, error_msg: str) -> Dict[str, Any]:
        """Create an error step result."""
        return {
            'step_number': step_number,
            'step_type': step_type,
            'input_tables': [],
            'output_table': None,
            'highlighted_cols': [],
            'highlighted_rows': [],
            'dimmed_rows': [],
            'explanation': f"Step {step_number}: {step_type} ({error_msg})"
        }
    
    def _update_step_state(self, step_result: Dict, current_df: Optional[pd.DataFrame], 
                          current_table_name: Optional[str]) -> tuple:
        """Update current state from step result. Returns (current_df, current_table_name)."""
        if step_result.get('_dataframe') is not None:
            return step_result['_dataframe'], step_result.get('_table_name', 'intermediate_result')
        elif step_result.get('output_table'):
            output_data = step_result['output_table']
            if output_data and output_data.get('data'):
                try:
                    return pd.DataFrame(output_data['data']), output_data.get('name', 'intermediate_result')
                except Exception as e:
                    DebugLogger.log("Warning: Could not convert output to DataFrame: {}", e)
        elif step_result.get('step_type') == 'FROM' and step_result.get('input_tables'):
            first_table = step_result['input_tables'][0]
            if first_table and first_table.get('data'):
                try:
                    df = pd.DataFrame(first_table['data'])
                    step_result['_dataframe'] = df
                    step_result['_table_name'] = first_table.get('name', 'base_table')
                    return df, first_table.get('name', 'base_table')
                except Exception as e:
                    DebugLogger.log("Warning: Could not convert FROM table to DataFrame: {}", e)
        
        return current_df, current_table_name
    
    def _normalize_column_names(self, extracted_cols: List[str], actual_cols: List[str]) -> List[str]:
        """Normalize extracted column names to match actual DataFrame column names.
        
        Handles cases where:
        - Extracted columns might be just column names (e.g., "ssn")
        - Actual columns might have table prefixes (e.g., "left_table.ssn", "works_on.essn")
        - Matches by exact name or by suffix after dot
        """
        if not extracted_cols or not actual_cols:
            return []
        
        # Build lookup dictionaries for O(1) access instead of O(n) searches
        actual_cols_lower_map = {c.lower(): c for c in actual_cols}
        suffix_map = {}  # Maps suffix -> list of columns with that suffix
        normalized = []
        seen = set()  # Track normalized columns to avoid duplicates
        
        # Build suffix map
        for actual_col in actual_cols:
            if '.' in actual_col:
                suffix = actual_col.split('.')[-1].lower()
                if suffix not in suffix_map:
                    suffix_map[suffix] = []
                suffix_map[suffix].append(actual_col)
        
        for col in extracted_cols:
            col_lower = col.lower()
            
            # Try exact match first (O(1))
            if col_lower in actual_cols_lower_map:
                matched_col = actual_cols_lower_map[col_lower]
                if matched_col not in seen:
                    normalized.append(matched_col)
                    seen.add(matched_col)
            # Try matching by suffix (O(1) lookup)
            elif col_lower in suffix_map:
                for actual_col in suffix_map[col_lower]:
                    if actual_col not in seen:
                        normalized.append(actual_col)
                        seen.add(actual_col)
        
        DebugLogger.log("Normalized columns: {} -> {}", extracted_cols, normalized)
        return normalized

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    
    def execute_query(self, query_text: str) -> Dict[str, Any]:
        """Execute the SQL query and return only the resulting table.

        Handles multi-line queries and input that may contain multiple
        statements by executing only the **last non-empty statement**.

        Returns a JSON-ready dict:
        {
            "columns": [col1, col2, ...],
            "rows": [ {col1: v1, col2: v2, ...}, ... ],
            "row_count": int,
        }
        """
        if not isinstance(query_text, str) or not query_text.strip():
            raise ValueError("Query text must be a non-empty string")

        # Normalize and extract the last non-empty statement
        cleaned = query_text.strip()
        statements = [stmt.strip() for stmt in cleaned.split(";") if stmt.strip()]
        if not statements:
            raise ValueError("Query text must contain at least one SQL statement")
        sql_to_run = statements[-1]

        tables = self._get_tables_as_dataframes()
        if not tables:
            raise ValueError("No tables available. Please upload data first.")

        # Fresh in-memory connection per call
        try:
            with self._db_connection(tables) as con:
                result_df: pd.DataFrame = con.execute(sql_to_run).fetchdf()
        except RuntimeError as exc:
            raise exc
        except Exception as exc:
            raise ValueError(f"Error executing SQL query: {exc}") from exc

        cleaned_rows = self._clean_for_json(result_df)
        return {
            "columns": list(result_df.columns),
            "rows": cleaned_rows,
            "row_count": len(result_df),
        }

    def execute_query_steps(self, query_text: str) -> Dict[str, Any]:
        """Execute query step-by-step and return intermediate results for visualization.
        
        Returns:
            {
                "steps": [step1, step2, ...],
                "final_result": {columns, rows, row_count}
            }
        """
        if not isinstance(query_text, str) or not query_text.strip():
            raise ValueError("Query text must be a non-empty string")
        
        # Normalize query text
        normalized_query = query_text.strip()
        if normalized_query.endswith(';'):
            normalized_query = normalized_query[:-1].strip()
        
        try:
            # Parse query to get steps
            DebugLogger.log("About to parse query in execute_query_steps: {}...", normalized_query[:100])
            parser = SQLQueryParser()
            step_definitions = parser.parse(normalized_query)
            
            DebugLogger.log("Parsed {} steps in execute_query_steps", len(step_definitions))
            for step in step_definitions:
                DebugLogger.log("Step {}: {}", step.get('step_number'), step.get('step_type'))
                if step.get('step_type') == 'JOIN':
                    DebugLogger.log("  - Right table: {}, Condition: {}", 
                                  step.get('right_table'), step.get('join_condition'))
                elif step.get('step_type') == 'HAVING':
                    DebugLogger.log("  - HAVING condition: {}", step.get('condition'))
        except Exception as e:
            # If parsing fails, fall back to just returning final result
            print(f"Error parsing query steps: {e}")
            print(traceback.format_exc())
            final_result = self.execute_query(query_text)
            DebugLogger.log(
                "execute_query_steps fallback (parse error) final_result: columns={}, row_count={}",
                final_result.get("columns", []),
                final_result.get("row_count", "N/A"),
            )
            cleaned_final_result = self._clean_for_json(final_result)
            return {"steps": [], "final_result": cleaned_final_result}
        
        if not step_definitions:
            final_result = self.execute_query(query_text)
            DebugLogger.log(
                "execute_query_steps: no step_definitions, final_result: columns={}, row_count={}",
                final_result.get("columns", []),
                final_result.get("row_count", "N/A"),
            )
            cleaned_final_result = self._clean_for_json(final_result)
            return {"steps": [], "final_result": cleaned_final_result}
        
        # Get tables
        tables = self._get_tables_as_dataframes()
        if not tables:
            raise ValueError("No tables available. Please upload data first.")
        
        # Execute steps incrementally
        steps = []
        current_df = None
        current_table_name = None
        
        # Check if this is a UNION query
        has_union = any(step.get('union_side') for step in step_definitions) or any(
            step.get('step_type') == STEP_UNION for step in step_definitions
        )
        
        if has_union:
            # Handle UNION query: execute left side, then right side, then UNION
            left_steps = [s for s in step_definitions if s.get('union_side') == 'left']
            right_steps = [s for s in step_definitions if s.get('union_side') == 'right']
            union_step = next((s for s in step_definitions if s.get('step_type') == STEP_UNION), None)
            
            left_df = None
            left_table_name = None
            right_df = None
            right_table_name = None
            
            try:
                # Execute left side steps
                for step_def in left_steps:
                    step_type = step_def['step_type']
                    DebugLogger.log("Executing step {} (left): {}", step_def.get('step_number'), step_type)
                    
                    step_result = self._execute_step(
                        step_def, left_df, left_table_name, tables, query_text
                    )
                    steps.append(step_result)
                    
                    # Update state for next left step
                    left_df, left_table_name = self._update_step_state(
                        step_result, left_df, left_table_name
                    )
                
                # Execute right side steps (start fresh)
                for step_def in right_steps:
                    step_type = step_def['step_type']
                    DebugLogger.log("Executing step {} (right): {}", step_def.get('step_number'), step_type)
                    
                    step_result = self._execute_step(
                        step_def, right_df, right_table_name, tables, query_text
                    )
                    steps.append(step_result)
                    
                    # Update state for next right step
                    right_df, right_table_name = self._update_step_state(
                        step_result, right_df, right_table_name
                    )
                
                # Execute UNION step with both results
                if union_step:
                    # Pass both left and right results to UNION step
                    union_step['_left_df'] = left_df
                    union_step['_left_table_name'] = left_table_name
                    union_step['_right_df'] = right_df
                    union_step['_right_table_name'] = right_table_name
                    
                    step_result = self._execute_step(
                        union_step, None, None, tables, query_text
                    )
                    steps.append(step_result)
                    
                    # Update state for final result
                    current_df, current_table_name = self._update_step_state(
                        step_result, None, None
                    )
            except Exception as e:
                print(f"Error executing UNION steps: {e}")
                print(traceback.format_exc())
        else:
            # Normal query execution (non-UNION)
            try:
                for step_def in step_definitions:
                    step_type = step_def['step_type']
                    DebugLogger.log("Executing step {}: {}", step_def.get('step_number'), step_type)
                    
                    step_result = self._execute_step(
                        step_def, current_df, current_table_name, tables, query_text
                    )
                    steps.append(step_result)
                    
                    # Update state for next step
                    current_df, current_table_name = self._update_step_state(
                        step_result, current_df, current_table_name
                    )
            except Exception as e:
                print(f"Error executing steps: {e}")
                print(traceback.format_exc())
        
        # Get final result
        try:
            final_result = self.execute_query(query_text)
            final_step = {
                'step_number': len(steps) + 1,
                'step_type': STEP_FINAL_RESULT,
                'input_tables': [],
                'output_table': {
                    'name': 'Final Result',
                    'columns': final_result.get('columns', []),
                    'data': final_result.get('rows', []),
                    'row_count': final_result.get('row_count', 0)
                },
                'highlighted_cols': [],
                'highlighted_rows': [],
                'dimmed_rows': [],
                'explanation': f"Final Result: {final_result.get('row_count', 0)} rows"
            }
            steps.append(final_step)
        except Exception as e:
            raise ValueError(f"Error executing query: {e}")
        
        # Clean all step results for JSON serialization
        cleaned_steps = []
        for step in steps:
            cleaned_step = {k: v for k, v in step.items() if not k.startswith('_')}
            cleaned_steps.append(self._clean_for_json(cleaned_step))
        
        # Clean final_result for JSON serialization
        cleaned_final_result = self._clean_for_json(final_result)
        
        return {"steps": cleaned_steps, "final_result": cleaned_final_result}
    
    def _execute_step(
        self, 
        step_def: Dict[str, Any], 
        current_df: Optional[pd.DataFrame],
        current_table_name: Optional[str],
        tables: Dict[str, pd.DataFrame],
        full_query: str
    ) -> Dict[str, Any]:
        """Execute a single step and return visualization data."""
        step_type = step_def['step_type']
        step_number = step_def.get('step_number', 0)
        
        step_executors = {
            STEP_FROM: self._execute_from_step,
            STEP_JOIN: self._execute_join_step,
            STEP_WHERE: self._execute_where_step,
            STEP_GROUP_BY: self._execute_group_by_step,
            STEP_HAVING: self._execute_having_step,
            STEP_SELECT: self._execute_select_step,
            STEP_ORDER_BY: self._execute_order_by_step,
            STEP_UNION_INPUT: self._execute_union_input_step,
            STEP_UNION: self._execute_union_step,
        }
        
        executor = step_executors.get(step_type)
        if executor:
            if step_type == STEP_FROM:
                return executor(step_def, tables)
            elif step_type == STEP_JOIN:
                return executor(step_def, current_df, current_table_name, tables)
            else:
                return executor(step_def, current_df, current_table_name, full_query)
        
        return self._create_error_step(step_type, step_number, 'unknown step type')
    
    def _execute_from_step(self, step_def: Dict, tables: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """Execute FROM step."""
        table_names = step_def.get('tables', [])
        input_tables = []
        first_df = None
        first_table_name = None
        
        for table_name in table_names:
            if table_name in tables:
                df = tables[table_name]
                input_tables.append(self._dataframe_to_table_dict(df, table_name))
                if first_df is None:
                    first_df = df
                    first_table_name = table_name
        
        output_table = input_tables[0] if input_tables else None
        
        step_result = {
            'step_number': step_def.get('step_number', 1),
            'step_type': STEP_FROM,
            'input_tables': input_tables,
            'output_table': output_table,
            'highlighted_cols': [],
            'highlighted_rows': [],
            'dimmed_rows': [],
            'explanation': f"Step {step_def.get('step_number', 1)}: Loading table(s) {', '.join(table_names)}"
        }
        
        if first_df is not None:
            step_result['_dataframe'] = first_df
            step_result['_table_name'] = first_table_name
        
        return step_result
    
    def _execute_join_step(
        self, 
        step_def: Dict, 
        current_df: Optional[pd.DataFrame],
        current_table_name: Optional[str],
        tables: Dict[str, pd.DataFrame]
    ) -> Dict[str, Any]:
        """Execute JOIN step."""
        right_table_name = step_def.get('right_table')
        join_type = step_def.get('join_type', 'INNER')
        join_columns = step_def.get('join_columns', [])
        from_tables = step_def.get('from_tables', [])
        left_table_name = step_def.get('left_table')
        
        # Load left table if needed
        if current_df is None:
            if left_table_name and left_table_name in tables:
                current_df = tables[left_table_name]
                current_table_name = left_table_name
            elif from_tables and from_tables[0] in tables:
                current_df = tables[from_tables[0]]
                current_table_name = from_tables[0]
            else:
                return self._create_error_step(STEP_JOIN, step_def.get('step_number', 0), 'missing left table')
        
        if right_table_name not in tables:
            return self._create_error_step(STEP_JOIN, step_def.get('step_number', 0), 
                                         f'missing right table: {right_table_name}')
        
        right_df = tables[right_table_name]
        left_table_dict = self._dataframe_to_table_dict(current_df, current_table_name or 'left_table')
        right_table_dict = self._dataframe_to_table_dict(right_df, right_table_name)
        
        # Execute join using DuckDB
        left_table_alias = step_def.get('left_table_alias')
        right_table_alias = step_def.get('right_table_alias')
        result_df, output_table = self._execute_join_query(
            current_df, right_df, join_type, step_def.get('join_condition', ''),
            current_table_name or TABLE_LEFT, right_table_name,
            left_table_alias, right_table_alias, current_df
        )
        
        # Normalize join columns to match actual table columns
        # For JOIN, we want to highlight columns in the input tables
        # Combine columns from both input tables for highlighting
        left_cols = current_df.columns.tolist()
        right_cols = right_df.columns.tolist()
        all_input_cols = list(set(left_cols + right_cols))  # Remove duplicates
        
        normalized_join_cols = self._normalize_column_names(join_columns, all_input_cols)
        DebugLogger.log("JOIN columns normalized: {} -> {}", join_columns, normalized_join_cols)
        
        step_result = {
            'step_number': step_def.get('step_number', 0),
            'step_type': STEP_JOIN,
            'input_tables': [left_table_dict, right_table_dict],
            'output_table': output_table,
            'highlighted_cols': normalized_join_cols,
            'highlighted_rows': [],
            'dimmed_rows': [],
            'explanation': f"Step {step_def.get('step_number', 0)}: {join_type} JOIN {right_table_name}",
            'join_info': {
                'left_table': current_table_name or TABLE_LEFT,
                'right_table': right_table_name,
                'join_columns': normalized_join_cols,
                'join_type': join_type
            }
        }
        
        if result_df is not None:
            step_result['_dataframe'] = result_df
            step_result['_table_name'] = TABLE_JOINED_RESULT
        else:
            DebugLogger.log("JOIN step FAILED - result_df is None, cannot store _dataframe")
        
        return step_result
    
    def _execute_join_query(
        self,
        left_df: pd.DataFrame,
        right_df: pd.DataFrame,
        join_type: str,
        join_condition: str,
        left_table_name: str,
        right_table_name: str,
        left_table_alias: Optional[str] = None,
        right_table_alias: Optional[str] = None,
        left_df_for_alias_resolution: Optional[pd.DataFrame] = None
    ) -> tuple:
        """Execute a JOIN query using DuckDB. Returns (result_df, output_table_dict)."""
        tables = {TABLE_LEFT: left_df, right_table_name: right_df}
        
        # Build join query
        join_keyword = self._normalize_join_keyword(join_type)
        # Use left_df_for_alias_resolution if provided (for resolving aliases from previous JOINs)
        alias_resolution_df = left_df_for_alias_resolution if left_df_for_alias_resolution is not None else left_df
        fixed_condition = self._fix_join_condition_aliases(
            join_condition, left_table_name, right_table_name,
            left_table_alias, right_table_alias, alias_resolution_df
        )
        
        join_sql = f"SELECT * FROM {TABLE_LEFT} {join_keyword} JOIN {right_table_name}"
        if fixed_condition:
            join_sql += f" ON {fixed_condition}"
        
        result_df = None
        try:
            with self._db_connection(tables) as con:
                result_df = con.execute(join_sql).fetchdf()
                output_table = self._dataframe_to_table_dict(result_df, TABLE_JOINED_RESULT)
                DebugLogger.log("JOIN executed successfully, result shape: {}, columns: {}", result_df.shape, list(result_df.columns))
        except Exception as e:
            DebugLogger.log("JOIN failed with error: {}, join_sql: {}", e, join_sql)
            # Fallback: try without explicit join type
            try:
                join_sql = f"SELECT * FROM {TABLE_LEFT} JOIN {right_table_name}"
                if fixed_condition:
                    join_sql += f" ON {fixed_condition}"
                with self._db_connection(tables) as con:
                    result_df = con.execute(join_sql).fetchdf()
                    output_table = self._dataframe_to_table_dict(result_df, TABLE_JOINED_RESULT)
                    DebugLogger.log("JOIN fallback succeeded, result shape: {}", result_df.shape)
            except Exception as e2:
                DebugLogger.log("JOIN fallback also failed: {}", e2)
                output_table = None
                result_df = None
        
        return result_df, output_table
    
    def _normalize_join_keyword(self, join_type: str) -> str:
        """Normalize join type to DuckDB-compatible keyword."""
        join_keyword = join_type.upper()
        if 'OUTER' in join_keyword:
            if 'LEFT' in join_keyword:
                return 'LEFT OUTER'
            elif 'RIGHT' in join_keyword:
                return 'RIGHT OUTER'
            elif 'FULL' in join_keyword:
                return 'FULL OUTER'
        return join_keyword
    
    def _fix_join_condition_aliases(
        self, 
        condition: str, 
        left_table: str, 
        right_table: str,
        left_alias: Optional[str] = None,
        right_alias: Optional[str] = None,
        left_df: Optional[pd.DataFrame] = None
    ) -> str:
        """Replace aliases in join condition with actual table names.
        
        For multi-join queries, aliases from previous JOINs (like 'p' from the first JOIN)
        need to be resolved to actual column names in the left_df, not table aliases.
        """
        if not condition:
            return condition
        
        condition_fixed = condition
        
        # First, handle right table alias (this is straightforward - always use right_table name)
        if right_alias:
            # Replace right alias (e.g., "m." -> "maintenance_types.")
            condition_fixed = re.sub(rf'\b{re.escape(right_alias)}\.', f'{right_table}.', condition_fixed)
        
        # For left table, we need to be smarter:
        # 1. If left_alias is provided and matches, use it
        # 2. Otherwise, check if the alias refers to a column in left_df (from previous JOINs)
        # 3. If found, remove the alias prefix (columns after JOIN typically don't have table prefixes)
        
        if left_alias:
            # Replace left alias (e.g., "a." -> "left_table.")
            condition_fixed = re.sub(rf'\b{re.escape(left_alias)}\.', f'{TABLE_LEFT}.', condition_fixed)
        
        # Handle aliases from previous JOINs that are now part of the left_df
        # Extract all table.column patterns and check if they need to be resolved
        if left_df is not None:
            left_cols_list = list(left_df.columns)
            left_cols = set(left_cols_list)
            # Build case-insensitive lookup map once for efficiency
            left_cols_lower_map = {c.lower(): c for c in left_cols_list}
            # Build suffix lookup map (columns ending with _<number>)
            suffix_map = {}
            for c in left_cols_list:
                if '_' in c and c.split('_')[-1].isdigit():
                    base = '_'.join(c.split('_')[:-1])
                    if base not in suffix_map:
                        suffix_map[base] = []
                    suffix_map[base].append(c)
            
            DebugLogger.log("_fix_join_condition_aliases: left_df columns: {}", left_cols_list)
            DebugLogger.log("_fix_join_condition_aliases: condition_fixed before alias resolution: {}", condition_fixed)
            
            # Find all alias.column patterns in the condition that haven't been replaced yet
            # Use module-level compiled regex for better performance
            matches = list(_RE_ALIAS_COL_PATTERN.finditer(condition_fixed))
            DebugLogger.log("_fix_join_condition_aliases: found {} alias.column patterns", len(matches))
            
            if matches:
                # Use list-based replacement for better performance with multiple replacements
                # Build replacement list in reverse order
                replacements = []
                for match in reversed(matches):
                    alias = match.group(1)
                    col = match.group(2)
                    
                    # Skip if this is the right_alias, left_alias, or a table name (already handled above)
                    if alias == right_alias or alias == left_alias or alias in [TABLE_LEFT, left_table, right_table]:
                        continue
                    
                    # This must be an alias from a previous JOIN
                    # Try to find matching column in left_df using optimized lookups
                    matching_col = None
                    
                    # Strategy 1: Look for columns ending with _<number> (DuckDB renames duplicates from JOINs)
                    if col in suffix_map:
                        matching_col = suffix_map[col][0]  # Use first match
                    # Strategy 2: Exact match (no suffix)
                    elif col in left_cols:
                        matching_col = col
                    # Strategy 3: Case-insensitive match
                    elif col.lower() in left_cols_lower_map:
                        matching_col = left_cols_lower_map[col.lower()]
                    
                    if matching_col:
                        replacements.append((match.start(), match.end(), matching_col))
                        DebugLogger.log("Resolved alias {}.{} to column {} (from previous JOIN)", alias, col, matching_col)
                    else:
                        # Strategy 4: Try with TABLE_LEFT prefix as last resort
                        replacement = f'{TABLE_LEFT}.{col}'
                        replacements.append((match.start(), match.end(), replacement))
                        DebugLogger.log("Resolved alias {}.{} to {} (fallback with table prefix)", alias, col, replacement)
                
                # Apply all replacements in reverse order (to preserve indices)
                if replacements:
                    # Convert to list of characters for efficient replacement
                    condition_chars = list(condition_fixed)
                    for start, end, replacement in replacements:
                        condition_chars[start:end] = replacement
                    condition_fixed = ''.join(condition_chars)
        
        # Fallback: Replace common single-letter aliases if not explicitly provided
        # This is a heuristic for cases where aliases weren't extracted
        if not left_alias and left_df is None:
            condition_fixed = re.sub(r'\ba\.', f'{TABLE_LEFT}.', condition_fixed)
        if not right_alias:
            condition_fixed = re.sub(r'\bm\.', f'{right_table}.', condition_fixed)
        
        DebugLogger.log("Original condition: {}, Fixed: {}", condition, condition_fixed)
        return condition_fixed
    
    def _execute_where_step(
        self, 
        step_def: Dict, 
        current_df: Optional[pd.DataFrame],
        current_table_name: Optional[str],
        full_query: str
    ) -> Dict[str, Any]:
        """Execute WHERE step."""
        if current_df is None:
            return self._create_error_step(STEP_WHERE, step_def.get('step_number', 0), 'no input')
        
        input_table = self._dataframe_to_table_dict(current_df, current_table_name or 'input')
        condition = step_def.get('condition', '')
        DebugLogger.log("WHERE step - condition: {}", condition)
        
        result_df, output_table, dimmed_indices = self._execute_filter_query(
            current_df, condition, TABLE_FILTERED_RESULT
        )
        
        where_columns = step_def.get('columns', [])
        DebugLogger.log("WHERE step - columns from step_def: {}", where_columns)
        if not where_columns and condition:
            # Use SQLQueryParser's method for consistency
            parser = SQLQueryParser()
            where_columns = parser._extract_columns_from_condition(condition, is_having=False)
            DebugLogger.log("WHERE step - extracted columns from condition: {}", where_columns)
        
        # Normalize column names to match actual DataFrame columns
        if current_df is not None and where_columns:
            actual_cols = current_df.columns.tolist()
            DebugLogger.log("WHERE step - actual DataFrame columns: {}", actual_cols)
            where_columns = self._normalize_column_names(where_columns, actual_cols)
            DebugLogger.log("WHERE step - normalized columns: {}", where_columns)
        
        step_result = {
            'step_number': step_def.get('step_number', 0),
            'step_type': STEP_WHERE,
            'input_tables': [input_table],
            'output_table': output_table,
            'highlighted_cols': where_columns,
            'highlighted_rows': [],
            'dimmed_rows': dimmed_indices[:50],
            'explanation': f"Step {step_def.get('step_number', 0)}: Filtering rows WHERE {condition}"
        }
        
        if result_df is not None:
            step_result['_dataframe'] = result_df
            step_result['_table_name'] = TABLE_FILTERED_RESULT
        
        return step_result
    
    def _execute_filter_query(
        self,
        df: pd.DataFrame,
        condition: str,
        result_table_name: str
    ) -> tuple:
        """Execute a WHERE/HAVING filter query. Returns (result_df, output_table_dict, dimmed_indices)."""
        fixed_condition = self._strip_table_aliases(condition)
        filter_sql = f"SELECT * FROM {TABLE_INPUT} WHERE {fixed_condition}"
        tables = {TABLE_INPUT: df}
        
        result_df = None
        dimmed_indices = []
        try:
            with self._db_connection(tables) as con:
                result_df = con.execute(filter_sql).fetchdf()
                # Optimize dimmed indices calculation - only compute if needed for visualization
                # The result_df indices don't correspond to original df indices after DuckDB processing
                # So we calculate dimmed rows differently: all rows not in result
                df_len = len(df)
                result_len = len(result_df)
                # For visualization, we can estimate dimmed as rows not matching
                # This is approximate since DuckDB may reorder
                if result_len < df_len:
                    dimmed_indices = list(range(result_len, df_len))
                output_table = self._dataframe_to_table_dict(result_df, result_table_name)
        except Exception as e:
            DebugLogger.log("Filter query failed: {}", e)
            output_table = None
            result_df = None
        
        return result_df, output_table, dimmed_indices
    
    def _execute_having_query(
        self,
        df: pd.DataFrame,
        condition: str,
        result_table_name: str,
        aggregates: List[Dict[str, Any]]
    ) -> tuple:
        """Execute a HAVING-like filter on an already-grouped table.

        We rewrite aggregate expressions in the original HAVING condition to
        refer to the alias columns produced by the GROUP BY step (e.g.,
        SUM(hours) -> sum_hours) and then apply the condition as a WHERE
        filter on the grouped result.
        """
        fixed_condition = self._strip_table_aliases(condition)

        for agg in aggregates or []:
            func = (agg.get('func') or '').upper()
            col = (agg.get('column') or '').split('.')[-1]
            alias = agg.get('alias')
            if not func or not col or not alias:
                continue
            # Use compiled regex with dynamic substitution
            # Handle both regular and DISTINCT variants
            pattern = re.compile(
                rf'\b{re.escape(func)}\s*\(\s*(?:distinct\s+)?{re.escape(col)}\s*\)',
                flags=re.IGNORECASE,
            )
            fixed_condition = pattern.sub(alias, fixed_condition)

        having_sql = f"SELECT * FROM {TABLE_INPUT} WHERE {fixed_condition}"
        tables = {TABLE_INPUT: df}
        
        result_df = None
        try:
            with self._db_connection(tables) as con:
                result_df = con.execute(having_sql).fetchdf()
                output_table = self._dataframe_to_table_dict(result_df, result_table_name)
        except Exception as e:
            DebugLogger.log("HAVING query failed: {}", e)
            output_table = None
            result_df = None
        
        return result_df, output_table
    
    def _execute_group_by_step(
        self, 
        step_def: Dict, 
        current_df: Optional[pd.DataFrame],
        current_table_name: Optional[str],
        full_query: str
    ) -> Dict[str, Any]:
        """Execute GROUP BY step."""
        if current_df is None:
            return self._create_error_step(STEP_GROUP_BY, step_def.get('step_number', 0), 'no input')
        
        input_table = self._dataframe_to_table_dict(current_df, current_table_name or 'input')
        group_cols = step_def.get('columns', [])
        aggregates = step_def.get('aggregates', [])
        
        result_df, output_table = self._execute_group_by_query(current_df, group_cols, aggregates)
        
        step_result = {
            'step_number': step_def.get('step_number', 0),
            'step_type': STEP_GROUP_BY,
            'input_tables': [input_table],
            'output_table': output_table,
            'highlighted_cols': group_cols,
            'highlighted_rows': [],
            'dimmed_rows': [],
            'explanation': f"Step {step_def.get('step_number', 0)}: Grouping by {', '.join(group_cols)}"
        }
        
        if result_df is not None:
            step_result['_dataframe'] = result_df
            step_result['_table_name'] = TABLE_GROUPED_RESULT
        
        return step_result
    
    def _execute_group_by_query(
        self,
        df: pd.DataFrame,
        group_cols: List[str],
        aggregates: List[Dict[str, Any]]
    ) -> tuple:
        """Execute a GROUP BY query. Returns (result_df, output_table_dict).

        If aggregates are provided, we compute them alongside the group keys
        so that the subsequent HAVING step can filter on the aggregate
        columns (e.g., SUM(hours) AS sum_hours). Otherwise, we just return
        distinct group keys.
        """
        tables = {TABLE_INPUT: df}
        
        # Strip table prefixes from group columns (e.g., "a.pnumber" -> "pnumber")
        # The intermediate DataFrame after JOINs might or might not have table prefixes
        actual_df_cols = list(df.columns)
        actual_df_cols_set = set(actual_df_cols)
        DebugLogger.log("GROUP BY - actual DataFrame columns: {}", actual_df_cols)
        
        # Build lookup maps once for efficiency
        actual_df_cols_lower_map = {c.lower(): c for c in actual_df_cols}
        suffix_to_cols = {}  # Maps suffix -> list of columns with that suffix
        for c in actual_df_cols:
            if '.' in c:
                suffix = c.split('.')[-1].lower()
                if suffix not in suffix_to_cols:
                    suffix_to_cols[suffix] = []
                suffix_to_cols[suffix].append(c)
        
        normalized_group_cols = []
        for col in group_cols:
            matching_col = None
            
            # If column has table prefix, try to match by suffix
            if '.' in col:
                col_suffix = col.split('.')[-1]
                # First try exact suffix match
                if col_suffix in actual_df_cols_set:
                    matching_col = col_suffix
                    DebugLogger.log("GROUP BY column '{}' matched by suffix '{}'", col, col_suffix)
                # Then try full name with prefix
                elif col in actual_df_cols_set:
                    matching_col = col
                    DebugLogger.log("GROUP BY column '{}' matched by full name", col)
                # Try case-insensitive match on suffix
                elif col_suffix.lower() in actual_df_cols_lower_map:
                    matching_col = actual_df_cols_lower_map[col_suffix.lower()]
                    DebugLogger.log("GROUP BY column '{}' matched case-insensitively to '{}'", col, matching_col)
                # Try matching with table prefix
                elif col_suffix.lower() in suffix_to_cols:
                    matching_col = suffix_to_cols[col_suffix.lower()][0]  # Use first match
                    DebugLogger.log("GROUP BY column '{}' matched to '{}'", col, matching_col)
                else:
                    # Last resort: use suffix and hope DuckDB can resolve it
                    matching_col = col_suffix
                    DebugLogger.log("GROUP BY column '{}' not found, using suffix '{}' (may fail)", col, col_suffix)
            else:
                # No prefix, use as-is but check if it exists
                if col in actual_df_cols_set:
                    matching_col = col
                # Try case-insensitive match
                elif col.lower() in actual_df_cols_lower_map:
                    matching_col = actual_df_cols_lower_map[col.lower()]
                    DebugLogger.log("GROUP BY column '{}' matched case-insensitively to '{}'", col, matching_col)
                else:
                    matching_col = col
                    DebugLogger.log("GROUP BY column '{}' not found, using as-is (may fail)", col)
            
            if matching_col:
                normalized_group_cols.append(matching_col)
        
        DebugLogger.log("GROUP BY columns normalized: {} -> {}", group_cols, normalized_group_cols)

        if aggregates:
            # Build SELECT with group keys and aggregate expressions.
            select_parts: List[str] = list(normalized_group_cols)
            for agg in aggregates:
                func = agg.get('func', '').upper()
                col = agg.get('column', '')
                alias = agg.get('alias', '')
                if not func or not col or not alias:
                    continue
                base_col = col.split('.')[-1]
                # Check if column exists in DataFrame
                if base_col not in actual_df_cols:
                    base_col_lower = base_col.lower()
                    matching_col = next((c for c in actual_df_cols if c.lower() == base_col_lower), None)
                    if matching_col:
                        base_col = matching_col
                    else:
                        # Column not found - log warning but continue (may fail at execution)
                        DebugLogger.log("GROUP BY aggregate column '{}' not found in DataFrame, using as-is (may fail)", base_col)
                
                # Handle DISTINCT in aggregate functions
                has_distinct = agg.get('distinct', False)
                if has_distinct:
                    agg_expr = f"{func}(DISTINCT {base_col})"
                else:
                    agg_expr = f"{func}({base_col})"
                select_parts.append(f"{agg_expr} AS {alias}")

            select_sql = ', '.join(select_parts)
            group_by_sql = f"SELECT {select_sql} FROM {TABLE_INPUT} GROUP BY {', '.join(normalized_group_cols)}"
        else:
            # Fallback: distinct group keys only.
            group_by_sql = f"SELECT DISTINCT {', '.join(normalized_group_cols)} FROM {TABLE_INPUT}"
        
        result_df = None
        try:
            with self._db_connection(tables) as con:
                result_df = con.execute(group_by_sql).fetchdf()
                output_table = self._dataframe_to_table_dict(result_df, TABLE_GROUPED_RESULT)
        except Exception as e:
            DebugLogger.log("GROUP BY query failed: {}", e)
            output_table = None
            result_df = None
        
        return result_df, output_table
    
    def _execute_having_step(
        self, 
        step_def: Dict, 
        current_df: Optional[pd.DataFrame],
        current_table_name: Optional[str],
        full_query: str
    ) -> Dict[str, Any]:
        """Execute HAVING step."""
        if current_df is None:
            return self._create_error_step(STEP_HAVING, step_def.get('step_number', 0), 'no input')
        
        input_table = self._dataframe_to_table_dict(current_df, current_table_name or 'input')
        condition = step_def.get('condition', '')
        aggregates = step_def.get('aggregates', [])
        
        result_df, output_table = self._execute_having_query(
            current_df, condition, TABLE_FILTERED_GROUPS, aggregates
        )
        
        having_columns = step_def.get('columns', [])
        DebugLogger.log("HAVING step - columns from step_def: {}", having_columns)
        if not having_columns and condition:
            # Use SQLQueryParser's method for consistency
            parser = SQLQueryParser()
            having_columns = parser._extract_columns_from_condition(condition, is_having=True)
            DebugLogger.log("HAVING step - extracted columns from condition: {}", having_columns)

        # Build highlighted columns:
        # - Start from columns referenced in the HAVING condition
        # - Also include aggregate aliases produced by GROUP BY (e.g., sum_hours)
        highlighted_cols: List[str] = []
        if current_df is not None:
            actual_cols = current_df.columns.tolist()
            DebugLogger.log("HAVING step - actual DataFrame columns: {}", actual_cols)

            if having_columns:
                normalized = self._normalize_column_names(having_columns, actual_cols)
                DebugLogger.log("HAVING step - normalized columns from condition: {}", normalized)
                highlighted_cols.extend(normalized)

            for agg in aggregates or []:
                alias = agg.get('alias')
                if alias and alias in actual_cols and alias not in highlighted_cols:
                    highlighted_cols.append(alias)
            DebugLogger.log("HAVING step - final highlighted columns: {}", highlighted_cols)
        
        step_result = {
            'step_number': step_def.get('step_number', 0),
            'step_type': STEP_HAVING,
            'input_tables': [input_table],
            'output_table': output_table,
            'highlighted_cols': highlighted_cols,
            'highlighted_rows': [],
            'dimmed_rows': [],
            'explanation': f"Step {step_def.get('step_number', 0)}: Filtering groups HAVING {condition}"
        }
        
        if result_df is not None:
            step_result['_dataframe'] = result_df
            step_result['_table_name'] = TABLE_FILTERED_GROUPS
        
        return step_result
    
    def _execute_select_step(
        self, 
        step_def: Dict, 
        current_df: Optional[pd.DataFrame],
        current_table_name: Optional[str],
        full_query: str
    ) -> Dict[str, Any]:
        """Execute SELECT step."""
        if current_df is None:
            return self._create_error_step(STEP_SELECT, step_def.get('step_number', 0), 'no input')
        
        input_table = self._dataframe_to_table_dict(current_df, current_table_name or 'input')
        select_cols = step_def.get('columns', [])
        
        result_df = None
        if not select_cols:
            output_table = input_table
            result_df = current_df
        else:
            try:
                result_df = current_df[select_cols]
                output_table = self._dataframe_to_table_dict(result_df, 'projected_result')
            except Exception as e:
                output_table = input_table
                result_df = current_df
        
        step_result = {
            'step_number': step_def.get('step_number', 0),
            'step_type': STEP_SELECT,
            'input_tables': [input_table],
            'output_table': output_table,
            'highlighted_cols': select_cols,
            'highlighted_rows': [],
            'dimmed_rows': [],
            'explanation': f"Step {step_def.get('step_number', 0)}: Selecting columns {', '.join(select_cols) if select_cols else 'ALL'}"
        }
        
        if result_df is not None:
            step_result['_dataframe'] = result_df
            step_result['_table_name'] = TABLE_PROJECTED_RESULT
        
        return step_result
    
    def _execute_order_by_step(
        self, 
        step_def: Dict, 
        current_df: Optional[pd.DataFrame],
        current_table_name: Optional[str],
        full_query: str
    ) -> Dict[str, Any]:
        """Execute ORDER BY step."""
        if current_df is None:
            return self._create_error_step(STEP_ORDER_BY, step_def.get('step_number', 0), 'no input')
        
        input_table = self._dataframe_to_table_dict(current_df, current_table_name or 'input')
        order_cols = step_def.get('columns', [])
        
        result_df, output_table = self._execute_order_by_query(current_df, order_cols, input_table)
        
        step_result = {
            'step_number': step_def.get('step_number', 0),
            'step_type': STEP_ORDER_BY,
            'input_tables': [input_table],
            'output_table': output_table,
            'highlighted_cols': order_cols,
            'highlighted_rows': [],
            'dimmed_rows': [],
            'explanation': f"Step {step_def.get('step_number', 0)}: Sorting by {', '.join(order_cols)}"
        }
        
        # Store DataFrame reference for next step (if any follow)
        if result_df is not None:
            step_result['_dataframe'] = result_df
            step_result['_table_name'] = TABLE_SORTED_RESULT
        
        return step_result
    
    def _execute_order_by_query(
        self,
        df: pd.DataFrame,
        order_cols: List[str],
        fallback_table: Dict[str, Any]
    ) -> tuple:
        """Execute an ORDER BY query. Returns (result_df, output_table_dict)."""
        tables = {TABLE_INPUT: df}
        order_by_sql = f"SELECT * FROM {TABLE_INPUT} ORDER BY {', '.join(order_cols)}"
        
        result_df = None
        try:
            with self._db_connection(tables) as con:
                result_df = con.execute(order_by_sql).fetchdf()
                output_table = self._dataframe_to_table_dict(result_df, TABLE_SORTED_RESULT)
        except Exception as e:
            DebugLogger.log("ORDER BY query failed: {}", e)
            output_table = fallback_table
            result_df = df  # Fallback to input if ORDER BY fails
        
        return result_df, output_table

    def _execute_union_input_step(
        self,
        step_def: Dict,
        current_df: Optional[pd.DataFrame],
        current_table_name: Optional[str],
        full_query: str,
    ) -> Dict[str, Any]:
        """Execute one side of a UNION as its own step.

        This runs the stored SELECT subquery against the current tables in
        `graph_builder` using DuckDB and exposes the result as an intermediate
        table.
        """
        side = step_def.get('side', 'left')
        subquery_sql = step_def.get('query', '')
        step_number = step_def.get('step_number', 0)

        tables = self._get_tables_as_dataframes()
        if not tables:
            return self._create_error_step(STEP_UNION_INPUT, step_number, 'no tables available')

        try:
            with self._db_connection(tables) as con:
                result_df: pd.DataFrame = con.execute(subquery_sql).fetchdf()
        except RuntimeError:
            return self._create_error_step(STEP_UNION_INPUT, step_number, 'DuckDB is not installed')
        except Exception as exc:
            DebugLogger.log("UNION_INPUT step failed for side {}: {}", side, exc)
            return self._create_error_step(
                STEP_UNION_INPUT,
                step_number,
                f'error executing {side} SELECT of UNION: {exc}',
            )

        table_name = f'union_{side}_result'
        output_table = self._dataframe_to_table_dict(result_df, table_name)

        step_result = {
            'step_number': step_number,
            'step_type': STEP_UNION_INPUT,
            'input_tables': [],
            'output_table': output_table,
            'highlighted_cols': [],
            'highlighted_rows': [],
            'dimmed_rows': [],
            'explanation': f"Step {step_number}: Executing {side.upper()} SELECT of UNION",
        }

        step_result['_dataframe'] = result_df
        step_result['_table_name'] = table_name

        return step_result

    def _execute_union_step(
        self,
        step_def: Dict,
        current_df: Optional[pd.DataFrame],
        current_table_name: Optional[str],
        full_query: str,
    ) -> Dict[str, Any]:
        """Execute the UNION step that combines two SELECT subqueries."""
        step_number = step_def.get('step_number', 0)
        left_query = step_def.get('left_query', '')
        right_query = step_def.get('right_query', '')
        
        # Check if we have pre-computed results from step-by-step execution
        left_df = step_def.get('_left_df')
        right_df = step_def.get('_right_df')

        if left_df is None or right_df is None:
            # Fallback: execute queries directly
            if not left_query or not right_query:
                return self._create_error_step(STEP_UNION, step_number, 'missing UNION subqueries')

            tables = self._get_tables_as_dataframes()
            if not tables:
                return self._create_error_step(STEP_UNION, step_number, 'no tables available')

            try:
                with self._db_connection(tables) as con:
                    left_df = con.execute(left_query).fetchdf()
                    right_df = con.execute(right_query).fetchdf()
            except RuntimeError:
                return self._create_error_step(STEP_UNION, step_number, 'DuckDB is not installed')
            except Exception as exc:
                DebugLogger.log("UNION step failed: {}", exc)
                return self._create_error_step(
                    STEP_UNION,
                    step_number,
                    f'error executing UNION subqueries: {exc}',
                )

        # Combine the dataframes using pandas concat (UNION removes duplicates)
        # Optimize: use ignore_index=True to avoid index conflicts, then drop duplicates efficiently
        union_df = None
        try:
            # Concatenate and drop duplicates in one go
            union_df = pd.concat([left_df, right_df], ignore_index=True)
            union_df = union_df.drop_duplicates().reset_index(drop=True)
        except Exception as exc:
            DebugLogger.log("UNION concatenation failed: {}", exc)
            return self._create_error_step(
                STEP_UNION,
                step_number,
                f'error combining UNION results: {exc}',
            )

        left_table_dict = self._dataframe_to_table_dict(left_df, 'left_result')
        right_table_dict = self._dataframe_to_table_dict(right_df, 'right_result')
        output_table = self._dataframe_to_table_dict(union_df, 'union_result')

        step_result = {
            'step_number': step_number,
            'step_type': STEP_UNION,
            'input_tables': [left_table_dict, right_table_dict],
            'output_table': output_table,
            'highlighted_cols': [],
            'highlighted_rows': [],
            'dimmed_rows': [],
            'explanation': f"Step {step_number}: UNION - combining results from both SELECT statements",
        }

        step_result['_dataframe'] = union_df
        step_result['_table_name'] = 'union_result'

        return step_result

    # ------------------------------------------------------------------
    # Backwards-compat convenience wrapper
    # ------------------------------------------------------------------
    
    def compile_query(
        self, query_text: str, query_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Keep old name alive but simply run the query once."""
        query_id = query_id or f"query_{hash(query_text)}"
        result = self.execute_query(query_text)
        return {"query_id": query_id, "result": result}

    def get_visual_state(
        self, query_id: str, line_index: int, sub_step_index: Optional[int] = None
    ) -> Dict[str, Any]:
        """Legacy API no longer supported."""
        raise RuntimeError(
            "Step-by-step visual query states have been removed; "
            "call `execute_query` instead."
        )
