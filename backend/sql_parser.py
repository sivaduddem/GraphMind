"""
SQL Parser for MySQL DDL
Parses CREATE TABLE and ALTER TABLE statements to extract tables and foreign keys
"""
import re
from typing import List, Dict, Any


class SQLParser:
    """Parser for MySQL DDL statements"""
    
    def __init__(self):
        self.tables = []
    
    def parse_sql(self, sql_content: str) -> List[Dict[str, Any]]:
        """
        Parse SQL content and extract table definitions, foreign keys, and data
        
        Returns:
            List of table dictionaries with name, columns, foreign_keys, and rows
        """
        self.tables = []
        
        # Store original SQL for INSERT parsing (before normalization)
        original_sql = sql_content
        
        # Normalize SQL content
        sql_content = self._normalize_sql(sql_content)
        
        # Parse CREATE TABLE statements
        create_table_pattern = r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(\w+)`?\s*\((.*?)\)(?:\s*ENGINE|\s*CHARSET|\s*DEFAULT|\s*;|$)'
        
        for match in re.finditer(create_table_pattern, sql_content, re.IGNORECASE | re.DOTALL):
            table_name = match.group(1)
            table_body = match.group(2)
            
            table_info = {
                'name': table_name,
                'columns': [],
                'foreign_keys': [],
                'rows': []
            }
            
            # Parse columns and inline foreign keys
            self._parse_table_body(table_body, table_info)
            
            self.tables.append(table_info)
        
        # Parse ALTER TABLE statements for foreign keys with constraints
        alter_table_pattern = r'ALTER\s+TABLE\s+`?(\w+)`?\s+ADD\s+(?:CONSTRAINT\s+`?\w+`?\s+)?FOREIGN\s+KEY\s*\(([^)]+)\)\s*REFERENCES\s+`?(\w+)`?\s*\(([^)]+)\)(?:\s+ON\s+DELETE\s+(\w+))?(?:\s+ON\s+UPDATE\s+(\w+))?'
        
        for match in re.finditer(alter_table_pattern, sql_content, re.IGNORECASE):
            table_name = match.group(1)
            fk_columns = [col.strip().strip('`') for col in match.group(2).split(',')]
            ref_table = match.group(3)
            ref_columns = [col.strip().strip('`') for col in match.group(4).split(',')]
            on_delete = match.group(5) if match.group(5) else 'RESTRICT'
            on_update = match.group(6) if match.group(6) else 'RESTRICT'
            
            # Find the table and add the foreign key
            for table in self.tables:
                if table['name'] == table_name:
                    table['foreign_keys'].append({
                        'columns': fk_columns,
                        'references_table': ref_table,
                        'referenced_columns': ref_columns,
                        'on_delete': on_delete.upper() if on_delete else 'RESTRICT',
                        'on_update': on_update.upper() if on_update else 'RESTRICT'
                    })
                    break
        
        # Parse INSERT statements to extract row data
        self._parse_insert_statements(original_sql)
        
        return self.tables
    
    def _normalize_sql(self, sql_content: str) -> str:
        """Normalize SQL content for easier parsing"""
        # Remove comments
        sql_content = re.sub(r'--.*?$', '', sql_content, flags=re.MULTILINE)
        sql_content = re.sub(r'/\*.*?\*/', '', sql_content, flags=re.DOTALL)
        
        # Normalize whitespace but preserve structure for table body parsing
        # Replace multiple spaces with single space, but keep newlines for now
        sql_content = re.sub(r'[ \t]+', ' ', sql_content)
        # Replace newlines with spaces only outside of parentheses
        # This is a simplified approach - for more complex cases, we'd need a proper parser
        sql_content = re.sub(r'\n', ' ', sql_content)
        
        return sql_content
    
    def _parse_table_body(self, table_body: str, table_info: Dict[str, Any]):
        """Parse table body to extract columns and inline foreign keys"""
        # Split by comma, but be careful with nested parentheses
        lines = self._split_table_body(table_body)
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # Skip PRIMARY KEY, UNIQUE KEY, and CONSTRAINT declarations (they're handled separately)
            if re.match(r'PRIMARY\s+KEY', line, re.IGNORECASE):
                # Skip PRIMARY KEY declarations - they're not columns or FKs
                continue
            
            if re.match(r'UNIQUE\s+KEY', line, re.IGNORECASE):
                # Skip UNIQUE KEY declarations
                continue
            
            # Handle CONSTRAINT declarations (may contain FOREIGN KEY)
            if re.match(r'CONSTRAINT', line, re.IGNORECASE):
                # Check if it's a CONSTRAINT with FOREIGN KEY
                fk_match = re.search(
                    r'(?:CONSTRAINT\s+`?\w+`?\s+)?FOREIGN\s+KEY\s*\(([^)]+)\)\s*REFERENCES\s+`?(\w+)`?\s*\(([^)]+)\)(?:\s+ON\s+DELETE\s+(\w+))?(?:\s+ON\s+UPDATE\s+(\w+))?',
                    line,
                    re.IGNORECASE
                )
                if fk_match:
                    fk_columns = [col.strip().strip('`') for col in fk_match.group(1).split(',')]
                    ref_table = fk_match.group(2)
                    ref_columns = [col.strip().strip('`') for col in fk_match.group(3).split(',')]
                    on_delete = fk_match.group(4) if fk_match.group(4) else 'RESTRICT'
                    on_update = fk_match.group(5) if fk_match.group(5) else 'RESTRICT'
                    
                    table_info['foreign_keys'].append({
                        'columns': fk_columns,
                        'references_table': ref_table,
                        'referenced_columns': ref_columns,
                        'on_delete': on_delete.upper() if on_delete else 'RESTRICT',
                        'on_update': on_update.upper() if on_update else 'RESTRICT'
                    })
                continue
            
            # Check for inline foreign key with ON DELETE/ON UPDATE constraints
            fk_match = re.search(
                r'(?:CONSTRAINT\s+`?\w+`?\s+)?FOREIGN\s+KEY\s*\(([^)]+)\)\s*REFERENCES\s+`?(\w+)`?\s*\(([^)]+)\)(?:\s+ON\s+DELETE\s+(\w+))?(?:\s+ON\s+UPDATE\s+(\w+))?',
                line,
                re.IGNORECASE
            )
            
            if fk_match:
                fk_columns = [col.strip().strip('`') for col in fk_match.group(1).split(',')]
                ref_table = fk_match.group(2)
                ref_columns = [col.strip().strip('`') for col in fk_match.group(3).split(',')]
                on_delete = fk_match.group(4) if fk_match.group(4) else 'RESTRICT'
                on_update = fk_match.group(5) if fk_match.group(5) else 'RESTRICT'
                
                table_info['foreign_keys'].append({
                    'columns': fk_columns,
                    'references_table': ref_table,
                    'referenced_columns': ref_columns,
                    'on_delete': on_delete.upper() if on_delete else 'RESTRICT',
                    'on_update': on_update.upper() if on_update else 'RESTRICT'
                })
            else:
                # Regular column definition - match column name and type (including type parameters)
                # Pattern: column_name type(parameters) or column_name type
                # Handle: varchar(100), char(20), integer, decimal(9,0), etc.
                # Skip any trailing constraints like "not null", "default null", etc.
                # Use a more robust pattern that handles commas in type parameters like decimal(9, 0)
                col_match = re.match(r'`?(\w+)`?\s+(\w+(?:\([^)]*(?:\([^)]*\)[^)]*)*\))?)', line, re.IGNORECASE)
                if not col_match:
                    # Fallback: simpler pattern for types without nested parentheses
                    col_match = re.match(r'`?(\w+)`?\s+(\w+)', line, re.IGNORECASE)
                
                if col_match:
                    col_name = col_match.group(1)
                    col_type = col_match.group(2)
                    # Only add if it's not a constraint keyword
                    if col_name.upper() not in ['PRIMARY', 'UNIQUE', 'CONSTRAINT', 'FOREIGN', 'KEY', 'INDEX']:
                        table_info['columns'].append({
                            'name': col_name,
                            'type': col_type
                        })
    
    def _split_table_body(self, body: str) -> List[str]:
        """Split table body by commas, respecting parentheses"""
        lines = []
        current = ""
        depth = 0
        
        for char in body:
            if char == '(':
                depth += 1
                current += char
            elif char == ')':
                depth -= 1
                current += char
            elif char == ',' and depth == 0:
                if current.strip():
                    lines.append(current.strip())
                current = ""
            else:
                current += char
        
        if current.strip():
            lines.append(current.strip())
        
        return lines
    
    def _parse_insert_statements(self, sql_content: str):
        """Parse INSERT statements and add rows to corresponding tables"""
        # Pattern to match INSERT INTO table VALUES (...), (...), ...
        # Handle both single and multi-row inserts
        insert_pattern = r'INSERT\s+INTO\s+`?(\w+)`?\s*(?:\([^)]+\))?\s*VALUES\s*((?:\([^)]+\)(?:\s*,\s*\([^)]+\))*))'
        
        for match in re.finditer(insert_pattern, sql_content, re.IGNORECASE | re.MULTILINE):
            table_name = match.group(1)
            values_block = match.group(2)
            
            # Find the table
            table_info = None
            for table in self.tables:
                if table['name'] == table_name:
                    table_info = table
                    break
            
            if not table_info:
                continue
            
            # Extract column names if specified: INSERT INTO table (col1, col2) VALUES ...
            column_names = None
            col_match = re.search(r'INSERT\s+INTO\s+`?' + re.escape(table_name) + r'`?\s*\(([^)]+)\)', match.group(0), re.IGNORECASE)
            if col_match:
                column_names = [col.strip().strip('`').strip("'") for col in col_match.group(1).split(',')]
            
            # Parse value tuples
            # Match individual value tuples: (val1, val2, ...)
            value_tuple_pattern = r'\(([^)]+)\)'
            for value_match in re.finditer(value_tuple_pattern, values_block):
                values_str = value_match.group(1)
                # Split by comma, but respect quoted strings
                values = self._parse_value_list(values_str)
                
                # Create row dictionary
                row = {}
                if column_names:
                    # Use specified column names
                    for i, col_name in enumerate(column_names):
                        if i < len(values):
                            row[col_name] = self._parse_value(values[i])
                else:
                    # Use column order from table definition
                    for i, col in enumerate(table_info['columns']):
                        if i < len(values):
                            row[col['name']] = self._parse_value(values[i])
                
                if row:
                    table_info['rows'].append(row)
    
    def _parse_value_list(self, values_str: str) -> List[str]:
        """Parse a comma-separated list of values, respecting quotes"""
        values = []
        current = ""
        in_quotes = False
        quote_char = None
        depth = 0
        
        for char in values_str:
            if char in ("'", '"') and (not current or current[-1] != '\\'):
                if not in_quotes:
                    in_quotes = True
                    quote_char = char
                elif char == quote_char:
                    in_quotes = False
                    quote_char = None
                current += char
            elif char == ',' and not in_quotes and depth == 0:
                values.append(current.strip())
                current = ""
            else:
                current += char
        
        if current.strip():
            values.append(current.strip())
        
        return values
    
    def _parse_value(self, value: str) -> Any:
        """Parse a single SQL value"""
        value = value.strip()
        
        # Remove quotes
        if (value.startswith("'") and value.endswith("'")) or (value.startswith('"') and value.endswith('"')):
            return value[1:-1]
        
        # Handle NULL
        if value.upper() == 'NULL':
            return None
        
        # Try to parse as number
        try:
            if '.' in value:
                return float(value)
            else:
                return int(value)
        except ValueError:
            pass
        
        return value

