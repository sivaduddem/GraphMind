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
        Parse SQL content and extract table definitions and foreign keys
        
        Returns:
            List of table dictionaries with name, columns, and foreign_keys
        """
        self.tables = []
        
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
                'foreign_keys': []
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
        
        return self.tables
    
    def _normalize_sql(self, sql_content: str) -> str:
        """Normalize SQL content for easier parsing"""
        # Remove comments
        sql_content = re.sub(r'--.*?$', '', sql_content, flags=re.MULTILINE)
        sql_content = re.sub(r'/\*.*?\*/', '', sql_content, flags=re.DOTALL)
        
        # Normalize whitespace
        sql_content = re.sub(r'\s+', ' ', sql_content)
        
        return sql_content
    
    def _parse_table_body(self, table_body: str, table_info: Dict[str, Any]):
        """Parse table body to extract columns and inline foreign keys"""
        # Split by comma, but be careful with nested parentheses
        lines = self._split_table_body(table_body)
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # Check for inline foreign key with ON DELETE/ON UPDATE constraints
            fk_match = re.search(
                r'FOREIGN\s+KEY\s*\(([^)]+)\)\s*REFERENCES\s+`?(\w+)`?\s*\(([^)]+)\)(?:\s+ON\s+DELETE\s+(\w+))?(?:\s+ON\s+UPDATE\s+(\w+))?',
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
                # Regular column definition
                col_match = re.match(r'`?(\w+)`?\s+(\w+)', line, re.IGNORECASE)
                if col_match:
                    col_name = col_match.group(1)
                    col_type = col_match.group(2)
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

