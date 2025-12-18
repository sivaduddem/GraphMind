/**
 * Query Visualizer Frontend
 * Handles SQL query visualization with step-by-step table transformations
 */

// API_BASE is already defined in graph.js (which loads first)
// Just reference it directly - no need to redeclare
// Fallback in case graph.js hasn't loaded yet
const QUERY_API_BASE = (typeof API_BASE !== 'undefined' ? API_BASE : 'http://localhost:8000/api');

// Define switchView function immediately (before DOMContentLoaded)
window.switchView = function(view) {
    console.log('=== switchView called with:', view, '===');
    try {
        const graphView = document.getElementById('graphView');
        const schemaView = document.getElementById('schemaView');
        const queryView = document.getElementById('queryView');
        const graphTab = document.getElementById('graphViewTab');
        const schemaTab = document.getElementById('schemaViewTab');
        const queryTab = document.getElementById('queryViewTab');
        const graphControls = document.getElementById('graphControls');
        const schemaControls = document.getElementById('schemaControls');
        const queryControls = document.getElementById('queryControls');
        
        console.log('Elements found:', { 
            graphView: !!graphView, 
            schemaView: !!schemaView,
            queryView: !!queryView, 
            graphTab: !!graphTab, 
            schemaTab: !!schemaTab,
            queryTab: !!queryTab,
            graphControls: !!graphControls,
            schemaControls: !!schemaControls,
            queryControls: !!queryControls
        });
        
        if (!graphView || !schemaView || !queryView || !graphTab || !schemaTab || !queryTab) {
            console.error('View elements not found!');
            alert('Error: View elements not found. Please refresh the page.');
            return;
        }
        
        if (view === 'graph') {
            console.log('Switching to Graph View');
            graphView.style.display = 'block';
            if (schemaView) schemaView.style.display = 'none';
            queryView.style.display = 'none';
            if (graphTab) graphTab.classList.add('active');
            if (schemaTab) schemaTab.classList.remove('active');
            if (queryTab) queryTab.classList.remove('active');
            if (graphControls) graphControls.style.display = 'block';
            if (schemaControls) schemaControls.style.display = 'none';
            if (queryControls) queryControls.style.display = 'none';
            
            const searchSection = document.getElementById('searchSection');
            const graphStatsSection = document.getElementById('graphStatsSection');
            if (searchSection) searchSection.style.display = 'block';
            if (graphStatsSection) graphStatsSection.style.display = 'block';
        } else if (view === 'schema') {
            console.log('Switching to Schema View');
            graphView.style.display = 'none';
            schemaView.style.display = 'block';
            queryView.style.display = 'none';
            if (graphTab) graphTab.classList.remove('active');
            if (schemaTab) schemaTab.classList.add('active');
            if (queryTab) queryTab.classList.remove('active');
            if (graphControls) graphControls.style.display = 'none';
            if (schemaControls) schemaControls.style.display = 'block';
            if (queryControls) queryControls.style.display = 'none';
            
            const searchSection = document.getElementById('searchSection');
            const graphStatsSection = document.getElementById('graphStatsSection');
            if (searchSection) searchSection.style.display = 'block';
            if (graphStatsSection) graphStatsSection.style.display = 'block';
            
            if (typeof loadSchema === 'function') {
                loadSchema();
            }
        } else if (view === 'query') {
            console.log('Switching to Query Visualizer');
            graphView.style.display = 'none';
            if (schemaView) schemaView.style.display = 'none';
            queryView.style.display = 'block';
            if (graphTab) graphTab.classList.remove('active');
            if (schemaTab) schemaTab.classList.remove('active');
            if (queryTab) queryTab.classList.add('active');
            if (graphControls) graphControls.style.display = 'none';
            if (schemaControls) schemaControls.style.display = 'none';
            if (queryControls) queryControls.style.display = 'block';
            
            // Hide Search and Graph Stats sections in Query Visualizer
            const searchSection = document.getElementById('searchSection');
            const graphStatsSection = document.getElementById('graphStatsSection');
            if (searchSection) searchSection.style.display = 'none';
            if (graphStatsSection) graphStatsSection.style.display = 'none';
            
            // Initialize query visualizer if not already done
            if (window.queryVisualizer) {
                if (!window.queryVisualizer.sqlEditor) {
                    console.log('Initializing query visualizer...');
                    window.queryVisualizer.init();
                } else {
                    // Reload datasets even if already initialized (in case new data was uploaded)
                    console.log('Reloading datasets...');
                    window.queryVisualizer.loadDatasets();
                }
            }
        }
        console.log('=== switchView completed successfully ===');
    } catch (error) {
        console.error('Error in switchView:', error);
        alert('Error switching view: ' + error.message);
    }
};

window.queryVisualizer = {
    currentQueryId: null,
    compiledQuery: null,
    currentLineIndex: 0,
    currentStepIndex: 0,
    totalLines: 0,
    totalSteps: 0,
    subSteps: [],
    visualStates: {},  // Cache visual states by line index
    isPlaying: false,
    playInterval: null,
    playbackSpeed: 1.0,
    sqlEditor: null,
    
    init() {
        // Initialize CodeMirror editor
        const editorElement = document.getElementById('sqlEditor');
        if (editorElement) {
            this.sqlEditor = CodeMirror.fromTextArea(editorElement, {
                mode: 'text/x-sql',
                theme: 'monokai',
                lineNumbers: true,
                lineWrapping: true,
                indentWithTabs: true,
                smartIndent: true,
                autofocus: true,
                firstLineNumber: 1,
                readOnly: false,
                // Prevent editing in gutter
                gutters: ['CodeMirror-linenumbers']
            });
            
            // Ensure editor content starts at line 0 (displayed as line 1)
            // Remove any content that might exist before line 0
            const currentValue = this.sqlEditor.getValue();
            if (currentValue) {
                const lines = currentValue.split('\n');
                // Ensure we start with content at line 0
                const cleanedValue = lines.join('\n');
                if (cleanedValue !== currentValue) {
                    this.sqlEditor.setValue(cleanedValue);
                }
            }
            
            // Prevent editing before line 1 (line 0 in CodeMirror) and in gutter area on ANY line
            this.sqlEditor.on('beforeChange', (cm, change) => {
                // Block any changes that affect lines before line 0 (the first line)
                if (change.from.line < 0 || change.to.line < 0) {
                    change.cancel();
                    return;
                }
                // Prevent changes that start before column 0 (in gutter area) on ANY line
                if (change.from.ch < 0 || change.to.ch < 0) {
                    change.cancel();
                    // Move cursor to start of line
                    setTimeout(() => {
                        const cursor = cm.getCursor();
                        cm.setCursor({line: Math.max(0, cursor.line), ch: 0});
                    }, 0);
                    return;
                }
                // Also prevent inserting newlines that would create content before line 0
                if (change.text && change.text.some((line, idx) => {
                    const lineNum = change.from.line + idx;
                    return lineNum < 0;
                })) {
                    change.cancel();
                    return;
                }
            });
            
            // Prevent cursor from going before line 1 (line 0 in CodeMirror) or before column 0 (gutter)
            const checkCursor = () => {
                const cursor = this.sqlEditor.getCursor();
                if (cursor.line < 0) {
                    this.sqlEditor.setCursor({line: 0, ch: 0});
                } else if (cursor.ch < 0) {
                    // Cursor is in gutter area, move to start of line
                    this.sqlEditor.setCursor({line: cursor.line, ch: 0});
                }
            };
            
            // Check cursor position on any activity
            this.sqlEditor.on('cursorActivity', (cm) => {
                checkCursor();
                this.onLineChange();
            });
            
            // Prevent clicking in gutter or before content starts on ANY line
            this.sqlEditor.on('mousedown', (cm, event) => {
                // Check if click is in the gutter area (line numbers)
                const gutter = cm.getGutterElement();
                if (gutter && gutter.contains(event.target)) {
                    event.preventDefault();
                    event.stopPropagation();
                    // Move cursor to start of the line that was clicked
                    const coords = cm.coordsChar({left: event.clientX, top: event.clientY});
                    if (coords) {
                        cm.setCursor({line: Math.max(0, coords.line), ch: 0});
                    }
                    return;
                }
                
                // Get the editor's display area to check if click is in gutter
                const display = cm.getWrapperElement();
                const rect = display.getBoundingClientRect();
                const gutterWidth = gutter ? gutter.offsetWidth : 0;
                
                // Check if click X position is within gutter area
                const clickX = event.clientX - rect.left;
                if (clickX < gutterWidth) {
                    // Click was in gutter area, prevent it and move cursor to start of line
                    event.preventDefault();
                    event.stopPropagation();
                    const coords = cm.coordsChar({left: event.clientX, top: event.clientY});
                    if (coords) {
                        cm.setCursor({line: Math.max(0, coords.line), ch: 0});
                    }
                    return;
                }
                
                // Check if click is before content starts (negative column) on any line
                const coords = cm.coordsChar({left: event.clientX, top: event.clientY});
                if (coords) {
                    if (coords.line < 0) {
                        event.preventDefault();
                        cm.setCursor({line: 0, ch: 0});
                        return;
                    } else if (coords.ch < 0) {
                        // Click was in gutter area, move to start of line
                        event.preventDefault();
                        cm.setCursor({line: coords.line, ch: 0});
                        return;
                    }
                }
                
                // Double-check after a short delay to catch any edge cases
                setTimeout(() => {
                    const cursor = cm.getCursor();
                    if (cursor.ch < 0) {
                        // Cursor somehow ended up in gutter, move to start of line
                        cm.setCursor({line: cursor.line, ch: 0});
                    }
                }, 10);
            });
            
            // Prevent key navigation before line 1 or in gutter area on ANY line
            this.sqlEditor.on('keydown', (cm, event) => {
                const cursor = cm.getCursor();
                if (cursor.line < 0) {
                    event.preventDefault();
                    cm.setCursor({line: 0, ch: 0});
                    return;
                }
                // Prevent cursor from being in gutter (negative column) on ANY line
                if (cursor.ch < 0) {
                    event.preventDefault();
                    cm.setCursor({line: cursor.line, ch: 0});
                    return;
                }
                // Prevent arrow up, home, page up from going below line 0
                if (cursor.line === 0) {
                    if (['ArrowUp', 'Home'].includes(event.key)) {
                        event.preventDefault();
                        cm.setCursor({line: 0, ch: 0});
                        return;
                    }
                }
                // Prevent arrow left from going into gutter on any line
                if (event.key === 'ArrowLeft' && cursor.ch === 0) {
                    // If at start of line, prevent going into gutter
                    // Allow moving to previous line if not on line 0
                    if (cursor.line > 0) {
                        // Will move to end of previous line, which is fine
                        return;
                    } else {
                        // On line 0, prevent any left movement
                        event.preventDefault();
                        return;
                    }
                }
                // Prevent typing when cursor might be in gutter (double-check after keydown)
                setTimeout(() => {
                    const newCursor = cm.getCursor();
                    if (newCursor.ch < 0) {
                        cm.setCursor({line: newCursor.line, ch: 0});
                    }
                }, 0);
            });
            
            // Prevent paste operations that would insert before line 0 or in gutter
            this.sqlEditor.on('paste', (cm, event) => {
                setTimeout(() => {
                    const cursor = cm.getCursor();
                    if (cursor.line < 0) {
                        cm.setCursor({line: 0, ch: 0});
                    } else if (cursor.ch < 0) {
                        // Cursor is in gutter area, move to start of line
                        cm.setCursor({line: cursor.line, ch: 0});
                    }
                }, 0);
            });
            
            // Additional protection: prevent input when cursor might be in gutter
            this.sqlEditor.on('inputRead', (cm) => {
                const cursor = cm.getCursor();
                if (cursor.ch < 0) {
                    // Cursor is in gutter, move to start of line
                    cm.setCursor({line: cursor.line, ch: 0});
                }
            });
            
            // Additional safety: check on focus and after any operation
            this.sqlEditor.on('focus', checkCursor);
            
            // After any change, ensure no content exists before line 0 and cursor is not in gutter
            this.sqlEditor.on('change', (cm) => {
                checkCursor();
                // Get all lines
                const lineCount = cm.lineCount();
                const doc = cm.getDoc();
                
                // Check if there's any content before line 0 (shouldn't happen, but be safe)
                // CodeMirror uses 0-based indexing, so line 0 is the first line
                // If somehow content exists before line 0, remove it
                if (lineCount > 0) {
                    // Ensure cursor is at least at line 0 and column 0 (not in gutter)
                    const cursor = cm.getCursor();
                    if (cursor.line < 0) {
                        cm.setCursor({line: 0, ch: 0});
                    } else if (cursor.ch < 0) {
                        // Cursor somehow got into gutter area, move to start of line
                        cm.setCursor({line: cursor.line, ch: 0});
                    }
                }
            });
            
            // Ensure editor starts at line 0, column 0 (not in gutter)
            this.sqlEditor.setCursor({line: 0, ch: 0});
            
            // Prevent any programmatic access that might create content before line 0
            const originalSetValue = this.sqlEditor.setValue.bind(this.sqlEditor);
            this.sqlEditor.setValue = (value) => {
                // Ensure value doesn't start with newlines that would create content before line 0
                const cleanedValue = value ? value.replace(/^\n+/, '') : value;
                originalSetValue(cleanedValue);
                // Reset cursor to line 0, column 0 (not in gutter)
                setTimeout(() => {
                    const cursor = this.sqlEditor.getCursor();
                    this.sqlEditor.setCursor({line: Math.max(0, cursor.line), ch: Math.max(0, cursor.ch)});
                }, 0);
            };
            
            // Additional protection: monitor cursor position continuously to prevent gutter typing on ANY line
            let lastCursorCheck = {line: 0, ch: 0};
            setInterval(() => {
                const cursor = this.sqlEditor.getCursor();
                if (cursor.line < 0) {
                    // Cursor is before line 0
                    this.sqlEditor.setCursor({line: 0, ch: 0});
                } else if (cursor.ch < 0) {
                    // Cursor is in gutter area on ANY line - move to start of that line
                    this.sqlEditor.setCursor({line: cursor.line, ch: 0});
                } else {
                    lastCursorCheck = cursor;
                }
            }, 50); // Check every 50ms for more responsive protection
        }
        
        // Load available datasets
        this.loadDatasets();
        
        // Load example queries
        this.loadExampleQueries();
    },
    
    async loadDatasets() {
        try {
            console.log('Loading datasets from:', `${QUERY_API_BASE}/query/datasets`);
            const response = await fetch(`${QUERY_API_BASE}/query/datasets`);
            
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            
            const data = await response.json();
            console.log('Datasets loaded:', data);
            
            const selector = document.getElementById('datasetSelector');
            if (selector) {
                selector.innerHTML = '<option value="">Select dataset...</option>';
                
                if (data.tables && data.tables.length > 0) {
                    data.tables.forEach(table => {
                        const option = document.createElement('option');
                        option.value = table.name;
                        option.textContent = `${table.name} (${table.row_count} rows)`;
                        selector.appendChild(option);
                    });
                    console.log(`Loaded ${data.tables.length} datasets into dropdown`);
                } else {
                    console.warn('No tables found in response');
                    const option = document.createElement('option');
                    option.value = '';
                    option.textContent = 'No datasets available - upload data first';
                    option.disabled = true;
                    selector.appendChild(option);
                }
            } else {
                console.error('Dataset selector element not found');
            }
        } catch (error) {
            console.error('Error loading datasets:', error);
            const selector = document.getElementById('datasetSelector');
            if (selector) {
                selector.innerHTML = '<option value="">Error loading datasets</option>';
            }
        }
    },
    
    loadExampleQueries() {
        const examples = [
            {
                name: 'Simple SELECT',
                query: 'SELECT *\nFROM employees\nWHERE department_id = 1'
            },
            {
                name: 'JOIN Query',
                query: 'SELECT e.name, d.department_name\nFROM employees e\nINNER JOIN departments d\nON e.department_id = d.id\nWHERE e.salary > 50000'
            },
            {
                name: 'GROUP BY Query',
                query: 'SELECT department_id, COUNT(*) as emp_count\nFROM employees\nGROUP BY department_id\nHAVING COUNT(*) > 5'
            }
        ];
        
        const selector = document.getElementById('exampleQuerySelector');
        if (selector) {
            examples.forEach((example, idx) => {
                const option = document.createElement('option');
                option.value = idx;
                option.textContent = example.name;
                selector.dataset.query = example.query;
                selector.appendChild(option);
            });
            
            selector.addEventListener('change', (e) => {
                const selected = e.target.options[e.target.selectedIndex];
                if (selected.value && selected.dataset.query) {
                    if (this.sqlEditor) {
                        this.sqlEditor.setValue(selected.dataset.query);
                    } else {
                        document.getElementById('sqlEditor').value = selected.dataset.query;
                    }
                }
            });
        }
    },
    
    async compileQuery() {
        const queryText = this.sqlEditor ? this.sqlEditor.getValue() : document.getElementById('sqlEditor').value;
        
        if (!queryText.trim()) {
            alert('Please enter a SQL query');
            return;
        }
        
        // Require semicolon at the end of the query
        const trimmedQuery = queryText.trim();
        if (!trimmedQuery.endsWith(';')) {
            alert('SQL query must end with a semicolon (;). Please add a semicolon at the end of your query.');
            return;
        }
        
        try {
            const response = await fetch(`${QUERY_API_BASE}/query/compile`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    query: queryText
                })
            });
            
            const result = await response.json();
            
            if (!response.ok) {
                throw new Error(result.detail || 'Failed to compile query');
            }
            
            this.compiledQuery = result;
            this.currentQueryId = result.query_id;
            this.totalLines = result.line_count;
            
            // Set up sub-steps if available
            if (result.sub_steps && result.sub_steps.length > 0) {
                this.subSteps = result.sub_steps;
                this.totalSteps = result.total_sub_steps || result.sub_steps.length;
            } else {
                this.subSteps = [];
                this.totalSteps = result.total_steps || result.line_count;
            }
            
            this.currentLineIndex = 0;
            this.currentStepIndex = 0;  // Track granular step index
            this.visualStates = {};
            
            // Update line info
            this.updateLineInfo();
            
            // Load initial state (use sub_step_index if available)
            if (this.subSteps && this.subSteps.length > 0) {
                await this.loadVisualState(0, 0);  // Pass sub_step_index 0
            } else {
                await this.loadVisualState(0);
            }
            
            // Highlight active line in editor
            this.highlightActiveLine(0);
            
        } catch (error) {
            console.error('Error compiling query:', error);
            alert(`Error: ${error.message}`);
        }
    },
    
    async loadVisualState(lineIndex, subStepIndex = null) {
        if (!this.currentQueryId) return;
        
        // Use combined key for cache if sub_step_index is provided
        const stateKey = subStepIndex !== null ? `${lineIndex}-${subStepIndex}` : lineIndex;
        
        // Check cache first
        if (this.visualStates[stateKey]) {
            this.renderVisualState(this.visualStates[stateKey], lineIndex, subStepIndex);
            return;
        }
        
        try {
            const payload = {
                query_id: this.currentQueryId,
                line_index: lineIndex
            };
            
            // Add sub_step_index if provided
            if (subStepIndex !== null) {
                payload.sub_step_index = subStepIndex;
            }
            
            const response = await fetch(`${QUERY_API_BASE}/query/state`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(payload)
            });
            
            if (!response.ok) {
                let errorText;
                try {
                    errorText = await response.text();
                    // Try to parse as JSON for better error messages
                    try {
                        const errorJson = JSON.parse(errorText);
                        errorText = errorJson.detail || errorJson.message || errorText;
                    } catch {
                        // Not JSON, use as-is (might be HTML error page)
                        if (errorText.length > 200) {
                            errorText = errorText.substring(0, 200) + '...';
                        }
                    }
                } catch {
                    errorText = `Server error ${response.status}`;
                }
                console.error('Server error:', response.status, errorText);
                throw new Error(`Server error (${response.status}): ${errorText}`);
            }
            
            const state = await response.json();
            
            // Debug logging
            console.log('Visual state received:', {
                lineIndex,
                explanation_text: state.explanation_text,
                step_type: state.step_type,
                has_output: !!state.output_table,
                output_rows: state.output_table?.row_count || 0
            });
            
            // Cache the state
            this.visualStates[stateKey] = state;
            
            // Render
            this.renderVisualState(state, lineIndex, subStepIndex);
            
        } catch (error) {
            console.error('Error loading visual state:', error);
        }
    },
    
    renderVisualState(state, lineIndex) {
        const canvas = document.getElementById('transformationCanvas');
        const inspector = document.getElementById('inspectorContent');
        
        if (!canvas || !inspector) return;
        
        // Render tables
        let canvasHTML = '';
        
        // For JOIN steps, show input tables side-by-side
        if (state.step_type === 'JOIN' && state.input_tables && state.input_tables.length >= 2) {
            canvasHTML += '<div class="join-tables-container">';
            state.input_tables.forEach((table, idx) => {
                const title = idx === 0 ? 'Left Table' : 'Right Table';
                canvasHTML += `<div class="join-input-table">${this.renderTable(table, `input-${idx}`, title, state.highlighted_cols)}</div>`;
            });
            canvasHTML += '</div>';
            
            // Show join result below
            if (state.output_table) {
                canvasHTML += '<div class="join-arrow">↓ JOIN RESULT ↓</div>';
                canvasHTML += this.renderTable(state.output_table, 'output', 'Joined Result', state.highlighted_cols, state.dimmed_rows);
            }
        } else {
            // For other steps, show input tables normally
            if (state.input_tables && state.input_tables.length > 0) {
                state.input_tables.forEach((table, idx) => {
                    canvasHTML += this.renderTable(table, `input-${idx}`, `Input Table ${idx + 1}`, state.highlighted_cols);
                });
            }
            
            // Render output table - always show result if available
            if (state.output_table) {
                const outputTitle = state.step_type === 'SELECT' || state.step_type === 'SELECT_COL' ? 'Final Result' : 
                                   state.step_type === 'WHERE' ? 'Filtered Result' :
                                   state.step_type === 'FROM' ? 'Loaded Table' :
                                   'Query Result';
                canvasHTML += this.renderTable(state.output_table, 'output', outputTitle, state.highlighted_cols, state.dimmed_rows);
            } else if (state.input_tables && state.input_tables.length > 0) {
                // For SELECT_COL steps, we should always have an output_table, but if not, show input as fallback
                // For other steps, show first input table as output if no output yet
                const fallbackTitle = state.step_type === 'SELECT_COL' ? 'Current State (before projection)' : 'Current State';
                canvasHTML += this.renderTable(state.input_tables[0], 'output', fallbackTitle, state.highlighted_cols, state.dimmed_rows);
            } else {
                // Only show placeholder if truly no data
                canvasHTML = '<div class="canvas-placeholder"><p>No data to display</p></div>';
            }
        }
        
        canvas.innerHTML = canvasHTML;
        
        // Render inspector
        let inspectorHTML = '';
        
        // Always show explanation - use state or fallback
        const explanation = state.explanation_text || 'Processing query step...';
        inspectorHTML += `
            <div class="inspector-section">
                <h4>What this line does:</h4>
                <p class="explanation">${explanation}</p>
            </div>
        `;
        
        if (state.step_type) {
            inspectorHTML += `
                <div class="inspector-section">
                    <h4>Step Type:</h4>
                    <span class="step-badge step-${state.step_type.toLowerCase()}">${state.step_type}</span>
                </div>
            `;
        }
        
        if (state.highlighted_cols && state.highlighted_cols.length > 0) {
            inspectorHTML += `
                <div class="inspector-section">
                    <h4>Highlighted Columns:</h4>
                    <div class="column-tags">
                        ${state.highlighted_cols.map(col => `<span class="column-tag">${col}</span>`).join('')}
                    </div>
                </div>
            `;
        }
        
        if (state.join_condition) {
            inspectorHTML += `
                <div class="inspector-section">
                    <h4>Join Condition:</h4>
                    <p>${state.join_condition.condition || 'N/A'}</p>
                    ${state.join_condition.columns ? `
                        <div class="column-tags">
                            ${state.join_condition.columns.map(col => `<span class="column-tag">${col}</span>`).join('')}
                        </div>
                    ` : ''}
                </div>
            `;
        }
        
        if (state.before_row_count !== undefined || state.after_row_count !== undefined) {
            inspectorHTML += `
                <div class="inspector-section">
                    <h4>Row Count:</h4>
                    <p>Before: ${state.before_row_count} → After: ${state.after_row_count}</p>
                </div>
            `;
        }
        
        inspector.innerHTML = inspectorHTML || '<p class="inspector-placeholder">No details available</p>';
    },
    
    renderTable(table, id, title, highlightedCols = [], dimmedRows = []) {
        if (!table || !table.data || table.data.length === 0) {
            return `<div class="table-container" id="${id}">
                <h4>${title}</h4>
                <p class="empty-table">No data</p>
            </div>`;
        }
        
        const columns = table.columns || Object.keys(table.data[0]);
        const rows = table.data.slice(0, 50); // Limit to 50 rows for display
        
        let html = `<div class="table-container" id="${id}">
            <h4>${title} <span class="row-count">(${table.row_count} rows${table.row_count > 50 ? ', showing first 50' : ''})</span></h4>
            <div class="table-wrapper">
                <table class="data-table">
                    <thead>
                        <tr>
                            ${columns.map(col => {
                                const isHighlighted = highlightedCols.includes(col);
                                return `<th class="${isHighlighted ? 'highlighted-col' : ''}">${col}</th>`;
                            }).join('')}
                        </tr>
                    </thead>
                    <tbody>
                        ${rows.map((row, rowIdx) => {
                            const isDimmed = dimmedRows.includes(rowIdx);
                            return `<tr class="${isDimmed ? 'dimmed-row' : ''}">
                                ${columns.map(col => {
                                    const isHighlighted = highlightedCols.includes(col);
                                    const value = row[col] !== undefined && row[col] !== null ? row[col] : 'NULL';
                                    return `<td class="${isHighlighted ? 'highlighted-col' : ''}">${value}</td>`;
                                }).join('')}
                            </tr>`;
                        }).join('')}
                    </tbody>
                </table>
            </div>
        </div>`;
        
        return html;
    },
    
    highlightActiveLine(lineIndex) {
        if (!this.sqlEditor) return;
        
        // Remove previous highlights
        this.sqlEditor.removeLineClass(this.currentLineIndex, 'background', 'active-line');
        
        // Add new highlight
        this.sqlEditor.addLineClass(lineIndex, 'background', 'active-line');
        this.sqlEditor.setCursor(lineIndex, 0);
    },
    
    updateLineInfo() {
        const infoElement = document.getElementById('currentLineInfo');
        if (infoElement) {
            if (this.subSteps && this.subSteps.length > 0) {
                infoElement.textContent = `Step ${this.currentStepIndex + 1} / ${this.totalSteps}`;
            } else {
                infoElement.textContent = `Line ${this.currentLineIndex + 1} / ${this.totalLines}`;
            }
        }
    },
    
    onLineChange() {
        if (!this.sqlEditor || !this.compiledQuery) return;
        
        const cursor = this.sqlEditor.getCursor();
        const lineIndex = cursor.line;
        
        if (lineIndex !== this.currentLineIndex && lineIndex >= 0 && lineIndex < this.totalLines) {
            this.currentLineIndex = lineIndex;
            this.updateLineInfo();
            this.loadVisualState(lineIndex);
            this.highlightActiveLine(lineIndex);
        }
    },
    
    async prevLine() {
        // Use granular stepping if available
        if (this.subSteps && this.subSteps.length > 0) {
            if (this.currentStepIndex > 0) {
                this.currentStepIndex--;
                // Update line index based on current step
                const subStep = this.subSteps[this.currentStepIndex];
                const stepIndex = subStep.step_index;
                const step = this.compiledQuery.steps[stepIndex];
                if (step && step.line_range) {
                    this.currentLineIndex = step.line_range[0];
                }
                this.updateLineInfo();
                await this.loadVisualState(this.currentLineIndex, this.currentStepIndex);
                this.highlightActiveLine(this.currentLineIndex);
            }
        } else {
            if (this.currentLineIndex > 0) {
                this.currentLineIndex--;
                this.updateLineInfo();
                await this.loadVisualState(this.currentLineIndex);
                this.highlightActiveLine(this.currentLineIndex);
            }
        }
    },
    
    async nextLine() {
        // Use granular stepping if available
        if (this.subSteps && this.subSteps.length > 0) {
            if (this.currentStepIndex < this.totalSteps - 1) {
                this.currentStepIndex++;
                // Update line index based on current step
                const subStep = this.subSteps[this.currentStepIndex];
                const stepIndex = subStep.step_index;
                const step = this.compiledQuery.steps[stepIndex];
                if (step && step.line_range) {
                    this.currentLineIndex = step.line_range[0];
                }
                this.updateLineInfo();
                await this.loadVisualState(this.currentLineIndex, this.currentStepIndex);
                this.highlightActiveLine(this.currentLineIndex);
            }
        } else {
            if (this.currentLineIndex < this.totalLines - 1) {
                this.currentLineIndex++;
                this.updateLineInfo();
                await this.loadVisualState(this.currentLineIndex);
                this.highlightActiveLine(this.currentLineIndex);
            }
        }
    },
    
    play() {
        if (this.isPlaying) return;
        
        this.isPlaying = true;
        document.getElementById('playBtn').style.display = 'none';
        document.getElementById('pauseBtn').style.display = 'inline-block';
        
        // Much slower playback - 2-3 seconds per step for word-by-word visualization
        const baseDelay = 2500; // 2.5 seconds base delay
        const speedMultiplier = 1 / this.playbackSpeed; // Slower when speed is lower
        const delay = baseDelay * speedMultiplier;
        
        this.playInterval = setInterval(() => {
            if (this.subSteps && this.subSteps.length > 0) {
                if (this.currentStepIndex < this.totalSteps - 1) {
                    this.nextLine();
                } else {
                    this.pause();
                }
            } else {
                if (this.currentLineIndex < this.totalLines - 1) {
                    this.nextLine();
                } else {
                    this.pause();
                }
            }
        }, delay);
    },
    
    pause() {
        this.isPlaying = false;
        document.getElementById('playBtn').style.display = 'inline-block';
        document.getElementById('pauseBtn').style.display = 'none';
        
        if (this.playInterval) {
            clearInterval(this.playInterval);
            this.playInterval = null;
        }
    },
    
    reset() {
        this.pause();
        this.currentLineIndex = 0;
        this.currentStepIndex = 0;
        this.updateLineInfo();
        if (this.subSteps && this.subSteps.length > 0) {
            this.loadVisualState(0, 0);
        } else {
            this.loadVisualState(0);
        }
        this.highlightActiveLine(0);
    },
    
    setSpeed(value) {
        this.playbackSpeed = parseFloat(value);
        const speedValue = document.getElementById('speedValue');
        if (speedValue) {
            speedValue.textContent = `${value}x`;
        }
        
        // Restart playback with new speed if playing
        if (this.isPlaying) {
            this.pause();
            this.play();
        }
    }
};

// switchView is now defined at the top of the file

// Initialize on page load
document.addEventListener('DOMContentLoaded', () => {
    // Add event listeners to tabs as backup
    const graphTab = document.getElementById('graphViewTab');
    const schemaTab = document.getElementById('schemaViewTab');
    const queryTab = document.getElementById('queryViewTab');
    
    if (graphTab) {
        graphTab.addEventListener('click', (e) => {
            e.preventDefault();
            window.switchView('graph');
        });
        graphTab.onclick = function(e) {
            e.preventDefault();
            e.stopPropagation();
            console.log('Graph tab clicked via onclick');
            window.switchView('graph');
        };
    }
    
    if (schemaTab) {
        schemaTab.addEventListener('click', (e) => {
            e.preventDefault();
            window.switchView('schema');
        });
        schemaTab.onclick = function(e) {
            e.preventDefault();
            e.stopPropagation();
            console.log('Schema tab clicked via onclick');
            window.switchView('schema');
        };
    }
    
    if (queryTab) {
        queryTab.addEventListener('click', (e) => {
            e.preventDefault();
            e.stopPropagation();
            console.log('Query tab clicked via event listener');
            window.switchView('query');
        });
        queryTab.onclick = function(e) {
            e.preventDefault();
            e.stopPropagation();
            console.log('Query tab clicked via onclick');
            window.switchView('query');
        };
    }
    
    // Query visualizer will be initialized when switching to query view
    // But we can also initialize the editor immediately if it exists
    const editorElement = document.getElementById('sqlEditor');
    if (editorElement) {
        // Delay initialization slightly to ensure DOM is ready
        setTimeout(() => {
            if (window.queryVisualizer && !window.queryVisualizer.sqlEditor) {
                window.queryVisualizer.init();
            }
        }, 100);
    }
    
    // Test if switchView is accessible
    console.log('switchView function available:', typeof window.switchView);
    console.log('queryVisualizer available:', typeof window.queryVisualizer);
});


