/**
 * GraphMind Frontend
 * D3.js interactive graph visualization
 */

const API_BASE = 'http://localhost:8000/api';
let graphData = { nodes: [], edges: [] };
let schemaData = { tables: [], relationships: [] };
let simulation = null;
let selectedNode = null;
let selectedEdge = null;
let impactViewActive = false;
let originalGraphData = null;

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    initializeGraph();
    initializeSchema();
    setupEventListeners();
    loadGraph();
    loadSchema();
});

function initializeGraph() {
    const svg = d3.select('#graphSvg');
    const width = window.innerWidth - 300;
    const height = window.innerHeight;

    svg.attr('width', width).attr('height', height);

    // Create zoom behavior
    const zoom = d3.zoom()
        .scaleExtent([0.1, 4])
        .on('zoom', (event) => {
            container.attr('transform', event.transform);
        });

    svg.call(zoom);

    // Create container for zoomable content
    const container = svg.append('g').attr('class', 'container');

    // Create arrow marker definitions
    const defs = svg.append('defs');
    
    // Arrow for FK edges - thinner arrowhead
    defs.append('marker')
        .attr('id', 'arrow-fk')
        .attr('viewBox', '0 -3 8 6')
        .attr('refX', 6)
        .attr('refY', 0)
        .attr('markerWidth', 8)
        .attr('markerHeight', 8)
        .attr('orient', 'auto')
        .append('path')
        .attr('d', 'M0,-3L8,0L0,3')
        .attr('fill', '#3498db')
        .attr('stroke', 'none');

    // Arrow for inferred edges - thinner arrowhead
    defs.append('marker')
        .attr('id', 'arrow-inferred')
        .attr('viewBox', '0 -3 8 6')
        .attr('refX', 6)
        .attr('refY', 0)
        .attr('markerWidth', 8)
        .attr('markerHeight', 8)
        .attr('orient', 'auto')
        .append('path')
        .attr('d', 'M0,-3L8,0L0,3')
        .attr('fill', '#e74c3c')
        .attr('stroke', 'none');

    // Store container reference globally
    window.graphContainer = container;
    window.graphSvg = svg;
}

function setupEventListeners() {
    // File uploads
    document.getElementById('sqlFile').addEventListener('change', handleSQLUpload);
    document.getElementById('csvFile').addEventListener('change', handleCSVUpload);

    // Drag and drop
    const sqlArea = document.getElementById('sqlUploadArea');
    const csvArea = document.getElementById('csvUploadArea');

    ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
        sqlArea.addEventListener(eventName, preventDefaults, false);
        csvArea.addEventListener(eventName, preventDefaults, false);
    });

    ['dragenter', 'dragover'].forEach(eventName => {
        sqlArea.addEventListener(eventName, () => sqlArea.classList.add('dragover'), false);
        csvArea.addEventListener(eventName, () => csvArea.classList.add('dragover'), false);
    });

    ['dragleave', 'drop'].forEach(eventName => {
        sqlArea.addEventListener(eventName, () => sqlArea.classList.remove('dragover'), false);
        csvArea.addEventListener(eventName, () => csvArea.classList.remove('dragover'), false);
    });

    sqlArea.addEventListener('drop', (e) => handleDrop(e, 'sql'), false);
    csvArea.addEventListener('drop', (e) => handleDrop(e, 'csv'), false);

    // Controls
    document.getElementById('showFK').addEventListener('change', () => renderGraph());
    document.getElementById('showInferred').addEventListener('change', () => renderGraph());
    const schemaShowFK = document.getElementById('schemaShowFK');
    const schemaShowInferred = document.getElementById('schemaShowInferred');
    if (schemaShowFK) {
        schemaShowFK.addEventListener('change', () => renderSchema());
    }
    if (schemaShowInferred) {
        schemaShowInferred.addEventListener('change', () => renderSchema());
    }
    const schemaReset = document.getElementById('schemaResetZoomBtn');
    if (schemaReset) {
        schemaReset.addEventListener('click', resetSchemaZoom);
    }
    
    // Load critical tables on initial load
    loadCriticalTables();

    // Search
    document.getElementById('searchBox').addEventListener('input', (e) => {
        searchTables(e.target.value);
    });
}

function preventDefaults(e) {
    e.preventDefault();
    e.stopPropagation();
}

function handleDrop(e, type) {
    const dt = e.dataTransfer;
    const files = dt.files;
    if (files.length > 0) {
        if (type === 'sql') {
            uploadSQLFile(files[0]);
        } else {
            uploadCSVFile(files[0]);
        }
    }
}

async function handleSQLUpload(e) {
    const file = e.target.files[0];
    if (file) {
        await uploadSQLFile(file);
    }
}

async function handleCSVUpload(e) {
    const file = e.target.files[0];
    if (file) {
        await uploadCSVFile(file);
    }
}

async function uploadSQLFile(file) {
    const formData = new FormData();
    formData.append('file', file);

    showStatus('Uploading SQL file...', 'success');

    try {
        const response = await fetch(`${API_BASE}/upload/sql`, {
            method: 'POST',
            body: formData
        });

        const result = await response.json();
        if (response.ok) {
            showStatus(`Successfully parsed ${result.tables_parsed} tables`, 'success');
            loadGraph();
            loadSchema();
            // Reload datasets in query visualizer if it exists
            if (window.queryVisualizer && typeof window.queryVisualizer.loadDatasets === 'function') {
                window.queryVisualizer.loadDatasets();
            }
        } else {
            showStatus(`Error: ${result.detail}`, 'error');
        }
    } catch (error) {
        showStatus(`Error: ${error.message}`, 'error');
    }
}

async function uploadCSVFile(file) {
    const formData = new FormData();
    formData.append('file', file);

    showStatus('Uploading CSV file...', 'success');

    try {
        const response = await fetch(`${API_BASE}/upload/csv`, {
            method: 'POST',
            body: formData
        });

        const result = await response.json();
        if (response.ok) {
            showStatus(
                `Successfully uploaded ${result.table_name}. Inferred ${result.inferred_relationships} relationships.`,
                'success'
            );
            loadGraph();
            loadSchema();
            // Reload datasets in query visualizer if it exists
            if (window.queryVisualizer && typeof window.queryVisualizer.loadDatasets === 'function') {
                window.queryVisualizer.loadDatasets();
            }
        } else {
            showStatus(`Error: ${result.detail}`, 'error');
        }
    } catch (error) {
        showStatus(`Error: ${error.message}`, 'error');
    }
}

async function loadGraph() {
    try {
        const response = await fetch(`${API_BASE}/graph`);
        graphData = await response.json();
        updateStats();
        renderGraph();
        
        // Load critical tables if toggle is enabled
        if (document.getElementById('showCritical')?.checked) {
            loadCriticalTables();
        }
    } catch (error) {
        console.error('Error loading graph:', error);
        showStatus(`Error loading graph: ${error.message}`, 'error');
    }
}

async function loadSchema() {
    try {
        const response = await fetch(`${API_BASE}/schema`);
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
        }
        schemaData = await response.json();
        renderSchema();
    } catch (error) {
        console.error('Error loading schema:', error);
    }
}

function renderGraph() {
    const container = window.graphContainer;
    if (!container) return;

    // Stop any existing simulation
    if (simulation) {
        simulation.stop();
        simulation = null;
    }

    // Clear existing graph
    container.selectAll('*').remove();

    const showFK = document.getElementById('showFK').checked;
    const showInferred = document.getElementById('showInferred').checked;

    // Filter edges based on visibility settings
    const visibleEdges = graphData.edges.filter(edge => {
        if (edge.kind === 'fk' && !showFK) return false;
        if (edge.kind === 'inferred' && !showInferred) return false;
        return true;
    });

    // Calculate dynamic radius for each node based on text length
    graphData.nodes.forEach(d => {
        const textLength = d.id.length;
        // Base radius of 30, add 3 pixels per character, max 70
        d.radius = Math.min(30 + (textLength * 3), 70);
    });

    // Calculate max radius for collision detection
    const maxRadius = Math.max(...graphData.nodes.map(d => d.radius), 40);

    // Ensure edges reference node objects, not strings
    const nodeMap = new Map(graphData.nodes.map(d => [d.id, d]));
    visibleEdges.forEach(edge => {
        edge.source = typeof edge.source === 'string' ? nodeMap.get(edge.source) : edge.source;
        edge.target = typeof edge.target === 'string' ? nodeMap.get(edge.target) : edge.target;
    });

    // Filter out edges with missing source or target
    const validEdges = visibleEdges.filter(edge => edge.source && edge.target);

    // Create force simulation with expanded layout and crossing minimization
    // Use stronger repulsion and longer distances to reduce edge crossings
    simulation = d3.forceSimulation(graphData.nodes)
        .force('link', d3.forceLink(validEdges).id(d => d.id).distance(300).strength(0.15))
        .force('charge', d3.forceManyBody().strength(-800))
        .force('center', d3.forceCenter(window.innerWidth / 2 - 150, window.innerHeight / 2).strength(0.03))
        .force('collision', d3.forceCollide().radius(d => d.radius + 50))
        .alphaDecay(0.06)  // Slower decay = more time to find optimal positions
        .velocityDecay(0.8)  // Higher velocity decay = more stable
        .alpha(0.6);  // Higher initial alpha = more energy to spread out and minimize crossings

    // Create links as curved paths to minimize visual crossings
    const link = container.append('g')
        .attr('class', 'links')
        .selectAll('path')
        .data(validEdges)
        .enter()
        .append('path')
        .attr('class', d => `link ${d.kind}`)
        .attr('marker-end', d => d.kind === 'fk' ? 'url(#arrow-fk)' : 'url(#arrow-inferred)')
        .attr('fill', 'none')
        .on('click', (event, d) => {
            event.stopPropagation();
            selectEdge(d);
        })
        .on('mouseover', (event, d) => {
            showTooltip(event, getEdgeTooltip(d));
        })
        .on('mouseout', hideTooltip);

    // Create nodes
    const node = container.append('g')
        .attr('class', 'nodes')
        .selectAll('g')
        .data(graphData.nodes)
        .enter()
        .append('g')
        .attr('class', d => `node ${d.source}`)
        .call(d3.drag()
            .on('start', dragstarted)
            .on('drag', dragged)
            .on('end', dragended))
        .on('click', (event, d) => {
            event.stopPropagation();
            selectNode(d);
        })
        .on('mouseover', (event, d) => {
            showTooltip(event, getNodeTooltip(d));
        })
        .on('mouseout', hideTooltip);

    node.append('circle')
        .attr('r', d => d.radius)
        .attr('fill', d => d.source === 'sql' ? '#3498db' : '#e74c3c');

    node.append('text')
        .text(d => d.id)
        .attr('dy', 4)
        .attr('font-size', '12px')
        .attr('text-anchor', 'middle')
        .attr('fill', 'white')
        .attr('font-weight', '500');

    // Update positions on simulation tick
    simulation.on('tick', () => {
        link.attr('d', d => {
            // Reverse edge direction for consistent parent->child visualization
            // In the graph data: edge goes from dependent table to referenced table (e.g., works_on -> employee)
            // For visualization: we show parent -> child (e.g., employee -> works_on)
            // So we swap source and target for rendering
            const visualSource = d.target;  // The referenced table (parent)
            const visualTarget = d.source;  // The dependent table (child)
            
            // Calculate edge start and end points on circle perimeters
            const dx = visualTarget.x - visualSource.x;
            const dy = visualTarget.y - visualSource.y;
            const sourceRadius = visualSource.radius || 30;
            const targetRadius = visualTarget.radius || 30;
            const len = Math.sqrt(dx * dx + dy * dy);
            
            if (len === 0) return '';
            
            // Start point on source circle (parent)
            const x1 = visualSource.x + (dx / len) * sourceRadius;
            const y1 = visualSource.y + (dy / len) * sourceRadius;
            
            // End point on target circle (child)
            const x2 = visualTarget.x - (dx / len) * targetRadius;
            const y2 = visualTarget.y - (dy / len) * targetRadius;
            
            // Calculate control point for quadratic curve
            // Offset perpendicular to the line to create a smooth curve
            const offset = Math.min(len * 0.3, 50); // Curve offset
            const perpX = -dy / len * offset;
            const perpY = dx / len * offset;
            
            // Control point (midpoint with perpendicular offset)
            const midX = (x1 + x2) / 2 + perpX;
            const midY = (y1 + y2) / 2 + perpY;
            
            // Create quadratic Bezier curve path (parent -> child)
            return `M ${x1} ${y1} Q ${midX} ${midY} ${x2} ${y2}`;
        });

        node
            .attr('transform', d => `translate(${d.x},${d.y})`);
    });
}

function dragstarted(event, d) {
    if (!event.active) simulation.alphaTarget(0.1).restart();
    d.fx = d.x;
    d.fy = d.y;
}

function dragged(event, d) {
    d.fx = event.x;
    d.fy = event.y;
}

function dragended(event, d) {
    if (!event.active) simulation.alphaTarget(0);
    d.fx = null;
    d.fy = null;
}

function selectNode(node) {
    // Ensure node is an object with id property
    const nodeObj = typeof node === 'string' 
        ? graphData.nodes.find(n => n.id === node) || { id: node }
        : node;
    
    selectedNode = nodeObj;
    selectedEdge = null;

    // Clear all visualizations (but keep impact view active)
    if (!impactViewActive) {
        clearAllHighlights();
    }

    // Update visual selection
    window.graphContainer.selectAll('.node').classed('selected', false);
    window.graphContainer.selectAll('.link').classed('selected', false);
    
    window.graphContainer.selectAll('.node')
        .filter(d => d.id === nodeObj.id)
        .classed('selected', true);

    // Show details panel
    showNodeDetails(nodeObj);
}

function clearAllHighlights() {
    // Clear impact highlighting
    window.graphContainer.selectAll('.node')
        .classed('impacted', false)
        .classed('impact-source', false);
    
    // Clear path highlighting
    window.graphContainer.selectAll('.node')
        .classed('path-node', false);
    window.graphContainer.selectAll('.link')
        .classed('path-link', false);
    
    // Note: We don't clear critical table highlighting as that's a global toggle
    // Critical highlighting is controlled by the sidebar checkbox
}

function selectEdge(edge) {
    selectedEdge = edge;
    selectedNode = null;

    // Clear all visualizations
    clearAllHighlights();

    // Update visual selection
    window.graphContainer.selectAll('.node').classed('selected', false);
    window.graphContainer.selectAll('.link').classed('selected', false);
    
    window.graphContainer.selectAll('.link')
        .filter(d => d === edge)
        .classed('selected', true);

    // Show details panel
    showEdgeDetails(edge);
}

async function getMaxDepth(tableName) {
    // Get actual max depth by calling impact API with max depth
    try {
        const response = await fetch(`${API_BASE}/table/${tableName}/impact?depth=10`);
        const impact = await response.json();
        if (impact.paths && impact.paths.length > 0) {
            const maxHops = Math.max(...impact.paths.map(p => p.hops || 0));
            return maxHops + 1; // hops + 1 = depth
        }
        return 1; // No relationships
    } catch (e) {
        console.error('Error getting max depth:', e);
        return 10; // Default fallback
    }
}

async function showNodeDetails(node) {
    try {
        const response = await fetch(`${API_BASE}/table/${node.id}`);
        const details = await response.json();
        
        // Get actual max depth for this table
        const actualMaxDepth = await getMaxDepth(node.id);
        
        // Get all tables for path finder dropdown
        let allTables = [];
        try {
            const graphResponse = await fetch(`${API_BASE}/graph`);
            const graphData = await graphResponse.json();
            allTables = graphData.nodes.map(n => n.id).filter(name => name !== details.name);
        } catch (e) {
            console.error('Error loading graph data:', e);
        }
        
        // Get table data (rows)
        let tableData = null;
        try {
            const dataResponse = await fetch(`${API_BASE}/table/${node.id}/data`);
            tableData = await dataResponse.json();
        } catch (e) {
            console.error('Error loading table data:', e);
        }
        
        // Get delete risk score
        let riskScore = null;
        try {
            const riskResponse = await fetch(`${API_BASE}/table/${node.id}/delete-risk`);
            riskScore = await riskResponse.json();
        } catch (e) {
            console.error('Error loading risk score:', e);
        }

        const panel = document.getElementById('detailsPanel');
        const content = document.getElementById('detailsContent');

        const riskBadge = riskScore && riskScore.risk_level !== 'none' 
            ? `<span class="risk-badge risk-${riskScore.risk_level}">${riskScore.risk_level.toUpperCase()} DELETE RISK</span>`
            : '';

        // Find primary key column (first column or column named 'id' or ends with '_id')
        const pkColumn = details.columns.find(col => 
            col.name.toLowerCase() === 'id' || 
            col.name.toLowerCase().endsWith('_id') ||
            col.name.toLowerCase().endsWith('id')
        ) || details.columns[0];

        // Add "View Full Graph" button at top if in impact view
        const impactViewButton = impactViewActive 
            ? `<div style="margin-bottom: 12px; padding-bottom: 12px; border-bottom: 1px solid #e0e0e0;">
                <button class="btn btn-secondary" onclick="restoreFullGraph()" style="width: 100%; font-size: 13px; padding: 8px;">View Full Graph</button>
               </div>`
            : '';

        content.innerHTML = `
            <div class="panel-header">
                <button class="close-btn" onclick="closeDetailsPanel()">×</button>
                ${impactViewButton}
                <h3>${details.name} ${riskBadge}</h3>
            </div>
            <div class="panel-content">
                <div class="info-card">
                    <div class="info-row">
                        <span class="info-label">Source:</span>
                        <span class="info-value">${details.source}</span>
                    </div>
                    ${riskScore ? `
                    <div class="info-row">
                        <span class="info-label">Risk Level:</span>
                        <span class="info-value risk-${riskScore.risk_level}">${riskScore.risk_level.toUpperCase()} (${riskScore.risk_score}/100)</span>
                    </div>
                    <div class="info-row">
                        <span class="info-label">Incoming FKs:</span>
                        <span class="info-value">${riskScore.incoming_fk_count} (RESTRICT: ${riskScore.restrict_count}, CASCADE: ${riskScore.cascade_count})</span>
                    </div>
                    ` : ''}
                </div>
            ${tableData && tableData.rows && tableData.rows.length > 0 ? `
            <div class="details-section" style="margin-top: 0;">
                <h4 style="margin-top: 0; margin-bottom: 16px;">Table Data (${tableData.rows.length} rows)</h4>
                <div class="data-table-container">
                    <table class="data-table">
                        <thead>
                            <tr>
                                <th><input type="checkbox" id="selectAll-${details.name}" onchange="toggleSelectAll('${details.name}')"></th>
                                ${tableData.columns.map(col => `<th>${col.name}</th>`).join('')}
                            </tr>
                        </thead>
                        <tbody>
                            ${tableData.rows.map((row, idx) => {
                                const rowId = row[pkColumn?.name] ?? idx;
                                return `<tr>
                                    <td><input type="checkbox" class="row-checkbox" data-table="${details.name}" data-row-id="${rowId}" data-row-index="${idx}"></td>
                                    ${tableData.columns.map(col => `<td>${row[col.name] ?? 'NULL'}</td>`).join('')}
                                </tr>`;
                            }).join('')}
                        </tbody>
                    </table>
                </div>
                <div class="simulation-controls" style="margin-top: 12px;">
                    <button class="btn btn-simulate" onclick="openDeleteSimulation('${details.name}')">Simulate DELETE</button>
                    <button class="btn btn-simulate" onclick="openUpdateSimulation('${details.name}')">Simulate UPDATE</button>
                </div>
                <div id="simulationResult-${details.name}" style="margin-top: 12px;"></div>
            </div>
            ` : ''}
            <div class="details-section">
                <h4>Impact Analysis</h4>
                <div class="impact-controls">
                    <div style="display: flex; align-items: center; gap: 10px;">
                        <button class="btn btn-impact" onclick="showDownstreamImpact('${details.name}')">Show Downstream Impact</button>
                        <label id="depthLabel-${details.name}" style="display: flex; align-items: center; font-size: 12px; color: #666; margin: 0; white-space: nowrap;">Depth (max ${actualMaxDepth}): <input type="number" id="impactDepth-${details.name}" value="${Math.min(3, actualMaxDepth)}" min="1" max="${actualMaxDepth}" style="width: 50px; margin-left: 5px; padding: 4px;"></label>
                    </div>
                </div>
                <div id="impactResult-${details.name}" style="margin-top: 12px;"></div>
            </div>
            <div class="details-section">
                <h4>Join Path Finder</h4>
                <div class="path-finder-controls">
                    <select id="pathTarget-${details.name}" class="form-control" style="margin-bottom: 8px;">
                        <option value="">Select target table...</option>
                        ${allTables.map(t => `<option value="${t}">${t}</option>`).join('')}
                    </select>
                    <button class="btn btn-path" onclick="findJoinPath('${details.name}')">Find Path</button>
                </div>
                <div id="pathResult-${details.name}" style="margin-top: 12px;"></div>
            </div>
            <div class="details-section">
                <h4>Columns (${details.columns.length})</h4>
                <ul class="column-list">
                    ${details.columns.map(col => `
                        <li>
                            <strong>${col.name}</strong> <span style="color: #999; font-size: 11px;">(${col.type || 'unknown'})</span>
                            ${col.distinct_count !== undefined ? `<span class="stats">${col.distinct_count} distinct</span>` : ''}
                        </li>
                    `).join('')}
                </ul>
            </div>
            ${details.outgoing_edges.length > 0 ? `
            <div class="details-section">
                <h4>Outgoing Relationships (${details.outgoing_edges.length})</h4>
                <div class="scrollable-content">
                    ${details.outgoing_edges.map(edge => `
                        <div class="edge-info">
                            <div style="display: flex; align-items: center; margin-bottom: 4px;">
                                <span class="kind ${edge.kind}">${edge.kind.toUpperCase()}</span>
                                <strong style="margin-left: 8px;">${edge.target}</strong>
                            </div>
                            <p style="margin: 4px 0; font-size: 11px; color: #666;">${edge.from_columns.join(', ')} → ${edge.to_columns.join(', ')}</p>
                            ${edge.on_delete ? `<p class="stats">ON DELETE: ${edge.on_delete}</p>` : ''}
                            ${edge.on_update ? `<p class="stats">ON UPDATE: ${edge.on_update}</p>` : ''}
                        </div>
                    `).join('')}
                </div>
            </div>
            ` : ''}
            ${details.incoming_edges.length > 0 ? `
            <div class="details-section">
                <h4>Incoming Relationships (${details.incoming_edges.length})</h4>
                <div class="scrollable-content">
                    ${details.incoming_edges.map(edge => `
                        <div class="edge-info">
                            <div style="display: flex; align-items: center; margin-bottom: 4px;">
                                <span class="kind ${edge.kind}">${edge.kind.toUpperCase()}</span>
                                <strong style="margin-left: 8px;">${edge.source}</strong>
                            </div>
                            <p style="margin: 4px 0; font-size: 11px; color: #666;">${edge.from_columns.join(', ')} → ${edge.to_columns.join(', ')}</p>
                            ${edge.on_delete ? `<p class="stats">ON DELETE: ${edge.on_delete}</p>` : ''}
                            ${edge.on_update ? `<p class="stats">ON UPDATE: ${edge.on_update}</p>` : ''}
                            ${edge.kind === 'inferred' ? `<p class="warning-text">⚠️ No referential constraint - deletion may break joins</p>` : ''}
                        </div>
                    `).join('')}
                </div>
            </div>
            ` : ''}
            </div>
        `;

        panel.classList.add('visible');
    } catch (error) {
        console.error('Error loading node details:', error);
    }
}

async function showEdgeDetails(edge) {
    try {
        const response = await fetch(`${API_BASE}/edge/${edge.source.id}/${edge.target.id}`);
        const details = await response.json();

        const panel = document.getElementById('detailsPanel');
        const content = document.getElementById('detailsContent');

        const edges = details.edges || [details];

        content.innerHTML = `
            <h3>Relationship</h3>
            <div class="details-section">
                <h4>From → To</h4>
                <p><strong>${details.from_table}</strong> → <strong>${details.to_table}</strong></p>
            </div>
            ${edges.map((e, idx) => `
                <div class="details-section">
                    <h4>Edge Details ${edges.length > 1 ? `(${idx + 1})` : ''}</h4>
                    <div class="edge-info">
                        <span class="kind ${e.kind}">${e.kind.toUpperCase()}</span>
                        <p><strong>Columns:</strong> ${e.from_columns.join(', ')} → ${e.to_columns.join(', ')}</p>
                        ${e.on_delete ? `<p class="stats"><strong>ON DELETE:</strong> ${e.on_delete}</p>` : ''}
                        ${e.on_update ? `<p class="stats"><strong>ON UPDATE:</strong> ${e.on_update}</p>` : ''}
                        ${e.confidence !== undefined ? `
                            <div class="confidence-breakdown" style="margin-top: 10px; padding: 10px; background: #f8f9fa; border-radius: 4px;">
                                <p><strong>Confidence: ${(e.confidence * 100).toFixed(1)}%</strong></p>
                                ${e.kind === 'inferred' && e.stats ? `
                                    <div class="confidence-details" style="margin-top: 8px;">
                                        <h5 style="font-size: 11px; margin-bottom: 5px;">Confidence Breakdown:</h5>
                                        <ul style="font-size: 11px; margin-left: 15px;">
                                            <li>Name Similarity: <strong>${(e.stats.name_similarity * 100).toFixed(1)}%</strong> (weight: 50%)</li>
                                            <li>Profile Match: <strong>${(e.stats.profile_match * 100).toFixed(1)}%</strong> (weight: 40%)</li>
                                            ${e.stats.type_match !== undefined ? `<li>Type Match: <strong>${(e.stats.type_match * 100).toFixed(1)}%</strong> (weight: 10%)</li>` : ''}
                                        </ul>
                                        ${e.stats.csv_uniqueness !== undefined ? `
                                            <p style="margin-top: 5px; font-size: 11px;">
                                                <strong>Direction Reasoning:</strong><br>
                                                ${details.from_table} column uniqueness: ${(e.stats.csv_uniqueness * 100).toFixed(1)}%<br>
                                                ${details.to_table} column uniqueness: ${(e.stats.existing_uniqueness !== undefined ? (e.stats.existing_uniqueness * 100).toFixed(1) + '%' : 'N/A')}
                                            </p>
                                        ` : ''}
                                    </div>
                                ` : ''}
                            </div>
                        ` : ''}
                        ${e.kind === 'inferred' ? `
                            <div class="simulation-warning" style="margin-top: 10px; padding: 8px;">
                                <strong>⚠️ Warning:</strong> This is an inferred relationship (not enforced by schema).
                                <p style="margin: 5px 0 0 0;">Deletion/update operations may succeed but break logical joins.</p>
                            </div>
                        ` : ''}
                    </div>
                </div>
            `).join('')}
        `;

        panel.classList.add('visible');
    } catch (error) {
        console.error('Error loading edge details:', error);
    }
}

function toggleCriticalTables() {
    const showCritical = document.getElementById('showCritical').checked;
    if (showCritical) {
        loadCriticalTables();
    } else {
        // Remove critical styling
        if (window.graphContainer) {
            window.graphContainer.selectAll('.node')
                .classed('critical-high', false)
                .classed('critical-medium', false);
        }
    }
}

function closeDetailsPanel() {
    document.getElementById('detailsPanel').classList.remove('visible');
    selectedNode = null;
    selectedEdge = null;
    
    // Clear all highlights and selections
    clearAllHighlights();
    window.graphContainer.selectAll('.node').classed('selected', false);
    window.graphContainer.selectAll('.link').classed('selected', false);
}

function toggleSection(header) {
    const section = header.closest('.details-section');
    const content = section.querySelector('.details-section-content');
    const isCollapsed = content.classList.contains('collapsed');
    
    if (isCollapsed) {
        content.classList.remove('collapsed');
        header.classList.remove('collapsed');
        // Set max-height to actual content height
        content.style.maxHeight = content.scrollHeight + 'px';
    } else {
        content.classList.add('collapsed');
        header.classList.add('collapsed');
        content.style.maxHeight = '0';
    }
}

function getNodeTooltip(node) {
    return `${node.id} (${node.source})<br/>${node.column_count} columns`;
}

function getEdgeTooltip(edge) {
    const fromCols = edge.from_columns.join(', ');
    const toCols = edge.to_columns.join(', ');
    const conf = edge.confidence !== undefined ? ` (${(edge.confidence * 100).toFixed(0)}%)` : '';
    return `${edge.source.id} → ${edge.target.id}<br/>${fromCols} → ${toCols}<br/>${edge.kind}${conf}`;
}

function showTooltip(event, text) {
    const tooltip = document.getElementById('tooltip');
    tooltip.innerHTML = text;
    tooltip.style.display = 'block';
    tooltip.style.left = (event.pageX + 10) + 'px';
    tooltip.style.top = (event.pageY + 10) + 'px';
}

function hideTooltip() {
    document.getElementById('tooltip').style.display = 'none';
}

function searchTables(query) {
    const normalized = (query || '').toLowerCase();
    const graphActive = document.getElementById('graphViewTab')?.classList.contains('active');
    const schemaActive = document.getElementById('schemaViewTab')?.classList.contains('active');

    if (graphActive && window.graphContainer) {
        if (!normalized) {
            window.graphContainer.selectAll('.node').style('opacity', 1);
        } else {
            window.graphContainer
                .selectAll('.node')
                .style('opacity', d => d.id.toLowerCase().includes(normalized) ? 1 : 0.2);
        }
    }

    if (schemaActive && window.schemaContainer) {
        if (!normalized) {
            window.schemaContainer.selectAll('.schema-table').style('opacity', 1);
        } else {
            window.schemaContainer
                .selectAll('.schema-table')
                .style('opacity', d => d.name.toLowerCase().includes(normalized) ? 1 : 0.2);
        }
    }
}

function updateStats() {
    document.getElementById('nodeCount').textContent = graphData.nodes.length;
    document.getElementById('edgeCount').textContent = graphData.edges.length;
}

function initializeSchema() {
    const svg = d3.select('#schemaSvg');
    if (svg.empty()) {
        return;
    }

    const width = window.innerWidth - 300;
    const height = window.innerHeight;

    svg.attr('width', width).attr('height', height);

    const container = svg.append('g').attr('class', 'schema-container-inner');

    const zoom = d3.zoom()
        .scaleExtent([0.25, 3])
        .on('zoom', (event) => {
            container.attr('transform', event.transform);
        });

    svg.call(zoom);

    const defs = svg.append('defs');

    defs.append('marker')
        .attr('id', 'schema-arrow-fk')
        .attr('viewBox', '0 -4 8 8')
        .attr('refX', 8)
        .attr('refY', 0)
        .attr('markerWidth', 8)
        .attr('markerHeight', 8)
        .attr('orient', 'auto')
        .append('path')
        .attr('d', 'M0,-4L8,0L0,4')
        .attr('fill', '#3498db');

    defs.append('marker')
        .attr('id', 'schema-arrow-inferred')
        .attr('viewBox', '0 -4 8 8')
        .attr('refX', 8)
        .attr('refY', 0)
        .attr('markerWidth', 8)
        .attr('markerHeight', 8)
        .attr('orient', 'auto')
        .append('path')
        .attr('d', 'M0,-4L8,0L0,4')
        .attr('fill', '#e74c3c');

    window.schemaSvg = svg;
    window.schemaContainer = container;
    window.schemaZoom = zoom;
}

function resetSchemaZoom() {
    if (!window.schemaSvg || !window.schemaZoom) return;
    window.schemaSvg.transition().duration(300).call(window.schemaZoom.transform, d3.zoomIdentity);
}

function showStatus(message, type) {
    const statusDiv = document.getElementById('uploadStatus');
    statusDiv.innerHTML = `<div class="status-message ${type}">${message}</div>`;
    setTimeout(() => {
        statusDiv.innerHTML = '';
    }, 5000);
}

function renderSchema() {
    const container = window.schemaContainer;
    if (!container || !schemaData || !schemaData.tables) return;

    container.selectAll('*').remove();

    const tables = schemaData.tables;
    const relationships = schemaData.relationships || [];

    const schemaShowFK = document.getElementById('schemaShowFK');
    const schemaShowInferred = document.getElementById('schemaShowInferred');
    const showFK = schemaShowFK ? schemaShowFK.checked : true;
    const showInferred = schemaShowInferred ? schemaShowInferred.checked : true;

    const visibleRels = relationships.filter(rel => {
        if (rel.kind === 'fk' && !showFK) return false;
        if (rel.kind === 'inferred' && !showInferred) return false;
        return true;
    });

    if (!tables.length) {
        return;
    }

    const cardWidth = 240;
    const headerHeight = 32;
    const rowHeight = 18;
    const verticalPadding = 10;
    const hGap = 80;
    const vGap = 56;

    const maxColumns = Math.max(
        1,
        ...tables.map(t => (t.columns ? t.columns.length : 0))
    );
    const cardHeight = headerHeight + verticalPadding * 2 + maxColumns * rowHeight;

    const maxCols = 4;
    const cols = Math.min(maxCols, Math.max(1, tables.length));
    const tableLayout = new Map();

    tables.forEach((table, index) => {
        const colIndex = index % cols;
        const rowIndex = Math.floor(index / cols);
        const colCount = table.columns ? table.columns.length : 0;
        const x = 80 + colIndex * (cardWidth + hGap);
        const y = 80 + rowIndex * (cardHeight + vGap);

        const columnAnchors = {};
        (table.columns || []).forEach((col, cIdx) => {
            const cy = y + headerHeight + verticalPadding + (cIdx + 0.5) * rowHeight;
            columnAnchors[col.name] = {
                left: { x, y: cy },
                right: { x: x + cardWidth, y: cy }
            };
        });

        tableLayout.set(table.name, {
            x,
            y,
            width: cardWidth,
            height: cardHeight,
            columns: table.columns || [],
            columnAnchors
        });
    });

    const tableGroups = container
        .selectAll('g.schema-table')
        .data(tables, d => d.name)
        .enter()
        .append('g')
        .attr('class', 'schema-table')
        .attr('transform', d => {
            const layout = tableLayout.get(d.name);
            return `translate(${layout.x},${layout.y})`;
        })
        .on('mouseover', (event, d) => {
            highlightSchemaTable(d.name);
            showSchemaTooltip(event, `${d.name} (${d.source})`);
        })
        .on('mouseout', () => {
            clearSchemaHighlights();
            hideSchemaTooltip();
        })
        .on('click', (event, d) => {
            event.stopPropagation();
            openSchemaDetails(d.name);
        });

    tableGroups
        .append('rect')
        .attr('class', 'schema-table-body')
        .attr('width', d => tableLayout.get(d.name).width)
        .attr('height', d => tableLayout.get(d.name).height);

    tableGroups
        .append('rect')
        .attr('class', 'schema-table-header')
        .attr('width', d => tableLayout.get(d.name).width)
        .attr('height', headerHeight);

    tableGroups
        .append('text')
        .attr('class', 'schema-table-title')
        .attr('x', 12)
        .attr('y', headerHeight / 2 + 4)
        .text(d => d.name);

    tableGroups.each(function (d) {
        const layout = tableLayout.get(d.name);
        const g = d3.select(this);
        (layout.columns || []).forEach((col, idx) => {
            const baseY = headerHeight + verticalPadding + idx * rowHeight;
            const textY = baseY + rowHeight * 0.7;

            // Column separator line
            g.append('line')
                .attr('x1', 10)
                .attr('x2', layout.width - 10)
                .attr('y1', baseY)
                .attr('y2', baseY)
                .attr('stroke', '#1f2937')
                .attr('stroke-width', 1);

            // Column name
            g.append('text')
                .attr('class', 'schema-column-text')
                .attr('x', 16)
                .attr('y', textY)
                .text(col.name);
        });
    });

    // One visual link per column pair so each FK column maps to its exact target column
    const linkSegments = visibleRels.flatMap(rel => {
        const fromCols = rel.from_columns || [];
        const toCols = rel.to_columns || [];
        if (!fromCols.length || !toCols.length) {
            return [Object.assign({}, rel, { _fromCol: null, _toCol: null })];
        }
        return fromCols.map((fromCol, idx) =>
            Object.assign({}, rel, {
                _fromCol: fromCol,
                _toCol: toCols[idx] || toCols[0]
            })
        );
    });

    container
        .append('g')
        .attr('class', 'schema-links')
        .selectAll('path')
        .data(linkSegments)
        .enter()
        .append('path')
        .attr('class', d => `schema-link ${d.kind}`)
        // Orthogonal (right-angle) connectors to reduce visual confusion
        .attr('d', d => {
            const fromLayout = tableLayout.get(d.from_table);
            const toLayout = tableLayout.get(d.to_table);
            if (!fromLayout || !toLayout) return null;

            const fromCol = d._fromCol || (d.from_columns || [])[0];
            const toCol = d._toCol || (d.to_columns || [])[0];

            const fromAnchor =
                (fromLayout.columnAnchors[fromCol] &&
                    fromLayout.columnAnchors[fromCol].right) || {
                    x: fromLayout.x + fromLayout.width,
                    y: fromLayout.y + fromLayout.height / 2
                };
            const toAnchor =
                (toLayout.columnAnchors[toCol] &&
                    toLayout.columnAnchors[toCol].left) || {
                    x: toLayout.x,
                    y: toLayout.y + toLayout.height / 2
                };

            // Route: horizontal out from source, vertical, then into target
            const midX = (fromAnchor.x + toAnchor.x) / 2;
            return [
                `M${fromAnchor.x},${fromAnchor.y}`,
                `L${midX},${fromAnchor.y}`,
                `L${midX},${toAnchor.y}`,
                `L${toAnchor.x},${toAnchor.y}`
            ].join(' ');
        })
        .on('mouseover', (event, d) => {
            highlightSchemaRelationship(d);
            const fromCols = (d.from_columns || []).join(', ');
            const toCols = (d.to_columns || []).join(', ');
            let text = `${d.from_table}.${fromCols} → ${d.to_table}.${toCols}<br/>${d.kind.toUpperCase()}`;
            if (d.on_delete) text += `<br/>ON DELETE: ${d.on_delete}`;
            if (d.on_update) text += `<br/>ON UPDATE: ${d.on_update}`;
            showSchemaTooltip(event, text);
        })
        .on('mouseout', () => {
            clearSchemaHighlights();
            hideSchemaTooltip();
        });

    tableGroups.raise();
}

function showSchemaTooltip(event, html) {
    const el = document.getElementById('schemaTooltip');
    if (!el) return;
    el.innerHTML = html;
    el.style.display = 'block';
    el.style.left = event.pageX + 10 + 'px';
    el.style.top = event.pageY + 10 + 'px';
}

function hideSchemaTooltip() {
    const el = document.getElementById('schemaTooltip');
    if (!el) return;
    el.style.display = 'none';
}

function highlightSchemaTable(tableName) {
    const container = window.schemaContainer;
    if (!container) return;
    container
        .selectAll('.schema-table')
        .classed('schema-highlighted', d => d.name === tableName);
    container
        .selectAll('.schema-link')
        .classed(
            'schema-highlighted',
            d => d.from_table === tableName || d.to_table === tableName
        );
}

function highlightSchemaRelationship(rel) {
    const container = window.schemaContainer;
    if (!container) return;
    // Remove any existing hover states
    container.selectAll('.schema-link').classed('schema-link-hovered', false);
    container.selectAll('.schema-table').classed('schema-table-hovered', false);
    // Add hover state to the relationship line
    container
        .selectAll('.schema-link')
        .filter(d => d === rel)
        .classed('schema-link-hovered', true);
    // Add hover state to connected tables
    container
        .selectAll('.schema-table')
        .filter(d => d.name === rel.from_table || d.name === rel.to_table)
        .classed('schema-table-hovered', true);
}

function clearSchemaHighlights() {
    const container = window.schemaContainer;
    if (!container) return;
    container.selectAll('.schema-table').classed('schema-highlighted', false);
    container.selectAll('.schema-link').classed('schema-highlighted', false);
    container.selectAll('.schema-table').classed('schema-table-hovered', false);
    container.selectAll('.schema-link').classed('schema-link-hovered', false);
}

async function openSchemaDetails(tableName) {
    try {
        const response = await fetch(`${API_BASE}/table/${tableName}`);
        if (!response.ok) return;
        const details = await response.json();

        const panel = document.getElementById('schemaDetailsPanel');
        const content = document.getElementById('schemaDetailsContent');
        if (!panel || !content) return;

        content.innerHTML = `
            <div style="display:flex; align-items:center; justify-content:space-between; padding:16px 20px; border-bottom:1px solid #e5e7eb;">
                <h3 style="margin:0;">${details.name}</h3>
                <button
                    type="button"
                    onclick="closeSchemaDetails()"
                    style="border:none; background:transparent; font-size:18px; line-height:1; cursor:pointer; color:#9ca3af; padding:4px 6px; border-radius:4px;"
                    aria-label="Close"
                >×</button>
            </div>
            <div class="schema-details-body">
                <div class="schema-detail-row">
                    <span class="schema-detail-label">Source:</span>
                    <span>${details.source}</span>
                </div>
                <div class="schema-detail-row">
                    <span class="schema-detail-label">Columns (${details.columns.length}):</span>
                    <ul class="schema-columns-list">
                        ${details.columns
                            .map(col => `<li><strong>${col.name}</strong> <span style="color:#9ca3af;">(${col.type || 'unknown'})</span></li>`)
                            .join('')}
                    </ul>
                </div>
            </div>
        `;

        panel.classList.add('visible');
    } catch (error) {
        console.error('Error opening schema details:', error);
    }
}

function closeSchemaDetails() {
    const panel = document.getElementById('schemaDetailsPanel');
    if (panel) {
        panel.classList.remove('visible');
    }
}

async function clearGraph() {
    if (confirm('Are you sure you want to clear the entire graph?')) {
        try {
            await fetch(`${API_BASE}/graph`, { method: 'DELETE' });
            showStatus('Graph cleared', 'success');
            loadGraph();
            loadSchema();
        } catch (error) {
            showStatus(`Error: ${error.message}`, 'error');
        }
    }
}

// Handle window resize
window.addEventListener('resize', () => {
    const svg = window.graphSvg;
    if (svg) {
        svg.attr('width', window.innerWidth - 300)
           .attr('height', window.innerHeight);
    }
    const schemaSvg = window.schemaSvg;
    if (schemaSvg) {
        schemaSvg.attr('width', window.innerWidth - 300)
                 .attr('height', window.innerHeight);
    }
});

function toggleSelectAll(tableName) {
    const selectAll = document.getElementById(`selectAll-${tableName}`);
    const checkboxes = document.querySelectorAll(`.row-checkbox[data-table="${tableName}"]`);
    checkboxes.forEach(cb => cb.checked = selectAll.checked);
}

function getSelectedRows(tableName) {
    const checkboxes = document.querySelectorAll(`.row-checkbox[data-table="${tableName}"]:checked`);
    return Array.from(checkboxes).map(cb => ({
        rowId: cb.getAttribute('data-row-id'),
        rowIndex: parseInt(cb.getAttribute('data-row-index'))
    }));
}

function openDeleteSimulation(tableName) {
    const selectedRows = getSelectedRows(tableName);
    const rowIdentifiers = selectedRows.length > 0 ? selectedRows.map(r => r.rowId) : null;
    simulateDelete(tableName, rowIdentifiers);
}

function openUpdateSimulation(tableName) {
    const selectedRows = getSelectedRows(tableName);
    const rowIdentifiers = selectedRows.length > 0 ? selectedRows.map(r => r.rowId) : null;
    
    // Get table details to show column selector
    fetch(`${API_BASE}/table/${tableName}`)
        .then(response => response.json())
        .then(details => {
            showUpdateDialog(tableName, details.columns, rowIdentifiers);
        })
        .catch(error => {
            console.error('Error loading table details:', error);
            simulateUpdate(tableName, null, rowIdentifiers, null);
        });
}

function showUpdateDialog(tableName, columns, rowIdentifiers) {
    const resultDiv = document.getElementById(`simulationResult-${tableName}`);
    if (!resultDiv) return;
    
    const columnOptions = columns.map(col => 
        `<option value="${col.name}">${col.name} (${col.type || 'unknown'})</option>`
    ).join('');
    
    resultDiv.innerHTML = `
        <div class="update-dialog">
            <h5>Configure UPDATE Simulation</h5>
            <div class="form-group">
                <label>Column to Update:</label>
                <select id="updateColumn-${tableName}" class="form-control">
                    ${columnOptions}
                </select>
            </div>
            <div class="form-group">
                <label>New Value:</label>
                <input type="text" id="updateValue-${tableName}" class="form-control" placeholder="Enter new value">
            </div>
            <div class="form-group">
                <button class="btn btn-simulate" onclick="executeUpdateSimulation('${tableName}')">Run Simulation</button>
            </div>
        </div>
    `;
}

function executeUpdateSimulation(tableName) {
    const column = document.getElementById(`updateColumn-${tableName}`)?.value;
    const newValue = document.getElementById(`updateValue-${tableName}`)?.value;
    const selectedRows = getSelectedRows(tableName);
    const rowIdentifiers = selectedRows.length > 0 ? selectedRows.map(r => r.rowId) : null;
    
    simulateUpdate(tableName, column, rowIdentifiers, newValue);
}

async function simulateDelete(tableName, rowIdentifiers = null) {
    const resultDiv = document.getElementById(`simulationResult-${tableName}`);
    if (!resultDiv) return;
    
    const rowInfo = rowIdentifiers && rowIdentifiers.length > 0 
        ? ` (${rowIdentifiers.length} selected row${rowIdentifiers.length > 1 ? 's' : ''})`
        : ' (all rows)';
    
    resultDiv.innerHTML = `<p>Simulating DELETE operation${rowInfo}...</p>`;
    
    try {
        const response = await fetch(`${API_BASE}/simulate/delete`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ 
                table: tableName,
                row_identifiers: rowIdentifiers
            })
        });
        
        const result = await response.json();
        
        let html = '<div class="simulation-result">';
        if (result.result === 'success') {
            html += `<p class="success-text">✓ DELETE would succeed${rowInfo}</p>`;
            html += `<p>${result.explanation}</p>`;
            if (result.cascade_tables && result.cascade_tables.length > 0) {
                html += `<p class="warning-text">⚠️ CASCADE deletion would affect: ${result.cascade_tables.join(', ')}</p>`;
            }
            if (result.inferred_risks && result.inferred_risks.length > 0) {
                html += `<p class="warning-text">⚠️ Inferred relationships at risk: ${result.inferred_risks.join(', ')}</p>`;
            }
        } else if (result.result === 'failure') {
            html += `<p class="error-text">✗ DELETE would fail${rowInfo}</p>`;
            html += `<p><strong>Error:</strong> ${result.error_type}</p>`;
            html += `<p>${result.explanation}</p>`;
            if (result.blocked_by && result.blocked_by.length > 0) {
                html += `<p><strong>Blocked by:</strong> ${result.blocked_by.join(', ')}</p>`;
            }
            if (result.detailed_explanations && result.detailed_explanations.length > 0) {
                html += '<ul>';
                result.detailed_explanations.forEach(exp => {
                    html += `<li>${exp}</li>`;
                });
                html += '</ul>';
            }
        } else {
            html += `<p class="error-text">Error: ${result.message || 'Unknown error'}</p>`;
        }
        html += '</div>';
        
        resultDiv.innerHTML = html;
    } catch (error) {
        resultDiv.innerHTML = `<div class="simulation-result"><p class="error-text">Error: ${error.message}</p></div>`;
    }
}

async function showDownstreamImpact(tableName) {
    const resultDiv = document.getElementById(`impactResult-${tableName}`);
    if (!resultDiv) return;
    
    const depth = parseInt(document.getElementById(`impactDepth-${tableName}`)?.value || 3);
    resultDiv.innerHTML = '<p>Analyzing downstream impact...</p>';
    
    try {
        const response = await fetch(`${API_BASE}/table/${tableName}/impact?depth=${depth}`);
        const impact = await response.json();
        
        if (impact.error) {
            resultDiv.innerHTML = `<div class="simulation-result"><p class="error-text">${impact.error}</p></div>`;
            return;
        }
        
        // Calculate actual max depth from paths
        let actualMaxDepth = depth;
        if (impact.paths && impact.paths.length > 0) {
            const maxHops = Math.max(...impact.paths.map(p => p.hops || 0));
            actualMaxDepth = maxHops + 1; // hops + 1 = depth
        }
        
        // Update depth input to reflect actual max depth
        const depthInput = document.getElementById(`impactDepth-${tableName}`);
        const depthLabel = document.getElementById(`depthLabel-${tableName}`);
        if (depthInput && depthLabel) {
            depthInput.max = actualMaxDepth;
            const currentValue = parseInt(depthInput.value) || depth;
            depthInput.value = Math.min(currentValue, actualMaxDepth);
            
            // Update label text to show actual range
            depthLabel.innerHTML = `Depth (max ${actualMaxDepth}): <input type="number" id="impactDepth-${tableName}" value="${Math.min(currentValue, actualMaxDepth)}" min="1" max="${actualMaxDepth}" style="width: 50px; margin-left: 5px; padding: 4px;">`;
        }
        
        let html = '<div class="impact-result">';
        html += `<p class="success-text">📊 Impact Analysis (Depth: ${depth})</p>`;
        html += `<p><strong>${impact.impact_count}</strong> downstream table(s) would be affected</p>`;
        if (actualMaxDepth < depth) {
            html += `<p class="stats" style="font-size: 11px; color: #666;">Note: Actual impact depth is ${actualMaxDepth} (requested depth ${depth} exceeds available relationships)</p>`;
        }
        
        if (impact.impacted_tables && impact.impacted_tables.length > 0) {
            html += '<div class="impacted-tables">';
            html += '<p><strong>Affected Tables:</strong></p>';
            html += '<ul class="impact-list">';
            impact.impacted_tables.forEach(table => {
                html += `<li>${table}</li>`;
            });
            html += '</ul>';
            html += '</div>';
            
            // Render impact subgraph as tree
            renderImpactSubgraph(tableName, impact, depth);
        } else {
            html += '<p>No downstream tables would be affected.</p>';
            // Restore full graph if no impact
            if (impactViewActive) {
                restoreFullGraph();
            }
        }
        
        html += '<div style="margin-top: 10px;">';
        html += `<button class="btn btn-secondary" onclick="restoreFullGraph()" style="font-size: 11px; padding: 5px 10px;">Show Full Graph</button>`;
        html += '</div>';
        html += '</div>';
        resultDiv.innerHTML = html;
    } catch (error) {
        resultDiv.innerHTML = `<div class="simulation-result"><p class="error-text">Error: ${error.message}</p></div>`;
    }
}

function renderImpactSubgraph(sourceTable, impact, depth) {
    // Store original graph data if not already stored
    if (!impactViewActive) {
        originalGraphData = JSON.parse(JSON.stringify(graphData));
        impactViewActive = true;
        
        // Add floating "View Full Graph" button
        const graphContainer = document.querySelector('.graph-container');
        let floatingButton = document.getElementById('viewFullGraphButton');
        if (!floatingButton) {
            floatingButton = document.createElement('button');
            floatingButton.id = 'viewFullGraphButton';
            floatingButton.className = 'btn btn-secondary';
            floatingButton.textContent = 'View Full Graph';
            floatingButton.style.cssText = 'position: absolute; top: 20px; left: 20px; z-index: 1000; padding: 8px 16px; font-size: 12px; width: auto; display: inline-block;';
            floatingButton.onclick = restoreFullGraph;
            graphContainer.appendChild(floatingButton);
        }
        floatingButton.style.display = 'block';
    }
    
    // Build impact subgraph: source table + all impacted tables
    const impactNodeSet = new Set([sourceTable, ...impact.impacted_tables]);
    
    // Filter nodes to only include impact subgraph
    const impactNodes = graphData.nodes.filter(node => impactNodeSet.has(node.id));
    
    // Filter edges to only include edges within the impact subgraph
    // Edges should point FROM impacted tables TO source (showing dependency direction)
    const impactEdges = graphData.edges.filter(edge => {
        const source = typeof edge.source === 'string' ? edge.source : edge.source.id;
        const target = typeof edge.target === 'string' ? edge.target : edge.target.id;
        // Include edges where target is sourceTable (tables that depend on source)
        return impactNodeSet.has(source) && impactNodeSet.has(target);
    });
    
    // Build tree structure for hierarchical layout
    const treeData = buildImpactTree(sourceTable, impact, impactNodes, impactEdges, depth);
    
    // Render tree layout
    renderTreeLayout(treeData, sourceTable);
}

function buildImpactTree(sourceTable, impact, nodes, edges, maxDepth) {
    // Create a tree structure: source is root, impacted tables are children
    const nodeMap = new Map(nodes.map(n => [n.id, n]));
    
    // Build parent-child map from edges
    // In the graph: impacted_table -> source_table (e.g., works_on -> employee)
    // In the tree: source_table (root) has impacted_table as children
    const childrenMap = new Map();
    const allTables = new Set([sourceTable, ...impact.impacted_tables]);
    allTables.forEach(table => childrenMap.set(table, []));
    
    // Process edges: if edge.target is sourceTable, then edge.source is a child of sourceTable
    edges.forEach(edge => {
        const source = typeof edge.source === 'string' ? edge.source : edge.source.id;
        const target = typeof edge.target === 'string' ? edge.target : edge.target.id;
        
        if (target === sourceTable && allTables.has(source)) {
            // This table directly depends on source - it's a direct child
            if (!childrenMap.get(sourceTable).includes(source)) {
                childrenMap.get(sourceTable).push(source);
            }
        } else if (allTables.has(source) && allTables.has(target) && target !== sourceTable) {
            // This is an edge between two impacted tables
            // The source depends on the target, so target is parent of source
            if (!childrenMap.get(target).includes(source)) {
                childrenMap.get(target).push(source);
            }
        }
    });
    
    // Build tree structure recursively
    function buildNode(tableName, currentDepth) {
        const node = nodeMap.get(tableName);
        if (!node) return null;
        
        const treeNode = {
            id: tableName,
            name: tableName,
            data: node,
            children: []
        };
        
        if (currentDepth < maxDepth) {
            const children = childrenMap.get(tableName) || [];
            children.forEach(childName => {
                const child = buildNode(childName, currentDepth + 1);
                if (child) {
                    treeNode.children.push(child);
                }
            });
        }
        
        return treeNode;
    }
    
    return buildNode(sourceTable, 0);
}

function renderTreeLayout(treeData, sourceTable) {
    const container = window.graphContainer;
    if (!container) return;
    
    // Stop any existing simulation
    if (simulation) {
        simulation.stop();
        simulation = null;
    }
    
    // Clear existing graph
    container.selectAll('*').remove();
    
    if (!treeData) {
        return;
    }
    
    const width = window.innerWidth - 300;
    const height = window.innerHeight;
    
    // Create tree layout (x = horizontal, y = vertical)
    const tree = d3.tree()
        .size([width - 200, height - 100])
        .separation((a, b) => (a.parent === b.parent ? 1 : 2) / a.depth);
    
    // Convert tree data to hierarchy
    const root = d3.hierarchy(treeData);
    tree(root);
    
    // Calculate node positions
    const nodes = root.descendants();
    const links = root.links();
    
    // Create links (tree edges go from parent to child)
    // D3 tree links: d.source is parent, d.target is child
    // In the tree: parent (source table) -> child (impacted table)
    // This matches the impact direction: source impacts child
    const link = container.append('g')
        .attr('class', 'links')
        .selectAll('path')
        .data(links)
        .enter()
        .append('path')
        .attr('class', 'link fk impact-link')
        .attr('marker-end', 'url(#arrow-fk)')  // Arrow at end pointing to child (impacted table)
        .attr('fill', 'none')
        .attr('stroke', '#2c3e50')  // Dark gray for edges
        .attr('stroke-width', 2)
        .attr('d', d3.linkVertical()
            .x(d => d.y + 100)  // Horizontal position (D3 tree uses y for horizontal)
            .y(d => d.x + 50)    // Vertical position (D3 tree uses x for vertical, parent at top)
        );
    
    // Create nodes
    const node = container.append('g')
        .attr('class', 'nodes')
        .selectAll('g')
        .data(nodes)
        .enter()
        .append('g')
        .attr('class', d => `node ${d.data.data?.source || 'sql'} ${d.data.id === sourceTable ? 'impact-source' : 'impacted'}`)
        .attr('transform', d => `translate(${d.y + 100},${d.x + 50})`)
        .call(d3.drag()
            .on('start', dragstarted)
            .on('drag', dragged)
            .on('end', dragended)
        )
        .on('click', (event, d) => {
            event.stopPropagation();
            selectNode(d.data.data);
        })
        .on('mouseover', (event, d) => {
            showTooltip(event, getNodeTooltip(d.data.data));
        })
        .on('mouseout', hideTooltip);
    
    // Add circles
    node.append('circle')
        .attr('r', d => {
            const textLength = d.data.id.length;
            return Math.min(30 + (textLength * 3), 70);
        })
        .attr('fill', '#3498db')  // Uniform blue for all nodes
        .attr('stroke', d => {
            // Apply orange border for impacted nodes, red for impact source
            if (d.data.id === sourceTable) {
                return '#e74c3c';  // Red for impact source
            } else {
                return '#f39c12';  // Orange for impacted nodes
            }
        })
        .attr('stroke-width', d => {
            // Thicker border for impact source
            return d.data.id === sourceTable ? 5 : 4;
        });
    
    // Add labels
    node.append('text')
        .text(d => d.data.id)
        .attr('dy', 4)
        .attr('font-size', '12px')
        .attr('text-anchor', 'middle')
        .attr('fill', 'white')
        .attr('font-weight', '500');
    
    // Center the tree
    const bounds = container.node().getBBox();
    const dx = width / 2 - (bounds.x + bounds.width / 2);
    const dy = height / 2 - (bounds.y + bounds.height / 2);
    container.attr('transform', `translate(${dx},${dy})`);
}

function restoreFullGraph() {
    impactViewActive = false;
    
    // Hide floating "View Full Graph" button
    const floatingButton = document.getElementById('viewFullGraphButton');
    if (floatingButton) {
        floatingButton.style.display = 'none';
    }
    
    // Stop any existing simulation (from tree layout)
    if (simulation) {
        simulation.stop();
        simulation = null;
    }
    
    // Reset container transform (tree layout may have offset it)
    const container = window.graphContainer;
    const svg = window.graphSvg;
    
    if (container && svg) {
        // Reset container transform to identity
        container.attr('transform', d3.zoomIdentity);
        
        // Reset zoom by getting the zoom transform and resetting it
        // Use the zoom transform API to reset
        const currentTransform = d3.zoomTransform(svg.node());
        if (currentTransform && (currentTransform.k !== 1 || currentTransform.x !== 0 || currentTransform.y !== 0)) {
            // Create a new zoom behavior and apply identity transform
            const zoom = d3.zoom()
                .scaleExtent([0.1, 4])
                .on('zoom', (event) => {
                    container.attr('transform', event.transform);
                });
            svg.call(zoom.transform, d3.zoomIdentity);
        }
    }
    
    if (originalGraphData) {
        graphData = JSON.parse(JSON.stringify(originalGraphData));
        originalGraphData = null;
    }
    
    // Reset node positions (they might have been set by tree layout)
    if (graphData.nodes) {
        graphData.nodes.forEach(node => {
            delete node.x;
            delete node.y;
            delete node.fx;
            delete node.fy;
            delete node.vx;
            delete node.vy;
        });
    }
    
    // Store the selected node ID before rendering
    const selectedNodeId = selectedNode ? (selectedNode.id || selectedNode) : null;
    
    // Reload graph from server to ensure clean state
    // This is more reliable than trying to manually reset everything
    loadGraph().then(() => {
        // Clear impact highlights
        clearAllHighlights();
        
        // Re-select node after graph is loaded
        if (selectedNodeId) {
            setTimeout(() => {
                const nodeObj = graphData.nodes.find(n => n.id === selectedNodeId);
                if (nodeObj) {
                    selectedNode = nodeObj;
                    const nodes = window.graphContainer.selectAll('.node');
                    if (nodes.size() > 0) {
                        nodes
                            .filter(d => d.id === selectedNodeId)
                            .classed('selected', true);
                        // Show details panel
                        showNodeDetails(nodeObj);
                    }
                }
            }, 200);
        }
    });
}

// Removed - now using tree layout instead

async function findJoinPath(fromTable) {
    const resultDiv = document.getElementById(`pathResult-${fromTable}`);
    if (!resultDiv) return;
    
    const targetTable = document.getElementById(`pathTarget-${fromTable}`)?.value;
    if (!targetTable) {
        resultDiv.innerHTML = '<p class="error-text">Please select a target table</p>';
        return;
    }
    
    resultDiv.innerHTML = '<p>Finding join paths...</p>';
    
    try {
        const response = await fetch(`${API_BASE}/path/${fromTable}/${targetTable}?max_depth=5`);
        const pathData = await response.json();
        
        if (pathData.error) {
            resultDiv.innerHTML = `<div class="simulation-result"><p class="error-text">${pathData.error}</p></div>`;
            return;
        }
        
        let html = '<div class="path-result">';
        
        if (!pathData.shortest_path) {
            html += `<p class="error-text">No path found between ${fromTable} and ${targetTable}</p>`;
        } else {
            html += `<p class="success-text">✓ Path Found!</p>`;
            html += `<p><strong>Shortest Path:</strong> ${pathData.shortest_path.join(' → ')}</p>`;
            html += `<p>Path Length: ${pathData.shortest_path_length} hop${pathData.shortest_path_length > 1 ? 's' : ''}</p>`;
            
            if (pathData.paths && pathData.paths.length > 0) {
                html += '<div class="path-details" style="margin-top: 10px;">';
                html += '<p><strong>Join Details:</strong></p>';
                pathData.paths.forEach((pathInfo, idx) => {
                    if (pathInfo.is_shortest || idx === 0) {
                        html += '<div class="path-info" style="background: #e3f2fd; padding: 8px; border-radius: 4px; margin: 5px 0;">';
                        html += `<p><strong>Path ${idx + 1}:</strong> ${pathInfo.path.join(' → ')}</p>`;
                        html += '<ul style="margin: 5px 0 0 20px; font-size: 11px;">';
                        pathInfo.edges.forEach(edge => {
                            const fromCols = edge.from_columns.join(', ');
                            const toCols = edge.to_columns.join(', ');
                            html += `<li>${edge.from_table}.${fromCols} → ${edge.to_table}.${toCols} (${edge.kind})</li>`;
                        });
                        html += '</ul>';
                        html += '</div>';
                    }
                });
                html += '</div>';
                
                // Highlight path in graph
                highlightPath(pathData.shortest_path);
            }
        }
        
        html += '</div>';
        resultDiv.innerHTML = html;
    } catch (error) {
        resultDiv.innerHTML = `<div class="simulation-result"><p class="error-text">Error: ${error.message}</p></div>`;
    }
}

function highlightPath(path) {
    // Reset all nodes and edges
    window.graphContainer.selectAll('.node').classed('path-node', false);
    window.graphContainer.selectAll('.link').classed('path-link', false);
    
    // Highlight nodes in path
    path.forEach(tableName => {
        window.graphContainer.selectAll('.node')
            .filter(d => d.id === tableName)
            .classed('path-node', true);
    });
    
    // Highlight edges in path
    for (let i = 0; i < path.length - 1; i++) {
        const source = path[i];
        const target = path[i + 1];
        window.graphContainer.selectAll('.link')
            .filter(d => {
                const src = typeof d.source === 'string' ? d.source : d.source.id;
                const tgt = typeof d.target === 'string' ? d.target : d.target.id;
                return src === source && tgt === target;
            })
            .classed('path-link', true);
    }
}

async function loadCriticalTables() {
    try {
        const response = await fetch(`${API_BASE}/graph/critical-tables`);
        const critical = await response.json();
        
        // Apply criticality styling to nodes
        if (window.graphContainer && critical.critical_tables) {
            window.graphContainer.selectAll('.node')
                .attr('data-criticality', d => {
                    const score = critical.critical_tables[d.id]?.criticality_score || 0;
                    return score;
                })
                .classed('critical-high', d => {
                    const score = critical.critical_tables[d.id]?.criticality_score || 0;
                    return score > 0.7;
                })
                .classed('critical-medium', d => {
                    const score = critical.critical_tables[d.id]?.criticality_score || 0;
                    return score > 0.4 && score <= 0.7;
                });
        }
        
        return critical;
    } catch (error) {
        console.error('Error loading critical tables:', error);
        return null;
    }
}

async function simulateUpdate(tableName, column = null, rowIdentifiers = null, newValue = null) {
    const resultDiv = document.getElementById(`simulationResult-${tableName}`);
    if (!resultDiv) return;
    
    const rowInfo = rowIdentifiers && rowIdentifiers.length > 0 
        ? ` (${rowIdentifiers.length} selected row${rowIdentifiers.length > 1 ? 's' : ''})`
        : ' (all rows)';
    const columnInfo = column ? ` on column '${column}'` : '';
    const valueInfo = newValue ? ` to '${newValue}'` : '';
    
    resultDiv.innerHTML = `<p>Simulating UPDATE operation${rowInfo}${columnInfo}${valueInfo}...</p>`;
    
    try {
        const response = await fetch(`${API_BASE}/simulate/update`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ 
                table: tableName,
                column: column,
                row_identifiers: rowIdentifiers,
                new_value: newValue
            })
        });
        
        const result = await response.json();
        
        let html = '<div class="simulation-result">';
        if (result.result === 'success') {
            html += `<p class="success-text">✓ UPDATE would succeed${rowInfo}${columnInfo}${valueInfo}</p>`;
            html += `<p>${result.explanation}</p>`;
            if (result.cascade_tables && result.cascade_tables.length > 0) {
                html += `<p class="warning-text">⚠️ CASCADE update would affect: ${result.cascade_tables.join(', ')}</p>`;
            }
            if (result.inferred_risks && result.inferred_risks.length > 0) {
                html += `<p class="warning-text">⚠️ Inferred relationships at risk: ${result.inferred_risks.join(', ')}</p>`;
            }
        } else if (result.result === 'failure') {
            html += `<p class="error-text">✗ UPDATE would fail${rowInfo}${columnInfo}${valueInfo}</p>`;
            html += `<p><strong>Error:</strong> ${result.error_type}</p>`;
            html += `<p>${result.explanation}</p>`;
            if (result.blocked_by && result.blocked_by.length > 0) {
                html += `<p><strong>Blocked by:</strong> ${result.blocked_by.join(', ')}</p>`;
            }
            if (result.detailed_explanations && result.detailed_explanations.length > 0) {
                html += '<ul>';
                result.detailed_explanations.forEach(exp => {
                    html += `<li>${exp}</li>`;
                });
                html += '</ul>';
            }
        } else {
            html += `<p class="error-text">Error: ${result.message || 'Unknown error'}</p>`;
        }
        html += '</div>';
        
        resultDiv.innerHTML = html;
    } catch (error) {
        resultDiv.innerHTML = `<div class="simulation-result"><p class="error-text">Error: ${error.message}</p></div>`;
    }
}

