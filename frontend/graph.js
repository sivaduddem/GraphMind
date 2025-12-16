/**
 * GraphMind Frontend
 * D3.js interactive graph visualization
 */

const API_BASE = 'http://localhost:8000/api';
let graphData = { nodes: [], edges: [] };
let simulation = null;
let selectedNode = null;
let selectedEdge = null;

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    initializeGraph();
    setupEventListeners();
    loadGraph();
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

function renderGraph() {
    const container = window.graphContainer;
    if (!container) return;

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
            // Calculate edge start and end points on circle perimeters
            const dx = d.target.x - d.source.x;
            const dy = d.target.y - d.source.y;
            const sourceRadius = d.source.radius || 30;
            const targetRadius = d.target.radius || 30;
            const len = Math.sqrt(dx * dx + dy * dy);
            
            if (len === 0) return '';
            
            // Start point on source circle
            const x1 = d.source.x + (dx / len) * sourceRadius;
            const y1 = d.source.y + (dy / len) * sourceRadius;
            
            // End point on target circle
            const x2 = d.target.x - (dx / len) * targetRadius;
            const y2 = d.target.y - (dy / len) * targetRadius;
            
            // Calculate control point for quadratic curve
            // Offset perpendicular to the line to create a smooth curve
            const offset = Math.min(len * 0.3, 50); // Curve offset
            const perpX = -dy / len * offset;
            const perpY = dx / len * offset;
            
            // Control point (midpoint with perpendicular offset)
            const midX = (x1 + x2) / 2 + perpX;
            const midY = (y1 + y2) / 2 + perpY;
            
            // Create quadratic Bezier curve path
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
    selectedNode = node;
    selectedEdge = null;

    // Clear all visualizations
    clearAllHighlights();

    // Update visual selection
    window.graphContainer.selectAll('.node').classed('selected', false);
    window.graphContainer.selectAll('.link').classed('selected', false);
    
    window.graphContainer.selectAll('.node')
        .filter(d => d.id === node.id)
        .classed('selected', true);

    // Show details panel
    showNodeDetails(node);
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

async function showNodeDetails(node) {
    try {
        const response = await fetch(`${API_BASE}/table/${node.id}`);
        const details = await response.json();
        
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

        content.innerHTML = `
            <h3>${details.name} ${riskBadge}</h3>
            <div class="details-section">
                <h4>Source</h4>
                <p>${details.source}</p>
            </div>
            ${riskScore ? `
            <div class="details-section">
                <h4>Delete Risk Score</h4>
                <p>Risk Level: <strong>${riskScore.risk_level.toUpperCase()}</strong> (${riskScore.risk_score}/100)</p>
                <p class="stats">Incoming FKs: ${riskScore.incoming_fk_count} | RESTRICT: ${riskScore.restrict_count} | CASCADE: ${riskScore.cascade_count}</p>
            </div>
            ` : ''}
            ${tableData && tableData.rows && tableData.rows.length > 0 ? `
            <div class="details-section">
                <h4>Table Data (${tableData.rows.length} rows)</h4>
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
            </div>
            ` : ''}
            <div class="details-section">
                <h4>Impact Analysis</h4>
                <div class="impact-controls">
                    <button class="btn btn-impact" onclick="showDownstreamImpact('${details.name}')">Show Downstream Impact</button>
                    <label>Depth: <input type="number" id="impactDepth-${details.name}" value="3" min="1" max="10" style="width: 50px;"></label>
                </div>
                <div id="impactResult-${details.name}"></div>
            </div>
            <div class="details-section">
                <h4>Simulation</h4>
                <div class="simulation-controls">
                    <button class="btn btn-simulate" onclick="openDeleteSimulation('${details.name}')">Simulate DELETE</button>
                    <button class="btn btn-simulate" onclick="openUpdateSimulation('${details.name}')">Simulate UPDATE</button>
                </div>
                <div id="simulationResult-${details.name}"></div>
            </div>
            <div class="details-section">
                <h4>Join Path Finder</h4>
                <div class="path-finder-controls">
                    <select id="pathTarget-${details.name}" class="form-control" style="margin-bottom: 5px;">
                        <option value="">Select target table...</option>
                        ${allTables.map(t => `<option value="${t}">${t}</option>`).join('')}
                    </select>
                    <button class="btn btn-path" onclick="findJoinPath('${details.name}')">Find Path</button>
                </div>
                <div id="pathResult-${details.name}"></div>
            </div>
            <div class="details-section">
                <h4>Columns (${details.columns.length})</h4>
                <ul class="column-list">
                    ${details.columns.map(col => `
                        <li>
                            <strong>${col.name}</strong> (${col.type || 'unknown'})
                            ${col.distinct_count !== undefined ? `<span class="stats">${col.distinct_count} distinct</span>` : ''}
                        </li>
                    `).join('')}
                </ul>
            </div>
            <div class="details-section">
                <h4>Outgoing Relationships (${details.outgoing_edges.length})</h4>
                ${details.outgoing_edges.map(edge => `
                    <div class="edge-info">
                        <span class="kind ${edge.kind}">${edge.kind.toUpperCase()}</span>
                        <strong>${edge.target}</strong>
                        <p>${edge.from_columns.join(', ')} → ${edge.to_columns.join(', ')}</p>
                        ${edge.on_delete ? `<p class="stats">ON DELETE: ${edge.on_delete}</p>` : ''}
                        ${edge.on_update ? `<p class="stats">ON UPDATE: ${edge.on_update}</p>` : ''}
                    </div>
                `).join('')}
            </div>
            <div class="details-section">
                <h4>Incoming Relationships (${details.incoming_edges.length})</h4>
                ${details.incoming_edges.map(edge => `
                    <div class="edge-info">
                        <span class="kind ${edge.kind}">${edge.kind.toUpperCase()}</span>
                        <strong>${edge.source}</strong>
                        <p>${edge.from_columns.join(', ')} → ${edge.to_columns.join(', ')}</p>
                        ${edge.on_delete ? `<p class="stats">ON DELETE: ${edge.on_delete}</p>` : ''}
                        ${edge.on_update ? `<p class="stats">ON UPDATE: ${edge.on_update}</p>` : ''}
                        ${edge.kind === 'inferred' ? `<p class="warning-text">⚠️ No referential constraint - deletion may break joins</p>` : ''}
                    </div>
                `).join('')}
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
    if (!query) {
        window.graphContainer.selectAll('.node').style('opacity', 1);
        return;
    }

    const queryLower = query.toLowerCase();
    window.graphContainer.selectAll('.node')
        .style('opacity', d => d.id.toLowerCase().includes(queryLower) ? 1 : 0.2);
}

function updateStats() {
    document.getElementById('nodeCount').textContent = graphData.nodes.length;
    document.getElementById('edgeCount').textContent = graphData.edges.length;
}

function showStatus(message, type) {
    const statusDiv = document.getElementById('uploadStatus');
    statusDiv.innerHTML = `<div class="status-message ${type}">${message}</div>`;
    setTimeout(() => {
        statusDiv.innerHTML = '';
    }, 5000);
}

async function clearGraph() {
    if (confirm('Are you sure you want to clear the entire graph?')) {
        try {
            await fetch(`${API_BASE}/graph`, { method: 'DELETE' });
            showStatus('Graph cleared', 'success');
            loadGraph();
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
        
        let html = '<div class="impact-result">';
        html += `<p class="success-text">📊 Impact Analysis (Depth: ${depth})</p>`;
        html += `<p><strong>${impact.impact_count}</strong> downstream table(s) would be affected</p>`;
        
        if (impact.impacted_tables && impact.impacted_tables.length > 0) {
            html += '<div class="impacted-tables">';
            html += '<p><strong>Affected Tables:</strong></p>';
            html += '<ul class="impact-list">';
            impact.impacted_tables.forEach(table => {
                html += `<li>${table}</li>`;
            });
            html += '</ul>';
            html += '</div>';
            
            // Highlight impacted tables in graph
            highlightImpactedTables(impact.impacted_tables, tableName);
        }
        
        if (impact.paths && impact.paths.length > 0) {
            html += '<div class="impact-paths" style="margin-top: 10px;">';
            html += '<p><strong>Impact Paths:</strong></p>';
            impact.paths.slice(0, 10).forEach(path => {
                html += `<p class="path-text">${path.path.join(' → ')} (${path.hops} hop${path.hops > 1 ? 's' : ''})</p>`;
            });
            html += '</div>';
        }
        
        html += '</div>';
        resultDiv.innerHTML = html;
    } catch (error) {
        resultDiv.innerHTML = `<div class="simulation-result"><p class="error-text">Error: ${error.message}</p></div>`;
    }
}

function highlightImpactedTables(tables, sourceTable) {
    // Reset all nodes
    window.graphContainer.selectAll('.node').classed('impacted', false).classed('impact-source', false);
    
    // Highlight source
    window.graphContainer.selectAll('.node')
        .filter(d => d.id === sourceTable)
        .classed('impact-source', true);
    
    // Highlight impacted tables
    window.graphContainer.selectAll('.node')
        .filter(d => tables.includes(d.id))
        .classed('impacted', true);
}

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

