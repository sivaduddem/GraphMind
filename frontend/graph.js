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
    document.getElementById('confidenceSlider').addEventListener('input', (e) => {
        document.getElementById('confidenceValue').textContent = e.target.value;
        loadGraph(parseFloat(e.target.value));
    });

    document.getElementById('showFK').addEventListener('change', () => renderGraph());
    document.getElementById('showInferred').addEventListener('change', () => renderGraph());

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

async function loadGraph(minConfidence = 0) {
    try {
        const response = await fetch(`${API_BASE}/graph?min_confidence=${minConfidence}`);
        graphData = await response.json();
        updateStats();
        renderGraph();
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

    // Update visual selection
    window.graphContainer.selectAll('.node').classed('selected', false);
    window.graphContainer.selectAll('.link').classed('selected', false);
    
    window.graphContainer.selectAll('.node')
        .filter(d => d.id === node.id)
        .classed('selected', true);

    // Show details panel
    showNodeDetails(node);
}

function selectEdge(edge) {
    selectedEdge = edge;
    selectedNode = null;

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
            <div class="details-section">
                <h4>Simulation</h4>
                <button class="btn btn-simulate" onclick="simulateDelete('${details.name}')">Simulate DELETE</button>
                <button class="btn btn-simulate" onclick="simulateUpdate('${details.name}')">Simulate UPDATE</button>
                <div id="simulationResult-${details.name}"></div>
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
            ${edges.map(e => `
                <div class="details-section">
                    <h4>Edge Details</h4>
                    <div class="edge-info">
                        <span class="kind ${e.kind}">${e.kind.toUpperCase()}</span>
                        <p><strong>Columns:</strong> ${e.from_columns.join(', ')} → ${e.to_columns.join(', ')}</p>
                        ${e.on_delete ? `<p class="stats"><strong>ON DELETE:</strong> ${e.on_delete}</p>` : ''}
                        ${e.on_update ? `<p class="stats"><strong>ON UPDATE:</strong> ${e.on_update}</p>` : ''}
                        ${e.confidence !== undefined ? `<p><strong>Confidence:</strong> ${(e.confidence * 100).toFixed(1)}%</p>` : ''}
                        ${e.kind === 'inferred' ? `
                            <div class="simulation-warning" style="margin-top: 10px; padding: 8px;">
                                <strong>⚠️ Warning:</strong> This is an inferred relationship (not enforced by schema).
                                <p style="margin: 5px 0 0 0;">Deletion/update operations may succeed but break logical joins.</p>
                            </div>
                        ` : ''}
                        ${e.stats ? `
                            <div class="stats">
                                <p>Name Similarity: ${(e.stats.name_similarity * 100).toFixed(1)}%</p>
                                <p>Profile Match: ${(e.stats.profile_match * 100).toFixed(1)}%</p>
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

function closeDetailsPanel() {
    document.getElementById('detailsPanel').classList.remove('visible');
    selectedNode = null;
    selectedEdge = null;
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

