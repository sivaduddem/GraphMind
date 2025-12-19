/**
 * Schema Visualizer
 * Handles interactive schema view with draggable tables and adaptive relationship routing
 */

// Global state
let schemaTablePositions = new Map(); // Store table positions for dragging

// Access schemaData from global scope (defined in graph.js)
// This will be set when schema data is loaded

/**
 * Calculate optimal route between two tables
 * Returns an array of points for an orthogonal path
 */
function calculateOptimalRoute(fromLayout, toLayout, fromAnchor, toAnchor) {
    const fromCenterX = fromLayout.x + fromLayout.width / 2;
    const fromCenterY = fromLayout.y + fromLayout.height / 2;
    const toCenterX = toLayout.x + toLayout.width / 2;
    const toCenterY = toLayout.y + toLayout.height / 2;
    
    const dx = toCenterX - fromCenterX;
    const dy = toCenterY - fromCenterY;
    const absDx = Math.abs(dx);
    const absDy = Math.abs(dy);
    
    // Determine which sides to use based on relative positions
    // Use a threshold to decide primary direction
    const isHorizontal = absDx > absDy;
    
    let fromSide, toSide;
    
    if (isHorizontal) {
        // Primarily horizontal relationship
        fromSide = dx > 0 ? 'right' : 'left';
        toSide = dx > 0 ? 'left' : 'right';
    } else {
        // Primarily vertical relationship
        fromSide = dy > 0 ? 'bottom' : 'top';
        toSide = dy > 0 ? 'top' : 'bottom';
    }
    
    // Calculate actual anchor points based on chosen sides
    let actualFromAnchor = { ...fromAnchor };
    let actualToAnchor = { ...toAnchor };
    
    // Adjust from anchor based on best exit side
    switch (fromSide) {
        case 'right':
            actualFromAnchor = {
                x: fromLayout.x + fromLayout.width,
                y: fromAnchor.y
            };
            break;
        case 'left':
            actualFromAnchor = {
                x: fromLayout.x,
                y: fromAnchor.y
            };
            break;
        case 'bottom':
            actualFromAnchor = {
                x: fromAnchor.x,
                y: fromLayout.y + fromLayout.height
            };
            break;
        case 'top':
            actualFromAnchor = {
                x: fromAnchor.x,
                y: fromLayout.y
            };
            break;
    }
    
    // Adjust to anchor based on best entry side
    switch (toSide) {
        case 'left':
            actualToAnchor = {
                x: toLayout.x,
                y: toAnchor.y
            };
            break;
        case 'right':
            actualToAnchor = {
                x: toLayout.x + toLayout.width,
                y: toAnchor.y
            };
            break;
        case 'top':
            actualToAnchor = {
                x: toAnchor.x,
                y: toLayout.y
            };
            break;
        case 'bottom':
            actualToAnchor = {
                x: toAnchor.x,
                y: toLayout.y + toLayout.height
            };
            break;
    }
    
    // Calculate route points for orthogonal path
    const points = [`M${actualFromAnchor.x},${actualFromAnchor.y}`];
    
    // If tables are very close, use direct connection
    const distance = Math.sqrt(
        Math.pow(actualToAnchor.x - actualFromAnchor.x, 2) +
        Math.pow(actualToAnchor.y - actualFromAnchor.y, 2)
    );
    
    if (distance < 30) {
        // Very close - direct line
        points.push(`L${actualToAnchor.x},${actualToAnchor.y}`);
    } else {
        // Use orthogonal routing with smart waypoints
        const minOffset = 15; // Minimum offset from table edges
        
        if (fromSide === 'right' && toSide === 'left') {
            // Source on right, target on left - horizontal route
            const midX = (actualFromAnchor.x + actualToAnchor.x) / 2;
            points.push(`L${midX},${actualFromAnchor.y}`);
            points.push(`L${midX},${actualToAnchor.y}`);
            points.push(`L${actualToAnchor.x},${actualToAnchor.y}`);
        } else if (fromSide === 'left' && toSide === 'right') {
            // Source on left, target on right - horizontal route
            const midX = (actualFromAnchor.x + actualToAnchor.x) / 2;
            points.push(`L${midX},${actualFromAnchor.y}`);
            points.push(`L${midX},${actualToAnchor.y}`);
            points.push(`L${actualToAnchor.x},${actualToAnchor.y}`);
        } else if (fromSide === 'bottom' && toSide === 'top') {
            // Source on bottom, target on top - vertical route
            const midY = (actualFromAnchor.y + actualToAnchor.y) / 2;
            points.push(`L${actualFromAnchor.x},${midY}`);
            points.push(`L${actualToAnchor.x},${midY}`);
            points.push(`L${actualToAnchor.x},${actualToAnchor.y}`);
        } else if (fromSide === 'top' && toSide === 'bottom') {
            // Source on top, target on bottom - vertical route
            const midY = (actualFromAnchor.y + actualToAnchor.y) / 2;
            points.push(`L${actualFromAnchor.x},${midY}`);
            points.push(`L${actualToAnchor.x},${midY}`);
            points.push(`L${actualToAnchor.x},${actualToAnchor.y}`);
        } else {
            // Mixed directions - use L-shaped route
            if (fromSide === 'right' || fromSide === 'left') {
                // Move horizontally first
                const offsetX = fromSide === 'right' 
                    ? actualFromAnchor.x + minOffset 
                    : actualFromAnchor.x - minOffset;
                points.push(`L${offsetX},${actualFromAnchor.y}`);
                // Then move vertically
                points.push(`L${offsetX},${actualToAnchor.y}`);
                // Finally move to target
                points.push(`L${actualToAnchor.x},${actualToAnchor.y}`);
            } else {
                // Move vertically first
                const offsetY = fromSide === 'bottom' 
                    ? actualFromAnchor.y + minOffset 
                    : actualFromAnchor.y - minOffset;
                points.push(`L${actualFromAnchor.x},${offsetY}`);
                // Then move horizontally
                points.push(`L${actualToAnchor.x},${offsetY}`);
                // Finally move to target
                points.push(`L${actualToAnchor.x},${actualToAnchor.y}`);
            }
        }
    }
    
    return points.join(' ');
}

/**
 * Get anchor point for a column on a specific side of a table
 */
function getColumnAnchor(layout, columnName, side, columnAnchors) {
    const headerHeight = 32;
    const rowHeight = 18;
    const verticalPadding = 10;
    
    // Find column index
    const colIndex = layout.columns.findIndex(col => col.name === columnName);
    if (colIndex === -1) {
        // Column not found, use center
        return {
            x: layout.x + layout.width / 2,
            y: layout.y + layout.height / 2
        };
    }
    
    const colY = layout.y + headerHeight + verticalPadding + (colIndex + 0.5) * rowHeight;
    
    switch (side) {
        case 'left':
            return { x: layout.x, y: colY };
        case 'right':
            return { x: layout.x + layout.width, y: colY };
        case 'top':
            return { x: layout.x + layout.width / 2, y: layout.y };
        case 'bottom':
            return { x: layout.x + layout.width / 2, y: layout.y + layout.height };
        default:
            // Use existing column anchor if available
            if (columnAnchors && columnAnchors[columnName]) {
                return columnAnchors[columnName].right || columnAnchors[columnName].left;
            }
            return { x: layout.x + layout.width, y: colY };
    }
}

/**
 * Generate path for a relationship link
 */
function generateLinkPath(d, tableLayout) {
    const fromLayout = tableLayout.get(d.from_table);
    const toLayout = tableLayout.get(d.to_table);
    if (!fromLayout || !toLayout) return null;

    const fromCol = d._fromCol || (d.from_columns || [])[0];
    const toCol = d._toCol || (d.to_columns || [])[0];

    // Get column anchors (will be adjusted by routing algorithm)
    const fromAnchor = getColumnAnchor(
        fromLayout, 
        fromCol, 
        'right', 
        fromLayout.columnAnchors
    );
    const toAnchor = getColumnAnchor(
        toLayout, 
        toCol, 
        'left', 
        toLayout.columnAnchors
    );

    // Calculate optimal route
    return calculateOptimalRoute(fromLayout, toLayout, fromAnchor, toAnchor);
}

/**
 * Update relationship links when tables are moved
 */
function updateSchemaLinks() {
    const linksGroup = window.schemaLinksGroup;
    const tableLayout = window.schemaTableLayout;
    if (!linksGroup || !tableLayout) return;
    
    // Access schemaData from global scope
    const relationships = (window.schemaData || {}).relationships || [];
    const schemaShowFK = document.getElementById('schemaShowFK');
    const schemaShowInferred = document.getElementById('schemaShowInferred');
    const showFK = schemaShowFK ? schemaShowFK.checked : true;
    const showInferred = schemaShowInferred ? schemaShowInferred.checked : true;
    
    const visibleRels = relationships.filter(rel => {
        if (rel.kind === 'fk' && !showFK) return false;
        if (rel.kind === 'inferred' && !showInferred) return false;
        return true;
    });
    
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
    
    // Update existing links
    const links = linksGroup.selectAll('path.schema-link')
        .data(linkSegments, d => `${d.from_table}-${d.to_table}-${d._fromCol}-${d._toCol}`);
    
    // Update path for existing links using smart routing
    links.attr('d', d => generateLinkPath(d, tableLayout));
}

/**
 * Drag handlers for schema tables
 */
function schemaDragStarted(event, d) {
    event.sourceEvent.stopPropagation();
}

function schemaDragged(event, d) {
    // event.x and event.y are already in the container's coordinate system
    const newX = event.x;
    const newY = event.y;
    
    // Update stored position
    schemaTablePositions.set(d.name, { x: newX, y: newY });
    
    // Update table layout
    const layout = window.schemaTableLayout.get(d.name);
    if (layout) {
        layout.x = newX;
        layout.y = newY;
        
        // Update column anchors
        const headerHeight = 32;
        const rowHeight = 18;
        const verticalPadding = 10;
        layout.columnAnchors = {};
        (d.columns || []).forEach((col, cIdx) => {
            const cy = newY + headerHeight + verticalPadding + (cIdx + 0.5) * rowHeight;
            layout.columnAnchors[col.name] = {
                left: { x: newX, y: cy },
                right: { x: newX + layout.width, y: cy }
            };
        });
    }
    
    // Update table transform
    d3.select(this)
        .attr('transform', `translate(${newX},${newY})`);
    
    // Update relationship links with adaptive routing
    updateSchemaLinks();
}

function schemaDragEnded(event, d) {
    // Nothing special needed on drag end
}

/**
 * Initialize schema view
 */
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

/**
 * Render schema view with draggable tables and adaptive routing
 */
function renderSchema() {
    const container = window.schemaContainer;
    // Access schemaData from global scope (defined in graph.js)
    if (!container || !window.schemaData || !window.schemaData.tables) return;
    
    const schemaData = window.schemaData;

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
        // Use stored position if available, otherwise calculate grid position
        let x, y;
        if (schemaTablePositions.has(table.name)) {
            const stored = schemaTablePositions.get(table.name);
            x = stored.x;
            y = stored.y;
        } else {
            const colIndex = index % cols;
            const rowIndex = Math.floor(index / cols);
            x = 80 + colIndex * (cardWidth + hGap);
            y = 80 + rowIndex * (cardHeight + vGap);
            // Store initial position
            schemaTablePositions.set(table.name, { x, y });
        }

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
        .data(tables, d => d.name);
    
    // Enter: create new table groups
    const tableGroupsEnter = tableGroups.enter()
        .append('g')
        .attr('class', 'schema-table');
    
    // Merge enter and update selections
    const tableGroupsMerged = tableGroupsEnter.merge(tableGroups);
    
    // Update transform for all (new and existing)
    tableGroupsMerged.attr('transform', d => {
        const layout = tableLayout.get(d.name);
        return `translate(${layout.x},${layout.y})`;
    });
    
    // Apply event handlers and drag to merged selection
    tableGroupsMerged
        .call(d3.drag()
            .on('start', schemaDragStarted)
            .on('drag', schemaDragged)
            .on('end', schemaDragEnded))
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

    // Only append to new elements (enter selection)
    tableGroupsEnter
        .append('rect')
        .attr('class', 'schema-table-body')
        .attr('width', d => tableLayout.get(d.name).width)
        .attr('height', d => tableLayout.get(d.name).height);

    tableGroupsEnter
        .append('rect')
        .attr('class', 'schema-table-header')
        .attr('width', d => tableLayout.get(d.name).width)
        .attr('height', headerHeight);

    tableGroupsEnter
        .append('text')
        .attr('class', 'schema-table-title')
        .attr('x', 12)
        .attr('y', headerHeight / 2 + 4)
        .text(d => d.name);

    tableGroupsEnter.each(function (d) {
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

    const linksGroup = container
        .append('g')
        .attr('class', 'schema-links');

    linksGroup
        .selectAll('path')
        .data(linkSegments)
        .enter()
        .append('path')
        .attr('class', d => `schema-link ${d.kind}`)
        .attr('d', d => generateLinkPath(d, tableLayout))
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

    tableGroupsMerged.raise();
    
    // Store reference to tableLayout and linksGroup for drag updates
    window.schemaTableLayout = tableLayout;
    window.schemaLinksGroup = linksGroup;
}

/**
 * Reset schema zoom
 */
function resetSchemaZoom() {
    if (!window.schemaSvg || !window.schemaZoom) return;
    window.schemaSvg.transition().duration(300).call(window.schemaZoom.transform, d3.zoomIdentity);
}

// Export functions to global scope for use in graph.js
window.schemaVisualizer = {
    initializeSchema,
    renderSchema,
    resetSchemaZoom,
    updateSchemaLinks,
    schemaTablePositions
};
