document.addEventListener('DOMContentLoaded', () => {
    const vinSelect = document.getElementById('vin-select');
    const tripsTableBody = document.getElementById('trips-table-body');
    const tableHeaderRow = document.getElementById('trip-table-header-row');
    const mapPanel = document.getElementById('map-panel');
    const mapPanelContent = document.getElementById('map-panel-content');
    const mapPanelTitle = document.getElementById('map-panel-title');
    const closeMapPanelBtn = document.getElementById('close-map-panel-btn');
    const columnSelector = document.getElementById('column-selector');
    const backfillControls = document.querySelector('.backfill-buttons');
    const backfillStatusMessage = document.getElementById('backfill-status-message');
    let currentSort = {
        by: 'start_timestamp',
        direction: 'desc'
    };

    async function loadVins() {
        try {
            const response = await fetch('/api/vehicles');
            const vehicles = await response.json();
            vinSelect.innerHTML = ''; // Clear
            if (vehicles.length > 0) {
                vehicles.forEach(vehicle => {
                    const option = document.createElement('option');
                    option.value = vehicle.vin;
                    option.textContent = `${vehicle.alias} (${vehicle.vin})`;
                    vinSelect.appendChild(option);
                });
                // Automatically load trips for the first vehicle
                loadTrips();
            } else {
                vinSelect.innerHTML = '<option>No vehicles found</option>';
                tripsTableBody.innerHTML = '<tr><td colspan="11">No vehicles found.</td></tr>';
            }
        } catch (e) {
            tripsTableBody.innerHTML = '<tr><td colspan="11">Could not load vehicle list.</td></tr>';
        }
    }

    async function loadTrips() {
        const selectedVin = vinSelect.value;
        if (!selectedVin) {
            tripsTableBody.innerHTML = '<tr><td colspan="11">Please select a vehicle.</td></tr>';
            return;
        }

        tripsTableBody.innerHTML = '<tr><td colspan="11">Loading...</td></tr>';
        mapPanel.style.display = 'none'; // Hide map when loading new trips

        updateSortIndicators();

        try {
            const response = await fetch(`/api/trips?vin=${selectedVin}&sort_by=${currentSort.by}&sort_direction=${currentSort.direction}`);
            const trips = await response.json();

            tripsTableBody.innerHTML = ''; // Clear loading message

            if (trips.length === 0) {
                tripsTableBody.innerHTML = '<tr><td colspan="11">No trips found for this vehicle.</td></tr>';
                return;
            }

            trips.forEach(trip => {
                const row = document.createElement('tr');

                // Use a Google Maps URL format that is specifically designed for embedding to avoid "refused to connect" errors.
                let embedUrl;
                if (trip.start_lat && trip.start_lon && trip.end_lat && trip.end_lon) {
                    embedUrl = `https://maps.google.com/maps?saddr=${trip.start_lat},${trip.start_lon}&daddr=${trip.end_lat},${trip.end_lon}&dirflg=c&output=embed`;
                } else {
                    embedUrl = `https://maps.google.com/maps?saddr=${encodeURIComponent(trip.start_address)}&daddr=${encodeURIComponent(trip.end_address)}&dirflg=c&output=embed`;
                }

                const formatTimestamp = (ts) => {
                    if (!ts) return 'N/A';
                    const d = new Date(ts);
                    return `${d.toLocaleDateString()}<br><span class="unit">${d.toLocaleTimeString()}</span>`;
                };
                const formatNumber = (num) => {
                    if (num === null || num === undefined) return 'N/A';
                    return Number(num).toFixed(2);
                };
                const formatDuration = (seconds) => {
                    if (seconds === null || seconds === undefined || seconds === 0) return 'N/A';
                    const h = Math.floor(seconds / 3600);
                    const m = Math.floor((seconds % 3600) / 60);
                    const s = Math.floor(seconds % 60);
                    let parts = [];
                    if (h > 0) parts.push(`${h}h`);
                    if (m > 0) parts.push(`${m}m`);
                    if (s > 0 || parts.length === 0) parts.push(`${s}s`);
                    return parts.join(' ');
                };

                row.innerHTML = `
                    <td data-column="start-time">${formatTimestamp(trip.start_timestamp)}</td>
                    <td data-column="end-time">${formatTimestamp(trip.end_timestamp)}</td>
                    <td data-column="distance">${formatNumber(trip.distance_km)}</td>
                    <td data-column="consumption">${formatNumber(trip.fuel_consumption_l_100km)}</td>
                    <td data-column="start-address">${trip.start_address || 'N/A'}</td>
                    <td data-column="end-address">${trip.end_address || 'N/A'}</td>
                    <td data-column="duration">${formatDuration(trip.duration_seconds)}</td>
                    <td data-column="avg-speed">${formatNumber(trip.average_speed_kmh)}</td>
                    <td data-column="ev-dist">${formatNumber(trip.ev_distance_km)}</td>
                    <td data-column="ev-dur">${formatDuration(trip.ev_duration_seconds)}</td>
                    <td data-column="score">${trip.score_global || 'N/A'}</td>
                `;

                row.addEventListener('click', () => {
                    mapPanelTitle.textContent = `Trip from ${formatTimestamp(trip.start_timestamp)}`;
                    mapPanelContent.innerHTML = `<iframe src="${embedUrl}"></iframe>`;
                    mapPanel.style.display = 'block';
                });

                tripsTableBody.appendChild(row);
            });
            // Apply column visibility after rows are rendered
            updateColumnVisibility();
            // Re-apply the saved column order to the newly rendered rows
            loadAndApplyColumnOrder();
        } catch (e) {
            console.error("Failed to load trips:", e);
            tripsTableBody.innerHTML = '<tr><td colspan="11">Error loading trips.</td></tr>';
        }
    }

    function updateSortIndicators() {
        // Remove indicators from all headers
        tableHeaderRow.querySelectorAll('th.sortable .sort-indicator').forEach(span => {
            span.textContent = '';
        });

        // Add indicator to the active column
        const activeHeader = tableHeaderRow.querySelector(`th[data-sort="${currentSort.by}"]`);
        if (activeHeader) {
            const indicator = activeHeader.querySelector('.sort-indicator');
            indicator.textContent = currentSort.direction === 'asc' ? ' ▲' : ' ▼';
        }
    }


    // --- Column Visibility Functions ---
    const COLUMN_PREF_KEY = 'mytoyota_trip_columns';

    function updateColumnVisibility() {
        const checkboxes = columnSelector.querySelectorAll('input[type="checkbox"]');
        checkboxes.forEach(checkbox => {
            const column = checkbox.dataset.column;
            const display = checkbox.checked ? '' : 'none';
            // Select all table headers and cells for this column
            const cells = document.querySelectorAll(`[data-column="${column}"]`);
            cells.forEach(cell => {
                cell.style.display = display;
            });
        });
    }

    function saveColumnPreferences() {
        const preferences = {};
        const checkboxes = columnSelector.querySelectorAll('input[type="checkbox"]');
        checkboxes.forEach(checkbox => {
            preferences[checkbox.dataset.column] = checkbox.checked;
        });
        localStorage.setItem(COLUMN_PREF_KEY, JSON.stringify(preferences));
    }

    function loadColumnPreferences() {
        const preferences = JSON.parse(localStorage.getItem(COLUMN_PREF_KEY));
        if (preferences) {
            const checkboxes = columnSelector.querySelectorAll('input[type="checkbox"]');
            checkboxes.forEach(checkbox => {
                // If a preference exists for this column, use it. Otherwise, default to checked.
                checkbox.checked = preferences[checkbox.dataset.column] !== false;
            });
        }
        updateColumnVisibility();
    }

    // --- Column Drag-and-Drop Functions ---
    const COLUMN_ORDER_KEY = 'mytoyota_trip_column_order';

    function reorderTableBody(order) {
        const bodyRows = Array.from(tripsTableBody.children);
        bodyRows.forEach(row => {
            // Guard against empty/placeholder rows
            if (row.children.length > 1) {
                const cells = Array.from(row.children);
                const rowFragment = document.createDocumentFragment();
                order.forEach(columnName => {
                    const cell = cells.find(c => c.dataset.column === columnName);
                    if (cell) rowFragment.appendChild(cell);
                });
                row.innerHTML = ''; // Clear existing cells before re-ordering
                row.appendChild(rowFragment);
            }
        });
    }

    function applyColumnOrder(order) {
        // Reorder headers
        const headers = Array.from(tableHeaderRow.querySelectorAll('th'));
        const fragment = document.createDocumentFragment();
        order.forEach(columnName => {
            const header = headers.find(h => h.dataset.column === columnName);
            if (header) fragment.appendChild(header);
        });
        tableHeaderRow.innerHTML = ''; // Clear existing headers
        tableHeaderRow.appendChild(fragment);

        // Reorder all body rows
        reorderTableBody(order);
    }

    function loadAndApplyColumnOrder() {
        const savedOrder = localStorage.getItem(COLUMN_ORDER_KEY);
        if (savedOrder) {
            try {
                applyColumnOrder(JSON.parse(savedOrder));
            } catch (e) {
                console.error("Failed to apply saved column order.", e);
            }
        }
    }

    function showBackfillStatus(message, type) {
        backfillStatusMessage.textContent = message;
        backfillStatusMessage.className = `status-message ${type}`;
        backfillStatusMessage.style.display = 'block';
    }

    closeMapPanelBtn.addEventListener('click', () => {
        mapPanel.style.display = 'none';
    });

    tableHeaderRow.addEventListener('click', (event) => {
        const headerCell = event.target.closest('th.sortable');
        if (!headerCell) return;

        const sortBy = headerCell.dataset.sort;
        if (currentSort.by === sortBy) {
            currentSort.direction = currentSort.direction === 'asc' ? 'desc' : 'asc';
        } else {
            currentSort.by = sortBy;
            currentSort.direction = 'desc';
        }
        loadTrips();
    });

    vinSelect.addEventListener('change', loadTrips);
    columnSelector.addEventListener('change', () => {
        updateColumnVisibility();
        saveColumnPreferences();
    });

    // Handle backfill button clicks
    backfillControls.addEventListener('click', async (event) => {
        if (event.target.tagName === 'BUTTON') {
            const vin = vinSelect.value;
            const period = event.target.dataset.period;

            if (!vin) {
                showBackfillStatus('Please select a vehicle.', 'error');
                return;
            }

            const button = event.target;
            button.disabled = true;
            button.textContent = 'Fetching...';
            showBackfillStatus(`Fetching trips for '${period}'... This may take a moment.`, 'info');

            try {
                const response = await fetch(`/api/vehicles/${vin}/fetch_trips`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ period: period })
                });

                const result = await response.json();
                if (response.ok) {
                    showBackfillStatus(result.message, 'success');
                    loadTrips(); // Refresh the trip list on success
                } else {
                    showBackfillStatus(`Error: ${result.detail}`, 'error');
                }
            } catch (error) {
                showBackfillStatus('An unexpected error occurred during the fetch.', 'error');
            } finally {
                button.disabled = false;
                button.textContent = `Fetch Last ${period.charAt(0).toUpperCase() + period.slice(1)}`;
            }
        }
    });

    // Initialize SortableJS for column dragging
    new Sortable(tableHeaderRow, {
        animation: 150,
        onEnd: (event) => {
            // SortableJS has already reordered the headers in the DOM.
            // We just need to save the new order and reorder the table body to match.
            const headers = Array.from(event.target.querySelectorAll('th'));
            const newOrder = headers.map(th => th.dataset.column);
            localStorage.setItem(COLUMN_ORDER_KEY, JSON.stringify(newOrder));
            reorderTableBody(newOrder);
        }
    });

    loadColumnPreferences();
    loadAndApplyColumnOrder();
    loadVins();
});