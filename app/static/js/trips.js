document.addEventListener('DOMContentLoaded', () => {
    // --- Element Selectors ---
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
    const geocodeProgressContainer = document.getElementById('geocode-progress-container');
    const geocodeProgressBar = document.getElementById('geocode-progress-bar');
    const geocodeProgressText = document.getElementById('geocode-progress-text');
    let currentSort = {
        by: 'start_timestamp',
        direction: 'desc'
    };
    let appConfig = { unit_system: 'metric' };
    let geocodeInterval;

    // --- Unit Conversion Helpers ---
    const KM_TO_MI = 0.621371;
    function l100kmToMpg(l100km) {
        if (l100km <= 0) return 0;
        return 235.214 / l100km;
    }

    async function loadConfig() {
        try {
            const response = await fetch('/api/config');
            if (response.ok) {
                appConfig = await response.json();
            } else {
                console.error("Failed to fetch config, using defaults.");
            }
        } catch (error) {
            console.error("Failed to load application config, using defaults.", error);
        }
    }

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
            const isImperial = appConfig.unit_system.startsWith('imperial');
            const isUk = appConfig.unit_system === 'imperial_uk';
            const units = {
                distance: isImperial ? '(mi)' : '(km)',
                consumption: isImperial ? (isUk ? '(UK MPG)' : '(US MPG)') : '(L/100km)',
                speed: isImperial ? '(mph)' : '(km/h)',
            };
            document.querySelectorAll('.unit').forEach(span => {
                const unitType = span.dataset.unitType;
                if (units[unitType]) {
                    span.textContent = units[unitType];
                }
            });

            const response = await fetch(`/api/trips?vin=${selectedVin}&sort_by=${currentSort.by}&sort_direction=${currentSort.direction}&unit_system=${appConfig.unit_system}`);
            const trips = await response.json();

            renderTable(trips);
        } catch (e) {
            console.error("Failed to load trips:", e);
            tripsTableBody.innerHTML = '<tr><td colspan="11">Error loading trips.</td></tr>';
        }
    }

    function renderTable(trips) {
        const isImperial = appConfig.unit_system.startsWith('imperial');
        const isUk = appConfig.unit_system === 'imperial_uk';
        tripsTableBody.innerHTML = '';

        if (trips.length === 0) {
            tripsTableBody.innerHTML = '<tr><td colspan="11">No trips found for this vehicle.</td></tr>';
            return;
        }

        trips.forEach(trip => {
            const row = document.createElement('tr');

            let embedUrl;
            if (trip.start_lat && trip.start_lon && trip.end_lat && trip.end_lon) {
                embedUrl = `https://maps.google.com/maps?saddr=${trip.start_lat},${trip.start_lon}&daddr=${trip.end_lat},${trip.end_lon}&dirflg=c&output=embed`;
            } else {
                embedUrl = `https://maps.google.com/maps?saddr=${encodeURIComponent(trip.start_address)}&daddr=${encodeURIComponent(trip.end_address)}&dirflg=c&output=embed`;
            }

            const formatTimestamp = (ts) => !ts ? 'N/A' : `${new Date(ts).toLocaleDateString()}<br><span class="unit">${new Date(ts).toLocaleTimeString()}</span>`;
            const formatNumber = (num) => (num === null || num === undefined) ? 'N/A' : Number(num).toFixed(2);
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

            let distance, consumption, avgSpeed, evDistance;
            if (isImperial) {
                distance = trip.distance_mi;
                consumption = isUk ? trip.mpg_uk : trip.mpg;
                avgSpeed = trip.average_speed_mph;
                evDistance = trip.ev_distance_mi;
            } else {
                distance = trip.distance_km;
                consumption = trip.fuel_consumption_l_100km;
                avgSpeed = trip.average_speed_kmh;
                evDistance = trip.ev_distance_km;
            }

            row.innerHTML = `
                <td data-column="start-time">${formatTimestamp(trip.start_timestamp)}</td>
                <td data-column="end-time">${formatTimestamp(trip.end_timestamp)}</td>
                <td data-column="distance">${formatNumber(distance)}</td>
                <td data-column="consumption">${formatNumber(consumption)}</td>
                <td data-column="start-address">${trip.start_address || 'N/A'}</td>
                <td data-column="end-address">${trip.end_address || 'N/A'}</td>
                <td data-column="duration">${formatDuration(trip.duration_seconds)}</td>
                <td data-column="avg-speed">${formatNumber(avgSpeed)}</td>
                <td data-column="ev-dist">${formatNumber(evDistance)}</td>
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

        updateColumnVisibility();
        loadAndApplyColumnOrder();
    }

    function updateSortIndicators() {
        tableHeaderRow.querySelectorAll('th.sortable .sort-indicator').forEach(span => {
            span.textContent = '';
        });

        const activeHeader = tableHeaderRow.querySelector(`th[data-sort="${currentSort.by}"]`);
        if (activeHeader) {
            const indicator = activeHeader.querySelector('.sort-indicator');
            indicator.textContent = currentSort.direction === 'asc' ? ' ▲' : ' ▼';
        }
    }

    const COLUMN_PREF_KEY = 'mytoyota_trip_columns';

    function updateColumnVisibility() {
        const checkboxes = columnSelector.querySelectorAll('input[type="checkbox"]');
        checkboxes.forEach(checkbox => {
            const column = checkbox.dataset.column;
            const display = checkbox.checked ? '' : 'none';
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
                checkbox.checked = preferences[checkbox.dataset.column] !== false;
            });
        }
        updateColumnVisibility();
    }

    const COLUMN_ORDER_KEY = 'mytoyota_trip_column_order';

    function reorderTableBody(order) {
        const bodyRows = Array.from(tripsTableBody.children);
        bodyRows.forEach(row => {
            if (row.children.length > 1) {
                const cells = Array.from(row.children);
                const rowFragment = document.createDocumentFragment();
                order.forEach(columnName => {
                    const cell = cells.find(c => c.dataset.column === columnName);
                    if (cell) rowFragment.appendChild(cell);
                });
                row.innerHTML = '';
                row.appendChild(rowFragment);
            }
        });
    }

    function applyColumnOrder(order) {
        const headers = Array.from(tableHeaderRow.querySelectorAll('th'));
        const fragment = document.createDocumentFragment();
        order.forEach(columnName => {
            const header = headers.find(h => h.dataset.column === columnName);
            if (header) fragment.appendChild(header);
        });
        tableHeaderRow.innerHTML = '';
        tableHeaderRow.appendChild(fragment);

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

    async function updateGeocodeProgress() {
        try {
            const response = await fetch('/api/geocode_status');
            const data = await response.json();

            if (data.pending > 0) {
                geocodeProgressContainer.style.display = 'block';
                const percent = Math.round(((data.total - data.pending) / data.total) * 100);
                geocodeProgressBar.value = percent;
                geocodeProgressText.textContent = `${data.total - data.pending} / ${data.total} trips geocoded (${percent}%).`;
            } else {
                geocodeProgressContainer.style.display = 'none';
                if (geocodeInterval) {
                    clearInterval(geocodeInterval);
                    geocodeInterval = null;
                }
            }
        } catch (error) {
            console.error("Failed to get geocode status:", error);
        }
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
                    loadTrips();
                    if (!geocodeInterval) {
                        geocodeInterval = setInterval(updateGeocodeProgress, 5000);
                    }
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

    new Sortable(tableHeaderRow, {
        animation: 150,
        onEnd: (event) => {
            const headers = Array.from(event.target.querySelectorAll('th'));
            const newOrder = headers.map(th => th.dataset.column);
            localStorage.setItem(COLUMN_ORDER_KEY, JSON.stringify(newOrder));
            reorderTableBody(newOrder);
        }
    });

    async function init() {
        await loadConfig();
        loadColumnPreferences();
        loadAndApplyColumnOrder();
        await loadVins();
        if (vinSelect.value) {
            await loadTrips();
        }
        const initialSortTh = document.querySelector(`th[data-sort="${currentSort.by}"]`);
        if (initialSortTh) {
            initialSortTh.querySelector('.sort-indicator').textContent = ' ▼';
        }
        if (!geocodeInterval) {
            geocodeInterval = setInterval(updateGeocodeProgress, 5000);
        }
    }

    init();
});