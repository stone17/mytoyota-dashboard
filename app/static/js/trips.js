document.addEventListener('DOMContentLoaded', () => {
    // --- Element Selectors ---
    const vinSelect = document.getElementById('vin-select');
    const tripsTableBody = document.getElementById('trips-table-body');
    const tableHeaderRow = document.getElementById('trip-table-header-row');
    const mapPanel = document.getElementById('map-panel');
    const mapPanelTitle = document.getElementById('map-panel-title');
    const mapTripStats = document.getElementById('map-trip-stats');
    const closeMapPanelBtn = document.getElementById('close-map-panel-btn');
    const prevTripBtn = document.getElementById('prev-trip-btn');
    const nextTripBtn = document.getElementById('next-trip-btn');
    const columnSelector = document.getElementById('column-selector');
    const backfillControls = document.querySelector('.backfill-buttons');
    const backfillStatusMessage = document.getElementById('backfill-status-message');
    const geocodeProgressContainer = document.getElementById('geocode-progress-container');
    const geocodeProgressBar = document.getElementById('geocode-progress-bar');
    const geocodeProgressText = document.getElementById('geocode-progress-text');
    const periodSelect = document.getElementById('period-select');
    
    let currentSort = { by: 'start_timestamp', direction: 'desc' };
    let appConfig = { unit_system: 'metric' };
    let geocodeInterval;
    let displayedTrips = [];
    let currentTripContext = { tripId: null, rowIndex: -1 };

    // --- Map variables ---
    let map;
    let currentMapLayers = [];

    // --- Custom Map Icons ---
    const startIcon = new L.Icon({
        iconUrl: 'https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-2x-green.png',
        shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/0.7.7/images/marker-shadow.png',
        iconSize: [25, 41], iconAnchor: [12, 41], popupAnchor: [1, -34], shadowSize: [41, 41]
    });
    const endIcon = new L.Icon({
        iconUrl: 'https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-2x-red.png',
        shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/0.7.7/images/marker-shadow.png',
        iconSize: [25, 41], iconAnchor: [12, 41], popupAnchor: [1, -34], shadowSize: [41, 41]
    });

    // --- Initialize the map with layer control ---
    function initMap() {
        const streets = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { maxZoom: 19, attribution: '© OpenStreetMap contributors' });
        const satellite = L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', { maxZoom: 19, attribution: 'Tiles &copy; Esri' });
        const dark = L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', { attribution: '&copy; OpenStreetMap contributors &copy; CARTO', subdomains: 'abcd', maxZoom: 20 });
        map = L.map('map', { center: [55.7, 13.2], zoom: 9, layers: [streets] });
        L.control.layers({ "Streets": streets, "Satellite": satellite, "Dark": dark }).addTo(map);
    }

    function updateUnitHeaders() {
        const isImperial = appConfig.unit_system.startsWith('imperial');
        const isUk = appConfig.unit_system === 'imperial_uk';
        const distanceUnits = document.querySelectorAll('th .unit[data-unit-type="distance"]');
        const speedUnit = document.querySelector('th .unit[data-unit-type="speed"]');
        const consumptionUnit = document.querySelector('th .unit[data-unit-type="consumption"]');
        distanceUnits.forEach(span => { span.textContent = isImperial ? 'mi' : 'km'; });
        if (speedUnit) { speedUnit.textContent = isImperial ? 'mph' : 'km/h'; }
        if (consumptionUnit) { consumptionUnit.textContent = isImperial ? (isUk ? 'mpg (UK)' : 'mpg (US)') : 'L/100km'; }
    }

    function plotGpsRoute(routePoints) {
        currentMapLayers.forEach(layer => map.removeLayer(layer));
        currentMapLayers = [];
        const latLngs = routePoints.map(p => [p.lat, p.lon]);
        const polyline = L.polyline(latLngs, { color: '#00529b', weight: 5 }).addTo(map);
        currentMapLayers.push(polyline);
        const startMarker = L.marker(latLngs[0], {icon: startIcon}).addTo(map).bindPopup('<b>Start of Trip</b>');
        currentMapLayers.push(startMarker);
        const endMarker = L.marker(latLngs[latLngs.length - 1], {icon: endIcon}).addTo(map).bindPopup('<b>End of Trip</b>');
        currentMapLayers.push(endMarker);
        map.fitBounds(polyline.getBounds().pad(0.1));
    }

    function plotEstimatedRoute(startLat, startLon, endLat, endLon) {
        currentMapLayers.forEach(layer => map.removeLayer(layer));
        currentMapLayers = [];
        if (!startLat || !startLon || !endLat || !endLon) return;
        const startLatLng = [startLat, startLon];
        const endLatLng = [endLat, endLon];
        const polyline = L.polyline([startLatLng, endLatLng], { color: '#d9534f', weight: 3, dashArray: '10, 10' }).addTo(map);
        currentMapLayers.push(polyline);
        const startMarker = L.marker(startLatLng, {icon: startIcon}).addTo(map).bindPopup('<b>Trip Start</b><br>(Estimated Route)');
        currentMapLayers.push(startMarker);
        const endMarker = L.marker(endLatLng, {icon: endIcon}).addTo(map).bindPopup('<b>Trip End</b><br>(Estimated Route)');
        currentMapLayers.push(endMarker);
        map.fitBounds([startLatLng, endLatLng], { padding: [50, 50] });
    }

    async function loadConfig() {
        try {
            const response = await fetch('/api/config');
            appConfig = response.ok ? await response.json() : { unit_system: 'metric' };
        } catch (error) { console.error("Failed to load application config, using defaults.", error); }
    }

    async function loadVins() {
        try {
            const response = await fetch('/api/vehicles');
            const vehicles = await response.json();
            vinSelect.innerHTML = '';
            if (vehicles.length > 0) {
                vehicles.forEach(vehicle => { vinSelect.appendChild(new Option(`${vehicle.alias} (${vehicle.vin})`, vehicle.vin)); });
            } else { vinSelect.innerHTML = '<option>No vehicles found</option>'; }
        } catch (e) { tripsTableBody.innerHTML = `<tr><td colspan="11">Could not load vehicle list: ${e.message}</td></tr>`; }
    }

    async function loadTrips() {
        const selectedVin = vinSelect.value;
        if (!selectedVin) return;
        tripsTableBody.innerHTML = '<tr><td colspan="11" style="text-align:center;">Loading trips...</td></tr>';
        mapPanel.style.display = 'none';
        updateSortIndicators();
        try {
            const periodDays = periodSelect.value;
            let startDate = '';
            if (periodDays !== 'all') {
                const date = new Date();
                date.setDate(date.getDate() - parseInt(periodDays, 10));
                startDate = date.toISOString().split('T')[0];
            }
            const params = new URLSearchParams({ vin: selectedVin, sort_by: currentSort.by, sort_direction: currentSort.direction, unit_system: appConfig.unit_system });
            if (startDate) { params.append('start_date', startDate); }
            const response = await fetch(`/api/trips?${params.toString()}`);
            const trips = await response.json();
            renderTable(trips);
        } catch (e) { tripsTableBody.innerHTML = `<tr><td colspan="11">Error loading trips: ${e.message}</td></tr>`; }
    }

    const isMobile = () => window.matchMedia('(max-width: 1024px)').matches;

    function updateNavButtonsState() {
        const isOverlay = mapPanel.classList.contains('is-overlay');
        if (!isOverlay) return;
        prevTripBtn.disabled = currentTripContext.rowIndex <= 0;
        nextTripBtn.disabled = currentTripContext.rowIndex >= displayedTrips.length - 1;
    }

    async function showTripOnMap(trip, rowIndex) {
        if (!trip) return;
        currentTripContext = { tripId: trip.id, rowIndex: rowIndex };
        mapPanelTitle.textContent = `Trip on ${new Date(trip.start_timestamp).toLocaleDateString()}`;
        if (isMobile()) {
            mapPanel.classList.add('is-overlay');
            
            // Calculate and display stats
            const isImperial = appConfig.unit_system.startsWith('imperial');
            const isUk = appConfig.unit_system === 'imperial_uk';
            const distance = isImperial ? trip.distance_mi : trip.distance_km;
            const consumption = isImperial ? (isUk ? trip.mpg_uk : trip.mpg) : trip.fuel_consumption_l_100km;
            const evRatio = (trip.distance_km > 0 && trip.ev_distance_km) ? (trip.ev_distance_km / trip.distance_km * 100) : 0;
            const duration = new Date(trip.duration_seconds * 1000).toISOString().slice(11, 19);

            mapTripStats.innerHTML = `
                <div><span>Time:</span> <strong>${duration}</strong></div>
                <div><span>Dist:</span> <strong>${distance.toFixed(1)} ${isImperial ? 'mi' : 'km'}</strong></div>
                <div><span>EV:</span> <strong>${evRatio.toFixed(0)}%</strong></div>
                <div><span>Fuel:</span> <strong>${consumption.toFixed(1)} ${isImperial ? 'mpg' : 'L/100km'}</strong></div>
            `;
        } else {
            mapPanel.classList.remove('is-overlay');
        }
        mapPanel.style.display = isMobile() ? 'flex' : 'block';
        updateNavButtonsState();
        currentMapLayers.forEach(layer => map.removeLayer(layer));
        currentMapLayers = [];
        const loadingPopup = L.popup().setLatLng(map.getCenter()).setContent('Loading route...').openOn(map);
        setTimeout(async () => {
            map.invalidateSize();
            try {
                const response = await fetch(`/api/trips/${trip.id}/route`);
                if (!response.ok) throw new Error('Route data not found');
                const data = await response.json();
                map.closePopup(loadingPopup);
                if (data.route && data.route.length > 0) { plotGpsRoute(data.route); } 
                else { plotEstimatedRoute(trip.start_lat, trip.start_lon, trip.end_lat, trip.end_lon); }
            } catch (e) {
                map.closePopup(loadingPopup);
                console.error(`Error loading trip route: ${e.message}`);
                plotEstimatedRoute(trip.start_lat, trip.start_lon, trip.end_lat, trip.end_lon);
            }
        }, 50);
    }

    function renderTable(trips) {
        displayedTrips = trips;
        const isImperial = appConfig.unit_system.startsWith('imperial');
        const isUk = appConfig.unit_system === 'imperial_uk';
        tripsTableBody.innerHTML = '';
        if (trips.length === 0) {
            tripsTableBody.innerHTML = '<tr><td colspan="11">No trips found for this vehicle.</td></tr>';
            return;
        }
        trips.forEach((trip, index) => {
            const row = tripsTableBody.insertRow();
            const formatTimestamp = (ts) => !ts ? 'N/A' : `${new Date(ts).toLocaleDateString()}<br><span class="unit">${new Date(ts).toLocaleTimeString()}</span>`;
            const formatNumber = (num) => (num === null || num === undefined) ? 'N/A' : Number(num).toFixed(2);
            const formatDuration = (s) => !s ? 'N/A' : new Date(s * 1000).toISOString().slice(11, 19);
            row.innerHTML = `
                <td data-column="start-time">${formatTimestamp(trip.start_timestamp)}</td>
                <td data-column="end-time">${formatTimestamp(trip.end_timestamp)}</td>
                <td data-column="distance">${formatNumber(isImperial ? trip.distance_mi : trip.distance_km)}</td>
                <td data-column="consumption">${formatNumber(isImperial ? (isUk ? trip.mpg_uk : trip.mpg) : trip.fuel_consumption_l_100km)}</td>
                <td data-column="start-address">${trip.start_address || 'N/A'}</td>
                <td data-column="end-address">${trip.end_address || 'N/A'}</td>
                <td data-column="duration">${formatDuration(trip.duration_seconds)}</td>
                <td data-column="avg-speed">${formatNumber(isImperial ? trip.average_speed_mph : trip.average_speed_kmh)}</td>
                <td data-column="ev-dist">${formatNumber(isImperial ? trip.ev_distance_mi : trip.ev_distance_km)}</td>
                <td data-column="ev-dur">${formatDuration(trip.ev_duration_seconds)}</td>
                <td data-column="score">${trip.score_global || 'N/A'}</td>`;
            row.addEventListener('click', () => showTripOnMap(trip, index));
        });
        updateColumnVisibility();
        loadAndApplyColumnOrder();
    }

    function savePeriodAndReload() {
        localStorage.setItem('mytoyota_trip_period', periodSelect.value);
        loadTrips();
    }
    
    function updateSortIndicators() {
        tableHeaderRow.querySelectorAll('th.sortable .sort-indicator').forEach(span => span.textContent = '');
        const activeHeader = tableHeaderRow.querySelector(`th[data-sort="${currentSort.by}"]`);
        if (activeHeader) { activeHeader.querySelector('.sort-indicator').textContent = currentSort.direction === 'asc' ? ' ▲' : ' ▼'; }
    }

    const COLUMN_PREF_KEY = 'mytoyota_trip_columns';
    function updateColumnVisibility() {
        columnSelector.querySelectorAll('input[type="checkbox"]').forEach(checkbox => {
            const column = checkbox.dataset.column;
            const display = checkbox.checked ? '' : 'none';
            document.querySelectorAll(`.trips-list-container th[data-column="${column}"], .trips-list-container td[data-column="${column}"]`).forEach(cell => { cell.style.display = display; });
        });
    }

    function saveColumnPreferences() {
        const preferences = {};
        columnSelector.querySelectorAll('input[type="checkbox"]').forEach(cb => preferences[cb.dataset.column] = cb.checked);
        localStorage.setItem(COLUMN_PREF_KEY, JSON.stringify(preferences));
    }

    function loadColumnPreferences() {
        const preferences = JSON.parse(localStorage.getItem(COLUMN_PREF_KEY));
        if (preferences) { columnSelector.querySelectorAll('input[type="checkbox"]').forEach(cb => cb.checked = preferences[cb.dataset.column] !== false); }
        updateColumnVisibility();
    }

    const COLUMN_ORDER_KEY = 'mytoyota_trip_column_order';
    function reorderTableBody(order) {
        tripsTableBody.querySelectorAll('tr').forEach(row => {
            if (row.children.length > 1) {
                const cells = Array.from(row.children);
                const fragment = document.createDocumentFragment();
                order.forEach(columnName => {
                    const cell = cells.find(c => c.dataset.column === columnName);
                    if (cell) fragment.appendChild(cell);
                });
                row.innerHTML = '';
                row.appendChild(fragment);
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
        if (savedOrder) { try { applyColumnOrder(JSON.parse(savedOrder)); } catch (e) { console.error("Failed to apply saved column order.", e); } }
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
                const percent = data.total > 0 ? Math.round(((data.total - data.pending) / data.total) * 100) : 0;
                geocodeProgressBar.value = percent;
                geocodeProgressText.textContent = `${data.total - data.pending} / ${data.total} trips geocoded (${percent}%).`;
            } else {
                if (geocodeInterval) { clearInterval(geocodeInterval); geocodeInterval = null; }
            }
        } catch (error) { console.error("Failed to get geocode status:", error); }
    }
    
    function closeMap() {
        mapPanel.style.display = 'none';
        mapPanel.classList.remove('is-overlay');
    }

    closeMapPanelBtn.addEventListener('click', closeMap);
    prevTripBtn.addEventListener('click', () => {
        const newIndex = currentTripContext.rowIndex - 1;
        if (newIndex >= 0) { showTripOnMap(displayedTrips[newIndex], newIndex); }
    });
    nextTripBtn.addEventListener('click', () => {
        const newIndex = currentTripContext.rowIndex + 1;
        if (newIndex < displayedTrips.length) { showTripOnMap(displayedTrips[newIndex], newIndex); }
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
    periodSelect.addEventListener('change', savePeriodAndReload);
    columnSelector.addEventListener('change', () => { updateColumnVisibility(); saveColumnPreferences(); });

    backfillControls.addEventListener('click', async (event) => {
        if (event.target.tagName !== 'BUTTON') return;
        const vin = vinSelect.value;
        const period = event.target.dataset.period;
        if (!vin) { showBackfillStatus('Please select a vehicle.', 'error'); return; }
        const button = event.target;
        button.disabled = true;
        button.textContent = 'Fetching...';
        showBackfillStatus(`Fetching trips for '${period}'... This may take a moment.`, 'info');
        try {
            const response = await fetch(`/api/vehicles/${vin}/fetch_trips`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ period: period }) });
            const result = await response.json();
            showBackfillStatus(response.ok ? result.message : `Error: ${result.detail}`, response.ok ? 'success' : 'error');
            if (response.ok) {
                loadTrips();
                if (!geocodeInterval) geocodeInterval = setInterval(updateGeocodeProgress, 5000);
            }
        } catch (error) {
            showBackfillStatus('An unexpected error occurred during the fetch.', 'error');
        } finally {
            button.disabled = false;
            button.textContent = `Fetch Last ${period.charAt(0).toUpperCase() + period.slice(1)}`;
        }
    });

    new Sortable(tableHeaderRow, {
        animation: 150,
        onEnd: (event) => {
            const newOrder = Array.from(event.target.querySelectorAll('th')).map(th => th.dataset.column);
            localStorage.setItem(COLUMN_ORDER_KEY, JSON.stringify(newOrder));
            reorderTableBody(newOrder);
        }
    });

    async function init() {
        const savedPeriod = localStorage.getItem('mytoyota_trip_period');
        if (savedPeriod) { periodSelect.value = savedPeriod; }
        initMap();
        await loadConfig();
        updateUnitHeaders();
        loadColumnPreferences();
        loadAndApplyColumnOrder();
        await loadVins();
        if (vinSelect.value) await loadTrips();
        updateSortIndicators();
        if (!geocodeInterval) geocodeInterval = setInterval(updateGeocodeProgress, 5000);
    }

    init();
});