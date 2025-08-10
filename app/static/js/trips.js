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
    const countrySelect = document.getElementById('country-select');
    const filterAreaBtn = document.getElementById('filter-area-btn');
    const filterStartAreaBtn = document.getElementById('filter-start-area-btn');
    const filterEndAreaBtn = document.getElementById('filter-end-area-btn');
    const clearMapFilterBtn = document.getElementById('clear-map-filter-btn');
    const activeFiltersDisplay = document.getElementById('active-filters-display');
    const filterStatusIndicator = document.getElementById('filter-status-indicator');
    const showHeatmapBtn = document.getElementById('show-heatmap-btn');
    const heatmapTitle = document.getElementById('heatmap-title');
    const heatmapOverlay = document.getElementById('heatmap-overlay');
    const heatmapMapContainer = document.getElementById('heatmap-map');
    const closeHeatmapBtn = document.getElementById('close-heatmap-btn');
    const heatmapLoading = document.getElementById('heatmap-loading');
    const tripCountDisplay = document.getElementById('trip-count-display');
    
    let currentSort = { by: 'start_timestamp', direction: 'desc' };
    let appConfig = { unit_system: 'metric' };
    let geocodeInterval;
    let originalTrips = [];
    let displayedTrips = [];
    let currentTripContext = { tripId: null, rowIndex: -1 };
    let areaFilterMode = null;
    let activeFilters = { 
        area: { bounds: null, layer: null }, 
        start: { bounds: null, layer: null }, 
        end: { bounds: null, layer: null } 
    };

    // --- Map variables ---
    let map;
    let heatmapMap;
    let heatLayer;
    let currentMapLayers = [];
    let drawControl;
    let drawnItems;

    const MAP_FILTERS_STORAGE_KEY = 'mytoyota_map_filters';
    const TRIP_FILTERS_STORAGE_KEY = 'mytoyota_trip_list_filters';

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

    function initMap() {
        const streets = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { maxZoom: 19, attribution: '© OpenStreetMap contributors' });
        const satellite = L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', { maxZoom: 19, attribution: 'Tiles &copy; Esri' });
        const dark = L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', { attribution: '&copy; OpenStreetMap contributors &copy; CARTO', subdomains: 'abcd', maxZoom: 20 });
        map = L.map('map', { center: [55.7, 13.2], zoom: 9, layers: [streets] });
        L.control.layers({ "Streets": streets, "Satellite": satellite, "Dark": dark }).addTo(map);

        drawnItems = new L.FeatureGroup();
        map.addLayer(drawnItems);
        drawControl = new L.Control.Draw({
            draw: {
                polygon: false, polyline: false, circle: false, marker: false, circlemarker: false,
                rectangle: { shapeOptions: { color: '#00529b' } }
            },
            edit: { featureGroup: drawnItems, remove: false, edit: false }
        });
        map.addControl(drawControl);
        map.on(L.Draw.Event.CREATED, function (e) {
            const layer = e.layer;
            const bounds = layer.getBounds();

            if (areaFilterMode === 'area') {
                if (activeFilters.area.layer) drawnItems.removeLayer(activeFilters.area.layer);
                activeFilters.area = { bounds: bounds, layer: layer };
            } else if (areaFilterMode === 'start') {
                if (activeFilters.start.layer) drawnItems.removeLayer(activeFilters.start.layer);
                activeFilters.start = { bounds: bounds, layer: layer };
            } else if (areaFilterMode === 'end') {
                if (activeFilters.end.layer) drawnItems.removeLayer(activeFilters.end.layer);
                activeFilters.end = { bounds: bounds, layer: layer };
            }
            drawnItems.addLayer(layer);
            applyFilters();
        });
    }

    function initHeatmapMap() {
        if (heatmapMap) return; // Initialize only once

        // Define the three base layers
        const streets = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            maxZoom: 19,
            attribution: '© OpenStreetMap contributors'
        });
        const satellite = L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', {
            maxZoom: 19,
            attribution: 'Tiles &copy; Esri'
        });
        const dark = L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
            attribution: '&copy; OpenStreetMap contributors &copy; CARTO',
            subdomains: 'abcd',
            maxZoom: 20
        });

        // Initialize the map, setting 'Streets' as the default layer
        heatmapMap = L.map(heatmapMapContainer, {
            center: [55.7, 13.2],
            zoom: 9,
            layers: [streets] // Set default layer to Streets
        });

        // Create the control object with all three layers
        const baseMaps = {
            "Streets": streets,
            "Satellite": satellite,
            "Dark": dark
        };

        // Add the layer control to the heatmap
        L.control.layers(baseMaps).addTo(heatmapMap);
    }

    function clearMap() {
        currentMapLayers.forEach(layer => map.removeLayer(layer));
        currentMapLayers = [];
        map.closePopup();
    }
    
    function fitMapToBoundsOfAllTrips() {
        if (originalTrips.length === 0) {
            map.setView([55.7, 13.2], 9);
            return;
        }
        const bounds = L.latLngBounds();
        originalTrips.forEach(trip => {
            if (trip.start_lat && trip.start_lon) { bounds.extend([trip.start_lat, trip.start_lon]); }
            if (trip.end_lat && trip.end_lon) { bounds.extend([trip.end_lat, trip.end_lon]); }
        });
        if (bounds.isValid()) { map.fitBounds(bounds.pad(0.1)); } 
        else { map.setView([55.7, 13.2], 9); }
    }

    function saveMapFiltersToLocalStorage() {
        const serializableFilters = {
            area: activeFilters.area.bounds ? activeFilters.area.bounds.toBBoxString() : null,
            start: activeFilters.start.bounds ? activeFilters.start.bounds.toBBoxString() : null,
            end: activeFilters.end.bounds ? activeFilters.end.bounds.toBBoxString() : null,
        };
        localStorage.setItem(MAP_FILTERS_STORAGE_KEY, JSON.stringify(serializableFilters));
    }
    
    function saveTripFilters() {
        const selectedCountries = Array.from(countrySelect.selectedOptions).map(opt => opt.value);
        const filters = {
            period: periodSelect.value,
            countries: selectedCountries
        };
        localStorage.setItem(TRIP_FILTERS_STORAGE_KEY, JSON.stringify(filters));
    }
    
    function loadAndApplyTripFilters() {
        const savedFilters = JSON.parse(localStorage.getItem(TRIP_FILTERS_STORAGE_KEY));
        if (savedFilters) {
            if (savedFilters.period) {
                periodSelect.value = savedFilters.period;
            }
        }
        return savedFilters || {};
    }

    function loadMapFiltersFromLocalStorage() {
        const savedFilters = JSON.parse(localStorage.getItem(MAP_FILTERS_STORAGE_KEY));
        if (!savedFilters) return;

        drawnItems.clearLayers();
        activeFilters = { 
            area: { bounds: null, layer: null }, 
            start: { bounds: null, layer: null }, 
            end: { bounds: null, layer: null } 
        };
        
        const createFilterFromBBox = (bboxString, color) => {
            if (!bboxString) return null;
            const coords = bboxString.split(',').map(Number); // west, south, east, north
            const southWest = [coords[1], coords[0]]; // [lat, lon]
            const northEast = [coords[3], coords[2]]; // [lat, lon]
            const bounds = L.latLngBounds([southWest, northEast]);
            const layer = L.rectangle(bounds, { color: color, weight: 1 });
            return { bounds, layer };
        };

        const areaFilter = createFilterFromBBox(savedFilters.area, '#5bc0de');
        if (areaFilter) {
            activeFilters.area = areaFilter;
            drawnItems.addLayer(areaFilter.layer);
        }
        const startFilter = createFilterFromBBox(savedFilters.start, '#5cb85c');
        if (startFilter) {
            activeFilters.start = startFilter;
            drawnItems.addLayer(startFilter.layer);
        }
        const endFilter = createFilterFromBBox(savedFilters.end, '#337ab7');
        if (endFilter) {
            activeFilters.end = endFilter;
            drawnItems.addLayer(endFilter.layer);
        }
    }

    function updateUnitHeaders() {
        const isImperial = appConfig.unit_system.startsWith('imperial');
        const isUk = appConfig.unit_system === 'imperial_uk';
        document.querySelectorAll('th .unit[data-unit-type="distance"]').forEach(span => { span.textContent = isImperial ? 'mi' : 'km'; });
        document.querySelectorAll('th .unit[data-unit-type="speed"]').forEach(span => { span.textContent = isImperial ? 'mph' : 'km/h'; });
        const consumptionUnit = document.querySelector('th .unit[data-unit-type="consumption"]');
        if (consumptionUnit) { consumptionUnit.textContent = isImperial ? (isUk ? 'mpg (UK)' : 'mpg (US)') : 'L/100km'; }
    }

    function plotGpsRoute(routePoints) {
        clearMap();
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
        clearMap();
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
        } catch (e) { tripsTableBody.innerHTML = `<tr><td colspan="27">Could not load vehicle list: ${e.message}</td></tr>`; }
    }

    async function loadCountryFilter(savedCountries = []) {
        const selectedVin = vinSelect.value;
        if (!selectedVin) return;
        try {
            const response = await fetch(`/api/vehicles/${selectedVin}/countries`);
            const countries = await response.json();
            countrySelect.innerHTML = '';
            
            const allOption = new Option('[All Countries]', 'all');
            countrySelect.appendChild(allOption);

            if (countries.length > 0) {
                countries.forEach(country => {
                    const option = new Option(country, country);
                    if (savedCountries.includes(country)) {
                        option.selected = true;
                    }
                    countrySelect.appendChild(option);
                });
            }
            
            if (savedCountries.length > 0 && countries.length > 0) {
                allOption.selected = false;
            } else {
                allOption.selected = true;
            }
        } catch (e) {
            console.error("Could not load country filter:", e.message);
            countrySelect.innerHTML = '<option disabled>Error loading countries</option>';
        }
    }

    async function loadTrips(keepFilters = false) {
        const selectedVin = vinSelect.value;
        if (!selectedVin) return;
        
        if (!keepFilters) {
            mapPanel.style.display = 'none';
        }
        tripsTableBody.innerHTML = `<tr><td colspan="27" style="text-align:center;">Loading trips...</td></tr>`;
        updateSortIndicators();
        
        try {
            const params = new URLSearchParams({
                vin: selectedVin,
                sort_by: currentSort.by,
                sort_direction: currentSort.direction,
                unit_system: appConfig.unit_system
            });

            const periodDays = periodSelect.value;
            if (periodDays !== 'all') {
                const date = new Date();
                date.setDate(date.getDate() - parseInt(periodDays, 10));
                params.append('start_date', date.toISOString().split('T')[0]);
            }
            
            const selectedCountries = Array.from(countrySelect.selectedOptions).map(opt => opt.value);
            if (selectedCountries.length > 0 && !selectedCountries.includes('all')) {
                params.append('countries', selectedCountries.join(','));
            }

            const response = await fetch(`/api/trips?${params.toString()}`);
            const trips = await response.json();
            originalTrips = trips;
            
            if (keepFilters) {
                applyFilters();
            } else {
                // This is a full reload (e.g. VIN change), so clear map filters
                clearMapFilterBtn.click();
            }
        } catch (e) { tripsTableBody.innerHTML = `<tr><td colspan="27">Error loading trips: ${e.message}</td></tr>`; }
    }

    const isMobile = () => window.matchMedia('(max-width: 1024px)').matches;

    function updateNavButtonsState() {
        const isOverlay = mapPanel.classList.contains('is-overlay');
        if (!isOverlay) return;
        prevTripBtn.disabled = currentTripContext.rowIndex <= 0;
        nextTripBtn.disabled = currentTripContext.rowIndex >= displayedTrips.length - 1;
    }

    function applyFilters() {
        let filteredTrips = originalTrips;

        if (activeFilters.area.bounds) {
            filteredTrips = filteredTrips.filter(trip => 
                (trip.start_lat && activeFilters.area.bounds.contains([trip.start_lat, trip.start_lon])) ||
                (trip.end_lat && activeFilters.area.bounds.contains([trip.end_lat, trip.end_lon]))
            );
        }
        if (activeFilters.start.bounds) {
            filteredTrips = filteredTrips.filter(trip => 
                trip.start_lat && activeFilters.start.bounds.contains([trip.start_lat, trip.start_lon])
            );
        }
        if (activeFilters.end.bounds) {
            filteredTrips = filteredTrips.filter(trip => 
                trip.end_lat && activeFilters.end.bounds.contains([trip.end_lat, trip.end_lon])
            );
        }

        renderTable(filteredTrips);
        updateActiveFiltersDisplay();
        saveMapFiltersToLocalStorage();
    }
    
    function updateActiveFiltersDisplay() {
        const activeMapFilters = [];
        if (activeFilters.area.bounds) activeMapFilters.push("Area (Start/End)");
        if (activeFilters.start.bounds) activeMapFilters.push("Start Area");
        if (activeFilters.end.bounds) activeMapFilters.push("End Area");
        
        const hasMapFilter = activeMapFilters.length > 0;
        if (hasMapFilter) {
            activeFiltersDisplay.textContent = `Active Map Filters: ${activeMapFilters.join(', ')}`;
            clearMapFilterBtn.style.display = 'inline-block';
        } else {
            activeFiltersDisplay.textContent = '';
            clearMapFilterBtn.style.display = 'none';
        }
        
        const selectedCountries = Array.from(countrySelect.selectedOptions).map(opt => opt.value);
        const hasCountryFilter = selectedCountries.length > 0 && !selectedCountries.includes('all');

        if (hasMapFilter || hasCountryFilter) {
            filterStatusIndicator.textContent = '(Filter Active)';
            filterStatusIndicator.style.display = 'inline';
        } else {
            filterStatusIndicator.style.display = 'none';
        }
    }

    function activateDrawingMode() {
        if (window.getComputedStyle(mapPanel).display === 'none') {
            clearMap();
            mapPanelTitle.textContent = "Select Area on Map";
            mapTripStats.innerHTML = '';
            if (isMobile()) {
                mapPanel.classList.add('is-overlay');
                mapPanel.style.display = 'flex';
            } else {
                mapPanel.classList.remove('is-overlay');
                mapPanel.style.display = 'block';
            }
            setTimeout(() => {
                map.invalidateSize();
                fitMapToBoundsOfAllTrips();
            }, 50);
        }
        new L.Draw.Rectangle(map, drawControl.options.draw.rectangle).enable();
    }

    async function showTripOnMap(trip, rowIndex) {
        if (!trip) return;
        currentTripContext = { tripId: trip.id, rowIndex: rowIndex };
        mapPanelTitle.textContent = `Trip on ${new Date(trip.start_timestamp).toLocaleDateString()}`;
        if (isMobile()) {
            mapPanel.classList.add('is-overlay');
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
        clearMap();
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
        if (tripCountDisplay) {
            tripCountDisplay.textContent = `(${trips.length} trips displayed)`;
        }
        displayedTrips = trips;
        const isImperial = appConfig.unit_system.startsWith('imperial');
        const isUk = appConfig.unit_system === 'imperial_uk';
        tripsTableBody.innerHTML = '';
        if (trips.length === 0) {
            tripsTableBody.innerHTML = `<tr><td colspan="27" style="text-align: center; padding: 20px;">No trips found for the current filter.</td></tr>`;
            return;
        }
        const formatTimestamp = (ts) => !ts ? 'N/A' : `${new Date(ts).toLocaleDateString()}<br><span class="unit">${new Date(ts).toLocaleTimeString()}</span>`;
        const formatNumber = (num, digits = 2) => (num === null || num === undefined) ? 'N/A' : Number(num).toFixed(digits);
        const formatDuration = (s) => (s === null || s === undefined) ? 'N/A' : new Date(s * 1000).toISOString().slice(11, 19);
        const formatBoolean = (b) => (b === null || b === undefined) ? 'N/A' : (b ? 'Yes' : 'No');
        const formatArray = (arr) => (arr && arr.length > 0) ? arr.join(', ') : 'N/A';
        
        trips.forEach((trip, index) => {
            const row = tripsTableBody.insertRow();
            row.innerHTML = `
                <td data-column="start-time">${formatTimestamp(trip.start_timestamp)}</td>
                <td data-column="end-time">${formatTimestamp(trip.end_timestamp)}</td>
                <td data-column="distance">${formatNumber(isImperial ? trip.distance_mi : trip.distance_km)}</td>
                <td data-column="consumption">${formatNumber(isImperial ? (isUk ? trip.mpg_uk : trip.mpg) : trip.fuel_consumption_l_100km)}</td>
                <td data-column="start-address">${trip.start_address || 'N/A'}</td>
                <td data-column="end-address">${trip.end_address || 'N/A'}</td>
                <td data-column="duration">${formatDuration(trip.duration_seconds)}</td>
                <td data-column="avg-speed">${formatNumber(isImperial ? trip.average_speed_mph : trip.average_speed_kmh)}</td>
                <td data-column="max-speed">${formatNumber(isImperial && trip.max_speed_kmh ? (trip.max_speed_kmh * 0.621371) : trip.max_speed_kmh, 0)}</td>
                <td data-column="score-global">${trip.score_global || 'N/A'}</td>
                <td data-column="night-trip">${formatBoolean(trip.night_trip)}</td>
                <td data-column="countries">${formatArray(trip.countries)}</td>
                <td data-column="overspeed-dist">${formatNumber(isImperial && trip.length_overspeed_km ? (trip.length_overspeed_km * 0.621371) : trip.length_overspeed_km)}</td>
                <td data-column="overspeed-dur">${formatDuration(trip.duration_overspeed_seconds)}</td>
                <td data-column="highway-dist">${formatNumber(isImperial && trip.length_highway_km ? (trip.length_highway_km * 0.621371) : trip.length_highway_km)}</td>
                <td data-column="highway-dur">${formatDuration(trip.duration_highway_seconds)}</td>
                <td data-column="score-accel">${trip.score_acceleration || 'N/A'}</td>
                <td data-column="score-brake">${trip.score_braking || 'N/A'}</td>
                <td data-column="score-const">${trip.score_constant_speed || 'N/A'}</td>
                <td data-column="ev-dist">${formatNumber(isImperial ? trip.ev_distance_mi : trip.ev_distance_km)}</td>
                <td data-column="ev-dur">${formatDuration(trip.ev_duration_seconds)}</td>
                <td data-column="hdc-eco-dist">${formatNumber(isImperial && trip.hdc_eco_distance_km ? (trip.hdc_eco_distance_km * 0.621371) : trip.hdc_eco_distance_km)}</td>
                <td data-column="hdc-eco-dur">${formatDuration(trip.hdc_eco_duration_seconds)}</td>
                <td data-column="hdc-pwr-dist">${formatNumber(isImperial && trip.hdc_power_distance_km ? (trip.hdc_power_distance_km * 0.621371) : trip.hdc_power_distance_km)}</td>
                <td data-column="hdc-pwr-dur">${formatDuration(trip.hdc_power_duration_seconds)}</td>
                <td data-column="hdc-chg-dist">${formatNumber(isImperial && trip.hdc_charge_distance_km ? (trip.hdc_charge_distance_km * 0.621371) : trip.hdc_charge_distance_km)}</td>
                <td data-column="hdc-chg-dur">${formatDuration(trip.hdc_charge_duration_seconds)}</td>`;
            row.addEventListener('click', () => showTripOnMap(trip, index));
        });
        updateColumnVisibility();
        loadAndApplyColumnOrder();
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
        if (preferences) {
            columnSelector.querySelectorAll('input[type="checkbox"]').forEach(cb => {
                // If a column is new (not in saved prefs), default it to checked.
                if (preferences[cb.dataset.column] === undefined) {
                    cb.checked = true;
                } else {
                    cb.checked = preferences[cb.dataset.column];
                }
            });
        }
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

    function applyColumnOrder(savedOrder) {
        const headers = Array.from(tableHeaderRow.querySelectorAll('th'));
        const fragment = document.createDocumentFragment();
        const savedColumns = new Set(savedOrder);

        // 1. Append headers that are in the saved order, in that order.
        savedOrder.forEach(columnName => {
            const header = headers.find(h => h.dataset.column === columnName);
            if (header) {
                fragment.appendChild(header);
            }
        });

        // 2. Append any new headers that weren't in the saved order to the end.
        headers.forEach(header => {
            if (!savedColumns.has(header.dataset.column)) {
                fragment.appendChild(header);
            }
        });

        tableHeaderRow.innerHTML = '';
        tableHeaderRow.appendChild(fragment);
        
        const newCompleteOrder = Array.from(tableHeaderRow.querySelectorAll('th')).map(th => th.dataset.column);
        reorderTableBody(newCompleteOrder);
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

    filterAreaBtn.addEventListener('click', () => {
        areaFilterMode = 'area';
        activeFilters.start = { bounds: null, layer: null };
        activeFilters.end = { bounds: null, layer: null };
        drawnItems.clearLayers();
        activateDrawingMode();
    });

    filterStartAreaBtn.addEventListener('click', () => {
        areaFilterMode = 'start';
        if (activeFilters.area.layer) drawnItems.removeLayer(activeFilters.area.layer);
        activeFilters.area = { bounds: null, layer: null };
        if (activeFilters.start.layer) drawnItems.removeLayer(activeFilters.start.layer);
        activateDrawingMode();
    });

    filterEndAreaBtn.addEventListener('click', () => {
        areaFilterMode = 'end';
        if (activeFilters.area.layer) drawnItems.removeLayer(activeFilters.area.layer);
        activeFilters.area = { bounds: null, layer: null };
        if (activeFilters.end.layer) drawnItems.removeLayer(activeFilters.end.layer);
        activateDrawingMode();
    });

    clearMapFilterBtn.addEventListener('click', () => {
        const resetFilter = { bounds: null, layer: null };
        activeFilters = { area: resetFilter, start: resetFilter, end: resetFilter };
        drawnItems.clearLayers();
        applyFilters();
    });

    showHeatmapBtn.addEventListener('click', async () => {
        if (displayedTrips.length === 0) {
            alert("No trips to display in the heatmap.");
            return;
        }
        
        heatmapOverlay.style.display = 'flex';
        initHeatmapMap();
        
        const originalTitle = "Trips Heatmap";
        let loadedCount = 0;
        const totalTrips = displayedTrips.length;
        heatmapTitle.textContent = `Loading trip data: 0 of ${totalTrips}...`;

        const routePromises = displayedTrips.map(trip => 
            fetch(`/api/trips/${trip.id}/route`)
                .then(res => {
                    if (!res.ok) {
                        console.error(`Failed to fetch route for trip ${trip.id}: Status ${res.status}`);
                        return null;
                    }
                    return res.json();
                })
                .then(result => {
                    loadedCount++;
                    heatmapTitle.textContent = `Loading trip data: ${loadedCount} of ${totalTrips}...`;
                    return result;
                })
                .catch(error => {
                    console.error(`Network error for trip ${trip.id}:`, error);
                    loadedCount++;
                    heatmapTitle.textContent = `Loading trip data: ${loadedCount} of ${totalTrips}...`;
                    return null;
                })
        );

        try {
            const results = await Promise.all(routePromises);
            heatmapTitle.textContent = 'Generating heatmap...';
            
            const allPoints = [];
            results.forEach(result => {
                if (result && result.route && result.route.length > 0) {
                    result.route.forEach(point => {
                        allPoints.push([point.lat, point.lon]);
                    });
                }
            });

            if (heatLayer) { heatmapMap.removeLayer(heatLayer); }

            if (allPoints.length === 0) {
                alert("None of the selected trips contain detailed route data for a heatmap.");
                heatmapTitle.textContent = originalTitle;
                return;
            }

            heatLayer = L.heatLayer(allPoints, { radius: 20, blur: 15, maxZoom: 18 }).addTo(heatmapMap);
            
            const bounds = L.latLngBounds(allPoints);
            setTimeout(() => {
                heatmapMap.invalidateSize();
                if (bounds.isValid()) {
                    heatmapMap.fitBounds(bounds.pad(0.1));
                } else {
                    heatmapMap.setView([55.7, 13.2], 9);
                }
                heatmapTitle.textContent = originalTitle; // Restore title on success
            }, 100);

        } catch (error) {
            console.error("A critical error during heatmap generation:", error);
            alert("An unexpected error occurred while generating the heatmap.");
            heatmapTitle.textContent = originalTitle; // Restore title on error
        }
    });
    
    closeHeatmapBtn.addEventListener('click', () => {
        heatmapOverlay.style.display = 'none';
    });

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
        loadTrips(true);
    });

    vinSelect.addEventListener('change', async () => {
        const savedFilters = loadAndApplyTripFilters();
        await loadCountryFilter(savedFilters.countries || []);
        loadTrips(false);
    });
    
    periodSelect.addEventListener('change', () => {
        saveTripFilters();
        loadTrips(true);
    });

    countrySelect.addEventListener('change', function() {
        const selectedValues = Array.from(this.selectedOptions).map(opt => opt.value);
        const allOption = this.querySelector('option[value="all"]');

        if (selectedValues.length > 1 && selectedValues.includes('all')) {
            // If the user selects another country while "All" is already selected,
            // we assume they want to filter by the new country, so we deselect "All".
            allOption.selected = false;
        } else if (selectedValues.includes('all')) {
            // If the user explicitly clicks "All", deselect everything else.
            Array.from(this.options).forEach(opt => {
                if (opt.value !== 'all') opt.selected = false;
            });
        } else if (selectedValues.length === 0) {
            // If the user deselects the last country, re-select "All" to clear the filter.
            allOption.selected = true;
        }
        
        saveTripFilters();
        loadTrips(true);
    });

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
                const savedFilters = loadAndApplyTripFilters();
                await loadCountryFilter(savedFilters.countries || []);
                loadTrips(false);
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
        const savedFilters = loadAndApplyTripFilters();
        initMap();
        loadMapFiltersFromLocalStorage();
        await loadConfig();
        updateUnitHeaders();
        loadColumnPreferences();
        loadAndApplyColumnOrder();
        await loadVins();
        if (vinSelect.value) {
            await loadCountryFilter(savedFilters.countries || []);
            await loadTrips(true);
        }
        updateSortIndicators();
        if (!geocodeInterval) geocodeInterval = setInterval(updateGeocodeProgress, 5000);
    }

    init();
});