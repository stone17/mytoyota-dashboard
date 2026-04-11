document.addEventListener('DOMContentLoaded', () => {
    // --- Element Selectors ---
    const credentialsForm = document.getElementById('credentials-form');
    const usernameInput = document.getElementById('username');
    const passwordInput = document.getElementById('password');
    const credentialsMessage = document.getElementById('credentials-message');

    const pollingSettingsForm = document.getElementById('polling-settings-form');
    const apiRetriesForm = document.getElementById('api-retries-form');
    const displaySettingsForm = document.getElementById('display-settings-form');
    const geocodingSettingsForm = document.getElementById('geocoding-settings-form');
    const geocodingProviderSelect = document.getElementById('geocoding-provider');
    const opencageKeyGroup = document.getElementById('opencage-key-group');
    const googleMapsKeyGroup = document.getElementById('google-maps-key-group');
    const testGeocodeBtn = document.getElementById('test-geocode-btn');
    const geocodingTestResult = document.getElementById('geocoding-test-result');

    const mqttSettingsForm = document.getElementById('mqtt-settings-form');

    const pollingStatusMessage = document.getElementById('polling-status-message');
    const apiRetriesStatusMessage = document.getElementById('api-retries-status-message');
    const displayStatusMessage = document.getElementById('display-status-message');
    const geocodingStatusMessage = document.getElementById('geocoding-status-message');
    const mqttStatusMessage = document.getElementById('mqtt-status-message');
    const mqttTestBtn = document.getElementById('mqtt-test-btn');
    const mqttSensorSelection = document.getElementById('mqtt-sensor-selection');

    const intervalSettingsDiv = document.getElementById('interval-settings');
    const fixedTimeSettingsDiv = document.getElementById('fixed-time-settings');

    const backfillGeocodeBtn = document.getElementById('backfill-geocode-btn');
    const forceRegeocodeBtn = document.getElementById('force-regeocode-btn');
    
    const testTripSelect = document.getElementById('test-trip-select');
    let geocodingMap = null;
    let startMarker = null;
    let endMarker = null;
    let routeLine = null;

    // --- Define available sensors ---
    const ALL_SENSORS = {
        'odometer': 'Odometer',
        'lock_status': 'Lock Status',
        'fuel_level': 'Fuel Level',
        'fuel_consumption': 'Fuel Consumption',
        'total_range': 'Total Range',
        'battery_level': 'EV Battery %',
        'ev_range': 'EV Range',
        'score': 'Global Score',
        'location_lat_long': 'Location (Lat/Long)',
        'location': 'Location (Address)',
        'highway_distance': 'Total Highway Distance',
        'total_ev_distance': 'Total EV Distance'
    };

    // --- Populate Sensor Checkboxes ---
    function populateCheckboxes(container, sensorMap, prefix) {
        for (const [key, label] of Object.entries(sensorMap)) {
            const labelEl = document.createElement('label');
            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.id = `${prefix}-${key}`;
            checkbox.name = key;
            checkbox.dataset.sensorKey = key;
            labelEl.appendChild(checkbox);
            labelEl.appendChild(document.createTextNode(` ${label}`));
            container.appendChild(labelEl);
        }
    }


    // --- Helper to display status messages ---
    function showMessage(element, message, type = 'info', duration = 5000) {
        if (!element) return;
        if (element.timeoutId) {
            clearTimeout(element.timeoutId);
        }
        element.textContent = message;
        element.className = `status-message ${type}`;
        element.style.display = 'block';
        if (duration > 0) {
            element.timeoutId = setTimeout(() => { element.style.display = 'none'; }, duration);
        }
    }

    // --- Init Map ---
    function initGeocodingMap() {
        if (!document.getElementById('geocoding-map')) return;
        geocodingMap = L.map('geocoding-map').setView([52.5200, 13.4050], 13);
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            maxZoom: 19,
            attribution: '© OpenStreetMap'
        }).addTo(geocodingMap);
    }

    // --- Fetch Recent Trips ---
    async function loadRecentTrips() {
        if (!testTripSelect) return;
        try {
            const vehiclesResp = await fetch('/api/vehicles');
            const vehicles = await vehiclesResp.json();
            if (!vehicles || vehicles.length === 0) return;

            const vin = vehicles[0].vin;
            const tripsResp = await fetch(`/api/trips?vin=${vin}`);
            const trips = await tripsResp.json();
            
            // Populate select with top 20 trips
            trips.slice(0, 20).forEach(trip => {
                const option = document.createElement('option');
                option.value = JSON.stringify({
                    start_lat: trip.start_lat, 
                    start_lon: trip.start_lon,
                    end_lat: trip.end_lat,
                    end_lon: trip.end_lon
                });
                const date = new Date(trip.start_timestamp).toLocaleString();
                option.textContent = `${date} - ${trip.distance_km.toFixed(1)} km`;
                testTripSelect.appendChild(option);
            });

            testTripSelect.addEventListener('change', (e) => {
                if (!e.target.value) {
                    document.getElementById('test-start-lat').value = '';
                    document.getElementById('test-start-lon').value = '';
                    document.getElementById('test-end-lat').value = '';
                    document.getElementById('test-end-lon').value = '';
                    
                    if (geocodingMap) {
                        if (startMarker) geocodingMap.removeLayer(startMarker);
                        if (endMarker) geocodingMap.removeLayer(endMarker);
                        if (routeLine) geocodingMap.removeLayer(routeLine);
                    }
                    return;
                }

                const coords = JSON.parse(e.target.value);
                document.getElementById('test-start-lat').value = coords.start_lat;
                document.getElementById('test-start-lon').value = coords.start_lon;
                document.getElementById('test-end-lat').value = coords.end_lat;
                document.getElementById('test-end-lon').value = coords.end_lon;
                
                updateMapWithCoords(coords.start_lat, coords.start_lon, coords.end_lat, coords.end_lon);
            });
        } catch (error) {
            console.error('Failed to load trips for geocoder testing:', error);
        }
    }

    function updateMapWithCoords(startLat, startLon, endLat, endLon) {
        if (!geocodingMap) return;
        
        if (startMarker) geocodingMap.removeLayer(startMarker);
        if (endMarker) geocodingMap.removeLayer(endMarker);
        if (routeLine) geocodingMap.removeLayer(routeLine);

        const bounds = [];
        
        if (startLat && startLon) {
            startMarker = L.marker([startLat, startLon]).addTo(geocodingMap);
            startMarker.bindTooltip("Start");
            bounds.push([startLat, startLon]);
        }
        
        if (endLat && endLon) {
            // Use a different color or icon for end marker if possible, 
            // but standard marker is fine for now, we'll label it.
            endMarker = L.marker([endLat, endLon]).addTo(geocodingMap);
            endMarker.bindTooltip("End");
            bounds.push([endLat, endLon]);
        }
        
        if (bounds.length === 2) {
            routeLine = L.polyline(bounds, {color: '#3388ff', weight: 3, dashArray: '5, 5'}).addTo(geocodingMap);
        }
        
        if (bounds.length > 0) {
            geocodingMap.fitBounds(L.latLngBounds(bounds).pad(0.2), { maxZoom: 15 });
        }
    }

    // --- Credentials Management ---
    async function loadUsername() {
        try {
            const response = await fetch('/api/credentials');
            const data = await response.json();
            if (data.username) {
                usernameInput.value = data.username;
            }
        } catch (error) {
            console.error('Failed to load username:', error);
        }
    }

    credentialsForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        const username = usernameInput.value;
        const password = passwordInput.value;

        if (!password) {
            showMessage(credentialsMessage, 'Password is required to save credentials.', 'error');
            return;
        }

        try {
            const response = await fetch('/api/credentials', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ username, password }),
            });
            const result = await response.json();
            if (response.ok) {
                showMessage(credentialsMessage, result.message, 'success');
                passwordInput.value = '';

                showMessage(credentialsMessage, 'Credentials saved. Triggering data fetch...', 'info');
                try {
                    const pollResponse = await fetch('/api/force_poll', { method: 'POST' });
                    const pollResult = await pollResponse.json();
                    if (pollResponse.ok) {
                        showMessage(credentialsMessage, 'Data fetch completed successfully!', 'success');
                    } else {
                        throw new Error(pollResult.detail || 'Polling failed.');
                    }
                } catch (pollError) {
                    showMessage(credentialsMessage, `Data fetch failed: ${pollError.message}`, 'error');
                }
            } else {
                throw new Error(result.detail || 'An unknown error occurred.');
            }
        } catch (error) {
            showMessage(credentialsMessage, `Error: ${error.message}`, 'error');
        }
    });

    // --- Load Settings ---
    async function loadSettings() {
        try {
            const response = await fetch('/api/config');
            const config = await response.json();

            const polling = config.web_server?.polling || {};
            document.querySelector(`input[name="poll_mode"][value="${polling.mode || 'interval'}"]`).checked = true;
            document.getElementById('refresh-interval').value = polling.interval_seconds || 3600;
            document.getElementById('fixed-time').value = polling.fixed_time || '07:00';
            togglePollingInputs();

            document.getElementById('api-retries').value = config.api_retries || 3;
            document.getElementById('api-retry-delay').value = config.api_retry_delay_seconds || 20;
            document.querySelector(`input[name="unit_system"][value="${config.unit_system || 'metric'}"]`).checked = true;

            document.getElementById('reverse-geocode-enabled').checked = config.reverse_geocode_enabled !== false;
            document.getElementById('fetch-full-route').checked = config.fetch_full_trip_route || false;
            
            const geocoding = config.geocoding || {};
            if (geocodingProviderSelect) {
                geocodingProviderSelect.value = geocoding.provider || 'nominatim';
                document.getElementById('opencage-api-key').value = geocoding.opencage_api_key || '';
                document.getElementById('google-maps-api-key').value = geocoding.google_maps_api_key || '';
                
                function toggleGeocodingProviderFields() {
                    const provider = geocodingProviderSelect.value;
                    if (opencageKeyGroup) opencageKeyGroup.style.display = provider === 'opencage' ? 'block' : 'none';
                    if (googleMapsKeyGroup) googleMapsKeyGroup.style.display = provider === 'google_maps' ? 'block' : 'none';
                }
                
                geocodingProviderSelect.addEventListener('change', toggleGeocodingProviderFields);
                toggleGeocodingProviderFields();
            }

            const mqtt = config.mqtt || {};
            document.getElementById('mqtt-enabled').checked = mqtt.enabled || false;
            document.getElementById('mqtt-host').value = mqtt.host || '';
            document.getElementById('mqtt-port').value = mqtt.port || 1883;
            document.getElementById('mqtt-username').value = mqtt.username || '';
            document.getElementById('mqtt-base-topic').value = mqtt.base_topic || '';
            document.getElementById('mqtt-discovery-prefix').value = mqtt.discovery_prefix || 'homeassistant';

            // Load sensor selection
            const enabledSensors = mqtt.enabled_sensors || {};
            document.querySelectorAll('#mqtt-sensor-selection input[type="checkbox"]').forEach(cb => {
                cb.checked = enabledSensors[cb.dataset.sensorKey] === true;
            });

        } catch (error) {
            console.error(`Failed to load settings: ${error.message}`);
            showMessage(pollingStatusMessage, `Failed to load settings: ${error.message}`, 'error');
        }
    }

    function togglePollingInputs() {
        const mode = document.querySelector('input[name="poll_mode"]:checked').value;
        intervalSettingsDiv.style.display = mode === 'interval' ? 'block' : 'none';
        fixedTimeSettingsDiv.style.display = mode === 'fixed_time' ? 'block' : 'none';
    }

    document.querySelectorAll('input[name="poll_mode"]').forEach(radio => {
        radio.addEventListener('change', togglePollingInputs);
    });

    // --- Save Settings Event Listeners ---
    pollingSettingsForm.addEventListener('submit', (e) => {
        e.preventDefault();
        const formData = new FormData(pollingSettingsForm);
        const newSettings = {
            web_server: {
                polling: {
                    mode: formData.get('poll_mode'),
                    interval_seconds: parseInt(formData.get('interval_seconds'), 10),
                    fixed_time: formData.get('fixed_time'),
                }
            }
        };
        saveConfig(newSettings, pollingStatusMessage);
    });
    
    mqttSettingsForm.addEventListener('submit', (e) => {
        e.preventDefault();
        
        const enabledSensors = {};
        document.querySelectorAll('#mqtt-sensor-selection input[type="checkbox"]').forEach(cb => {
            enabledSensors[cb.dataset.sensorKey] = cb.checked;
        });

        const newSettings = {
            mqtt: {
                enabled: document.getElementById('mqtt-enabled').checked,
                host: document.getElementById('mqtt-host').value,
                port: parseInt(document.getElementById('mqtt-port').value, 10),
                username: document.getElementById('mqtt-username').value,
                password: document.getElementById('mqtt-password').value,
                base_topic: document.getElementById('mqtt-base-topic').value,
                discovery_prefix: document.getElementById('mqtt-discovery-prefix').value,
                enabled_sensors: enabledSensors
            }
        };

        if (!newSettings.mqtt.password) {
            delete newSettings.mqtt.password;
        }
        saveConfig(newSettings, mqttStatusMessage);
    });

    mqttTestBtn.addEventListener('click', async () => {
        showMessage(mqttStatusMessage, 'Sending test message based on latest saved settings...', 'info');
        try {
            const response = await fetch('/api/mqtt/test', { method: 'POST' });
            const result = await response.json();
            if (response.ok) {
                showMessage(mqttStatusMessage, result.message, 'success');
            } else {
                throw new Error(result.detail || 'An unknown error occurred.');
            }
        } catch (error) {
            showMessage(mqttStatusMessage, `Error: ${error.message}`, 'error');
        }
    });

    apiRetriesForm.addEventListener('submit', (e) => {
        e.preventDefault();
        const formData = new FormData(apiRetriesForm);
        const newSettings = {
            api_retries: parseInt(formData.get('api_retries'), 10),
            api_retry_delay_seconds: parseInt(formData.get('api_retry_delay_seconds'), 10),
        };
        saveConfig(newSettings, apiRetriesStatusMessage);
    });

    displaySettingsForm.addEventListener('submit', (e) => {
        e.preventDefault();
        const formData = new FormData(displaySettingsForm);
        const newSettings = {
            unit_system: formData.get('unit_system'),
        };
        saveConfig(newSettings, displayStatusMessage);
    });

    geocodingSettingsForm.addEventListener('submit', (e) => {
        e.preventDefault();
        const newSettings = {
            reverse_geocode_enabled: document.getElementById('reverse-geocode-enabled').checked,
            fetch_full_trip_route: document.getElementById('fetch-full-route').checked,
            geocoding: {
                provider: geocodingProviderSelect.value,
                opencage_api_key: document.getElementById('opencage-api-key').value,
                google_maps_api_key: document.getElementById('google-maps-api-key').value
            }
        };
        saveConfig(newSettings, geocodingStatusMessage);
    });

    async function saveConfig(newSettings, messageElement) {
        console.log("Attempting to save new settings:", JSON.stringify(newSettings, null, 2));
        try {
            const response = await fetch('/api/config', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(newSettings),
            });
            const result = await response.json();
            console.log("Received response from server:", {
                ok: response.ok,
                status: response.status,
                body: result
            });
            if (response.ok) {
                showMessage(messageElement, result.message, 'success');
            } else {
                throw new Error(result.detail || 'An unknown error occurred.');
            }
        } catch (error) {
            showMessage(messageElement, `Error: ${error.message}`, 'error');
            console.error("Error during saveConfig:", error);
        }
    }

    if (backfillGeocodeBtn) {
        backfillGeocodeBtn.addEventListener('click', async () => {
            backfillGeocodeBtn.disabled = true;
            backfillGeocodeBtn.textContent = 'Queuing...';
            showMessage(geocodingStatusMessage, 'Starting geocoding backfill. This may take some time.', 'info');

            try {
                const response = await fetch('/api/backfill_geocoding', { method: 'POST' });
                const result = await response.json();
                if (response.ok) {
                    showMessage(geocodingStatusMessage, result.message, 'success');
                } else {
                    throw new Error(result.detail || 'Unknown error');
                }
            } catch (error) {
                showMessage(geocodingStatusMessage, `Error: ${error.message}`, 'error');
            } finally {
                backfillGeocodeBtn.disabled = false;
                backfillGeocodeBtn.textContent = 'Geocode Missing Addresses';
            }
        });
    }

    if (forceRegeocodeBtn) {
        forceRegeocodeBtn.addEventListener('click', async () => {
            if (!confirm("Are you sure you want to re-geocode ALL trips? This might take a long time and consume API quota.")) {
                return;
            }
            forceRegeocodeBtn.disabled = true;
            forceRegeocodeBtn.textContent = 'Queuing...';
            showMessage(geocodingStatusMessage, 'Starting forced geocoding for all trips. This will take a while.', 'info');

            try {
                const response = await fetch('/api/backfill_geocoding?force_all=true', { method: 'POST' });
                const result = await response.json();
                if (response.ok) {
                    showMessage(geocodingStatusMessage, result.message, 'success');
                } else {
                    throw new Error(result.detail || 'Unknown error');
                }
            } catch (error) {
                showMessage(geocodingStatusMessage, `Error: ${error.message}`, 'error');
            } finally {
                forceRegeocodeBtn.disabled = false;
                forceRegeocodeBtn.textContent = 'Force Re-Geocode All';
            }
        });
    }

    if (testGeocodeBtn) {
        testGeocodeBtn.addEventListener('click', async () => {
            const startLat = document.getElementById('test-start-lat').value;
            const startLon = document.getElementById('test-start-lon').value;
            const endLat = document.getElementById('test-end-lat').value;
            const endLon = document.getElementById('test-end-lon').value;
            
            if ((!startLat || !startLon) && (!endLat || !endLon)) {
                showMessage(geocodingTestResult, 'Please provide at least one set of coordinates.', 'error', 0);
                return;
            }

            const provider = geocodingProviderSelect.value;
            const opencageKey = document.getElementById('opencage-api-key').value;
            const googleMapsKey = document.getElementById('google-maps-api-key').value;

            showMessage(geocodingTestResult, 'Testing geocoder...', 'info', 0);
            testGeocodeBtn.disabled = true;
            
            updateMapWithCoords(startLat, startLon, endLat, endLon);

            try {
                let resultsHTML = '';
                
                if (startLat && startLon) {
                    const startRes = await fetch('/api/geocoding/test', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ 
                            lat: parseFloat(startLat), 
                            lon: parseFloat(startLon),
                            provider: provider,
                            opencage_api_key: opencageKey,
                            google_maps_api_key: googleMapsKey
                        })
                    });
                    const startData = await startRes.json();
                    if (startRes.ok) {
                        resultsHTML += `<strong>Start:</strong> ${startData.address}<br>`;
                        if (startMarker) startMarker.bindPopup(`<b>Start Result:</b><br>${startData.address}`).openPopup();
                    } else {
                        resultsHTML += `<strong>Start Error:</strong> ${startData.detail || 'Test failed'}<br>`;
                    }
                }
                
                if (endLat && endLon) {
                    const endRes = await fetch('/api/geocoding/test', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ 
                            lat: parseFloat(endLat), 
                            lon: parseFloat(endLon),
                            provider: provider,
                            opencage_api_key: opencageKey,
                            google_maps_api_key: googleMapsKey
                        })
                    });
                    const endData = await endRes.json();
                    if (endRes.ok) {
                        resultsHTML += `<strong>End:</strong> ${endData.address}<br>`;
                        if (endMarker) endMarker.bindPopup(`<b>End Result:</b><br>${endData.address}`).openPopup();
                    } else {
                        resultsHTML += `<strong>End Error:</strong> ${endData.detail || 'Test failed'}<br>`;
                    }
                }

                if (geocodingTestResult.timeoutId) clearTimeout(geocodingTestResult.timeoutId);
                geocodingTestResult.innerHTML = resultsHTML;
                geocodingTestResult.className = 'status-message success';
                geocodingTestResult.style.display = 'block';

            } catch (error) {
                showMessage(geocodingTestResult, `Error: ${error.message}`, 'error', 0);
            } finally {
                testGeocodeBtn.disabled = false;
            }
        });
    }

    // --- Initial Load ---
    if (document.getElementById('geocoding-map')) {
        initGeocodingMap();
    }
    populateCheckboxes(mqttSensorSelection, ALL_SENSORS, 'mqtt-sensor');
    loadUsername();
    loadSettings();
    loadRecentTrips();
});