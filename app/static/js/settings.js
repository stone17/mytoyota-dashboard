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
        if (!container) return;
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
        
        // Keep error messages on screen indefinitely
        if (type === 'error') {
            duration = 0;
        }

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
                    const startLatEl = document.getElementById('test-start-lat');
                    const startLonEl = document.getElementById('test-start-lon');
                    const endLatEl = document.getElementById('test-end-lat');
                    const endLonEl = document.getElementById('test-end-lon');
                    if (startLatEl) startLatEl.value = '';
                    if (startLonEl) startLonEl.value = '';
                    if (endLatEl) endLatEl.value = '';
                    if (endLonEl) endLonEl.value = '';
                    
                    if (geocodingMap) {
                        if (startMarker) geocodingMap.removeLayer(startMarker);
                        if (endMarker) geocodingMap.removeLayer(endMarker);
                        if (routeLine) geocodingMap.removeLayer(routeLine);
                    }
                    return;
                }

                const coords = JSON.parse(e.target.value);
                const startLatEl = document.getElementById('test-start-lat');
                const startLonEl = document.getElementById('test-start-lon');
                const endLatEl = document.getElementById('test-end-lat');
                const endLonEl = document.getElementById('test-end-lon');
                
                if (startLatEl) startLatEl.value = coords.start_lat;
                if (startLonEl) startLonEl.value = coords.start_lon;
                if (endLatEl) endLatEl.value = coords.end_lat;
                if (endLonEl) endLonEl.value = coords.end_lon;
                
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

    
    // --- Language Selection ---
    const langSelect = document.getElementById('language-select');
    if (langSelect) {
        langSelect.value = localStorage.getItem('language') || 'en';
        langSelect.addEventListener('change', async (e) => {
            const newLang = e.target.value;
            localStorage.setItem('language', newLang);
            window.i18n.locale = newLang;
            await window.i18n.init();
            
            // Dispatch event so charts and other components can redraw
            window.dispatchEvent(new Event('languageChanged'));
            window.dispatchEvent(new Event('themeChanged')); // Redraw charts
        });
    }

    // --- Credentials Management ---
    async function loadUsername() {
        try {
            const response = await fetch('/api/credentials');
            const data = await response.json();
            if (data.username && usernameInput) {
                usernameInput.value = data.username;
            }
        } catch (error) {
            console.error('Failed to load username:', error);
        }
    }

    if (credentialsForm) {
        credentialsForm.addEventListener('submit', async (e) => {
            e.preventDefault();
            const username = usernameInput ? usernameInput.value : '';
            const password = passwordInput ? passwordInput.value : '';

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
                    if (passwordInput) passwordInput.value = '';

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
    }

    // --- Load Settings ---
    async function loadSettings() {
        try {
            const response = await fetch('/api/config');
            const config = await response.json();
            console.log("Loaded config from API:", config);

            const setVal = (id, val) => { const el = document.getElementById(id); if (el) el.value = val; };
            const setCheck = (id, val) => { const el = document.getElementById(id); if (el) el.checked = val; };

            const polling = config.web_server?.polling || {};
            const pollModeEl = document.querySelector(`input[name="poll_mode"][value="${polling.mode || 'interval'}"]`);
            if (pollModeEl) pollModeEl.checked = true;
            
            setVal('refresh-interval', polling.interval_seconds || 3600);
            setVal('fixed-time', polling.fixed_time || '07:00');
            togglePollingInputs();

            setVal('api-retries', config.api_retries || 3);
            setVal('api-retry-delay', config.api_retry_delay_seconds || 20);
            
            const unitSystemEl = document.querySelector(`input[name="unit_system"][value="${config.unit_system || 'metric'}"]`);
            if (unitSystemEl) unitSystemEl.checked = true;

            setCheck('reverse-geocode-enabled', config.reverse_geocode_enabled !== false);
            setCheck('fetch-full-route', config.fetch_full_trip_route || false);
            
            const geocoding = config.geocoding || {};
            if (geocodingProviderSelect) {
                geocodingProviderSelect.value = geocoding.provider || 'nominatim';
                setVal('opencage-api-key', geocoding.opencage_api_key || '');
                setVal('google-maps-api-key', geocoding.google_maps_api_key || '');
                
                function toggleGeocodingProviderFields() {
                    const provider = geocodingProviderSelect.value;
                    if (opencageKeyGroup) opencageKeyGroup.style.display = provider === 'opencage' ? 'block' : 'none';
                    if (googleMapsKeyGroup) googleMapsKeyGroup.style.display = provider === 'google_maps' ? 'block' : 'none';
                }
                
                geocodingProviderSelect.addEventListener('change', toggleGeocodingProviderFields);
                toggleGeocodingProviderFields();
            }

            const mqtt = config.mqtt || {};
            console.log("MQTT config to apply:", mqtt);
            setCheck('mqtt-enabled', mqtt.enabled || false);
            setVal('mqtt-host', mqtt.host || mqtt.broker || '');
            setVal('mqtt-port', mqtt.port || 1883);
            setVal('mqtt-username', mqtt.username || '');
            setVal('mqtt-password', mqtt.password || '');
            setVal('mqtt-base-topic', mqtt.base_topic || '');
            setVal('mqtt-discovery-prefix', mqtt.discovery_prefix || 'homeassistant');

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
        const checkedMode = document.querySelector('input[name="poll_mode"]:checked');
        if (!checkedMode) return;
        const mode = checkedMode.value;
        if (intervalSettingsDiv) intervalSettingsDiv.style.display = mode === 'interval' ? 'block' : 'none';
        if (fixedTimeSettingsDiv) fixedTimeSettingsDiv.style.display = mode === 'fixed_time' ? 'block' : 'none';
    }

    document.querySelectorAll('input[name="poll_mode"]').forEach(radio => {
        radio.addEventListener('change', togglePollingInputs);
    });

    // --- Save Settings Event Listeners ---
    if (pollingSettingsForm) {
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
    }
    
    if (mqttSettingsForm) {
        mqttSettingsForm.addEventListener('submit', (e) => {
            e.preventDefault();
            
            const enabledSensors = {};
            document.querySelectorAll('#mqtt-sensor-selection input[type="checkbox"]').forEach(cb => {
                enabledSensors[cb.dataset.sensorKey] = cb.checked;
            });

            const getVal = (id) => { const el = document.getElementById(id); return el ? el.value : ''; };
            const getCheck = (id) => { const el = document.getElementById(id); return el ? el.checked : false; };

            const newSettings = {
                mqtt: {
                    enabled: getCheck('mqtt-enabled'),
                    host: getVal('mqtt-host'),
                    port: parseInt(getVal('mqtt-port') || 1883, 10),
                    username: getVal('mqtt-username'),
                    password: getVal('mqtt-password'),
                    base_topic: getVal('mqtt-base-topic'),
                    discovery_prefix: getVal('mqtt-discovery-prefix'),
                    enabled_sensors: enabledSensors
                }
            };

            if (!newSettings.mqtt.password) {
                delete newSettings.mqtt.password;
            }
            saveConfig(newSettings, mqttStatusMessage);
        });
    }

    if (mqttTestBtn) {
        mqttTestBtn.addEventListener('click', async () => {
            showMessage(mqttStatusMessage, 'Testing MQTT connection with current settings...', 'info');
            
            const enabledSensors = {};
            document.querySelectorAll('#mqtt-sensor-selection input[type="checkbox"]').forEach(cb => {
                enabledSensors[cb.dataset.sensorKey] = cb.checked;
            });

            const getVal = (id) => { const el = document.getElementById(id); return el ? el.value : ''; };
            const getCheck = (id) => { const el = document.getElementById(id); return el ? el.checked : false; };

            const testSettings = {
                enabled: getCheck('mqtt-enabled'),
                host: getVal('mqtt-host'),
                port: parseInt(getVal('mqtt-port') || 1883, 10),
                username: getVal('mqtt-username'),
                password: getVal('mqtt-password'),
                base_topic: getVal('mqtt-base-topic'),
                discovery_prefix: getVal('mqtt-discovery-prefix'),
                enabled_sensors: enabledSensors
            };

            try {
                const response = await fetch('/api/mqtt/test', { 
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(testSettings)
                });
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
    }

    if (apiRetriesForm) {
        apiRetriesForm.addEventListener('submit', (e) => {
            e.preventDefault();
            const formData = new FormData(apiRetriesForm);
            const newSettings = {
                api_retries: parseInt(formData.get('api_retries'), 10),
                api_retry_delay_seconds: parseInt(formData.get('api_retry_delay_seconds'), 10),
            };
            saveConfig(newSettings, apiRetriesStatusMessage);
        });
    }

    if (displaySettingsForm) {
        displaySettingsForm.addEventListener('submit', (e) => {
            e.preventDefault();
            const formData = new FormData(displaySettingsForm);
            const newSettings = {
                unit_system: formData.get('unit_system'),
            };
            saveConfig(newSettings, displayStatusMessage);
        });
    }

    if (geocodingSettingsForm) {
        geocodingSettingsForm.addEventListener('submit', (e) => {
            e.preventDefault();
            const getVal = (id) => { const el = document.getElementById(id); return el ? el.value : ''; };
            const getCheck = (id) => { const el = document.getElementById(id); return el ? el.checked : false; };

            const newSettings = {
                reverse_geocode_enabled: getCheck('reverse-geocode-enabled'),
                fetch_full_trip_route: getCheck('fetch-full-route'),
                geocoding: {
                    provider: geocodingProviderSelect ? geocodingProviderSelect.value : 'nominatim',
                    opencage_api_key: getVal('opencage-api-key'),
                    google_maps_api_key: getVal('google-maps-api-key')
                }
            };
            saveConfig(newSettings, geocodingStatusMessage);
        });
    }

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
            const startLatEl = document.getElementById('test-start-lat');
            const startLonEl = document.getElementById('test-start-lon');
            const endLatEl = document.getElementById('test-end-lat');
            const endLonEl = document.getElementById('test-end-lon');
            
            const startLat = startLatEl ? startLatEl.value : '';
            const startLon = startLonEl ? startLonEl.value : '';
            const endLat = endLatEl ? endLatEl.value : '';
            const endLon = endLonEl ? endLonEl.value : '';
            
            if ((!startLat || !startLon) && (!endLat || !endLon)) {
                showMessage(geocodingTestResult, 'Please provide at least one set of coordinates.', 'error', 0);
                return;
            }

            const provider = geocodingProviderSelect ? geocodingProviderSelect.value : 'nominatim';
            const opencageKeyEl = document.getElementById('opencage-api-key');
            const googleMapsKeyEl = document.getElementById('google-maps-api-key');
            const opencageKey = opencageKeyEl ? opencageKeyEl.value : '';
            const googleMapsKey = googleMapsKeyEl ? googleMapsKeyEl.value : '';

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
