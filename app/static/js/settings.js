document.addEventListener('DOMContentLoaded', () => {
    // --- Element Selectors ---
    const credentialsForm = document.getElementById('credentials-form');
    const usernameInput = document.getElementById('username');
    const passwordInput = document.getElementById('password');
    const credentialsMessage = document.getElementById('credentials-message');

    const pollingSettingsForm = document.getElementById('polling-settings-form');
    const apiRetriesForm = document.getElementById('api-retries-form');
    const displaySettingsForm = document.getElementById('display-settings-form');
    const dashboardDisplayForm = document.getElementById('dashboard-display-form');
    const loggingSettingsForm = document.getElementById('logging-settings-form');
    const geocodingSettingsForm = document.getElementById('geocoding-settings-form');
    const mqttSettingsForm = document.getElementById('mqtt-settings-form');

    const pollingStatusMessage = document.getElementById('polling-status-message');
    const apiRetriesStatusMessage = document.getElementById('api-retries-status-message');
    const displayStatusMessage = document.getElementById('display-status-message');
    const dashboardDisplayStatusMessage = document.getElementById('dashboard-display-status-message');
    const loggingStatusMessage = document.getElementById('logging-status-message');
    const geocodingStatusMessage = document.getElementById('geocoding-status-message');
    const mqttStatusMessage = document.getElementById('mqtt-status-message');
    const mqttTestBtn = document.getElementById('mqtt-test-btn');
    const mqttSensorSelection = document.getElementById('mqtt-sensor-selection');
    const dashboardSensorSelection = document.getElementById('dashboard-sensor-selection');

    const intervalSettingsDiv = document.getElementById('interval-settings');
    const fixedTimeSettingsDiv = document.getElementById('fixed-time-settings');

    const backfillGeocodeBtn = document.getElementById('backfill-geocode-btn');
    
    // --- Define available sensors ---
    const ALL_SENSORS = {
        'odometer': 'Odometer',
        'lock_status': 'Lock Status',
        'fuel_level': 'Fuel Level',
        'fuel_consumption': 'Fuel Consumption',
        'total_range': 'Total Range',
        'battery_level': 'EV Battery %',
        'ev_range': 'EV Range'
    };
    
    const ALL_DASHBOARD_STATS = {
        'odometer': 'Odometer',
        'range': 'Range Left',
        'total_ev_distance': 'Total EV Distance',
        'fuel_level': 'Fuel Level',
        'daily_distance': "Today's Distance",
        'consumption': 'Overall Consumption',
        'total_fuel': 'Total Fuel Used',
        'duration': 'Total Time Driven',
        'ev_level': 'EV Battery Level',
        'ev_range': 'EV Range',
        'charging_status': 'Charging Status',
        'max_speed': 'Max Speed Ever',
        'countries': 'Countries Visited',
        'highway_distance': 'Highway Distance'
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
    function showMessage(element, message, type = 'info') {
        element.textContent = message;
        element.className = `status-message ${type}`;
        element.style.display = 'block';
        setTimeout(() => { element.style.display = 'none'; }, 5000);
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

            const enabledDashboardSensors = config.dashboard_sensors || {};
            document.querySelectorAll('#dashboard-sensor-selection input[type="checkbox"]').forEach(cb => {
                cb.checked = enabledDashboardSensors[cb.dataset.sensorKey] !== false; // Default to true if not present
            });

            document.getElementById('log-history-size').value = config.log_history_size || 200;
            document.getElementById('reverse-geocode-enabled').checked = config.reverse_geocode_enabled !== false;
            document.getElementById('fetch-full-route').checked = config.fetch_full_trip_route || false;
            
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
    
    dashboardDisplayForm.addEventListener('submit', (e) => {
        e.preventDefault();
        const enabledSensors = {};
        document.querySelectorAll('#dashboard-sensor-selection input[type="checkbox"]').forEach(cb => {
            enabledSensors[cb.dataset.sensorKey] = cb.checked;
        });
        const newSettings = { dashboard_sensors: enabledSensors };
        saveConfig(newSettings, dashboardDisplayStatusMessage);
    });

    loggingSettingsForm.addEventListener('submit', (e) => {
        e.preventDefault();
        const formData = new FormData(loggingSettingsForm);
        const newSettings = {
            log_history_size: parseInt(formData.get('log_history_size'), 10),
        };
        saveConfig(newSettings, loggingStatusMessage);
    });

    geocodingSettingsForm.addEventListener('submit', (e) => {
        e.preventDefault();
        const newSettings = {
            reverse_geocode_enabled: document.getElementById('reverse-geocode-enabled').checked,
            fetch_full_trip_route: document.getElementById('fetch-full-route').checked
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

    // --- Initial Load ---
    populateCheckboxes(mqttSensorSelection, ALL_SENSORS, 'mqtt-sensor');
    populateCheckboxes(dashboardSensorSelection, ALL_DASHBOARD_STATS, 'dashboard-sensor');
    loadUsername();
    loadSettings();
});