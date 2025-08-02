document.addEventListener('DOMContentLoaded', () => {
    // --- Element Selectors ---
    const credentialsForm = document.getElementById('credentials-form');
    const usernameInput = document.getElementById('username');
    const passwordInput = document.getElementById('password');
    const credentialsMessage = document.getElementById('credentials-message');

    const pollingSettingsForm = document.getElementById('polling-settings-form');
    const apiRetriesForm = document.getElementById('api-retries-form');
    const displaySettingsForm = document.getElementById('display-settings-form');
    const loggingSettingsForm = document.getElementById('logging-settings-form');
    const geocodingSettingsForm = document.getElementById('geocoding-settings-form');

    const pollingStatusMessage = document.getElementById('polling-status-message');
    const apiRetriesStatusMessage = document.getElementById('api-retries-status-message');
    const displayStatusMessage = document.getElementById('display-status-message');
    const loggingStatusMessage = document.getElementById('logging-status-message');
    const geocodingStatusMessage = document.getElementById('geocoding-status-message');

    const intervalSettingsDiv = document.getElementById('interval-settings');
    const fixedTimeSettingsDiv = document.getElementById('fixed-time-settings');

    const importForm = document.getElementById('import-form');
    const importStatusMessage = document.getElementById('import-status-message');

    const backfillUnitsBtn = document.getElementById('backfill-units-btn');
    const backfillUnitsMessage = document.getElementById('backfill-units-message');
    const backfillGeocodeBtn = document.getElementById('backfill-geocode-btn');

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
                passwordInput.value = ''; // Clear password field after successful save

                // --- Trigger a data poll after saving credentials ---
                // Show an immediate feedback message
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

            // Polling settings
            const polling = config.web_server?.polling || {};
            document.querySelector(`input[name="poll_mode"][value="${polling.mode || 'interval'}"]`).checked = true;
            document.getElementById('refresh-interval').value = polling.interval_seconds || 3600;
            document.getElementById('fixed-time').value = polling.fixed_time || '07:00';
            togglePollingInputs();

            // API Retries
            document.getElementById('api-retries').value = config.api_retries || 3;
            document.getElementById('api-retry-delay').value = config.api_retry_delay_seconds || 20;

            // Display Settings
            document.querySelector(`input[name="unit_system"][value="${config.unit_system || 'metric'}"]`).checked = true;

            // Log History Size
            document.getElementById('log-history-size').value = config.log_history_size || 200;

            // Geocoding
            document.getElementById('reverse-geocode-enabled').checked = config.reverse_geocode_enabled !== false; // Default to true if not present

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
    pollingSettingsForm.addEventListener('submit', async (e) => {
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
        await saveConfig(newSettings, pollingStatusMessage);
    });

    apiRetriesForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        const formData = new FormData(apiRetriesForm);
        const newSettings = {
            api_retries: parseInt(formData.get('api_retries'), 10),
            api_retry_delay_seconds: parseInt(formData.get('api_retry_delay_seconds'), 10),
        };
        await saveConfig(newSettings, apiRetriesStatusMessage);
    });

    displaySettingsForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        const formData = new FormData(displaySettingsForm);
        const newSettings = {
            unit_system: formData.get('unit_system'),
        };
        await saveConfig(newSettings, displayStatusMessage);
    });

    loggingSettingsForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        const formData = new FormData(loggingSettingsForm);
        const newSettings = {
            log_history_size: parseInt(formData.get('log_history_size'), 10),
        };
        await saveConfig(newSettings, loggingStatusMessage);
    });

    geocodingSettingsForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        const newSettings = {
            reverse_geocode_enabled: document.getElementById('reverse-geocode-enabled').checked,
        };
        await saveConfig(newSettings, geocodingStatusMessage);
    });

    async function saveConfig(newSettings, messageElement) {
        try {
            const response = await fetch('/api/config', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(newSettings),
            });
            const result = await response.json();
            if (response.ok) {
                showMessage(messageElement, result.message, 'success');
            } else {
                throw new Error(result.detail || 'An unknown error occurred.');
            }
        } catch (error) {
            showMessage(messageElement, `Error: ${error.message}`, 'error');
        }
    }

    // --- CSV Import ---
    importForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        // This is a placeholder for the import logic which is already implemented.
        // The form submission is handled by the browser by default if not prevented.
        // For AJAX-based submission, the logic would go here.
        showMessage(importStatusMessage, 'Import functionality is handled via a separate endpoint.', 'info');
    });

    // --- Data Utilities ---
    if (backfillUnitsBtn) {
        backfillUnitsBtn.addEventListener('click', async () => {
            backfillUnitsBtn.disabled = true;
            backfillUnitsBtn.textContent = 'Backfilling...';
            showMessage(backfillUnitsMessage, 'Starting backfill process. This may take a moment...', 'info');
    
            try {
                const response = await fetch('/api/backfill_units', { method: 'POST' });
                const result = await response.json();
                if (response.ok) {
                    showMessage(backfillUnitsMessage, result.message, 'success');
                } else {
                    throw new Error(result.detail || 'Unknown error');
                }
            } catch (error) {
                showMessage(backfillUnitsMessage, `Error: ${error.message}`, 'error');
            } finally {
                backfillUnitsBtn.disabled = false;
                backfillUnitsBtn.textContent = 'Backfill Imperial Units';
            }
        });
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
    loadUsername();
    loadSettings();
});