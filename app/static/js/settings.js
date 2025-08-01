document.addEventListener('DOMContentLoaded', () => {
    // --- Element Selectors ---
    const credentialsForm = document.getElementById('credentials-form');
    const usernameInput = document.getElementById('username');
    const passwordInput = document.getElementById('password');
    const credentialsMessage = document.getElementById('credentials-message');

    const settingsForm = document.getElementById('settings-form');
    const statusMessage = document.getElementById('status-message');
    const intervalSettingsDiv = document.getElementById('interval-settings');
    const fixedTimeSettingsDiv = document.getElementById('fixed-time-settings');

    const importForm = document.getElementById('import-form');
    const importStatusMessage = document.getElementById('import-status-message');

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
            } else {
                throw new Error(result.detail || 'An unknown error occurred.');
            }
        } catch (error) {
            showMessage(credentialsMessage, `Error: ${error.message}`, 'error');
        }
    });

    // --- Polling and General Settings ---
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

            // Log History Size
            document.getElementById('log-history-size').value = config.log_history_size || 200;

        } catch (error) {
            showMessage(statusMessage, `Failed to load settings: ${error.message}`, 'error');
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

    settingsForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        const formData = new FormData(settingsForm);
        const newSettings = {
            web_server: {
                polling: {
                    mode: formData.get('poll_mode'),
                    interval_seconds: parseInt(formData.get('interval_seconds'), 10),
                    fixed_time: formData.get('fixed_time'),
                }
            },
            api_retries: parseInt(formData.get('api_retries'), 10),
            api_retry_delay_seconds: parseInt(formData.get('api_retry_delay_seconds'), 10),
            log_history_size: parseInt(formData.get('log_history_size'), 10),
        };

        try {
            const response = await fetch('/api/config', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(newSettings),
            });
            const result = await response.json();
            if (response.ok) {
                showMessage(statusMessage, result.message, 'success');
            } else {
                throw new Error(result.detail || 'An unknown error occurred.');
            }
        } catch (error) {
            showMessage(statusMessage, `Error: ${error.message}`, 'error');
        }
    });

    // --- CSV Import ---
    importForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        // This is a placeholder for the import logic which is already implemented.
        // The form submission is handled by the browser by default if not prevented.
        // For AJAX-based submission, the logic would go here.
        showMessage(importStatusMessage, 'Import functionality is handled via a separate endpoint.', 'info');
    });

    // --- Initial Load ---
    loadUsername();
    loadSettings();
});