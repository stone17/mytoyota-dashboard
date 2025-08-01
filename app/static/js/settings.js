document.addEventListener('DOMContentLoaded', () => {
    const form = document.getElementById('settings-form');
    const credsForm = document.getElementById('credentials-form');
    const statusMessage = document.getElementById('status-message');
    const importForm = document.getElementById('import-form');
    const importStatusMessage = document.getElementById('import-status-message');
    const pollModeRadios = document.querySelectorAll('input[name="poll_mode"]');
    const intervalSettings = document.getElementById('interval-settings');
    const fixedTimeSettings = document.getElementById('fixed-time-settings');

    // --- Credentials Logic ---
    if (credsForm) {
        const usernameInput = document.getElementById('username');
        const passwordInput = document.getElementById('password');
        const credsMessage = document.getElementById('credentials-message');

        async function loadCredentials() {
            try {
                const response = await fetch('/api/credentials');
                if (!response.ok) throw new Error('Failed to fetch credentials');
                const data = await response.json();
                if (data.username) {
                    usernameInput.value = data.username;
                }
            } catch (error) {
                showCredsStatus('Error loading username.', 'error');
            }
        }

        credsForm.addEventListener('submit', async (event) => {
            event.preventDefault();
            const username = usernameInput.value;
            const password = passwordInput.value;

            if (!password) {
                showCredsStatus('Password is required to save.', 'error');
                return;
            }

            try {
                const response = await fetch('/api/credentials', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ username, password })
                });
                const result = await response.json();
                showCredsStatus(result.message || result.detail, response.ok ? 'success' : 'error');
                if (response.ok) {
                    passwordInput.value = ''; // Clear password field on success
                }
            } catch (error) {
                showCredsStatus('Failed to save credentials.', 'error');
            }
        });

        function showCredsStatus(message, type) {
            credsMessage.textContent = message;
            credsMessage.className = `status-message ${type}`;
            credsMessage.style.display = 'block';
        }

        loadCredentials();
    }

    // Load current settings
    async function loadSettings() {
        try {
            const response = await fetch('/api/config');
            const config = await response.json();
            
            const pollConfig = config.web_server.polling || {};
            const mode = pollConfig.mode || 'interval';

            document.querySelector(`input[name="poll_mode"][value="${mode}"]`).checked = true;
            document.getElementById('refresh-interval').value = pollConfig.interval_seconds || 3600;
            document.getElementById('fixed-time').value = pollConfig.fixed_time || '07:00';

            document.getElementById('api-retries').value = config.api_retries;
            document.getElementById('api-retry-delay').value = config.api_retry_delay_seconds;

            // Trigger change event to set initial visibility
            document.querySelector(`input[name="poll_mode"][value="${mode}"]`).dispatchEvent(new Event('change'));

        } catch (error) {
            showStatus('Error loading settings.', 'error');
        }
    }

    pollModeRadios.forEach(radio => {
        radio.addEventListener('change', (event) => {
            const isInterval = event.target.value === 'interval';
            intervalSettings.style.display = isInterval ? 'block' : 'none';
            fixedTimeSettings.style.display = isInterval ? 'none' : 'block';
        });
    });

    // Handle form submission
    form.addEventListener('submit', async (event) => {
        event.preventDefault();
        
        const pollMode = document.querySelector('input[name="poll_mode"]:checked').value;
        const updatedConfig = {
            web_server: {
                polling: {
                    mode: pollMode,
                    interval_seconds: parseInt(document.getElementById('refresh-interval').value, 10),
                    fixed_time: document.getElementById('fixed-time').value
                }
            },
            api_retries: parseInt(document.getElementById('api-retries').value, 10),
            api_retry_delay_seconds: parseInt(document.getElementById('api-retry-delay').value, 10)
        };

        try {
            const response = await fetch('/api/config', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(updatedConfig)
            });

            const result = await response.json();
            showStatus(result.message, response.ok ? 'success' : 'error');

        } catch (error) {
            showStatus('Failed to save settings.', 'error');
        }
    });

    // Handle import form submission
    importForm.addEventListener('submit', async (event) => {
        event.preventDefault();
        const fileInput = document.getElementById('csv-file');
        const file = fileInput.files[0];

        if (!file) {
            showImportStatus('Please select a file to upload.', 'error');
            return;
        }

        const formData = new FormData();
        formData.append('file', file);

        showImportStatus('Uploading and processing... Please wait.', 'info');

        try {
            const response = await fetch('/api/import/trips', {
                method: 'POST',
                body: formData
            });

            const result = await response.json();

            if (response.ok) {
                const message = `Import successful! New: ${result.imported}, Updated: ${result.updated}, Skipped: ${result.skipped_duplicates_or_errors}.`;
                showImportStatus(message, 'success');
            } else {
                showImportStatus(`Error: ${result.detail}`, 'error');
            }
        } catch (error) {
            showImportStatus('An unexpected error occurred during import.', 'error');
        }
    });

    function showStatus(message, type) {
        statusMessage.textContent = message;
        statusMessage.className = type;
        statusMessage.style.display = 'block';
    }

    function showImportStatus(message, type) {
        importStatusMessage.textContent = message;
        importStatusMessage.className = type;
        importStatusMessage.style.display = 'block';
    }

    loadSettings();
});