document.addEventListener('DOMContentLoaded', () => {
    const logContent = document.getElementById('log-content');
    const MAX_LOG_LINES = 1000; // Maximum number of log lines to display

    if (!logContent) {
        console.error("Log page elements not found.");
        return;
    }

    function connectToLogStream() {
        logContent.textContent = 'Connecting to log stream...';

        const eventSource = new EventSource('/api/logs');

        function appendLogLine(logData, fragment = null) {
            const logLine = document.createElement('span');
            logLine.className = `log-line ${logData.level}`;
            logLine.textContent = logData.message;

            const target = fragment || logContent;
            target.appendChild(logLine);
            target.appendChild(document.createTextNode('\n'));
        }

        eventSource.addEventListener('history', function(event) {
            try {
                logContent.innerHTML = ''; // Clear "Connecting..." message
                const history = JSON.parse(event.data);
                const fragment = document.createDocumentFragment();

                history.forEach(logData => appendLogLine(logData, fragment));

                logContent.appendChild(fragment);
                logContent.scrollTop = logContent.scrollHeight; // Scroll to bottom after batch update
            } catch (e) {
                console.error("Failed to parse log history:", event.data, e);
                logContent.textContent = 'Error loading log history.';
            }
        });

        eventSource.addEventListener('message', function(event) {
            try {
                if (logContent.textContent.startsWith('Connecting')) {
                    logContent.innerHTML = ''; // Clear "Connecting..." if no history was received
                }
                const logData = JSON.parse(event.data);
                appendLogLine(logData);

                // Trim old log lines if the total exceeds the limit
                while (logContent.childNodes.length > MAX_LOG_LINES * 2) { // *2 because of text nodes
                    logContent.removeChild(logContent.firstChild);
                    logContent.removeChild(logContent.firstChild); // Remove accompanying text node
                }

                // Auto-scroll to the bottom
                logContent.scrollTop = logContent.scrollHeight;
            } catch (e) {
                console.error("Failed to parse log data:", event.data, e);
            }
        });

        eventSource.onerror = function() {
            logContent.textContent += '\n--- Connection to log stream lost. Reconnecting... ---\n';
            // The browser will automatically attempt to reconnect.
        };
    }

    // Start the connection
    connectToLogStream();

    // --- Logging Settings ---
    const saveLogSettingsBtn = document.getElementById('save-log-settings-btn');
    const logSettingsStatusMessage = document.getElementById('log-settings-status-message');

    async function loadLogSettings() {
        try {
            const response = await fetch('/api/config');
            const config = await response.json();

            const logging = config.logging || {};
            const levels = logging.levels || {};
            document.getElementById('app-log-level').value = levels.app || 'INFO';

            document.getElementById('log-history-size').value = config.log_history_size || 200;
        } catch (error) {
            console.error(`Failed to load settings: ${error.message}`);
            showMessage(logSettingsStatusMessage, `Failed to load settings: ${error.message}`, 'error');
        }
    }

    saveLogSettingsBtn.addEventListener('click', () => {
        const appLogLevel = document.getElementById('app-log-level').value;
        const logHistorySize = parseInt(document.getElementById('log-history-size').value, 10);

        const newSettings = {
            logging: {
                levels: {
                    app: appLogLevel,
                }
            },
            log_history_size: logHistorySize,
        };
        saveConfig(newSettings, logSettingsStatusMessage);
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

    function showMessage(element, message, type = 'info') {
        element.textContent = message;
        element.className = `status-message ${type}`;
        element.style.display = 'inline-block';
        setTimeout(() => { element.style.display = 'none'; }, 5000);
    }

    loadLogSettings();
});