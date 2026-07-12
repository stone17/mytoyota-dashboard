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

    const saveRawCheckbox = document.getElementById('save-raw-responses');
    const rawRetentionGroup = document.getElementById('raw-retention-group');
    const rawRetentionSelect = document.getElementById('raw-responses-retention');
    const rawViewerPanel = document.getElementById('raw-viewer-panel');
    const rawPollSelector = document.getElementById('raw-poll-selector');
    const refreshRawBtn = document.getElementById('refresh-raw-btn');
    const downloadPollLink = document.getElementById('download-poll-link');
    const rawFilesContainer = document.getElementById('raw-files-container');
    const rawContent = document.getElementById('raw-response-content');

    let allPolls = [];

    async function fetchRawPollsList() {
        try {
            const res = await fetch('/api/raw_responses');
            if (!res.ok) throw new Error('Failed to fetch polls list');
            allPolls = await res.json();
            
            // Keep the default option
            const defaultOption = rawPollSelector.querySelector('option[disabled]');
            rawPollSelector.innerHTML = '';
            if (defaultOption) rawPollSelector.appendChild(defaultOption);
            
            allPolls.forEach(poll => {
                const opt = document.createElement('option');
                opt.value = poll.poll_id;
                
                const match = poll.poll_id.match(/^(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})_(.+)$/);
                if (match) {
                    let typeStr = match[7].replace(/_/g, ' ');
                    typeStr = typeStr.charAt(0).toUpperCase() + typeStr.slice(1);
                    opt.textContent = `${match[1]}-${match[2]}-${match[3]} ${match[4]}:${match[5]}:${match[6]} (${typeStr})`;
                } else {
                    opt.textContent = poll.poll_id;
                }
                rawPollSelector.appendChild(opt);
            });
        } catch (e) {
            console.error(e);
        }
    }

    async function loadRawFile(pollId, filename, itemEl) {
        if (itemEl) {
            document.querySelectorAll('.raw-file-item').forEach(el => el.classList.remove('active'));
            itemEl.classList.add('active');
        }
        
        const url = `/api/raw_responses/${encodeURIComponent(pollId)}/${encodeURIComponent(filename)}`;
        
        try {
            const res = await fetch(url);
            if (!res.ok) throw new Error(res.status === 404 ? 'File not found.' : 'Fetch error.');
            const data = await res.json();
            rawContent.textContent = JSON.stringify(data, null, 2);
        } catch (e) {
            rawContent.textContent = e.message;
        }
    }

    if (saveRawCheckbox) {
        saveRawCheckbox.addEventListener('change', () => {
            const isChecked = saveRawCheckbox.checked;
            rawRetentionGroup.style.display = isChecked ? 'block' : 'none';
            rawViewerPanel.style.display = isChecked ? 'flex' : 'none';
            if (isChecked) fetchRawPollsList();
        });
        rawPollSelector.addEventListener('change', () => {
            const pollId = rawPollSelector.value;
            rawFilesContainer.innerHTML = '';
            rawContent.textContent = '';
            
            const poll = allPolls.find(p => p.poll_id === pollId);
            if (poll && poll.files.length > 0) {
                downloadPollLink.style.display = 'inline-block';
                downloadPollLink.href = `/api/raw_responses/${encodeURIComponent(pollId)}/download`;
                
                poll.files.forEach((file, index) => {
                    const item = document.createElement('div');
                    item.className = 'raw-file-item';
                    item.textContent = file.filename;
                    item.title = file.filename;
                    item.addEventListener('click', () => loadRawFile(pollId, file.filename, item));
                    rawFilesContainer.appendChild(item);
                    
                    if (index === 0) {
                        loadRawFile(pollId, file.filename, item);
                    }
                });
            } else {
                downloadPollLink.style.display = 'none';
            }
        });
        refreshRawBtn.addEventListener('click', () => {
            fetchRawPollsList().then(() => {
                if (rawPollSelector.value) {
                    rawPollSelector.dispatchEvent(new Event('change'));
                }
            });
        });
    }

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
            
            if (saveRawCheckbox) {
                saveRawCheckbox.checked = config.save_raw_responses === true;
                rawRetentionGroup.style.display = saveRawCheckbox.checked ? 'block' : 'none';
                rawViewerPanel.style.display = saveRawCheckbox.checked ? 'flex' : 'none';
                if (rawRetentionSelect) {
                    rawRetentionSelect.value = config.raw_responses_retention || 'always';
                }
                if (saveRawCheckbox.checked) fetchRawPollsList();
            }
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
            save_raw_responses: saveRawCheckbox ? saveRawCheckbox.checked : false,
            raw_responses_retention: rawRetentionSelect ? rawRetentionSelect.value : 'always'
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
