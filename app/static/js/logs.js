document.addEventListener('DOMContentLoaded', () => {
    const logContent = document.getElementById('log-content');

    if (!logContent) {
        console.error("Log page elements not found.");
        return;
    }

    function connectToLogStream() {
        logContent.textContent = 'Connecting to log stream...';

        const eventSource = new EventSource('/api/logs');
        let isFirstMessage = true;

        eventSource.onmessage = function(event) {
            try {
                if (isFirstMessage) {
                    logContent.innerHTML = ''; // Clear "Connecting..." message
                    isFirstMessage = false;
                }
                const logData = JSON.parse(event.data);
                const logLine = document.createElement('span');
                logLine.className = `log-line ${logData.level}`;
                logLine.textContent = logData.message;
                
                logContent.appendChild(logLine);
                logContent.appendChild(document.createTextNode('\n')); // Keep the newline separation
                
                // Auto-scroll to the bottom
                logContent.scrollTop = logContent.scrollHeight;
            } catch (e) {
                console.error("Failed to parse log data:", event.data, e);
            }
        };

        eventSource.onerror = function() {
            logContent.textContent += '\n--- Connection to log stream lost. Reconnecting... ---\n';
            // The browser will automatically attempt to reconnect.
        };
    }

    // Start the connection
    connectToLogStream();
});