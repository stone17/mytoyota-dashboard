document.addEventListener('DOMContentLoaded', function() {
    // This single function will now load all initial data for the page.
    loadPageData();
});

// A helper function to render the service history table
function renderServiceHistory(history, unit = 'km') {
    const tbody = document.getElementById('service-history-tbody');
    const placeholder = document.getElementById('service-history-placeholder');
    
    tbody.innerHTML = ''; // Clear previous results

    if (history && history.length > 0) {
        placeholder.style.display = 'none'; // Hide placeholder
        history.forEach(item => {
            const row = tbody.insertRow();
            row.innerHTML = `
                <td>${item.service_date || 'N/A'}</td>
                <td>${item.mileage || 'N/A'} ${item.unit || unit}</td>
                <td>${item.service_category || 'N/A'}</td>
            `;
        });
    } else {
        placeholder.style.display = 'block'; // Show placeholder
        placeholder.textContent = 'No service history found for this vehicle.';
    }
}

// Main function to load data on page startup
async function loadPageData() {
    const notificationsContainer = document.getElementById('notifications-container');
    const fetchBtn = document.getElementById('fetch-service-history-btn');
    const serviceHistoryContainer = document.getElementById('service-history-results');
    
    try {
        const response = await fetch('/api/vehicles');
        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
        
        const vehicles = await response.json();
        notificationsContainer.innerHTML = ''; // Clear spinner

        if (vehicles.length === 0) {
            notificationsContainer.innerHTML = '<p class="message-box warning">No vehicle data found.</p>';
            return;
        }

        const vehicle = vehicles[0]; // Assuming one vehicle for this page
        const vin = vehicle.vin;

        // --- 1. Load and Render Notifications ---
        let foundNotifications = false;
        if (vehicle.notifications && vehicle.notifications.length > 0) {
            foundNotifications = true;
            const notificationsList = document.createElement('ul');
            notificationsList.className = 'notifications-list';
            vehicle.notifications.forEach(notification => {
                const listItem = document.createElement('li');
                
                const contentDiv = document.createElement('div');
                
                const messageDiv = document.createElement('div');
                messageDiv.className = 'notification-message';
                messageDiv.textContent = notification.message;
                contentDiv.appendChild(messageDiv);

                // --- ADDED: Render the timestamp ---
                if (notification.date) {
                    const timeDiv = document.createElement('div');
                    timeDiv.className = 'notification-time';
                    timeDiv.textContent = new Date(notification.date).toLocaleString();
                    contentDiv.appendChild(timeDiv);
                }
                
                listItem.appendChild(contentDiv);

                if (notification.read === null) {
                    const badge = document.createElement('span');
                    badge.className = 'notification-badge';
                    badge.textContent = 'New';
                    listItem.appendChild(badge);
                }
                notificationsList.appendChild(listItem);
            });
            notificationsContainer.appendChild(notificationsList);
        }
        if (!foundNotifications) {
             notificationsContainer.innerHTML = '<p class="message-box info">No notifications found.</p>';
        }

        // --- 2. Check for and Render Existing Service History ---
        if (vehicle.service_history) {
            console.log("Found existing service history in cache, rendering...");
            renderServiceHistory(vehicle.service_history);
        }

        // --- 3. Wire up the Fetch Button ---
        if (fetchBtn) {
            // Prevent adding multiple listeners if this function is ever called more than once
            if (!fetchBtn.handlerAttached) {
                fetchBtn.handlerAttached = true;
                fetchBtn.addEventListener('click', async function() {
                    fetchBtn.disabled = true;
                    fetchBtn.textContent = 'Fetching...';
                    
                    try {
                        const fetchResponse = await fetch(`/api/vehicles/${vin}/service_history`, { method: 'POST' });
                        if (!fetchResponse.ok) {
                            const errorResult = await fetchResponse.json();
                            throw new Error(errorResult.detail || 'Failed to fetch service history.');
                        }
                        const historyData = await fetchResponse.json();
                        renderServiceHistory(historyData.service_histories);
                    } catch (error) {
                        const placeholder = document.getElementById('service-history-placeholder');
                        placeholder.textContent = `Error: ${error.message}`;
                        placeholder.style.color = '#d9534f';
                        console.error("Service history fetch failed:", error);
                    } finally {
                        fetchBtn.disabled = false;
                        fetchBtn.textContent = 'Fetch History';
                    }
                });
            }
        }

    } catch (error) {
        console.error('Error loading page data:', error);
        notificationsContainer.innerHTML = '<p class="message-box error">Failed to load page data.</p>';
    }
}