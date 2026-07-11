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

window.appConfig = window.appConfig || { timezone: 'UTC' };

function renderVehicleData(vehicle) {
    const notificationsContainer = document.getElementById('notifications-container');
    const fetchBtn = document.getElementById('fetch-service-history-btn');
    const vin = vehicle.vin;

    // --- 1. Load and Render Notifications ---
    notificationsContainer.innerHTML = '';
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
                timeDiv.textContent = new Date(notification.date).toLocaleString(undefined, { timeZone: window.appConfig?.timezone || 'UTC' });
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
         notificationsContainer.innerHTML = window.i18n ? `<p class="message-box info">${window.i18n.t('notifications.no_notifications')}</p>` : '<p class="message-box info">No notifications found.</p>';
    }

    // --- 2. Check for and Render Existing Service History ---
    if (vehicle.service_history) {
        console.log("Found existing service history in cache, rendering...");
        renderServiceHistory(vehicle.service_history);
    } else {
        renderServiceHistory([]); // Clear it if none exists
    }

    // --- 3. Wire up the Fetch Button ---
    if (fetchBtn) {
        // Clone the button to remove any previous event listeners
        const newBtn = fetchBtn.cloneNode(true);
        fetchBtn.parentNode.replaceChild(newBtn, fetchBtn);
        
        newBtn.addEventListener('click', async function() {
            if (vin === 'all') {
                alert("Please select a specific vehicle to fetch service history.");
                return;
            }
            newBtn.disabled = true;
            newBtn.textContent = 'Fetching...';
            
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
                placeholder.style.display = 'block';
                placeholder.style.color = '#d9534f';
                document.getElementById('service-history-tbody').innerHTML = '';
                console.error("Service history fetch failed:", error);
            } finally {
                newBtn.disabled = false;
                newBtn.textContent = 'Fetch History';
            }
        });
    }
}

// Main function to load data on page startup
async function loadPageData() {
    const notificationsContainer = document.getElementById('notifications-container');
    
    try {
        const configResponse = await fetch('/api/config');
        if (configResponse.ok) Object.assign(window.appConfig, await configResponse.json());

        const response = await fetch('/api/vehicles');
        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
        
        const vehicles = await response.json();
        notificationsContainer.innerHTML = ''; // Clear spinner

        if (vehicles.length === 0) {
            notificationsContainer.innerHTML = window.i18n ? `<p class="message-box warning">${window.i18n.t('notifications.no_vehicles')}</p>` : '<p class="message-box warning">No vehicle data found.</p>';
            return;
        }

        const vinSelect = document.getElementById('global-vehicle-select');
        if (vinSelect) {
            vinSelect.innerHTML = '';
            if (vehicles.length > 1) {
                vinSelect.style.display = 'inline-block';
                vinSelect.appendChild(new Option("All Cars", "all"));
                vehicles.forEach(v => {
                    vinSelect.appendChild(new Option(`${v.alias} (${v.vin})`, v.vin));
                });
                
                let savedVin = localStorage.getItem('selected_vin');
                if (savedVin && (savedVin === 'all' || vehicles.some(v => v.vin === savedVin))) {
                    vinSelect.value = savedVin;
                } else {
                    localStorage.setItem('selected_vin', vehicles[0].vin);
                    vinSelect.value = vehicles[0].vin;
                }
                
                // Replace element to clear old event listeners
                const newSelect = vinSelect.cloneNode(true);
                vinSelect.parentNode.replaceChild(newSelect, vinSelect);
                newSelect.addEventListener('change', (e) => {
                    localStorage.setItem('selected_vin', e.target.value);
                    let selectedVehicle;
                    if (e.target.value === 'all') {
                        selectedVehicle = {
                            vin: 'all',
                            alias: 'All Cars',
                            notifications: vehicles.flatMap(v => v.notifications || []).sort((a, b) => new Date(b.date) - new Date(a.date)),
                            service_history: vehicles.flatMap(v => v.service_history || []).sort((a, b) => new Date(b.service_date) - new Date(a.service_date))
                        };
                    } else {
                        selectedVehicle = vehicles.find(v => v.vin === e.target.value);
                    }
                    renderVehicleData(selectedVehicle);
                });
            } else {
                vinSelect.style.display = 'none';
            }
        }

        let savedVin = localStorage.getItem('selected_vin');
        let vehicleToRender;
        if (savedVin === 'all' && vehicles.length > 1) {
            vehicleToRender = {
                vin: 'all',
                alias: 'All Cars',
                notifications: vehicles.flatMap(v => v.notifications || []).sort((a, b) => new Date(b.date) - new Date(a.date)),
                service_history: vehicles.flatMap(v => v.service_history || []).sort((a, b) => new Date(b.service_date) - new Date(a.service_date))
            };
        } else {
            vehicleToRender = vehicles.find(v => v.vin === savedVin) || vehicles[0];
        }
        
        renderVehicleData(vehicleToRender);

    } catch (error) {
        console.error('Error loading page data:', error);
        notificationsContainer.innerHTML = window.i18n ? `<p class="message-box error">${window.i18n.t('notifications.error_loading')}</p>` : '<p class="message-box error">Failed to load page data.</p>';
    }
}
