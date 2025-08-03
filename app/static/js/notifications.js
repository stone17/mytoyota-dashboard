document.addEventListener('DOMContentLoaded', function() {
    loadNotifications();
});

async function loadNotifications() {
    const container = document.getElementById('notifications-container');
    try {
        const response = await fetch('/api/vehicles');
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        const vehicles = await response.json();

        container.innerHTML = ''; // Clear spinner

        if (vehicles.length === 0) {
            container.innerHTML = '<p class="message-box warning">No vehicle data found. Please wait for the next data poll or trigger one from the Settings page.</p>';
            return;
        }

        let foundNotifications = false;
        vehicles.forEach(vehicle => {
            if (vehicle.notifications && vehicle.notifications.length > 0) {
                foundNotifications = true;
                const vehicleHeader = document.createElement('h3');
                vehicleHeader.textContent = `Notifications for ${vehicle.alias}`;
                vehicleHeader.style.marginBottom = '15px';

                const notificationsList = document.createElement('ul');
                notificationsList.className = 'notifications-list';

                vehicle.notifications.forEach(notification => {
                    const listItem = document.createElement('li');
                    
                    const messageSpan = document.createElement('span');
                    messageSpan.textContent = notification.message;
                    
                    listItem.appendChild(messageSpan);

                    if (notification.read === null) {
                        const badge = document.createElement('span');
                        badge.className = 'notification-badge';
                        badge.textContent = 'New';
                        listItem.appendChild(badge);
                    }
                    notificationsList.appendChild(listItem);
                });
                
                container.appendChild(vehicleHeader);
                container.appendChild(notificationsList);
            }
        });

        if (!foundNotifications) {
             container.innerHTML = '<p class="message-box info">No notifications found for any vehicle.</p>';
        }

    } catch (error) {
        console.error('Error fetching vehicle data:', error);
        container.innerHTML = '<p class="message-box error">Failed to load notifications. Please check the console for errors.</p>';
    }
}