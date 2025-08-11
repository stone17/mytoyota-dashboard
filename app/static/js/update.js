document.addEventListener('DOMContentLoaded', function () {
    const updateBtn = document.getElementById('update-app-btn');
    const updateMessage = document.getElementById('update-app-message');

    if (updateBtn) {
        updateBtn.addEventListener('click', function () {
            if (!confirm('Are you sure you want to update the application? This will restart the service.')) {
                return;
            }

            updateMessage.textContent = 'Updating... Please wait.';
            updateMessage.className = 'status-message info';
            updateMessage.style.display = 'block';

            fetch('/api/update', {
                method: 'POST'
            })
            .then(response => response.json().then(data => ({ ok: response.ok, data })))
            .then(({ ok, data }) => {
                if (ok) {
                    updateMessage.textContent = data.message || 'Update successful! The application is restarting.';
                    updateMessage.className = 'status-message success';
                } else {
                    updateMessage.textContent = data.detail || 'An error occurred during the update.';
                    updateMessage.className = 'status-message error';
                }
            })
            .catch(error => {
                console.error('Error during update:', error);
                updateMessage.textContent = 'An unexpected error occurred. Check the browser console for details.';
                updateMessage.className = 'status-message error';
            });
        });
    }
});