document.addEventListener('DOMContentLoaded', () => {
    const vehicleContainer = document.getElementById('vehicle-container');
    const vehicleTemplate = document.getElementById('vehicle-template');
    const lastUpdatedSpan = document.getElementById('last-updated');

    // This is a safeguard. If the script is on a page without the required
    // elements, it will stop execution gracefully instead of crashing.
    if (!vehicleContainer || !vehicleTemplate || !lastUpdatedSpan) {
        console.error("Dashboard UI elements are missing. This script should only run on the main dashboard page.");
        return;
    }

    let vehicleCharts = {}; // To hold chart instances, keyed by VIN

    async function renderHistoryChart(vin, canvas, metric1, metric2, period) {
        try {
            // Destroy existing chart for this VIN if it exists to prevent memory leaks
            if (vehicleCharts[vin]) {
                vehicleCharts[vin].destroy();
            }
            const response = await fetch(`/api/vehicles/${vin}/daily_summary?days=${period}`);
            const dailyData = await response.json();

            const labels = dailyData.map(d => new Date(d.date).getDate()); // Just the day number

            const metricConfig = {
                distance_km: { label: 'Distance', unit: 'km', color: '#00529b' },
                fuel_consumption_l_100km: { label: 'Consumption', unit: 'L/100km', color: '#d9534f' },
                ev_distance_km: { label: 'EV Distance', unit: 'km', color: '#5cb85c' },
                ev_duration_seconds: { label: 'EV Duration', unit: 'minutes', color: '#f0ad4e' },
                score_global: { label: 'Driving Score', unit: 'Score', color: '#5bc0de' },
                average_speed_kmh: { label: 'Average Speed', unit: 'km/h', color: '#337ab7' },
                duration_seconds: { label: 'Trip Duration', unit: 'minutes', color: '#777' },
                none: { label: 'None', unit: '', color: '#fff' }
            };

            const datasets = [];
            const yAxes = {};

            // Function to create a dataset
            const createDataset = (metric, yAxisID) => {
                if (!metric || metric === 'none') return null;

                let data = dailyData.map(d => d[metric]);
                const config = metricConfig[metric];

                if (metric === 'ev_duration_seconds' || metric === 'duration_seconds') {
                    data = data.map(s => s ? (s / 60).toFixed(1) : 0);
                }

                return {
                    label: config.label,
                    data: data,
                    borderColor: config.color,
                    backgroundColor: `${config.color}33`,
                    fill: true,
                    tension: 0.1,
                    pointRadius: 2,
                    yAxisID: yAxisID
                };
            };

            // Left Axis (y)
            const dataset1 = createDataset(metric1, 'y');
            if (dataset1) {
                datasets.push(dataset1);
                const config1 = metricConfig[metric1];
                yAxes.y = {
                    type: 'linear',
                    display: true,
                    position: 'left',
                    title: { display: true, text: `${config1.label} (${config1.unit})` },
                    grid: { color: '#ddd' }
                };
            }

            // Right Axis (y1)
            const dataset2 = createDataset(metric2, 'y1');
            if (dataset2) {
                datasets.push(dataset2);
                const config2 = metricConfig[metric2];
                yAxes.y1 = {
                    type: 'linear',
                    display: true,
                    position: 'right',
                    title: { display: true, text: `${config2.label} (${config2.unit})` },
                    grid: { drawOnChartArea: false } // Don't draw grid lines for the second axis
                };
            }

            // If only one axis is active, ensure it's displayed
            if (datasets.length === 1 && !yAxes.y) {
                yAxes.y = { display: true, position: 'left' };
            }
            if (datasets.length === 0) {
                // Handle case where no metrics are selected
                yAxes.y = { display: true, beginAtZero: true };
            }

            const chart = new Chart(canvas, {
                type: 'line',
                data: {
                    labels: labels,
                    datasets: datasets
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    interaction: {
                        mode: 'index',
                        intersect: false,
                    },
                    scales: {
                        x: { 
                            title: { display: true, text: 'Day of Month' }
                        },
                        ...yAxes
                    },
                    plugins: { 
                        legend: { display: datasets.length > 1 }, // Show legend only for multiple datasets
                        tooltip: {
                            callbacks: {
                                title: function(tooltipItems) {
                                    const index = tooltipItems[0].dataIndex;
                                    return new Date(dailyData[index].date).toLocaleDateString();
                                },
                                label: function(context) {
                                    const metric = context.dataset.yAxisID === 'y' ? metric1 : metric2;
                                    const config = metricConfig[metric];
                                    let label = context.dataset.label || '';
                                    if (label) {
                                        label += ': ';
                                    }
                                    if (context.parsed.y !== null) {
                                        label += `${context.parsed.y} ${config.unit}`;
                                    }
                                    return label;
                                }
                            }
                        }
                    }
                }
            });

            // Store the new chart instance
            vehicleCharts[vin] = chart;

            // --- Calculate and display averages ---
            const summaryContainer = canvas.closest('.charts-panel').querySelector('.chart-summary');
            if (!summaryContainer) {
                console.error("Chart summary container not found!");
                return;
            }

            const calculateAverage = (metric) => {
                if (!metric || metric === 'none') return null;

                const config = metricConfig[metric];
                if (!config) {
                    console.error(`Metric configuration not found for: ${metric}`);
                    return null;
                }

                const values = dailyData.map(d => d[metric]).filter(v => v !== null && v > 0);
                if (values.length === 0) return null;
                
                const sum = values.reduce((a, b) => a + b, 0);
                const avg = sum / values.length;
                
                let formattedAvg;
                if (metric === 'ev_duration_seconds' || metric === 'duration_seconds') {
                    const totalMinutes = avg / 60;
                    formattedAvg = `${totalMinutes.toFixed(1)} ${config.unit}`;
                } else {
                    formattedAvg = `${avg.toFixed(2)} ${config.unit}`;
                }
                return `Avg ${config.label}: <strong>${formattedAvg}</strong>`;
            };

            const summaryParts = [calculateAverage(metric1), calculateAverage(metric2)].filter(Boolean);
            summaryContainer.innerHTML = summaryParts.join(' | ');

        } catch (error) {
            console.error(`[renderHistoryChart] CRITICAL ERROR for VIN ${vin}:`, error);
            const summaryContainer = canvas.closest('.charts-panel').querySelector('.chart-summary');
            if (summaryContainer) summaryContainer.innerHTML = `<span class="error">Error rendering chart. See console for details.</span>`;
        }
    }
    function updateStatusPanel(panel, vehicleStatus) {
        if (!vehicleStatus) return;
    
        const updateItem = (key, isClosed, isLocked) => {
            const liElement = panel.querySelector(`li[data-status-key="${key}"]`);
            if (!liElement) return;
            const statusIconElement = liElement.querySelector('.status-icon');
    
            let statusSymbol = '❔';
            let statusClass = 'unknown';
    
            if (isClosed === false) {
                statusSymbol = '●'; // A simple dot for open, color will handle the warning
                statusClass = 'open';
            } else if (isClosed === true) {
                if (isLocked === true) {
                    statusSymbol = '🔒';
                    statusClass = 'locked';
                } else { // isLocked is false or null (for windows/hood)
                    statusSymbol = '●';
                    statusClass = 'closed';
                }
            }
            statusIconElement.textContent = statusSymbol;
            liElement.className = statusClass; // Reset and set the new class for styling
        };
    
        // Doors
        if (vehicleStatus.doors) {
            updateItem('doors.front_left', vehicleStatus.doors.front_left?.closed, vehicleStatus.doors.front_left?.locked);
            updateItem('doors.front_right', vehicleStatus.doors.front_right?.closed, vehicleStatus.doors.front_right?.locked);
            updateItem('doors.rear_left', vehicleStatus.doors.rear_left?.closed, vehicleStatus.doors.rear_left?.locked);
            updateItem('doors.rear_right', vehicleStatus.doors.rear_right?.closed, vehicleStatus.doors.rear_right?.locked);
        }

        // Windows
        if (vehicleStatus.windows) {
            updateItem('windows.front_left', vehicleStatus.windows.front_left?.closed, null);
            updateItem('windows.front_right', vehicleStatus.windows.front_right?.closed, null);
            updateItem('windows.rear_left', vehicleStatus.windows.rear_left?.closed, null);
            updateItem('windows.rear_right', vehicleStatus.windows.rear_right?.closed, null);
        }
        
        // Trunk & Hood
        updateItem('trunk', vehicleStatus.trunk_closed, vehicleStatus.trunk_locked);
        updateItem('hood', vehicleStatus.hood_closed, null); // Hood doesn't have a lock status
    }

    async function loadVehicleData() {
        try {
            const response = await fetch('/api/vehicles');
            if (!response.ok) {
                vehicleContainer.innerHTML = `<p class="error">Error: Could not load vehicle data. Server responded with status ${response.status}.</p>`;
                return;
            }
            const vehicles = await response.json();

            // Clear previous entries
            vehicleContainer.innerHTML = '';

            if (vehicles.length === 0) {
                vehicleContainer.innerHTML = `<p>No vehicle data found.</p>`;
            }

            vehicles.forEach(vehicle => {
                const vehicleFragment = vehicleTemplate.content.cloneNode(true);
                const vehicleCard = vehicleFragment.querySelector('.vehicle-wrapper');
                
                // Helper to safely get nested properties
                const get = (obj, path, def = 'N/A') => path.split('.').reduce((o, k) => (o && o[k] != null) ? o[k] : def, obj);

                vehicleCard.querySelector('.alias').textContent = get(vehicle, 'alias');
                vehicleCard.querySelector('.model-name').textContent = get(vehicle, 'model_name');
                vehicleCard.querySelector('.odometer').textContent = get(vehicle, 'dashboard.odometer');
                vehicleCard.querySelector('.fuel_level').textContent = get(vehicle, 'dashboard.fuel_level');
                vehicleCard.querySelector('.total_range').textContent = get(vehicle, 'dashboard.total_range');
                vehicleCard.querySelector('.daily_distance').textContent = get(vehicle, 'statistics.daily.distance', 0);
                vehicleCard.querySelector('.total_ev_distance_km').textContent = get(vehicle, 'statistics.overall.total_ev_distance_km', '0.00');
                vehicleCard.querySelector('.ev_ratio_percent').textContent = get(vehicle, 'statistics.overall.ev_ratio_percent', 'N/A');
                vehicleCard.querySelector('.overall_fuel_consumption').textContent = get(vehicle, 'statistics.overall.fuel_consumption_l_100km', 'N/A');
                vehicleCard.querySelector('.vin span').textContent = get(vehicle, 'vin');

                // Add current location map
                const lat = get(vehicle, 'dashboard.latitude', null);
                const lon = get(vehicle, 'dashboard.longitude', null);
                const mapContainer = vehicleCard.querySelector('.location-map-container');

                if (lat && lon) {
                    const embedUrl = `https://maps.google.com/maps?q=${lat},${lon}&z=15&output=embed`;
                    mapContainer.innerHTML = `<iframe src="${embedUrl}"></iframe>`;
                } else {
                    mapContainer.innerHTML = '<p style="text-align: center; padding-top: 50px; color: #888;">Location data not available.</p>';
                }

                // Populate and update the status panel
                const vehicleStatus = get(vehicle, 'status', null);
                updateStatusPanel(vehicleCard, vehicleStatus);

                // --- Refresh Button ---
                const refreshBtn = vehicleCard.querySelector('.refresh-btn');
                if (refreshBtn) {
                    refreshBtn.addEventListener('click', (e) => handlePollRequest('/api/force_poll', e.target));
                }

                // --- New Chart Logic ---
                const metricSelects = vehicleCard.querySelectorAll('.chart-metric-select');
                const periodSelect = vehicleCard.querySelector('.chart-period-select');
                const chartCanvas = vehicleCard.querySelector('.history-chart');

                // Define a unique key for this vehicle's chart settings in localStorage.
                const settingsKey = `chartSettings-${vehicle.vin}`;

                // Load saved chart settings from localStorage, if they exist.
                const savedSettings = localStorage.getItem(settingsKey);
                if (savedSettings) {
                    try {
                        const settings = JSON.parse(savedSettings);
                        vehicleCard.querySelector('.chart-metric-select[data-axis="left"]').value = settings.metric1;
                        vehicleCard.querySelector('.chart-metric-select[data-axis="right"]').value = settings.metric2;
                        periodSelect.value = settings.period;
                    } catch (e) {
                        console.error(`Error parsing saved chart settings for ${vehicle.vin}:`, e);
                        localStorage.removeItem(settingsKey); // Clear corrupted data
                    }
                }

                const updateChart = () => {
                    const metric1 = vehicleCard.querySelector('.chart-metric-select[data-axis="left"]').value;
                    const metric2 = vehicleCard.querySelector('.chart-metric-select[data-axis="right"]').value;
                    const period = vehicleCard.querySelector('.chart-period-select').value;
                    // Save the current selections to localStorage for persistence.
                    localStorage.setItem(settingsKey, JSON.stringify({ metric1, metric2, period }));
                    renderHistoryChart(vehicle.vin, chartCanvas, metric1, metric2, period);
                };

                metricSelects.forEach(select => select.addEventListener('change', updateChart));
                periodSelect.addEventListener('change', updateChart);

                // Initial chart render
                updateChart();
                
                vehicleContainer.appendChild(vehicleFragment);
            });

            lastUpdatedSpan.textContent = new Date().toLocaleString();
        } catch (error) {
            vehicleContainer.innerHTML = `<p class="error">Failed to fetch data. Is the backend running? Error: ${error.message}</p>`;
        }
    }

    async function handlePollRequest(url, clickedButton) {
        const allPollButtons = document.querySelectorAll('.refresh-btn');
        allPollButtons.forEach(btn => btn.disabled = true);

        const originalText = clickedButton.textContent;
        clickedButton.textContent = 'Updating...';
        lastUpdatedSpan.textContent = 'Polling now...';

        try {
            const response = await fetch(url, { method: 'POST' });
            if (response.ok) {
                await loadVehicleData();
            } else {
                const result = await response.json();
                lastUpdatedSpan.textContent = `Error: ${result.detail}`;
            }
        } catch (error) {
            lastUpdatedSpan.textContent = `Error: ${error.message}`;
        } finally {
            allPollButtons.forEach(btn => btn.disabled = false);
            clickedButton.textContent = originalText;
        }
    }

    // Load data on page load
    loadVehicleData();
});