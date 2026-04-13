document.addEventListener('DOMContentLoaded', () => {
    const vehicleContainer = document.getElementById('vehicle-container');
    const vehicleTemplate = document.getElementById('vehicle-template');

    if (!vehicleContainer || !vehicleTemplate) {
        console.error("Dashboard UI elements are missing.");
        return;
    }

    let appConfig = {
        unit_system: 'metric',
        dashboard_sensors: {}
    };

    const KM_TO_MI = 0.621371;
    const L_TO_GAL_US = 0.264172;
    const L_TO_GAL_UK = 0.219969;

    const ALL_DASHBOARD_STATS = {
        'odometer': { 'title': 'Odometer', 'element': '<span><span class="odometer">N/A</span></span>' },
        'range': { 'title': 'Range Left', 'element': '<span><span class="total_range">N/A</span></span>' },
        'total_ev_distance': { 'title': 'Total EV Distance', 'element': '<span><span class="total_ev_distance_km">N/A</span> (<span class="ev_ratio_percent">N/A</span>%)</span>' },
        'fuel_level': { 'title': 'Fuel Level (%)', 'element': '<span><span class="fuel_level">N/A</span></span>' },
        'daily_distance': { 'title': "Today's Distance", 'element': '<span><span class="daily_distance">N/A</span></span>' },
        'consumption': { 'title': 'Consumption', 'element': '<span><span class="overall_fuel_consumption">N/A</span></span>' },
        'total_fuel': { 'title': 'Total Fuel', 'element': '<span><span class="total_fuel_l">N/A</span></span>' },
        'duration': { 'title': 'Time Driven (h)', 'element': '<span><span class="total_duration">N/A</span></span>' },
        'ev_level': { 'title': 'EV Level', 'element': '<span><span class="battery_level">N/A</span>%</span>' },
        'ev_range': { 'title': 'EV Range', 'element': '<span><span class="battery_range">N/A</span> <span class="battery_range_ac_span" style="font-size: 0.7em; color: #666;">(AC: <span class="battery_range_with_ac">N/A</span>)</span></span>' },
        'charging_status': { 'title': 'Charging', 'element': '<span><span class="charging_status">N/A</span></span>' },
        'max_speed': { 'title': 'Max Speed Ever', 'element': '<span><span class="overall_max_speed">N/A</span></span>' },
        'countries': { 'title': 'Countries Visited', 'element': '<span><span class="all_countries">N/A</span></span>' },
        'highway_distance': { 'title': 'Highway Distance', 'element': '<span><span class="total_highway_distance_km">N/A</span> (<span class="highway_ratio_percent">N/A</span>%)</span>' },
    };

    function l100kmToMpg(l100km, isUk = false) {
        if (l100km <= 0) return 0;
        const factor = isUk ? 282.481 : 235.214;
        return factor / l100km;
    }

    async function loadConfig() {
        try {
            const response = await fetch('/api/config');
            appConfig = await response.json();
            if (!appConfig.dashboard_sensors) {
                appConfig.dashboard_sensors = {};
            }
        }
        catch (error) {
            console.error("Failed to load application config, using defaults.", error);
        }
    }

    let vehicleCharts = {};

    function updateChartColors() {
        const style = getComputedStyle(document.documentElement);
        const gridColor = style.getPropertyValue('--border-color').trim() || '#e0e0e0';
        const textColor = style.getPropertyValue('--text-subtle').trim() || '#888';
        
        Chart.defaults.color = textColor;
        Chart.defaults.borderColor = gridColor;
        
        // Update existing charts
        Object.values(vehicleCharts).forEach(chart => {
            chart.update();
        });
    }

    // Call initially
    updateChartColors();

    window.addEventListener('themeChanged', () => {
        setTimeout(updateChartColors, 50);
    });


    // Listen for theme changes (we can intercept the toggle click)
    const toggleBtn = document.getElementById('theme-toggle');
    if (toggleBtn) {
        toggleBtn.addEventListener('click', () => {
            // Give the browser a tiny moment to apply the new CSS variables before reading them
            setTimeout(updateChartColors, 50);
        });
    }


    function calculateSummary(values, metric, isImperial, isUk, metricConfig, aggregationType) {
        if (!metric || metric === 'none') return null;
        const config = metricConfig[metric];
        if (!config) return null;

        let processedValues = [...values];
        if (isImperial && config.convert) {
            processedValues = processedValues.map(val => (val !== null && val !== undefined) ? config.convert(val) : null);
        }
        if (metric === 'ev_duration_seconds' || metric === 'duration_seconds') {
            processedValues = processedValues.map(s => (s !== null && s !== undefined) ? (s / 60) : null);
        }

        const filteredValues = processedValues.filter(v => v !== null && v > 0);
        if (filteredValues.length === 0) {
            const avgText = window.i18n ? window.i18n.t('dashboard.avg') : 'Avg'; return `${avgText} ${config.label}: <strong>N/A</strong>`;
        }

        const sum = filteredValues.reduce((a, b) => a + b, 0);
        const avg = sum / filteredValues.length;

        const sortedValues = [...filteredValues].sort((a, b) => a - b);
        const mid = Math.floor(sortedValues.length / 2);
        const median = sortedValues.length % 2 !== 0 ? sortedValues[mid] : (sortedValues[mid - 1] + sortedValues[mid]) / 2;

        const unit = config.unit[isImperial ? 'imperial' : 'metric'];
        const contextString = aggregationType === 'day' ? 'Daily' : 'Per Trip';
        
        return `${contextString} Avg. ${config.label}: <strong>${avg.toFixed(1)} ${unit}</strong> (Median: <strong>${median.toFixed(1)} ${unit}</strong>)`;
    };

    function calculateWeightedRollingAverage(dailyData, windowSize = 7) {
        const rollingAverages = [];
        for (let i = 0; i < dailyData.length; i++) {
            if (i < windowSize - 1) {
                rollingAverages.push(null);
            } else {
                let totalFuel = 0;
                let totalDistance = 0;
                for (let j = 0; j < windowSize; j++) {
                    const dataPoint = dailyData[i - j];
                    totalFuel += dataPoint.fuel_total_l || 0;
                    totalDistance += dataPoint.distance_km || 0;
                }
                const average = totalDistance > 0 ? (totalFuel / totalDistance) * 100 : 0;
                rollingAverages.push(average);
            }
        }
        return rollingAverages;
    }

    function calculateSimpleRollingAverage(data, windowSize = 7) {
        const rollingAverages = [];
        for (let i = 0; i < data.length; i++) {
            if (i < windowSize - 1) {
                rollingAverages.push(null);
            } else {
                let sum = 0;
                let count = 0;
                for (let j = 0; j < windowSize; j++) {
                    const value = data[i - j];
                    if (value !== null && value !== undefined) {
                        sum += value;
                        count++;
                    }
                }
                const average = count > 0 ? sum / count : null;
                rollingAverages.push(average);
            }
        }
        return rollingAverages;
    }

    async function renderHistoryChart(vin, canvas, metric1, metric2, period, isHistogram, isRollingAvgLeft, isRollingAvgRight) {
        // Centralized chart destruction
        if (vehicleCharts[vin]) {
            vehicleCharts[vin].destroy();
            delete vehicleCharts[vin];
        }

        try {
            const isImperial = appConfig.unit_system.startsWith('imperial');
            const isUk = appConfig.unit_system === 'imperial_uk';
            const metricConfig = {
                distance_km: {
                    label: window.i18n ? window.i18n.t('dashboard.metrics.distance_km') : 'Distance', unit: { metric: 'km', imperial: 'mi' }, color: '#00529b',
                    convert: (val) => val * KM_TO_MI
                },
                fuel_consumption_l_100km: {
                    label: window.i18n ? window.i18n.t('dashboard.metrics.fuel_consumption_l_100km') : 'Consumption', unit: { metric: 'L/100km', imperial: isUk ? 'UK MPG' : 'US MPG' }, color: '#d9534f',
                    convert: (val) => l100kmToMpg(val, isUk)
                },
                ev_distance_km: {
                    label: window.i18n ? window.i18n.t('dashboard.metrics.ev_distance_km') : 'EV Distance', unit: { metric: 'km', imperial: 'mi' }, color: '#5cb85c',
                    convert: (val) => val * KM_TO_MI
                },
                ev_duration_seconds: {
                    label: window.i18n ? window.i18n.t('dashboard.metrics.ev_duration_seconds') : 'EV Duration', unit: { metric: 'minutes', imperial: 'minutes' }, color: '#f0ad4e'
                },
                score_global: {
                    label: window.i18n ? window.i18n.t('dashboard.metrics.score_global') : 'Driving Score', unit: { metric: 'Score', imperial: 'Score' }, color: '#5bc0de'
                },
                average_speed_kmh: {
                    label: window.i18n ? window.i18n.t('dashboard.metrics.average_speed_kmh') : 'Average Speed', unit: { metric: 'km/h', imperial: 'mph' }, color: '#337ab7',
                    convert: (val) => val * KM_TO_MI
                },
                max_speed_kmh: {
                    label: window.i18n ? window.i18n.t('dashboard.metrics.max_speed_kmh') : 'Max Speed', unit: { metric: 'km/h', imperial: 'mph' }, color: '#BF55EC',
                    convert: (val) => val * KM_TO_MI
                },
                duration_seconds: {
                    label: window.i18n ? window.i18n.t('dashboard.metrics.duration_seconds') : 'Trip Duration', unit: { metric: 'minutes', imperial: 'minutes' }, color: '#777'
                },
                 none: { label: window.i18n ? window.i18n.t('dashboard.metrics.none') : 'None', unit: { metric: '', imperial: '' }, color: '#fff' }
            };

            try {
                const countResponse = await fetch(`/api/vehicles/${vin}/trip_count?period=${period}`);
                const countData = await countResponse.json();
                const tripCountEl = canvas.closest('.charts-panel').querySelector('.trip-count');
                if (tripCountEl) tripCountEl.textContent = `(${countData.trip_count} trips)`;
            } catch (error) {
                console.error("Failed to fetch trip count:", error);
            }

            const summaryContainer = canvas.closest('.charts-panel').querySelector('.chart-summary');

            if (isHistogram) {
                const tripDataResponse = await fetch(`/api/vehicles/${vin}/trip_data?period=${period}&metric=${metric1}`);
                const tripData = await tripDataResponse.json();
                renderHistogramPlot(canvas, tripData.values, metric1, isImperial, isUk, metricConfig, vin);
                summaryContainer.innerHTML = calculateSummary(tripData.values, metric1, isImperial, isUk, metricConfig, 'trip');
            } else {
                // --- CHANGE 1: Adjust period for fetch if rolling average is on ---
                let adjustedPeriod = period;
                const rollingAvgWindow = 7; // Based on calculateWeightedRollingAverage
                const isRollingAvg = isRollingAvgLeft || isRollingAvgRight;

                // Check if period is a number (e.g., "30", "90") and not "all"
                if (isRollingAvg && !isNaN(parseInt(period, 10))) {
                    adjustedPeriod = parseInt(period, 10) + rollingAvgWindow;
                }
                // --- End of change ---

                const dailyResponse = await fetch(`/api/vehicles/${vin}/daily_summary?period=${adjustedPeriod}`);
                const dailyData = await dailyResponse.json();
                
                // Pass the *original* period to renderLineChart so it knows how to trim
                renderLineChart(canvas, dailyData, metric1, metric2, isImperial, isUk, metricConfig, vin, isRollingAvgLeft, isRollingAvgRight, period);

                const summary1 = calculateSummary(dailyData.map(d => d[metric1]), metric1, isImperial, isUk, metricConfig, 'day');
                const summary2 = calculateSummary(dailyData.map(d => d[metric2]), metric2, isImperial, isUk, metricConfig, 'day');
                summaryContainer.innerHTML = [summary1, summary2].filter(Boolean).join('<br>');
            }
        }
        catch (error) {
            console.error(`[renderHistoryChart] CRITICAL ERROR for VIN ${vin}:`, error);
            const summaryContainer = canvas.closest('.charts-panel').querySelector('.chart-summary');
            if (summaryContainer) summaryContainer.innerHTML = `<span class="error">${window.i18n ? window.i18n.t('dashboard.error_chart') : 'Error rendering chart. See console for details.'}</span>`;
        }
    }

    // --- CHANGE 2: Add 'originalPeriod' parameter and logic to trim data ---
    function renderLineChart(canvas, dailyData, metric1, metric2, isImperial, isUk, metricConfig, vin, isRollingAvgLeft, isRollingAvgRight, originalPeriod) {
        if (!dailyData || dailyData.length === 0) {
            const ctx = canvas.getContext('2d');
            ctx.clearRect(0, 0, canvas.width, canvas.height);
            ctx.font = "16px sans-serif";
            ctx.fillStyle = "#888";
            ctx.textAlign = "center";
            const noData = window.i18n ? window.i18n.t('dashboard.no_data') : "No historical data available for this period."; ctx.fillText(noData, canvas.width / 2, canvas.height / 2);
            return;
        }

        const isRollingAvg = isRollingAvgLeft || isRollingAvgRight;
        
        // --- Slicing Logic ---
        // If we fetched extra data for rolling avg, slice it now for the chart
        let chartData = [...dailyData];
        if (isRollingAvg && !isNaN(parseInt(originalPeriod, 10)) && dailyData.length > parseInt(originalPeriod, 10)) {
            const startIndex = dailyData.length - parseInt(originalPeriod, 10);
            chartData = dailyData.slice(startIndex);
        }
        // --- End Slicing Logic ---

        // Use the sliced 'chartData' for labels
        const labels = chartData.map(d => new Date(d.date).toLocaleDateString(undefined, { month: 'short', day: 'numeric' }));
        const datasets = [];
        const yAxes = {};

        // If metrics are the same, force both datasets to use the 'y' (left) axis.
        const syncAxes = (metric1 === metric2 && metric1 !== 'none');
        const yAxisID2 = syncAxes ? 'y' : 'y1';

        const createDataset = (metric, yAxisID, isRollingAvgY) => { // Renamed isRollingAvg to isRollingAvgY to avoid scope conflict
            if (!metric || metric === 'none') return null;

            const config = metricConfig[metric];
            let data;

            if (isRollingAvgY) {
                if (metric === 'fuel_consumption_l_100km') {
                    // Calculate based on the *full* dataset
                    data = calculateWeightedRollingAverage(dailyData);
                } else {
                    // Calculate based on the *full* dataset
                    data = calculateSimpleRollingAverage(dailyData.map(d => d[metric]));
                }

                // --- Slice the calculated rolling data ---
                if (isRollingAvg && !isNaN(parseInt(originalPeriod, 10)) && dailyData.length > parseInt(originalPeriod, 10)) {
                    const startIndex = dailyData.length - parseInt(originalPeriod, 10);
                    data = data.slice(startIndex);
                }
                // --- End slice ---

            } else {
                // Non-rolling data comes from the *sliced* chartData
                data = chartData.map(d => d[metric]);
            }

            if (isImperial && config.convert) {
                data = data.map(val => (val === null || val === undefined) ? null : config.convert(val));
            }
            if (metric === 'ev_duration_seconds' || metric === 'duration_seconds') {
                data = data.map(s => s ? (s / 60) : 0);
            }

            return {
                label: isRollingAvgY ? `${config.label} (7-day Avg)` : config.label,
                data: data,
                borderColor: config.color,
                backgroundColor: isRollingAvgY ? 'transparent' : `${config.color}33`,
                yAxisID: yAxisID,
                tension: 0.1,
                fill: !isRollingAvgY,
                borderDash: isRollingAvgY ? [5, 5] : [],
                pointRadius: isRollingAvgY ? 0 : 2
            };
        };

        const dataset1 = createDataset(metric1, 'y', isRollingAvgLeft);
        if (dataset1) datasets.push(dataset1);

        // Use the dynamic yAxisID2 here
        const dataset2 = createDataset(metric2, yAxisID2, isRollingAvgRight);
        if (dataset2) datasets.push(dataset2);

        const config1 = metricConfig[metric1];
        if (metric1 !== 'none') {
            yAxes.y = {
                type: 'linear', display: true, position: 'left',
                title: { display: true, text: `${config1.label} (${config1.unit[isImperial ? 'imperial' : 'metric']})` },
                grid: { color: '#ddd' }
            };
        }

        const config2 = metricConfig[metric2];
        // Only create the right axis if we are NOT syncing
        if (metric2 !== 'none' && !syncAxes) {
            yAxes.y1 = {
                type: 'linear', display: true, position: 'right',
                title: { display: true, text: `${config2.label} (${config2.unit[isImperial ? 'imperial' : 'metric']})` },
                grid: { drawOnChartArea: false }
            };
        }

        if (datasets.length === 0) {
            yAxes.y = { display: true, beginAtZero: true };
        }

        vehicleCharts[vin] = new Chart(canvas, {
            type: 'line',
            data: { labels: labels, datasets: datasets },
            options: {
                responsive: true, maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                scales: { x: { title: { display: true, text: 'Date' } }, ...yAxes },
                plugins: {
                    tooltip: {
                        callbacks: {
                            // Use sliced 'chartData' for the tooltip title
                            title: (tooltipItems) => new Date(chartData[tooltipItems[0].dataIndex].date).toLocaleDateString(),
                            label: (context) => {
                                let metric = context.dataset.yAxisID === 'y' ? metric1 : metric2;
                                let config = metricConfig[metric];
                                let label = context.dataset.label || '';
                                if (label) label += ': ';
                                if (context.parsed.y !== null) {
                                    let unit = config.unit[isImperial ? 'imperial' : 'metric'];
                                    label += `${context.parsed.y.toFixed(1)} ${unit}`;
                                }
                                return label;
                            }
                        }
                    }
                }
            }
        });
    }

    function renderHistogramPlot(canvas, tripValues, metric, isImperial, isUk, metricConfig, vin) {
        if (metric === 'none') return;
        const config = metricConfig[metric];
        
        let values = [...tripValues];
        if (isImperial && config.convert) {
            values = values.map(config.convert);
        }

        const summaryContainer = canvas.closest('.charts-panel').querySelector('.chart-summary');

        if (values.length < 4) {
            const ctx = canvas.getContext('2d');
            ctx.clearRect(0, 0, canvas.width, canvas.height);
            ctx.font = "16px sans-serif";
            ctx.fillStyle = "#888";
            ctx.textAlign = "center";
            ctx.fillText("Not enough data for histogram.", canvas.width / 2, canvas.height / 2);
            return;
        }

        const sortedValues = [...values].sort((a, b) => a - b);
        const q1 = sortedValues[Math.floor((sortedValues.length / 4))];
        const q3 = sortedValues[Math.floor((sortedValues.length * 3) / 4)];
        const iqr = q3 - q1;
        const lowerBound = q1 - 1.5 * iqr;
        const upperBound = q3 + 1.5 * iqr;

        const filteredValues = values.filter(v => v >= lowerBound && v <= upperBound);
        const outliersRemovedCount = values.length - filteredValues.length;
        
        if (summaryContainer && outliersRemovedCount > 0) {
            const outlierNote = document.createElement('div');
            outlierNote.style.fontSize = '0.8em';
            outlierNote.style.fontStyle = 'italic';
            outlierNote.style.marginTop = '5px';
            outlierNote.textContent = `(${outliersRemovedCount} outlier trip(s) not shown in chart)`;
            summaryContainer.appendChild(outlierNote);
        }

        const minValue = Math.min(...filteredValues);
        const maxValue = Math.max(...filteredValues);
        const range = maxValue - minValue;

        if (range === 0) {
            const ctx = canvas.getContext('2d');
            ctx.clearRect(0, 0, canvas.width, canvas.height);
            ctx.font = "16px sans-serif";
            ctx.fillStyle = "#888";
            ctx.textAlign = "center";
            ctx.fillText(`All trips have the same value: ${minValue.toFixed(1)}`, canvas.width / 2, canvas.height / 2);
            return;
        }

        const numBins = Math.ceil(1 + Math.log2(filteredValues.length));
        const binSize = range / numBins;
        const bins = new Array(numBins).fill(0);
        const labels = [];
        for (let i = 0; i < numBins; i++) {
            labels.push((minValue + i * binSize).toFixed(1));
        }
        for (const value of filteredValues) {
            let binIndex = Math.floor((value - minValue) / binSize);
            if (value === maxValue) binIndex = numBins - 1;
            if (binIndex >= 0 && binIndex < numBins) bins[binIndex]++;
        }

        const datasets = [{
            type: 'bar',
            label: window.i18n ? window.i18n.t('dashboard.trips') : `Trips`,
            data: bins,
            yAxisID: 'y',
            backgroundColor: `${config.color}B3`,
            barPercentage: 1.0,
            categoryPercentage: 1.0
        }];

        const yAxes = {
            y: {
                type: 'linear',
                position: 'left',
                title: { display: true, text: `Number of Trips` },
            }
        };

        vehicleCharts[vin] = new Chart(canvas, {
            type: 'bar',
            data: { labels: labels, datasets },
            options: {
                responsive: true, maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                scales: {
                    x: { title: { display: true, text: `${config.label} (${config.unit[isImperial ? 'imperial' : 'metric']})` } },
                    ...yAxes
                },
                plugins: { legend: { display: false } }
            }
        });
    }

    function updateStatusPanel(panel, vehicleStatus) {
        if (!vehicleStatus) return;

        const statusTimestamp = panel.querySelector('.status-timestamp span');
        if (statusTimestamp && vehicleStatus.last_update_timestamp) {
            statusTimestamp.textContent = new Date(vehicleStatus.last_update_timestamp).toLocaleString();
        } else if (statusTimestamp) {
            statusTimestamp.textContent = 'N/A';
        }

        const lockStatusText = panel.querySelector('.lock-status-text');
        const openStatusText = panel.querySelector('.open-status-text');
        let isCompletelyLocked = true;
        const openItems = [];
        const updateItem = (key, isClosed, isLocked) => {
            const liElement = panel.querySelector(`li[data-status-key="${key}"]`);
            if (!liElement) return;
            const statusIconElement = liElement.querySelector('.status-icon');
            let statusSymbol = '❔';
            let statusClass = 'unknown';
            if (isClosed === false) {
                statusSymbol = '●';
                statusClass = 'open';
            } else if (isClosed === true) {
                if (isLocked === true) {
                    statusSymbol = '🔒';
                    statusClass = 'locked';
                } else {
                    statusSymbol = '●';
                    statusClass = 'closed';
                }
            }
            statusIconElement.textContent = statusSymbol;
            liElement.className = statusClass;
        };
        if (vehicleStatus.doors) {
            Object.values(vehicleStatus.doors).forEach(door => {
                if (door.closed === false) openItems.push('door');
                if (door.locked === false) isCompletelyLocked = false;
            });
            updateItem('doors.front_left', vehicleStatus.doors.front_left?.closed, vehicleStatus.doors.front_left?.locked);
            updateItem('doors.front_right', vehicleStatus.doors.front_right?.closed, vehicleStatus.doors.front_right?.locked);
            updateItem('doors.rear_left', vehicleStatus.doors.rear_left?.closed, vehicleStatus.doors.rear_left?.locked);
            updateItem('doors.rear_right', vehicleStatus.doors.rear_right?.closed, vehicleStatus.doors.rear_right?.locked);
        } else {
            isCompletelyLocked = false;
        }
        if (vehicleStatus.windows) {
             Object.values(vehicleStatus.windows).forEach(window => {
                if (window.closed === false) openItems.push('window');
            });
            updateItem('windows.front_left', vehicleStatus.windows.front_left?.closed, null);
            updateItem('windows.front_right', vehicleStatus.windows.front_right?.closed, null);
            updateItem('windows.rear_left', vehicleStatus.windows.rear_left?.closed, null);
            updateItem('windows.rear_right', vehicleStatus.windows.rear_right?.closed, null);
        }
        if (vehicleStatus.trunk_closed === false) openItems.push('trunk');
        if (vehicleStatus.hood_closed === false) openItems.push('hood');
        if (vehicleStatus.trunk_locked === false) isComcompletelyLocked = false;
        updateItem('trunk', vehicleStatus.trunk_closed, vehicleStatus.trunk_locked);
        updateItem('hood', vehicleStatus.hood_closed, null);
        if (lockStatusText) {
            if (isCompletelyLocked) {
                lockStatusText.textContent = window.i18n ? '(' + window.i18n.t('dashboard.status.locked') + ')' : '(Locked)';
                lockStatusText.className = 'lock-status-text locked';
            } else {
                lockStatusText.textContent = window.i18n ? '(' + window.i18n.t('dashboard.status.unlocked') + ')' : '(Unlocked)';
                lockStatusText.className = 'lock-status-text unlocked';
            }
        }
        if (openStatusText) {
            const uniqueOpenItems = [...new Set(openItems)];
            if (uniqueOpenItems.length > 0) {
                const message = uniqueOpenItems.map(item => item.charAt(0).toUpperCase() + item.slice(1) + '(s)').join(' & ') + ' open';
                openStatusText.textContent = window.i18n ? `${window.i18n.t('dashboard.status.warning')}: ${message}` : `Warning: ${message}`;
            } else {
                openStatusText.textContent = '';
            }
        }
    }

    async function loadVehicleData() {
        const isImperial = appConfig.unit_system.startsWith('imperial');
        const isUk = appConfig.unit_system === 'imperial_uk';

        try {
            const response = await fetch('/api/vehicles');
            if (!response.ok) {
                vehicleContainer.innerHTML = `<p class="error">Error: Could not load vehicle data. Server responded with status ${response.status}.</p>`;
                return;
            }
            const vehicles = await response.json();
            vehicleContainer.innerHTML = '';
            let vehicleToRender;
            if (vehicles.length === 0) {
                vehicleToRender = {
                    vin: "N/A", alias: "<a href=\"/settings\">Please enter credentials</a>", model_name: "",
                    dashboard: {}, statistics: { overall: {}, daily: {} }, status: {}, last_updated: "Never"
                };
            } else {
                vehicleToRender = vehicles[0];
            }
            const vehicleFragment = vehicleTemplate.content.cloneNode(true);
            const vehicleCard = vehicleFragment.querySelector('.vehicle-wrapper');
            const get = (obj, path, def = 'N/A') => path.split('.').reduce((o, k) => (o && o[k] != null) ? o[k] : def, obj);
            
            const distanceUnit = isImperial ? 'mi' : 'km';
            const consumptionUnit = isImperial ? (isUk ? 'UK MPG' : 'US MPG') : 'L/100km';
            const fuelUnit = isImperial ? (isUk ? 'UK gal' : 'US gal') : 'L';
            const speedUnit = isImperial ? 'mph' : 'km/h';
            
            // Generate stat elements
            const statsContainer = vehicleCard.querySelector('.vehicle-stats');
            for (const key in ALL_DASHBOARD_STATS) {
                const statInfo = ALL_DASHBOARD_STATS[key];
                const statEl = document.createElement('div');
                statEl.className = `stat stat-${key}`;
                statEl.dataset.statKey = key;

                const h3 = document.createElement('h3');
                h3.textContent = window.i18n ? window.i18n.t(`dashboard.stats.${key}`) : statInfo.title;

                const p = document.createElement('p');
                p.innerHTML = statInfo.element;

                statEl.appendChild(h3);
                statEl.appendChild(p);
                statsContainer.appendChild(statEl);
            }

            // Update unit labels for dynamically generated stats
            const updateUnit = (key, unit) => {
                const h3 = statsContainer.querySelector(`.stat-${key} h3`);
                if(h3) {
                    const translatedTitle = window.i18n ? window.i18n.t(`dashboard.stats.${key}`) : ALL_DASHBOARD_STATS[key].title;
                    h3.textContent = `${translatedTitle} (${unit})`;
                }
            }
            updateUnit('odometer', distanceUnit);
            updateUnit('range', distanceUnit);
            updateUnit('total_ev_distance', distanceUnit);
            updateUnit('daily_distance', distanceUnit);
            updateUnit('consumption', consumptionUnit);
            updateUnit('total_fuel', fuelUnit);
            updateUnit('max_speed', speedUnit);
            updateUnit('highway_distance', distanceUnit);
            const evRangeEl = statsContainer.querySelector(`.stat-ev_range h3`);
            if (evRangeEl) { const tTitle = window.i18n ? window.i18n.t('dashboard.stats.ev_range') : 'EV Range'; evRangeEl.innerHTML = `${tTitle} (<span class="distance_unit">${distanceUnit}</span>)`; }


            const dashboard = vehicleToRender.dashboard || {};
            const statsOverall = vehicleToRender.statistics.overall || {};
            const statsDaily = vehicleToRender.statistics.daily || {};

            const odometerKm = dashboard.odometer || 0;
            const rangeKm = dashboard.total_range || 0;
            const batteryRangeKm = dashboard.battery_range || 'N/A';
            const batteryRangeWithAcKm = dashboard.battery_range_with_ac || 'N/A';
            const evDistanceKm = statsOverall.total_ev_distance_km || 0;
            const dailyDistanceKm = statsDaily.distance || 0;
            const consumptionL100km = statsOverall.fuel_consumption_l_100km || 0;
            const totalFuelL = statsOverall.total_fuel_l || 0;
            const overallMaxSpeedKmh = statsOverall.overall_max_speed_kmh || 'N/A';
            const allCountries = statsOverall.countries || 'N/A';
            const totalHighwayDistKm = statsOverall.total_highway_distance_km || 0;
            const highwayRatio = statsOverall.highway_ratio_percent !== undefined ? statsOverall.highway_ratio_percent : 'N/A';

            vehicleCard.querySelector('.alias').innerHTML = vehicleToRender.alias;
            vehicleCard.querySelector('.model-name').textContent = vehicleToRender.model_name;

            const setVal = (selector, val) => {
                const el = vehicleCard.querySelector(selector);
                if (el) el.textContent = val;
            };

            setVal('.fuel_level', dashboard.fuel_level !== undefined ? dashboard.fuel_level : 'N/A');
            setVal('.ev_ratio_percent', statsOverall.ev_ratio_percent !== undefined ? statsOverall.ev_ratio_percent : 'N/A');
            const totalSeconds = statsOverall.total_duration_seconds || 0;
            setVal('.total_duration', Math.round(totalSeconds / 3600));
            setVal('.highway_ratio_percent', highwayRatio);

            if (isImperial) {
                setVal('.odometer', Math.round(odometerKm * KM_TO_MI));
                setVal('.total_range', Math.round(rangeKm * KM_TO_MI));
                setVal('.total_ev_distance_km', Math.round(evDistanceKm * KM_TO_MI));
                setVal('.daily_distance', (dailyDistanceKm * KM_TO_MI).toFixed(1));
                setVal('.overall_fuel_consumption', l100kmToMpg(consumptionL100km, isUk).toFixed(1));
                setVal('.total_fuel_l', (totalFuelL * (isUk ? L_TO_GAL_UK : L_TO_GAL_US)).toFixed(2));
                setVal('.battery_range', batteryRangeKm !== 'N/A' ? Math.round(batteryRangeKm * KM_TO_MI) : 'N/A');
                setVal('.battery_range_with_ac', batteryRangeWithAcKm !== 'N/A' ? Math.round(batteryRangeWithAcKm * KM_TO_MI) : 'N/A');
                setVal('.overall_max_speed', overallMaxSpeedKmh !== 'N/A' ? Math.round(overallMaxSpeedKmh * KM_TO_MI) : 'N/A');
                setVal('.total_highway_distance_km', Math.round(totalHighwayDistKm * KM_TO_MI));
            } else {
                setVal('.odometer', Math.round(odometerKm));
                setVal('.total_range', Math.round(rangeKm));
                setVal('.total_ev_distance_km', Math.round(evDistanceKm));
                setVal('.daily_distance', (dailyDistanceKm || 0).toFixed(1));
                setVal('.overall_fuel_consumption', consumptionL100km.toFixed(1));
                setVal('.total_fuel_l', totalFuelL.toFixed(2));
                setVal('.battery_range', batteryRangeKm !== 'N/A' ? Math.round(batteryRangeKm) : 'N/A');
                setVal('.battery_range_with_ac', batteryRangeWithAcKm !== 'N/A' ? Math.round(batteryRangeWithAcKm) : 'N/A');
                setVal('.overall_max_speed', overallMaxSpeedKmh !== 'N/A' ? Math.round(overallMaxSpeedKmh) : 'N/A');
                setVal('.total_highway_distance_km', Math.round(totalHighwayDistKm));
            }

            setVal('.all_countries', allCountries);

            setVal('.battery_level', dashboard.battery_level !== undefined ? dashboard.battery_level : 'N/A');
            
            let chargeStatus = get(vehicleToRender, 'dashboard.charging_status', 'N/A');
            if (chargeStatus && typeof chargeStatus === 'string') {
                chargeStatus = chargeStatus.replace(/([A-Z])/g, ' $1').trim();
                chargeStatus = chargeStatus.charAt(0).toUpperCase() + chargeStatus.slice(1);
            }
            setVal('.charging_status', chargeStatus);

            setVal('.vin span', vehicleToRender.vin);
            const lastUpdatedSpan = vehicleCard.querySelector('.last-updated-time');
            const lastUpdated = vehicleToRender.last_updated;
            lastUpdatedSpan.textContent = lastUpdated ? new Date(lastUpdated).toLocaleString() : "Never";
            
            const lat = dashboard.latitude;
            const lon = dashboard.longitude;
            const mapContainer = vehicleCard.querySelector('.location-map-container');
            if (lat && lon) {
                const embedUrl = `https://maps.google.com/maps?q=${lat},${lon}&z=15&output=embed`;
                mapContainer.innerHTML = `<iframe src="${embedUrl}"></iframe>`;
            } else {
                mapContainer.innerHTML = '<p style="text-align: center; padding-top: 50px; color: #888;">Location data not available.</p>';
            }

            const enabledSensors = appConfig.dashboard_sensors || {};
            vehicleCard.querySelectorAll('.stat[data-stat-key]').forEach(el => {
                const key = el.dataset.statKey;
                if (enabledSensors[key] === false) {
                    el.style.display = 'none';
                } else {
                    el.style.display = '';
                }
            });

            const visibleStats = Array.from(vehicleCard.querySelectorAll('.stat')).filter(
                el => el.style.display !== 'none'
            );
            visibleStats.forEach(stat => stat.style.gridColumn = '');
            if (visibleStats.length % 2 !== 0) {
                const lastVisibleStat = visibleStats[visibleStats.length - 1];
                if (lastVisibleStat) {
                    lastVisibleStat.style.gridColumn = 'span 2';
                }
            }

            updateStatusPanel(vehicleCard, vehicleToRender.status);

            applyStatOrder(vehicleCard, vehicleToRender.vin);
            // Pass VIN to the new setup function
            setupStatEditing(vehicleCard, vehicleToRender.vin);


            const refreshBtn = vehicleCard.querySelector('.force-poll');
            if (refreshBtn) {
                refreshBtn.addEventListener('click', (e) => handlePollRequest('/api/force_poll', e.target));
            }

            const leftMetricSelect = vehicleCard.querySelector('.chart-metric-select[data-axis="left"]');
            const rightMetricSelect = vehicleCard.querySelector('.chart-metric-select[data-axis="right"]');
            const periodSelect = vehicleCard.querySelector('.chart-period-select');
            const histogramToggleBtn = vehicleCard.querySelector('.histogram-toggle-btn');
            const rollingAvgBtnLeft = vehicleCard.querySelector('.rolling-avg-btn[data-axis="left"]');
            const rollingAvgBtnRight = vehicleCard.querySelector('.rolling-avg-btn[data-axis="right"]');
            const chartCanvas = vehicleCard.querySelector('.history-chart');
            const settingsKey = `chartSettings-${vehicleToRender.vin}`;

            const updateChart = () => {
                const metric1 = leftMetricSelect.value;
                const metric2 = rightMetricSelect.value;
                const period = periodSelect.value;
                const isHistogram = histogramToggleBtn.classList.contains('active');
                const isRollingAvgLeft = rollingAvgBtnLeft.classList.contains('active');
                const isRollingAvgRight = rollingAvgBtnRight.classList.contains('active');

                localStorage.setItem(settingsKey, JSON.stringify({
                    metric1, metric2, period, isHistogram, isRollingAvgLeft, isRollingAvgRight
                }));

                renderHistoryChart(vehicleToRender.vin, chartCanvas, metric1, metric2, period, isHistogram, isRollingAvgLeft, isRollingAvgRight);
            };

            const savedSettings = localStorage.getItem(settingsKey);
            if (savedSettings) {
                try {
                    const settings = JSON.parse(savedSettings);
                    leftMetricSelect.value = settings.metric1 || 'distance_km';
                    rightMetricSelect.value = settings.metric2 || 'none';
                    periodSelect.value = settings.period || '30';
                    if (settings.isHistogram) histogramToggleBtn.classList.add('active');
                    if (settings.isRollingAvgLeft) rollingAvgBtnLeft.classList.add('active');
                    if (settings.isRollingAvgRight) rollingAvgBtnRight.classList.add('active');
                } catch (e) {
                    console.error(`Error parsing saved chart settings:`, e);
                    localStorage.removeItem(settingsKey);
                }
            }

            const setUIState = () => {
                const isHistogram = histogramToggleBtn.classList.contains('active');
                const isRollingAvg = rollingAvgBtnLeft.classList.contains('active') || rollingAvgBtnRight.classList.contains('active');

                // Mutual exclusion: Histogram vs Rolling Average
                if (isHistogram && isRollingAvg) {
                    // This is handled by the click handlers
                    rollingAvgBtnLeft.classList.remove('active');
                    rollingAvgBtnRight.classList.remove('active');

                }

                // Update right axis controls based on histogram state
                rightMetricSelect.disabled = isHistogram;
                rollingAvgBtnRight.disabled = isHistogram;
                if (isHistogram) {
                    rightMetricSelect.value = 'none';
                    rollingAvgBtnRight.classList.remove('active');
                }
            };

            histogramToggleBtn.addEventListener('click', () => {
                histogramToggleBtn.classList.toggle('active');
                if (histogramToggleBtn.classList.contains('active')) {
                    // When turning histogram on, turn rolling average off
                    rollingAvgBtnLeft.classList.remove('active');
                    rollingAvgBtnRight.classList.remove('active');
                }
                setUIState();
                updateChart();
            });

            rollingAvgBtnLeft.addEventListener('click', () => {
                rollingAvgBtnLeft.classList.toggle('active');
                if (rollingAvgBtnLeft.classList.contains('active')) {
                    // When turning rolling average on, turn histogram off
                    histogramToggleBtn.classList.remove('active');
                }
                setUIState();
                updateChart();
            });

            rollingAvgBtnRight.addEventListener('click', () => {
                rollingAvgBtnRight.classList.toggle('active');
                // No need to disable histogram here as the button is already disabled if histogram is active
                setUIState();
                updateChart();
            });

            leftMetricSelect.addEventListener('change', updateChart);
            rightMetricSelect.addEventListener('change', updateChart);
            periodSelect.addEventListener('change', updateChart);

            setUIState(); // Set initial state
            updateChart(); // Initial chart render

            window.i18n.translateDOM(vehicleFragment);
            vehicleContainer.appendChild(vehicleFragment);
        }
        catch (error) {
            console.error("CRITICAL ERROR in loadVehicleData:", error);
            vehicleContainer.innerHTML = `<p class="error">Failed to fetch data. Is the backend running? Error: ${error.message}</p>`;
        }
    }

    async function handlePollRequest(url, clickedButton) {
        const allPollButtons = document.querySelectorAll('.force-poll');
        allPollButtons.forEach(btn => btn.disabled = true);
        const originalText = clickedButton.textContent;
        clickedButton.textContent = 'Updating...';
        try {
            const response = await fetch(url, { method: 'POST' });
            if (response.ok) {
                await loadConfig();
                await loadVehicleData();
            } else {
                const result = await response.json();
                console.error("Poll request failed:", result.detail);
            }
        }
        catch (error) {
            console.error("Poll request failed:", error);
        } finally {
            allPollButtons.forEach(btn => btn.disabled = false);
            clickedButton.textContent = originalText;
        }
    }

    function setupStatEditing(vehicleCard, vin) {
        const statsContainer = vehicleCard.querySelector('.vehicle-stats');
        const editBtn = vehicleCard.querySelector('.edit-stats-btn');
        let sortableInstance = null;
        let isEditMode = false;

        function toggleEditMode() {
            isEditMode = !isEditMode;
            statsContainer.classList.toggle('edit-mode', isEditMode);
            editBtn.textContent = isEditMode ? 'Done' : 'Edit';

            if (isEditMode) {
                // Show all stats and add controls
                Object.keys(ALL_DASHBOARD_STATS).forEach(key => {
                    const statEl = statsContainer.querySelector(`.stat[data-stat-key="${key}"]`);
                    if (statEl) {
                        statEl.style.display = ''; // Make sure it's visible
                        const isEnabled = appConfig.dashboard_sensors[key] !== false;
                        statEl.classList.toggle('disabled', !isEnabled);

                        // Add a checkbox if it doesn't exist
                        if (!statEl.querySelector('input[type="checkbox"]')) {
                            const checkbox = document.createElement('input');
                            checkbox.type = 'checkbox';
                            checkbox.checked = isEnabled;
                            checkbox.addEventListener('change', (e) => {
                                statEl.classList.toggle('disabled', !e.target.checked);
                                appConfig.dashboard_sensors[key] = e.target.checked;
                            });
                            statEl.insertBefore(checkbox, statEl.firstChild);
                         }
                    }
                });

                // Initialize Sortable
                sortableInstance = new Sortable(statsContainer, {
                    animation: 150,
                    ghostClass: 'sortable-ghost',
                    onUpdate: () => {
                        // The order is saved when exiting edit mode
                    },
                });

            } else {
                // Destroy Sortable
                if (sortableInstance) {
                    sortableInstance.destroy();
                    sortableInstance = null;
                }

                // Save settings and hide disabled stats
                const statOrder = Array.from(statsContainer.children).map(s => s.dataset.statKey);
                localStorage.setItem(`statOrder-${vin}`, JSON.stringify(statOrder));

                // Remove checkboxes and hide disabled stats
                Object.keys(ALL_DASHBOARD_STATS).forEach(key => {
                    const statEl = statsContainer.querySelector(`.stat[data-stat-key="${key}"]`);
                    if (statEl) {
                        const checkbox = statEl.querySelector('input[type="checkbox"]');
                        if (checkbox) {
                            statEl.removeChild(checkbox);
                        }
                        if (appConfig.dashboard_sensors[key] === false) {
                            statEl.style.display = 'none';
                        }
                    }
                });

                // Persist the enabled/disabled state to the server
                saveDashboardSensorsConfig(appConfig.dashboard_sensors);

                // Re-apply grid styling for odd numbers of items
                const visibleStats = Array.from(statsContainer.querySelectorAll('.stat')).filter(el => el.style.display !== 'none');
                visibleStats.forEach(stat => stat.style.gridColumn = '');
                if (visibleStats.length % 2 !== 0) {
                    visibleStats[visibleStats.length - 1].style.gridColumn = 'span 2';
                }
            }
        }

        editBtn.addEventListener('click', toggleEditMode);
    }

    async function saveDashboardSensorsConfig(sensorsConfig) {
        try {
            const response = await fetch('/api/config', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ dashboard_sensors: sensorsConfig }),
            });
            if (!response.ok) {
                const result = await response.json();
                console.error("Failed to save dashboard sensor settings:", result.detail);
            }
        } catch (error) {
            console.error("Error saving dashboard sensor settings:", error);
        }
    }


    function applyStatOrder(vehicleCard, vin) {
        const savedOrder = localStorage.getItem(`statOrder-${vin}`);
        if (savedOrder) {
            try {
                const statOrder = JSON.parse(savedOrder);
                const statsContainer = vehicleCard.querySelector('.vehicle-stats');
                const stats = Array.from(statsContainer.children);
                const statMap = new Map(stats.map(s => [s.dataset.statKey, s]));

                statOrder.forEach(key => {
                    const stat = statMap.get(key);
                    if (stat) {
                        statsContainer.appendChild(stat);
                    }
                });
            } catch (e) {
                console.error("Error applying saved stat order:", e);
                localStorage.removeItem(`statOrder-${vin}`);
            }
        }
    }

    async function init() {
        await loadConfig();
        await loadVehicleData();
    }
    init();
});