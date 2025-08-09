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
            return `Avg ${config.label}: <strong>N/A</strong>`;
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

    async function renderHistoryChart(vin, canvas, metric1, metric2, period, isHistogram) {
        try {
            if (vehicleCharts[vin]) {
                vehicleCharts[vin].destroy();
            }

            const isImperial = appConfig.unit_system.startsWith('imperial');
            const isUk = appConfig.unit_system === 'imperial_uk';
            const metricConfig = {
                distance_km: {
                    label: 'Distance', unit: { metric: 'km', imperial: 'mi' }, color: '#00529b',
                    convert: (val) => val * KM_TO_MI
                },
                fuel_consumption_l_100km: {
                    label: 'Consumption', unit: { metric: 'L/100km', imperial: isUk ? 'UK MPG' : 'US MPG' }, color: '#d9534f',
                    convert: (val) => l100kmToMpg(val, isUk)
                },
                ev_distance_km: {
                    label: 'EV Distance', unit: { metric: 'km', imperial: 'mi' }, color: '#5cb85c',
                    convert: (val) => val * KM_TO_MI
                },
                ev_duration_seconds: {
                    label: 'EV Duration', unit: { metric: 'minutes', imperial: 'minutes' }, color: '#f0ad4e'
                },
                score_global: {
                    label: 'Driving Score', unit: { metric: 'Score', imperial: 'Score' }, color: '#5bc0de'
                },
                average_speed_kmh: {
                    label: 'Average Speed', unit: { metric: 'km/h', imperial: 'mph' }, color: '#337ab7',
                    convert: (val) => val * KM_TO_MI
                },
                duration_seconds: {
                    label: 'Trip Duration', unit: { metric: 'minutes', imperial: 'minutes' }, color: '#777'
                },
                none: { label: 'None', unit: '', color: '#fff' }
            };

            // Fetch total trip count for the header
            try {
                const countResponse = await fetch(`/api/vehicles/${vin}/trip_count?period=${period}`);
                const countData = await countResponse.json();
                const tripCountEl = canvas.closest('.charts-panel').querySelector('.trip-count');
                if (tripCountEl) {
                    tripCountEl.textContent = `(${countData.trip_count} trips)`;
                }
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
                const dailyResponse = await fetch(`/api/vehicles/${vin}/daily_summary?period=${period}`);
                const dailyData = await dailyResponse.json();
                renderLineChart(canvas, dailyData, metric1, metric2, isImperial, isUk, metricConfig, vin);

                const summary1 = calculateSummary(dailyData.map(d => d[metric1]), metric1, isImperial, isUk, metricConfig, 'day');
                const summary2 = calculateSummary(dailyData.map(d => d[metric2]), metric2, isImperial, isUk, metricConfig, 'day');
                summaryContainer.innerHTML = [summary1, summary2].filter(Boolean).join('<br>');
            }
        }
        catch (error) {
            console.error(`[renderHistoryChart] CRITICAL ERROR for VIN ${vin}:`, error);
            const summaryContainer = canvas.closest('.charts-panel').querySelector('.chart-summary');
            if (summaryContainer) summaryContainer.innerHTML = `<span class="error">Error rendering chart. See console for details.</span>`;
        }
    }

    function renderLineChart(canvas, dailyData, metric1, metric2, isImperial, isUk, metricConfig, vin) {
        const labels = dailyData.map(d => new Date(d.date).toLocaleDateString(undefined, { month: 'short', day: 'numeric' }));
        const datasets = [];
        const yAxes = {};
        const createDataset = (metric, yAxisID) => {
            if (!metric || metric === 'none') return null;
            let data = dailyData.map(d => d[metric]);
            const config = metricConfig[metric];
            if (isImperial && config.convert) {
                data = data.map(val => val === null ? null : config.convert(val));
            }
            if (metric === 'ev_duration_seconds' || metric === 'duration_seconds') {
                data = data.map(s => s ? (s / 60) : 0);
            }
            return {
                label: config.label, data: data, borderColor: config.color,
                backgroundColor: `${config.color}33`, fill: true, tension: 0.1,
                pointRadius: 2, yAxisID: yAxisID
            };
        };
        const dataset1 = createDataset(metric1, 'y');
        if (dataset1) {
            datasets.push(dataset1);
            const config1 = metricConfig[metric1];
            yAxes.y = {
                type: 'linear', display: true, position: 'left',
                title: { display: true, text: `${config1.label} (${config1.unit[isImperial ? 'imperial' : 'metric']})` },
                grid: { color: '#ddd' }
            };
        }
        const dataset2 = createDataset(metric2, 'y1');
        if (dataset2) {
            datasets.push(dataset2);
            const config2 = metricConfig[metric2];
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
            type: 'line', data: { labels: labels, datasets: datasets },
            options: {
                responsive: true, maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false, },
                scales: { x: { title: { display: true, text: 'Date' } }, ...yAxes },
                plugins: {
                    legend: { display: datasets.length > 1 },
                    tooltip: {
                        callbacks: {
                            title: (tooltipItems) => new Date(dailyData[tooltipItems[0].dataIndex].date).toLocaleDateString(),
                            label: (context) => {
                                const metric = context.dataset.yAxisID === 'y' ? metric1 : metric2;
                                const config = metricConfig[metric];
                                let label = context.dataset.label || '';
                                if (label) label += ': ';
                                if (context.parsed.y !== null) {
                                    const unit = config.unit[isImperial ? 'imperial' : 'metric'];
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
        const config = metricConfig[metric];
        if (!config || metric === 'none') return;
        
        let values = [...tripValues];
        if (isImperial && config.convert) {
            values = values.map(config.convert);
        }

        const summaryContainer = canvas.closest('.charts-panel').querySelector('.chart-summary');

        // It's possible to have trips but no data for a specific metric (e.g., driving score)
        if (values.length < 4) {
            const ctx = canvas.getContext('2d');
            ctx.clearRect(0, 0, canvas.width, canvas.height);
            ctx.font = "16px sans-serif";
            ctx.fillStyle = "#888";
            ctx.textAlign = "center";
            ctx.fillText("Not enough data for histogram.", canvas.width / 2, canvas.height / 2);
            return;
        }

        // --- Outlier removal using IQR method ---
        const sortedValues = [...values].sort((a, b) => a - b);
        const q1 = sortedValues[Math.floor((sortedValues.length / 4))];
        const q3 = sortedValues[Math.floor((sortedValues.length * 3) / 4)];
        const iqr = q3 - q1;
        const lowerBound = q1 - 1.5 * iqr;
        const upperBound = q3 + 1.5 * iqr;

        const filteredValues = values.filter(v => v >= lowerBound && v <= upperBound);
        const outliersRemovedCount = values.length - filteredValues.length;
        
        // Add a note about outliers to the summary text if any were removed
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

        // Handle cases where all data points are the same after filtering
        if (range === 0) {
            const ctx = canvas.getContext('2d');
            ctx.clearRect(0, 0, canvas.width, canvas.height);
            ctx.font = "16px sans-serif";
            ctx.fillStyle = "#888";
            ctx.textAlign = "center";
            ctx.fillText(`All trips have the same value: ${minValue.toFixed(1)}`, canvas.width / 2, canvas.height / 2);
            return;
        }

        // Build the histogram from the cleaned data
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
            label: `Trips`,
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
    
    // NEW: Extracted original line chart logic into its own function for clarity
    function renderLineChart(canvas, dailyData, metric1, metric2, isImperial, isUk, metricConfig, vin) {
        const labels = dailyData.map(d => new Date(d.date).toLocaleDateString(undefined, { month: 'short', day: 'numeric' }));
        const datasets = [];
        const yAxes = {};
        const createDataset = (metric, yAxisID) => {
            if (!metric || metric === 'none') return null;
            let data = dailyData.map(d => d[metric]);
            const config = metricConfig[metric];
            if (isImperial && config.convert) {
                data = data.map(val => val === null ? null : config.convert(val));
            }
            if (metric === 'ev_duration_seconds' || metric === 'duration_seconds') {
                data = data.map(s => s ? (s / 60) : 0);
            }
            return {
                label: config.label, data: data, borderColor: config.color,
                backgroundColor: `${config.color}33`, fill: true, tension: 0.1,
                pointRadius: 2, yAxisID: yAxisID
            };
        };
        const dataset1 = createDataset(metric1, 'y');
        if (dataset1) {
            datasets.push(dataset1);
            const config1 = metricConfig[metric1];
            yAxes.y = {
                type: 'linear', display: true, position: 'left',
                title: { display: true, text: `${config1.label} (${config1.unit[isImperial ? 'imperial' : 'metric']})` },
                grid: { color: '#ddd' }
            };
        }
        const dataset2 = createDataset(metric2, 'y1');
        if (dataset2) {
            datasets.push(dataset2);
            const config2 = metricConfig[metric2];
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
            type: 'line', data: { labels: labels, datasets: datasets },
            options: {
                responsive: true, maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false, },
                scales: { x: { title: { display: true, text: 'Date' } }, ...yAxes },
                plugins: {
                    legend: { display: datasets.length > 1 },
                    tooltip: {
                        callbacks: {
                            title: (tooltipItems) => new Date(dailyData[tooltipItems[0].dataIndex].date).toLocaleDateString(),
                            label: (context) => {
                                const metric = context.dataset.yAxisID === 'y' ? metric1 : metric2;
                                const config = metricConfig[metric];
                                let label = context.dataset.label || '';
                                if (label) label += ': ';
                                if (context.parsed.y !== null) {
                                    const unit = config.unit[isImperial ? 'imperial' : 'metric'];
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

    // NEW: Function for rendering the distribution plot
    function renderDistributionPlot(canvas, dailyData, metric1, metric2, isImperial, isUk, metricConfig, vin) {
        const datasets = [];
        const yAxes = {};
        let allLabels = [];
        const getMetricData = (metric) => {
            let values = dailyData.map(d => d[metric]).filter(v => v !== null && v !== undefined && v > 0);
            const config = metricConfig[metric];
            if (isImperial && config.convert) {
                values = values.map(config.convert);
            }
            return values;
        };
        const calculateStats = (data) => {
            if (data.length === 0) return { mean: 0, stdDev: 0 };
            const mean = data.reduce((a, b) => a + b) / data.length;
            const stdDev = Math.sqrt(data.map(x => Math.pow(x - mean, 2)).reduce((a, b) => a + b) / data.length);
            return { mean, stdDev };
        };
        const generateNormalCurve = (data, bins, binSize, minValue) => {
            if (data.length < 2) return [];
            const { mean, stdDev } = calculateStats(data);
            if (stdDev === 0) return [];
            const curvePoints = [];
            const normalPdf = (x) => (1 / (stdDev * Math.sqrt(2 * Math.PI))) * Math.exp(-0.5 * Math.pow((x - mean) / stdDev, 2));
            for (let i = 0; i < bins.length; i++) {
                const x = minValue + (i + 0.5) * binSize;
                curvePoints.push(normalPdf(x));
            }
            return curvePoints;
        };
        const processMetric = (metric, yAxisID) => {
            const config = metricConfig[metric];
            const values = getMetricData(metric);
            if (values.length < 4) return;
            const numBins = Math.ceil(1 + Math.log2(values.length));
            const minValue = Math.min(...values);
            const maxValue = Math.max(...values);
            const range = maxValue - minValue;
            if (range === 0) return;
            const binSize = range / numBins;
            const bins = new Array(numBins).fill(0);
            const labels = [];
            for (let i = 0; i < numBins; i++) {
                labels.push((minValue + i * binSize).toFixed(1));
            }
            if (labels.length > allLabels.length) allLabels = labels;
            for (const value of values) {
                let binIndex = Math.floor((value - minValue) / binSize);
                if (value === maxValue) binIndex = numBins - 1;
                if (binIndex >= 0 && binIndex < numBins) bins[binIndex]++;
            }
            const curveData = generateNormalCurve(values, bins, binSize, minValue);
            if (yAxisID === 'y' && metric2 === 'none') {
                datasets.push({
                    type: 'bar', label: `Frequency`, data: bins.map(d => d / values.length),
                    yAxisID: yAxisID, backgroundColor: `${config.color}66`, barPercentage: 1.0, categoryPercentage: 1.0
                });
            }
            datasets.push({
                type: 'line', label: config.label, data: curveData, yAxisID: yAxisID,
                borderColor: config.color, backgroundColor: 'transparent',
                pointRadius: 0, borderWidth: 2, tension: 0.4
            });
            yAxes[yAxisID] = {
                type: 'linear', position: yAxisID === 'y' ? 'left' : 'right',
                title: { display: true, text: `Probability Density` },
                grid: { drawOnChartArea: yAxisID === 'y1' ? false : true, color: '#ddd' },
            };
        };
        if (metric1 && metric1 !== 'none') processMetric(metric1, 'y');
        if (metric2 && metric2 !== 'none') processMetric(metric2, 'y1');
        if (datasets.length === 0) {
            const ctx = canvas.getContext('2d');
            ctx.clearRect(0, 0, canvas.width, canvas.height);
            ctx.font = "16px sans-serif";
            ctx.fillStyle = "#888";
            ctx.textAlign = "center";
            ctx.fillText("Not enough data for distribution plot.", canvas.width / 2, canvas.height / 2);
            return;
        }
        vehicleCharts[vin] = new Chart(canvas, {
            type: 'bar', data: { labels: allLabels, datasets },
            options: {
                responsive: true, maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                scales: { x: { title: { display: true, text: `Value` } }, ...yAxes },
                plugins: { tooltip: { callbacks: { title: (context) => metricConfig[context[0].dataset.label] || context[0].dataset.label } } }
            }
        });
    }

    // This function remains unchanged from your base file
    function updateStatusPanel(panel, vehicleStatus) {
        if (!vehicleStatus) return;
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
        if (vehicleStatus.trunk_locked === false) isCompletelyLocked = false;
        updateItem('trunk', vehicleStatus.trunk_closed, vehicleStatus.trunk_locked);
        updateItem('hood', vehicleStatus.hood_closed, null);
        if (lockStatusText) {
            if (isCompletelyLocked) {
                lockStatusText.textContent = '(Locked)';
                lockStatusText.className = 'lock-status-text locked';
            } else {
                lockStatusText.textContent = '(Unlocked)';
                lockStatusText.className = 'lock-status-text unlocked';
            }
        }
        if (openStatusText) {
            const uniqueOpenItems = [...new Set(openItems)];
            if (uniqueOpenItems.length > 0) {
                const message = uniqueOpenItems.map(item => item.charAt(0).toUpperCase() + item.slice(1) + '(s)').join(' & ') + ' open';
                openStatusText.textContent = `Warning: ${message}`;
            } else {
                openStatusText.textContent = '';
            }
        }
    }

    // This function's content is mostly the same, but the chart update logic is modified
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
            
            vehicleCard.querySelector('.stat-odometer h3').textContent = `Odometer (${distanceUnit})`;
            vehicleCard.querySelector('.stat-range h3').textContent = `Range Left (${distanceUnit})`;
            vehicleCard.querySelector('.stat-ev-distance h3').textContent = `Total EV Distance (${distanceUnit})`;
            vehicleCard.querySelector('.stat-daily-distance h3').textContent = `Today's Distance (${distanceUnit})`;
            vehicleCard.querySelector('.stat-consumption h3').textContent = `Consumption (${consumptionUnit})`;
            vehicleCard.querySelector('.stat-total-fuel h3').textContent = `Total Fuel (${fuelUnit})`;
            vehicleCard.querySelector('.stat-ev-range .distance_unit').textContent = distanceUnit;

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

            if (isImperial) {
                setVal('.odometer', Math.round(odometerKm * KM_TO_MI));
                setVal('.total_range', Math.round(rangeKm * KM_TO_MI));
                setVal('.total_ev_distance_km', Math.round(evDistanceKm * KM_TO_MI));
                setVal('.daily_distance', (dailyDistanceKm * KM_TO_MI).toFixed(1));
                setVal('.overall_fuel_consumption', l100kmToMpg(consumptionL100km, isUk).toFixed(1));
                setVal('.total_fuel_l', (totalFuelL * (isUk ? L_TO_GAL_UK : L_TO_GAL_US)).toFixed(2));
                setVal('.battery_range', batteryRangeKm !== 'N/A' ? Math.round(batteryRangeKm * KM_TO_MI) : 'N/A');
                setVal('.battery_range_with_ac', batteryRangeWithAcKm !== 'N/A' ? Math.round(batteryRangeWithAcKm * KM_TO_MI) : 'N/A');
            } else {
                setVal('.odometer', Math.round(odometerKm));
                setVal('.total_range', Math.round(rangeKm));
                setVal('.total_ev_distance_km', Math.round(evDistanceKm));
                setVal('.daily_distance', (dailyDistanceKm || 0).toFixed(1));
                setVal('.overall_fuel_consumption', consumptionL100km.toFixed(1));
                setVal('.total_fuel_l', totalFuelL.toFixed(2));
                setVal('.battery_range', batteryRangeKm !== 'N/A' ? Math.round(batteryRangeKm) : 'N/A');
                setVal('.battery_range_with_ac', batteryRangeWithAcKm !== 'N/A' ? Math.round(batteryRangeWithAcKm) : 'N/A');
            }

            // Populate new EV fields
            setVal('.battery_level', dashboard.battery_level !== undefined ? dashboard.battery_level : 'N/A');
            
            let chargeStatus = get(vehicleToRender, 'dashboard.charging_status', 'N/A');
            if (chargeStatus && typeof chargeStatus === 'string') {
                chargeStatus = chargeStatus.replace(/([A-Z])/g, ' $1').trim(); // "chargeComplete" -> "charge Complete"
                chargeStatus = chargeStatus.charAt(0).toUpperCase() + chargeStatus.slice(1); // "charge Complete" -> "Charge Complete"
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
                const embedUrl = `https://www.google.com/maps?q=${lat},${lon}&z=15&output=embed`;
                mapContainer.innerHTML = `<iframe src="${embedUrl}"></iframe>`;
            } else {
                mapContainer.innerHTML = '<p style="text-align: center; padding-top: 50px; color: #888;">Location data not available.</p>';
            }

            // Apply visibility settings
            const enabledSensors = appConfig.dashboard_sensors || {};
            vehicleCard.querySelectorAll('.stat[data-stat-key]').forEach(el => {
                const key = el.dataset.statKey;
                // Default to showing the stat if it's not in the config
                if (enabledSensors[key] === false) {
                    el.style.display = 'none';
                }
            });

            updateStatusPanel(vehicleCard, vehicleToRender.status);
            
            const refreshBtn = vehicleCard.querySelector('.force-poll');
            if (refreshBtn) {
                refreshBtn.addEventListener('click', (e) => handlePollRequest('/api/force_poll', e.target));
            }

            const leftMetricSelect = vehicleCard.querySelector('.chart-metric-select[data-axis="left"]');
            const rightMetricSelect = vehicleCard.querySelector('.chart-metric-select[data-axis="right"]');
            const periodSelect = vehicleCard.querySelector('.chart-period-select');
            const histogramToggleBtn = vehicleCard.querySelector('.histogram-toggle-btn');
            const chartCanvas = vehicleCard.querySelector('.history-chart');
            const settingsKey = `chartSettings-${vehicleToRender.vin}`;

            const updateChart = () => {
                const metric1 = leftMetricSelect.value;
                let metric2 = rightMetricSelect.value;
                const period = periodSelect.value;
                const isHistogram = histogramToggleBtn ? histogramToggleBtn.classList.contains('active') : false;
                
                if (isHistogram) {
                    metric2 = 'none';
                }

                localStorage.setItem(settingsKey, JSON.stringify({ metric1, metric2, period, isHistogram }));
                renderHistoryChart(vehicleToRender.vin, chartCanvas, metric1, metric2, period, isHistogram);
            };
            
            const savedSettings = localStorage.getItem(settingsKey);
            if (savedSettings) {
                try {
                    const settings = JSON.parse(savedSettings);
                    leftMetricSelect.value = settings.metric1 || 'distance_km';
                    rightMetricSelect.value = settings.metric2 || 'none';
                    periodSelect.value = settings.period || '30';
                    if (histogramToggleBtn && (settings.isHistogram || settings.isDistribution)) {
                        histogramToggleBtn.classList.add('active');
                    }
                }
                catch (e) {
                    console.error(`Error parsing saved chart settings for ${vehicleToRender.vin}:`, e);
                    localStorage.removeItem(settingsKey);
                }
            }

            if (histogramToggleBtn) {
                histogramToggleBtn.addEventListener('click', () => {
                    histogramToggleBtn.classList.toggle('active');
                    rightMetricSelect.disabled = histogramToggleBtn.classList.contains('active');
                    if (histogramToggleBtn.classList.contains('active')) {
                        rightMetricSelect.value = 'none';
                    }
                    updateChart();
                });
                rightMetricSelect.disabled = histogramToggleBtn.classList.contains('active');
            }

            leftMetricSelect.addEventListener('change', updateChart);
            rightMetricSelect.addEventListener('change', updateChart);
            periodSelect.addEventListener('change', updateChart);

            updateChart();
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
                await loadConfig(); // Reload config in case it changed
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

    async function init() {
        await loadConfig();
        await loadVehicleData();
    }
    init();
});