// CEO Dashboard JavaScript - Data fetching and visualization

const API_URL = 'http://localhost:5000/api/dashboard';
let dashboardData = [];
let currentWeeksFilter = 4;
let currentChannelFilter = 'all'; // 'all', 'amazon', 'bonsai'
let charts = {};

// Initialize dashboard
document.addEventListener('DOMContentLoaded', () => {
    setupEventListeners();
    loadDashboardData();
});

// Setup event listeners
function setupEventListeners() {
    // Time filter buttons
    const filterButtons = document.querySelectorAll('.filter-btn');
    filterButtons.forEach(btn => {
        btn.addEventListener('click', (e) => {
            filterButtons.forEach(b => b.classList.remove('active'));
            e.target.classList.add('active');
            const weeks = e.target.dataset.weeks;
            currentWeeksFilter = weeks === 'ytd' ? 'ytd' : parseInt(weeks);
            updateDashboard();
        });
    });

    // Channel filter buttons
    const channelButtons = document.querySelectorAll('.channel-btn');
    channelButtons.forEach(btn => {
        btn.addEventListener('click', (e) => {
            channelButtons.forEach(b => b.classList.remove('active'));
            e.target.classList.add('active');
            currentChannelFilter = e.target.dataset.channel;
            updateDashboard();
        });
    });
}

// Load data from API
async function loadDashboardData() {
    const loadingOverlay = document.getElementById('loadingOverlay');
    const errorMessage = document.getElementById('errorMessage');

    try {
        loadingOverlay.style.display = 'flex';
        errorMessage.style.display = 'none';

        const response = await fetch(API_URL);
        const result = await response.json();

        if (result.success && result.data && result.data.length > 0) {
            dashboardData = result.data;
            updateLastUpdated(result.timestamp);
            updateDashboard();
            loadingOverlay.style.display = 'none';
        } else {
            throw new Error(result.error || 'No data returned from API');
        }
    } catch (error) {
        console.error('CRITICAL: Error loading dashboard data:', error);
        errorMessage.style.display = 'block';
        errorMessage.querySelector('p').textContent = `Failed to load dashboard data: ${error.message}`;
        loadingOverlay.style.display = 'none';
    }
}

// Update last updated timestamp
function updateLastUpdated(timestamp) {
    const lastUpdated = document.getElementById('lastUpdated');
    const date = new Date(timestamp);
    lastUpdated.textContent = `Updated: ${date.toLocaleString()}`;
}

// Update dashboard with filtered data
function updateDashboard() {
    let filteredData;
    if (currentWeeksFilter === 'ytd') {
        const currentYear = new Date().getFullYear();
        filteredData = dashboardData.filter(d => d.year === currentYear);
    } else {
        filteredData = dashboardData.slice(0, currentWeeksFilter);
    }
    updateKPIs(filteredData);
    updateCharts(filteredData);
}

// Update KPI cards
function updateKPIs(data) {
    if (data.length === 0) return;

    // Handle card visibility based on channel filter
    const cards = document.querySelectorAll('.kpi-card');
    cards.forEach(card => {
        const cardChannel = card.dataset.channel;
        if (currentChannelFilter === 'all' || cardChannel === 'all' || cardChannel === currentChannelFilter) {
            card.classList.remove('hidden');
        } else {
            card.classList.add('hidden');
        }
    });

    const latest = data[0];
    const previous = dashboardData[dashboardData.indexOf(latest) + 1] || latest;

    // Find the comparison period (same weeks last year)
    const prevPeriodData = data.map(d => {
        // Force UTC parsing to avoid timezone shifts
        const currentWeekDate = new Date(d.week_start + 'T00:00:00Z');
        const targetDate = new Date(currentWeekDate);
        targetDate.setUTCDate(targetDate.getUTCDate() - 364);
        const targetStr = targetDate.toISOString().split('T')[0];

        const match = dashboardData.find(w => w.week_start === targetStr);
        if (!match) console.warn(`No historical match found for ${d.week_start} -> Target: ${targetStr}`);
        return match;
    }).filter(w => !!w);

    // Calculate totals for current and previous periods based on channel filter
    let totalRevenue = 0, prevTotalRevenue = 0;
    let latestRevenue = 0, previousRevenueForWow = 0;
    let totalBonsaiOrders = 0, prevTotalBonsaiOrders = 0;
    let totalAmazonUnits = 0, prevTotalAmazonUnits = 0;
    let totalBonsaiCustomers = 0, prevTotalBonsaiCustomers = 0;
    let totalAmazonNet = 0, prevTotalAmazonNet = 0;

    if (currentChannelFilter === 'amazon') {
        totalRevenue = data.reduce((sum, d) => sum + (d.amazon_revenue || 0), 0);
        prevTotalRevenue = prevPeriodData.reduce((sum, d) => sum + (d.amazon_revenue || 0), 0);
        latestRevenue = latest.amazon_revenue || 0;
        previousRevenueForWow = previous.amazon_revenue || 0;
    } else if (currentChannelFilter === 'bonsai') {
        totalRevenue = data.reduce((sum, d) => sum + (d.bonsai_revenue || 0), 0);
        prevTotalRevenue = prevPeriodData.reduce((sum, d) => sum + (d.bonsai_revenue || 0), 0);
        latestRevenue = latest.bonsai_revenue || 0;
        previousRevenueForWow = previous.bonsai_revenue || 0;
    } else if (currentChannelFilter === 'wholesale') {
        totalRevenue = data.reduce((sum, d) => sum + (d.wholesale_revenue || 0), 0);
        prevTotalRevenue = prevPeriodData.reduce((sum, d) => sum + (d.wholesale_revenue || 0), 0);
        latestRevenue = latest.wholesale_revenue || 0;
        previousRevenueForWow = previous.wholesale_revenue || 0;
    } else {
        totalRevenue = data.reduce((sum, d) => sum + (d.total_company_revenue || 0), 0);
        prevTotalRevenue = prevPeriodData.reduce((sum, d) => sum + (d.total_company_revenue || 0), 0);
        latestRevenue = latest.total_company_revenue || 0;
        previousRevenueForWow = previous.total_company_revenue || 0;
    }

    // Determine what change to show in the small red/green label
    // If YTD is selected, show change vs same period last year
    // Otherwise show WoW change
    const isYTD = currentWeeksFilter === 'ytd';

    // Total Revenue
    document.getElementById('totalRevenue').textContent = formatCurrency(totalRevenue);
    const revenueChange = isYTD ? (totalRevenue - prevTotalRevenue) : (latestRevenue - previousRevenueForWow);
    updateChangeElement('revenueChange', revenueChange, isYTD ? 'vs LY' : '');
    document.getElementById('revenuePrev').textContent = isYTD ? `LY: ${formatCurrency(prevTotalRevenue)}` : `Prev: ${formatCurrency(previousRevenueForWow)}`;

    // WoW Growth
    const wowGrowth = calculatePercentChange(latestRevenue, previousRevenueForWow);
    document.getElementById('wowGrowth').textContent = formatPercent(wowGrowth);
    updateChangeElement('wowChange', wowGrowth, 'WoW');
    // Calculate previous week's WOW for context if possible
    const prevWeek = dashboardData[dashboardData.indexOf(latest) + 1];
    const prevPrevWeek = dashboardData[dashboardData.indexOf(latest) + 2];
    if (prevWeek && prevPrevWeek) {
        const prevWow = calculatePercentChange(prevWeek.total_company_revenue || 0, prevPrevWeek.total_company_revenue || 0);
        document.getElementById('wowPrev').textContent = `Prev: ${formatPercent(prevWow)}`;
    } else {
        document.getElementById('wowPrev').textContent = '';
    }

    // YoY Growth
    const yoyGrowth = calculatePercentChange(totalRevenue, prevTotalRevenue);
    document.getElementById('yoyGrowth').textContent = formatPercent(yoyGrowth);
    updateChangeElement('yoyChange', yoyGrowth, isYTD ? 'YTD' : 'YoY');
    document.getElementById('yoyPrev').textContent = isYTD ? `2025: ${formatCurrency(prevTotalRevenue)}` : `LY: ${formatCurrency(prevTotalRevenue)}`;

    // Bonsai Orders
    if (currentChannelFilter === 'amazon' || currentChannelFilter === 'wholesale') {
        document.getElementById('bonsaiOrders').textContent = 'N/A';
        updateChangeElement('ordersChange', 0);
        document.getElementById('ordersPrev').textContent = '';
    } else {
        const totalBonsaiOrders = data.reduce((sum, d) => sum + (d.bonsai_orders || 0), 0);
        const prevTotalBonsaiOrders = prevPeriodData.reduce((sum, d) => sum + (d.bonsai_orders || 0), 0);
        document.getElementById('bonsaiOrders').textContent = formatNumber(totalBonsaiOrders);
        const ordersChange = isYTD ? (totalBonsaiOrders - prevTotalBonsaiOrders) : (latest.bonsai_orders - (previous.bonsai_orders || latest.bonsai_orders));
        updateChangeElement('ordersChange', ordersChange, isYTD ? 'vs LY' : '');
        document.getElementById('ordersPrev').textContent = isYTD ? `LY: ${formatNumber(prevTotalBonsaiOrders)}` : `Prev: ${formatNumber(previous.bonsai_orders)}`;
    }

    // Wholesale Orders
    if (currentChannelFilter === 'amazon' || currentChannelFilter === 'bonsai') {
        document.getElementById('wholesaleOrders').textContent = 'N/A';
        updateChangeElement('wholesaleOrdersChange', 0);
        document.getElementById('wholesaleOrdersPrev').textContent = '';
    } else {
        const totalWholesaleOrders = data.reduce((sum, d) => sum + (d.wholesale_orders || 0), 0);
        const prevWholesaleOrders = prevPeriodData.reduce((sum, d) => sum + (d.wholesale_orders || 0), 0);
        document.getElementById('wholesaleOrders').textContent = formatNumber(totalWholesaleOrders);
        const ordersChange = isYTD ? (totalWholesaleOrders - prevWholesaleOrders) : (latest.wholesale_orders - (previous.wholesale_orders || latest.wholesale_orders));
        updateChangeElement('wholesaleOrdersChange', ordersChange, isYTD ? 'vs LY' : '');
        document.getElementById('wholesaleOrdersPrev').textContent = isYTD ? `LY: ${formatNumber(prevWholesaleOrders)}` : `Prev: ${formatNumber(previous.wholesale_orders)}`;
    }

    // Amazon Units
    if (currentChannelFilter === 'bonsai') {
        document.getElementById('amazonUnits').textContent = 'N/A';
        updateChangeElement('unitsChange', 0);
        document.getElementById('unitsPrev').textContent = '';
    } else {
        totalAmazonUnits = data.reduce((sum, d) => sum + (d.amazon_units || 0), 0);
        prevTotalAmazonUnits = prevPeriodData.reduce((sum, d) => sum + (d.amazon_units || 0), 0);
        document.getElementById('amazonUnits').textContent = formatNumber(totalAmazonUnits);
        const unitsChange = isYTD ? (totalAmazonUnits - prevTotalAmazonUnits) : (latest.amazon_units - (previous.amazon_units || latest.amazon_units));
        updateChangeElement('unitsChange', unitsChange, isYTD ? 'vs LY' : '');
        document.getElementById('unitsPrev').textContent = isYTD ? `LY: ${formatNumber(prevTotalAmazonUnits)}` : `Prev: ${formatNumber(previous.amazon_units)}`;
    }

    // Amazon Sessions
    if (currentChannelFilter === 'bonsai') {
        document.getElementById('amazonSessions').textContent = 'N/A';
        updateChangeElement('sessionsChange', 0);
        document.getElementById('sessionsPrev').textContent = '';
    } else {
        const totalSessions = data.reduce((sum, d) => sum + (d.amazon_sessions || 0), 0);
        const prevSessions = prevPeriodData.reduce((sum, d) => sum + (d.amazon_sessions || 0), 0);
        document.getElementById('amazonSessions').textContent = formatNumber(totalSessions);
        const sessionsChange = isYTD ? (totalSessions - prevSessions) : (latest.amazon_sessions - (previous.amazon_sessions || latest.amazon_sessions));
        updateChangeElement('sessionsChange', sessionsChange, isYTD ? 'vs LY' : '');
        document.getElementById('sessionsPrev').textContent = isYTD ? `LY: ${formatNumber(prevSessions)}` : `Prev: ${formatNumber(previous.amazon_sessions)}`;
    }

    // Amazon Conv Rate
    if (currentChannelFilter === 'bonsai') {
        document.getElementById('amazonConvRate').textContent = 'N/A';
        updateChangeElement('cvrChange', 0);
        document.getElementById('cvrPrev').textContent = '';
    } else {
        const totalUnits = data.reduce((sum, d) => sum + (d.amazon_units || 0), 0);
        const totalSessions = data.reduce((sum, d) => sum + (d.amazon_sessions || 0), 0);
        const periodCVR = totalSessions > 0 ? (totalUnits / totalSessions) * 100 : 0;

        const prevUnits = prevPeriodData.reduce((sum, d) => sum + (d.amazon_units || 0), 0);
        const prevSessions = prevPeriodData.reduce((sum, d) => sum + (d.amazon_sessions || 0), 0);
        const prevCVR = prevSessions > 0 ? (prevUnits / prevSessions) * 100 : 0;

        document.getElementById('amazonConvRate').textContent = formatPercent(periodCVR);
        const cvrChange = isYTD ? (periodCVR - prevCVR) : ((latest.amazon_cvr || 0) - (previous.amazon_cvr || 0));
        updateChangeElement('cvrChange', cvrChange, isYTD ? 'pp LY' : 'pp');
        document.getElementById('cvrPrev').textContent = isYTD ? `LY: ${formatPercent(prevCVR)}` : `Prev: ${formatPercent(previous.amazon_cvr)}`;
    }

    // Average Order Value
    if (currentChannelFilter === 'amazon') {
        document.getElementById('avgOrderValue').textContent = 'N/A';
        updateChangeElement('aovChange', 0);
        document.getElementById('aovPrev').textContent = '';
    } else if (currentChannelFilter === 'wholesale') {
        const totalWholesaleRev = data.reduce((sum, d) => sum + (d.wholesale_revenue || 0), 0);
        const orders = data.reduce((sum, d) => sum + (d.wholesale_orders || 0), 0);
        const periodAOV = orders > 0 ? totalWholesaleRev / orders : 0;
        const prevWholesaleRev = prevPeriodData.reduce((sum, d) => sum + (d.wholesale_revenue || 0), 0);
        const prevOrders = prevPeriodData.reduce((sum, d) => sum + (d.wholesale_orders || 0), 0);
        const prevAOV = prevOrders > 0 ? prevWholesaleRev / prevOrders : 0;
        document.getElementById('avgOrderValue').textContent = formatCurrency(periodAOV);
        const aovChange = isYTD ? (periodAOV - prevAOV) : (latest.wholesale_aov - (previous.wholesale_aov || latest.wholesale_aov));
        updateChangeElement('aovChange', aovChange, isYTD ? 'vs LY' : '');
        document.getElementById('aovPrev').textContent = isYTD ? `LY: ${formatCurrency(prevAOV)}` : `Prev: ${formatCurrency(previous.wholesale_aov)}`;
    } else {
        const totalBonsaiRev = data.reduce((sum, d) => sum + (d.bonsai_revenue || 0), 0);
        const orders = data.reduce((sum, d) => sum + (d.bonsai_orders || 0), 0);
        const periodAOV = orders > 0 ? totalBonsaiRev / orders : 0;

        const prevBonsaiRev = prevPeriodData.reduce((sum, d) => sum + (d.bonsai_revenue || 0), 0);
        const prevOrders = prevPeriodData.reduce((sum, d) => sum + (d.bonsai_orders || 0), 0);
        const prevAOV = prevOrders > 0 ? prevBonsaiRev / prevOrders : 0;

        document.getElementById('avgOrderValue').textContent = formatCurrency(periodAOV);
        const aovChange = isYTD ? (periodAOV - prevAOV) : (latest.bonsai_aov - (previous.bonsai_aov || latest.bonsai_aov));
        updateChangeElement('aovChange', aovChange, isYTD ? 'vs LY' : '');
        document.getElementById('aovPrev').textContent = isYTD ? `LY: ${formatCurrency(prevAOV)}` : `Prev: ${formatCurrency(previous.bonsai_aov)}`;
    }

    // Amazon Margin
    if (currentChannelFilter === 'bonsai') {
        document.getElementById('amazonMargin').textContent = 'N/A';
        updateChangeElement('marginChange', 0);
        document.getElementById('marginPrev').textContent = '';
    } else {
        totalAmazonNet = data.reduce((sum, d) => sum + (d.amazon_net_proceeds || 0), 0);
        const totalSales = data.reduce((sum, d) => sum + (d.amazon_revenue || 0), 0);
        const periodMargin = totalSales > 0 ? (totalAmazonNet / totalSales) * 100 : 0;

        prevTotalAmazonNet = prevPeriodData.reduce((sum, d) => sum + (d.amazon_net_proceeds || 0), 0);
        const prevTotalSales = prevPeriodData.reduce((sum, d) => sum + (d.amazon_revenue || 0), 0);
        const prevMargin = prevTotalSales > 0 ? (prevTotalAmazonNet / prevTotalSales) * 100 : 0;

        document.getElementById('amazonMargin').textContent = formatPercent(periodMargin);
        const marginChange = isYTD ? (periodMargin - prevMargin) : ((latest.amazon_margin_pct || 0) - (previous.amazon_margin_pct || 0));
        updateChangeElement('marginChange', marginChange, isYTD ? 'pp LY' : 'pp');
        document.getElementById('marginPrev').textContent = isYTD ? `LY: ${formatPercent(prevMargin)}` : `Prev: ${formatPercent(previous.amazon_margin_pct)}`;
    }

    // Amazon Net Proceeds
    if (currentChannelFilter === 'bonsai') {
        document.getElementById('amazonNetProceeds').textContent = 'N/A';
        updateChangeElement('netProceedsChange', 0);
        document.getElementById('netProceedsPrev').textContent = '';
    } else {
        totalAmazonNet = data.reduce((sum, d) => sum + (d.amazon_net_proceeds || 0), 0);
        prevTotalAmazonNet = prevPeriodData.reduce((sum, d) => sum + (d.amazon_net_proceeds || 0), 0);

        document.getElementById('amazonNetProceeds').textContent = formatCurrency(totalAmazonNet);
        const netChange = isYTD ? (totalAmazonNet - prevTotalAmazonNet) : (latest.amazon_net_proceeds - (previous.amazon_net_proceeds || latest.amazon_net_proceeds));
        updateChangeElement('netProceedsChange', netChange, isYTD ? 'vs LY' : '');
        document.getElementById('netProceedsPrev').textContent = isYTD ? `LY: ${formatCurrency(prevTotalAmazonNet)}` : `Prev: ${formatCurrency(previous.amazon_net_proceeds)}`;
    }

    // Customers
    if (currentChannelFilter === 'amazon') {
        document.getElementById('customers').textContent = 'N/A';
        updateChangeElement('customersChange', 0);
        document.getElementById('customersPrev').textContent = '';
    } else {
        totalBonsaiCustomers = data.reduce((sum, d) => sum + (d.bonsai_customers || 0), 0);
        prevTotalBonsaiCustomers = prevPeriodData.reduce((sum, d) => sum + (d.bonsai_customers || 0), 0);
        document.getElementById('customers').textContent = formatNumber(totalBonsaiCustomers);
        const custChange = isYTD ? (totalBonsaiCustomers - prevTotalBonsaiCustomers) : (latest.bonsai_customers - (previous.bonsai_customers || latest.bonsai_customers));
        updateChangeElement('customersChange', custChange, isYTD ? 'vs LY' : '');
        document.getElementById('customersPrev').textContent = isYTD ? `LY: ${formatNumber(prevTotalBonsaiCustomers)}` : `Prev: ${formatNumber(previous.bonsai_customers)}`;
    }

    // Bonsai Sessions (from GA4)
    if (currentChannelFilter === 'amazon') {
        document.getElementById('bonsaiSessions').textContent = 'N/A';
        updateChangeElement('bonsaiSessionsChange', 0);
        document.getElementById('bonsaiSessionsPrev').textContent = '';
    } else {
        const totalBonsaiSessions = data.reduce((sum, d) => sum + (d.bonsai_sessions || 0), 0);
        const prevBonsaiSessions = prevPeriodData.reduce((sum, d) => sum + (d.bonsai_sessions || 0), 0);
        document.getElementById('bonsaiSessions').textContent = formatNumber(totalBonsaiSessions);
        const sessionsChange = isYTD ? (totalBonsaiSessions - prevBonsaiSessions) : ((latest.bonsai_sessions || 0) - (previous.bonsai_sessions || 0));
        updateChangeElement('bonsaiSessionsChange', sessionsChange, isYTD ? 'vs LY' : '');
        document.getElementById('bonsaiSessionsPrev').textContent = isYTD ? `LY: ${formatNumber(prevBonsaiSessions)}` : `Prev: ${formatNumber(previous.bonsai_sessions)}`;
    }

    // Bonsai Conv Rate (from GA4)
    if (currentChannelFilter === 'amazon') {
        document.getElementById('bonsaiConvRate').textContent = 'N/A';
        updateChangeElement('bonsaiCvrChange', 0);
        document.getElementById('bonsaiCvrPrev').textContent = '';
    } else {
        const totalOrders = data.reduce((sum, d) => sum + (d.bonsai_orders || 0), 0);
        const totalSessions = data.reduce((sum, d) => sum + (d.bonsai_sessions || 0), 0);
        const periodCVR = totalSessions > 0 ? (totalOrders / totalSessions) * 100 : 0;

        const prevOrders = prevPeriodData.reduce((sum, d) => sum + (d.bonsai_orders || 0), 0);
        const prevSessions = prevPeriodData.reduce((sum, d) => sum + (d.bonsai_sessions || 0), 0);
        const prevCVR = prevSessions > 0 ? (prevOrders / prevSessions) * 100 : 0;

        document.getElementById('bonsaiConvRate').textContent = formatPercent(periodCVR);
        const cvrChange = isYTD ? (periodCVR - prevCVR) : ((latest.bonsai_cvr || 0) - (previous.bonsai_cvr || 0));
        updateChangeElement('bonsaiCvrChange', cvrChange, isYTD ? 'pp LY' : 'pp');
        document.getElementById('bonsaiCvrPrev').textContent = isYTD ? `LY: ${formatPercent(prevCVR)}` : `Prev: ${formatPercent(previous.bonsai_cvr)}`;
    }
}

// Update charts with defensive error handling
function updateCharts(data) {
    if (!data || data.length === 0) return;
    const reversedData = [...data].reverse();

    // Run each chart update in its own try-catch so one failure doesn't kill the dashboard
    const runUpdate = (name, fn, args) => {
        try {
            fn(args);
        } catch (err) {
            console.error(`Error updating ${name}:`, err);
        }
    };

    runUpdate('Revenue Trend', updateRevenueTrendChart, reversedData);

    if (currentChannelFilter === 'all') {
        runUpdate('Channel Mix', updateChannelMixChart, reversedData);
    } else {
        if (charts.channelMix) charts.channelMix.destroy();
    }

    runUpdate('WoW Growth', updateWowGrowthChart, reversedData);
    runUpdate('YoY Comparison', updateYoyComparisonChart, data);
}

// Revenue Trend Chart
function updateRevenueTrendChart(data) {
    const ctx = document.getElementById('revenueTrendChart');

    if (charts.revenueTrend) {
        charts.revenueTrend.destroy();
    }

    let datasets = [];

    if (currentChannelFilter === 'all') {
        datasets = [
            {
                label: 'Total Revenue',
                data: data.map(d => d.total_company_revenue || 0),
                borderColor: '#6366f1',
                backgroundColor: 'rgba(99, 102, 241, 0.1)',
                tension: 0.4,
                fill: true
            },
            {
                label: 'Bonsai Revenue',
                data: data.map(d => d.bonsai_revenue || 0),
                borderColor: '#10b981',
                backgroundColor: 'rgba(16, 185, 129, 0.1)',
                tension: 0.4,
                fill: true
            },
            {
                label: 'Amazon Revenue',
                data: data.map(d => d.amazon_revenue || 0),
                borderColor: '#f59e0b',
                backgroundColor: 'rgba(245, 158, 11, 0.1)',
                tension: 0.4,
                fill: true
            },
            {
                label: 'Wholesale Revenue',
                data: data.map(d => d.wholesale_revenue || 0),
                borderColor: '#8b5cf6',
                backgroundColor: 'rgba(139, 92, 246, 0.1)',
                tension: 0.4,
                fill: true
            }
        ];
    } else if (currentChannelFilter === 'amazon') {
        datasets = [
            {
                label: 'Amazon Revenue',
                data: data.map(d => d.amazon_revenue || 0),
                borderColor: '#f59e0b',
                backgroundColor: 'rgba(245, 158, 11, 0.1)',
                tension: 0.4,
                fill: true
            }
        ];
    } else if (currentChannelFilter === 'wholesale') {
        datasets = [
            {
                label: 'Wholesale Revenue',
                data: data.map(d => d.wholesale_revenue || 0),
                borderColor: '#8b5cf6',
                backgroundColor: 'rgba(139, 92, 246, 0.1)',
                tension: 0.4,
                fill: true
            }
        ];
    } else {
        datasets = [
            {
                label: 'Bonsai Revenue',
                data: data.map(d => d.bonsai_revenue || 0),
                borderColor: '#10b981',
                backgroundColor: 'rgba(16, 185, 129, 0.1)',
                tension: 0.4,
                fill: true
            }
        ];
    }

    charts.revenueTrend = new Chart(ctx, {
        type: 'line',
        data: {
            labels: data.map(d => formatDate(d.week_start)),
            datasets: datasets
        },
        options: getChartOptions('Revenue ($)')
    });
}

// Channel Mix Chart
function updateChannelMixChart(data) {
    const ctx = document.getElementById('channelMixChart');

    if (charts.channelMix) {
        charts.channelMix.destroy();
    }

    charts.channelMix = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: data.map(d => formatDate(d.week_start)),
            datasets: [
                {
                    label: 'Bonsai',
                    data: data.map(d => d.bonsai_revenue || 0),
                    backgroundColor: '#10b981'
                },
                {
                    label: 'Amazon',
                    data: data.map(d => d.amazon_revenue || 0),
                    backgroundColor: '#f59e0b'
                },
                {
                    label: 'Wholesale',
                    data: data.map(d => d.wholesale_revenue || 0),
                    backgroundColor: '#8b5cf6'
                }
            ]
        },
        options: {
            ...getChartOptions('Revenue ($)'),
            scales: {
                ...getChartOptions('Revenue ($)').scales,
                x: { ...getChartOptions('Revenue ($)').scales.x, stacked: true },
                y: { ...getChartOptions('Revenue ($)').scales.y, stacked: true }
            }
        }
    });
}

// YoY Growth Chart (compares each week to the same week last year)
function updateWowGrowthChart(data) {
    const ctx = document.getElementById('wowGrowthChart');

    if (charts.wowGrowth) {
        charts.wowGrowth.destroy();
    }

    const yoyData = data.map((d) => {
        // Find the same week from last year (364 days ago)
        const currentWeekDate = new Date(d.week_start + 'T00:00:00Z');
        const targetDate = new Date(currentWeekDate);
        targetDate.setUTCDate(targetDate.getUTCDate() - 364);
        const targetStr = targetDate.toISOString().split('T')[0];

        const lastYearWeek = dashboardData.find(w => w.week_start === targetStr);
        if (!lastYearWeek) return 0;

        let current, previous;
        if (currentChannelFilter === 'amazon') {
            current = d.amazon_revenue || 0;
            previous = lastYearWeek.amazon_revenue || 0;
        } else if (currentChannelFilter === 'bonsai') {
            current = d.bonsai_revenue || 0;
            previous = lastYearWeek.bonsai_revenue || 0;
        } else {
            current = d.total_company_revenue || 0;
            previous = lastYearWeek.total_company_revenue || 0;
        }

        return calculatePercentChange(current, previous);
    });

    charts.wowGrowth = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: data.map(d => formatDate(d.week_start)),
            datasets: [{
                label: 'YoY Growth %',
                data: yoyData,
                backgroundColor: yoyData.map(v => v >= 0 ? '#10b981' : '#ef4444')
            }]
        },
        options: getChartOptions('Growth %')
    });
}

// YoY Comparison Chart - Should show full year progress comparison
function updateYoyComparisonChart(data) {
    const ctx = document.getElementById('yoyComparisonChart');

    if (charts.yoyComparison) {
        charts.yoyComparison.destroy();
    }

    // Always fetch full year data from dashboardData for this specific chart
    const data2025 = dashboardData.filter(d => d.year === 2025).reverse();
    const data2026 = dashboardData.filter(d => d.year === 2026).reverse();

    if (data2026.length === 0) return;

    let data2025Values, data2026Values;

    if (currentChannelFilter === 'amazon') {
        data2025Values = data2025.map(d => d.amazon_revenue || 0);
        data2026Values = data2026.map(d => d.amazon_revenue || 0);
    } else if (currentChannelFilter === 'bonsai') {
        data2025Values = data2025.map(d => d.bonsai_revenue || 0);
        data2026Values = data2026.map(d => d.bonsai_revenue || 0);
    } else {
        data2025Values = data2025.map(d => d.total_company_revenue || 0);
        data2026Values = data2026.map(d => d.total_company_revenue || 0);
    }

    charts.yoyComparison = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: data2026.map(d => formatDate(d.week_start)), // Use real dates instead of "Week X"
            datasets: [
                {
                    label: '2025 Revenue',
                    data: data2025Values.slice(0, data2026Values.length), // Align length
                    backgroundColor: 'rgba(99, 102, 241, 0.5)',
                    borderColor: '#6366f1',
                    borderWidth: 1
                },
                {
                    label: '2026 Revenue',
                    data: data2026Values,
                    backgroundColor: '#8b5cf6',
                    borderColor: '#8b5cf6',
                    borderWidth: 1
                }
            ]
        },
        options: getChartOptions('Revenue ($)')
    });
}

// Chart options template
function getChartOptions(yAxisLabel) {
    return {
        responsive: true,
        maintainAspectRatio: true,
        plugins: {
            legend: {
                labels: {
                    color: '#a0aec0',
                    font: { size: 12 }
                }
            },
            tooltip: {
                backgroundColor: '#1a2332',
                titleColor: '#ffffff',
                bodyColor: '#a0aec0',
                borderColor: '#2d3748',
                borderWidth: 1
            }
        },
        scales: {
            x: {
                grid: { color: '#2d3748' },
                ticks: { color: '#a0aec0', font: { size: 11 } }
            },
            y: {
                grid: { color: '#2d3748' },
                ticks: { color: '#a0aec0', font: { size: 11 } },
                title: {
                    display: true,
                    text: yAxisLabel,
                    color: '#a0aec0'
                }
            }
        }
    };
}

// Helper functions
function calculateChange(current, previous) {
    return current - previous;
}

function calculatePercentChange(current, previous) {
    if (previous === 0) return 0;
    return ((current - previous) / previous) * 100;
}

function updateChangeElement(elementId, value, suffix = '') {
    const element = document.getElementById(elementId);
    const absValue = Math.abs(value);
    const sign = value >= 0 ? '+' : '-';
    const className = value > 0 ? 'positive' : value < 0 ? 'negative' : 'neutral';

    element.className = `kpi-change ${className}`;

    if (suffix === 'WoW' || suffix === 'YoY') {
        element.textContent = suffix;
    } else if (suffix === 'pp') {
        element.textContent = `${sign}${absValue.toFixed(2)}pp`;
    } else {
        element.textContent = `${sign}${formatNumber(absValue)}`;
    }
}

function formatCurrency(value) {
    if (value === null || value === undefined) return '$0';
    return new Intl.NumberFormat('en-US', {
        style: 'currency',
        currency: 'USD',
        minimumFractionDigits: 0,
        maximumFractionDigits: 0
    }).format(value);
}

function formatNumber(value) {
    if (value === null || value === undefined) return '0';
    return new Intl.NumberFormat('en-US').format(Math.round(value));
}

function formatPercent(value) {
    if (value === null || value === undefined) return '0.0%';
    return `${value >= 0 ? '+' : ''}${value.toFixed(1)}%`;
}

function formatDate(dateString) {
    const date = new Date(dateString);
    return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
}
