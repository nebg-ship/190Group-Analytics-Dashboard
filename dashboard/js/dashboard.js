// CEO Dashboard JavaScript - Data fetching and visualization

const API_URL = 'http://localhost:5000/api/dashboard';
let dashboardData = [];
let wholesaleCustomers = [];
let currentWeeksFilter = 'ytd';
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
            currentWeeksFilter = e.target.dataset.period;
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
            wholesaleCustomers = result.wholesale_customers || [];
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
    const now = new Date();
    const currentYear = now.getFullYear();
    const currentMonth = now.getMonth(); // 0-indexed

    if (currentWeeksFilter === 'ytd') {
        filteredData = dashboardData.filter(d => Number(d.year) == currentYear);
    } else if (currentWeeksFilter === 'month') {
        filteredData = dashboardData.filter(d => {
            const dDate = new Date(d.week_start + 'T00:00:00Z');
            return Number(d.year) == currentYear && dDate.getUTCMonth() === currentMonth;
        });
    } else if (currentWeeksFilter === 'last-month') {
        let targetMonth = currentMonth - 1;
        let targetYear = currentYear;
        if (targetMonth < 0) {
            targetMonth = 11;
            targetYear = currentYear - 1;
        }
        filteredData = dashboardData.filter(d => {
            const dDate = new Date(d.week_start + 'T00:00:00Z');
            return Number(d.year) == targetYear && dDate.getUTCMonth() === targetMonth;
        });
    } else if (currentWeeksFilter === 'quarter') {
        const currentQuarter = Math.floor(currentMonth / 3);
        filteredData = dashboardData.filter(d => {
            const dDate = new Date(d.week_start + 'T00:00:00Z');
            const dQuarter = Math.floor(dDate.getUTCMonth() / 3);
            return Number(d.year) == currentYear && dQuarter === currentQuarter;
        });
    } else {
        // Default / Fallback - if it's a number, slice that many weeks
        const weeks = parseInt(currentWeeksFilter);
        filteredData = dashboardData.slice(0, isNaN(weeks) ? 4 : weeks);
    }

    // Find the comparison period (fuzzy matching for same weeks last year)
    const prevPeriodData = filteredData.map(d => {
        const currentWeekDate = new Date(d.week_start + 'T00:00:00Z');
        const targetDate = new Date(currentWeekDate);
        targetDate.setUTCDate(targetDate.getUTCDate() - 364);

        // Fuzzy search: find a week that starts within +/- 3 days of the target
        // This handles year boundaries and split-week alignment
        const match = dashboardData.find(w => {
            const wDate = new Date(w.week_start + 'T00:00:00Z');
            const diffDays = Math.abs((wDate - targetDate) / (1000 * 60 * 60 * 24));
            return diffDays <= 3;
        });

        return match;
    }).filter(w => !!w);

    updateKPIs(filteredData, prevPeriodData);
    updateCharts(filteredData);
}

// Update KPI cards
function updateKPIs(data, prevPeriodData) {
    if (!data || !prevPeriodData) return;

    if (data.length === 0) {
        // Reset KPIs if no data for period
        document.querySelectorAll('.kpi-value').forEach(el => el.textContent = '0');
        return;
    }

    // Handle card visibility based on channel filter
    const cards = document.querySelectorAll('.kpi-card');
    cards.forEach(card => {
        const cardChannel = card.dataset.channel;

        // Google Analytics cards are ONLY shown in the google_analytics tab
        if (cardChannel === 'google_analytics') {
            if (currentChannelFilter === 'google_analytics') {
                card.classList.remove('hidden');
            } else {
                card.classList.add('hidden');
            }
        } else {
            // Non-GA cards: show if filter is 'all' or matches channel
            // EXCEPTION: hide when in google_analytics tab
            if (currentChannelFilter === 'google_analytics') {
                card.classList.add('hidden');
            } else if (currentChannelFilter === 'all' || cardChannel === 'all' || cardChannel === currentChannelFilter) {
                card.classList.remove('hidden');
            } else {
                card.classList.add('hidden');
            }
        }
    });

    // Handle Wholesale Customers Section visibility
    const wholesaleSection = document.getElementById('wholesaleCustomersSection');
    const customerTypeTrendCard = document.getElementById('customerTypeTrendCard');
    const channelMixCard = document.getElementById('channelMixCard');

    if (currentChannelFilter === 'wholesale') {
        wholesaleSection.classList.remove('hidden');
        renderWholesaleCustomers(wholesaleCustomers);
    } else {
        wholesaleSection.classList.add('hidden');
    }

    if (currentChannelFilter === 'bonsai') {
        customerTypeTrendCard.classList.remove('hidden');
    } else {
        customerTypeTrendCard.classList.add('hidden');
    }

    if (currentChannelFilter === 'all') {
        channelMixCard.classList.remove('hidden');
    } else {
        channelMixCard.classList.add('hidden');
    }

    const gaTrafficTrendCard = document.getElementById('gaTrafficTrendCard');
    if (currentChannelFilter === 'google_analytics') {
        gaTrafficTrendCard.classList.remove('hidden');
    } else {
        gaTrafficTrendCard.classList.add('hidden');
    }


    // Calculate totals for current and previous periods based on channel filter
    let totalRevenue = 0, prevTotalRevenue = 0;
    let totalBonsaiOrders = 0, prevTotalBonsaiOrders = 0;
    let totalAmazonUnits = 0, prevTotalAmazonUnits = 0;
    let totalBonsaiCustomers = 0, prevTotalBonsaiCustomers = 0;
    let totalAmazonNet = 0, prevTotalAmazonNet = 0;

    if (currentChannelFilter === 'amazon') {
        totalRevenue = data.reduce((sum, d) => sum + (d.amazon_revenue || 0), 0);
        prevTotalRevenue = prevPeriodData.reduce((sum, d) => sum + (d.amazon_revenue || 0), 0);
    } else if (currentChannelFilter === 'bonsai') {
        totalRevenue = data.reduce((sum, d) => sum + (d.bonsai_revenue || 0), 0);
        prevTotalRevenue = prevPeriodData.reduce((sum, d) => sum + (d.bonsai_revenue || 0), 0);
    } else if (currentChannelFilter === 'wholesale') {
        totalRevenue = data.reduce((sum, d) => sum + (d.wholesale_revenue || 0), 0);
        prevTotalRevenue = prevPeriodData.reduce((sum, d) => sum + (d.wholesale_revenue || 0), 0);
    } else {
        totalRevenue = data.reduce((sum, d) => sum + (d.total_company_revenue || 0), 0);
        prevTotalRevenue = prevPeriodData.reduce((sum, d) => sum + (d.total_company_revenue || 0), 0);
    }



    // Total Revenue
    document.getElementById('totalRevenue').textContent = formatCurrency(totalRevenue);
    const revenueChange = totalRevenue - prevTotalRevenue;
    updateChangeElement('revenueChange', revenueChange, 'vs LY');
    document.getElementById('revenuePrev').textContent = `LY: ${formatCurrency(prevTotalRevenue)}`;

    // YoY Growth
    const yoyGrowth = calculatePercentChange(totalRevenue, prevTotalRevenue);
    document.getElementById('yoyGrowth').textContent = formatPercent(yoyGrowth);
    updateChangeElement('yoyChange', yoyGrowth, 'YoY');
    document.getElementById('yoyPrev').textContent = `LY: ${formatCurrency(prevTotalRevenue)}`;

    // Bonsai Orders
    if (currentChannelFilter === 'amazon' || currentChannelFilter === 'wholesale') {
        document.getElementById('bonsaiOrders').textContent = 'N/A';
        updateChangeElement('ordersChange', 0);
        document.getElementById('ordersPrev').textContent = '';
    } else {
        const totalBonsaiOrders = data.reduce((sum, d) => sum + (d.bonsai_orders || 0), 0);
        const prevTotalBonsaiOrders = prevPeriodData.reduce((sum, d) => sum + (d.bonsai_orders || 0), 0);
        document.getElementById('bonsaiOrders').textContent = formatNumber(totalBonsaiOrders);
        const ordersChange = totalBonsaiOrders - prevTotalBonsaiOrders;
        updateChangeElement('ordersChange', ordersChange, 'vs LY');
        document.getElementById('ordersPrev').textContent = `LY: ${formatNumber(prevTotalBonsaiOrders)}`;
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
        const ordersChange = totalWholesaleOrders - prevWholesaleOrders;
        updateChangeElement('wholesaleOrdersChange', ordersChange, 'vs LY');
        document.getElementById('wholesaleOrdersPrev').textContent = `LY: ${formatNumber(prevWholesaleOrders)}`;
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
        const unitsChange = totalAmazonUnits - prevTotalAmazonUnits;
        updateChangeElement('unitsChange', unitsChange, 'vs LY');
        document.getElementById('unitsPrev').textContent = `LY: ${formatNumber(prevTotalAmazonUnits)}`;
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
        const sessionsChange = totalSessions - prevSessions;
        updateChangeElement('sessionsChange', sessionsChange, 'vs LY');
        document.getElementById('sessionsPrev').textContent = `LY: ${formatNumber(prevSessions)}`;
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
        const cvrChange = periodCVR - prevCVR;
        updateChangeElement('cvrChange', cvrChange, 'pp LY');
        document.getElementById('cvrPrev').textContent = `LY: ${formatPercent(prevCVR)}`;
    }

    // Average Order Value
    if (currentChannelFilter === 'amazon') {
        const totalAmazonRev = data.reduce((sum, d) => sum + (d.amazon_revenue || 0), 0);
        const orders = data.reduce((sum, d) => sum + (d.amazon_orders || 0), 0);
        const periodAOV = orders > 0 ? totalAmazonRev / orders : 0;

        const prevAmazonRev = prevPeriodData.reduce((sum, d) => sum + (d.amazon_revenue || 0), 0);
        const prevOrders = prevPeriodData.reduce((sum, d) => sum + (d.amazon_orders || 0), 0);
        const prevAOV = prevOrders > 0 ? prevAmazonRev / prevOrders : 0;

        document.getElementById('avgOrderValue').textContent = formatCurrency(periodAOV);
        const aovChange = periodAOV - prevAOV;
        updateChangeElement('aovChange', aovChange, 'vs LY');
        document.getElementById('aovPrev').textContent = `LY: ${formatCurrency(prevAOV)}`;
    } else if (currentChannelFilter === 'wholesale') {
        const totalWholesaleRev = data.reduce((sum, d) => sum + (d.wholesale_revenue || 0), 0);
        const orders = data.reduce((sum, d) => sum + (d.wholesale_orders || 0), 0);
        const periodAOV = orders > 0 ? totalWholesaleRev / orders : 0;
        const prevWholesaleRev = prevPeriodData.reduce((sum, d) => sum + (d.wholesale_revenue || 0), 0);
        const prevOrders = prevPeriodData.reduce((sum, d) => sum + (d.wholesale_orders || 0), 0);
        const prevAOV = prevOrders > 0 ? prevWholesaleRev / prevOrders : 0;
        document.getElementById('avgOrderValue').textContent = formatCurrency(periodAOV);
        const aovChange = periodAOV - prevAOV;
        updateChangeElement('aovChange', aovChange, 'vs LY');
        document.getElementById('aovPrev').textContent = `LY: ${formatCurrency(prevAOV)}`;
    } else {
        const totalBonsaiRev = data.reduce((sum, d) => sum + (d.bonsai_revenue || 0), 0);
        const orders = data.reduce((sum, d) => sum + (d.bonsai_orders || 0), 0);
        const periodAOV = orders > 0 ? totalBonsaiRev / orders : 0;

        const prevBonsaiRev = prevPeriodData.reduce((sum, d) => sum + (d.bonsai_revenue || 0), 0);
        const prevOrders = prevPeriodData.reduce((sum, d) => sum + (d.bonsai_orders || 0), 0);
        const prevAOV = prevOrders > 0 ? prevBonsaiRev / prevOrders : 0;

        document.getElementById('avgOrderValue').textContent = formatCurrency(periodAOV);
        const aovChange = periodAOV - prevAOV;
        updateChangeElement('aovChange', aovChange, 'vs LY');
        document.getElementById('aovPrev').textContent = `LY: ${formatCurrency(prevAOV)}`;
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
        const marginChange = periodMargin - prevMargin;
        updateChangeElement('marginChange', marginChange, 'pp LY');
        document.getElementById('marginPrev').textContent = `LY: ${formatPercent(prevMargin)}`;
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
        const netChange = totalAmazonNet - prevTotalAmazonNet;
        updateChangeElement('netProceedsChange', netChange, 'vs LY');
        document.getElementById('netProceedsPrev').textContent = `LY: ${formatCurrency(prevTotalAmazonNet)}`;
    }

    // Customers
    if (currentChannelFilter === 'amazon') {
        document.getElementById('customers').textContent = 'N/A';
        updateChangeElement('customersChange', 0);
        document.getElementById('customersPrev').textContent = '';

        document.getElementById('newCustomers').textContent = 'N/A';
        updateChangeElement('newCustomersChange', 0);
        document.getElementById('newCustomersPrev').textContent = '';

        document.getElementById('returningCustomers').textContent = 'N/A';
        updateChangeElement('returningCustomersChange', 0);
        document.getElementById('returningCustomersPrev').textContent = '';
    } else {
        const totalBonsaiCustomers = data.reduce((sum, d) => sum + (d.bonsai_customers || 0), 0);
        const prevTotalBonsaiCustomers = prevPeriodData.reduce((sum, d) => sum + (d.bonsai_customers || 0), 0);
        document.getElementById('customers').textContent = formatNumber(totalBonsaiCustomers);
        const custChange = totalBonsaiCustomers - prevTotalBonsaiCustomers;
        updateChangeElement('customersChange', custChange, 'vs LY');
        document.getElementById('customersPrev').textContent = `LY: ${formatNumber(prevTotalBonsaiCustomers)}`;

        const totalNew = data.reduce((sum, d) => sum + (d.bonsai_new_customers || 0), 0);
        const prevNew = prevPeriodData.reduce((sum, d) => sum + (d.bonsai_new_customers || 0), 0);
        document.getElementById('newCustomers').textContent = formatNumber(totalNew);
        updateChangeElement('newCustomersChange', totalNew - prevNew, 'vs LY');
        document.getElementById('newCustomersPrev').textContent = `LY: ${formatNumber(prevNew)}`;

        const totalReturning = data.reduce((sum, d) => sum + (d.bonsai_returning_customers || 0), 0);
        const prevReturning = prevPeriodData.reduce((sum, d) => sum + (d.bonsai_returning_customers || 0), 0);
        document.getElementById('returningCustomers').textContent = formatNumber(totalReturning);
        updateChangeElement('returningCustomersChange', totalReturning - prevReturning, 'vs LY');
        document.getElementById('returningCustomersPrev').textContent = `LY: ${formatNumber(prevReturning)}`;
    }

    // Bonsai Users (from GA4)
    if (currentChannelFilter === 'amazon' || currentChannelFilter === 'wholesale') {
        document.getElementById('bonsaiUsers').textContent = 'N/A';
        updateChangeElement('bonsaiUsersChange', 0);
        document.getElementById('bonsaiUsersPrev').textContent = '';
    } else {
        const totalBonsaiUsers = data.reduce((sum, d) => sum + (d.bonsai_users || 0), 0);
        const prevBonsaiUsers = prevPeriodData.reduce((sum, d) => sum + (d.bonsai_users || 0), 0);
        document.getElementById('bonsaiUsers').textContent = formatNumber(totalBonsaiUsers);
        const userChange = totalBonsaiUsers - prevBonsaiUsers;
        updateChangeElement('bonsaiUsersChange', userChange, 'vs LY');
        document.getElementById('bonsaiUsersPrev').textContent = `LY: ${formatNumber(prevBonsaiUsers)}`;
    }

    // Bonsai Sessions (from GA4)
    if (currentChannelFilter === 'amazon' || currentChannelFilter === 'wholesale') {
        document.getElementById('bonsaiSessions').textContent = 'N/A';
        updateChangeElement('bonsaiSessionsChange', 0);
        document.getElementById('bonsaiSessionsPrev').textContent = '';
    } else {
        const totalBonsaiSessions = data.reduce((sum, d) => sum + (d.bonsai_sessions || 0), 0);
        const prevBonsaiSessions = prevPeriodData.reduce((sum, d) => sum + (d.bonsai_sessions || 0), 0);
        document.getElementById('bonsaiSessions').textContent = formatNumber(totalBonsaiSessions);
        const sessionChange = totalBonsaiSessions - prevBonsaiSessions;
        updateChangeElement('bonsaiSessionsChange', sessionChange, 'vs LY');
        document.getElementById('bonsaiSessionsPrev').textContent = `LY: ${formatNumber(prevBonsaiSessions)}`;
    }

    // Bonsai Conv Rate (from GA4)
    if (currentChannelFilter === 'amazon' || currentChannelFilter === 'wholesale') {
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
        const cvrChange = periodCVR - prevCVR;
        updateChangeElement('bonsaiCvrChange', cvrChange, 'pp LY');
        document.getElementById('bonsaiCvrPrev').textContent = `LY: ${formatPercent(prevCVR)}`;
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

    runUpdate('WoW Trend', updateWowTrendChart, reversedData);
    runUpdate('YoY Trend', updateYoyTrendChart, reversedData);
    runUpdate('YoY Comparison', updateYoyComparisonChart, data);

    if (currentChannelFilter === 'bonsai') {
        runUpdate('Bonsai Customer Breakdown', updateCustomerTypeTrendChart, reversedData);
    } else {
        if (charts.customerTypeTrend) charts.customerTypeTrend.destroy();
    }

    if (currentChannelFilter === 'google_analytics') {
        runUpdate('GA Traffic Trend', updateGATrafficChart, reversedData);
    } else {
        if (charts.gaTrafficTrend) charts.gaTrafficTrend.destroy();
    }
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

// WoW Trend Chart (compares each week to the previous week)
function updateWowTrendChart(data) {
    const ctx = document.getElementById('wowTrendChart');

    if (charts.wowTrend) {
        charts.wowTrend.destroy();
    }

    const wowData = data.map((d) => {
        // Find previous week in the full dashboardData
        const currentIndex = dashboardData.findIndex(w => w.week_start === d.week_start);
        const prevWeek = dashboardData[currentIndex + 1]; // dashboardData is newest to oldest

        if (!prevWeek) return 0;

        const currentRev = (currentChannelFilter === 'amazon') ? (d.amazon_revenue || 0) :
            (currentChannelFilter === 'bonsai') ? (d.bonsai_revenue || 0) :
                (currentChannelFilter === 'wholesale') ? (d.wholesale_revenue || 0) :
                    (d.total_company_revenue || 0);

        const prevRev = (currentChannelFilter === 'amazon') ? (prevWeek.amazon_revenue || 0) :
            (currentChannelFilter === 'bonsai') ? (prevWeek.bonsai_revenue || 0) :
                (currentChannelFilter === 'wholesale') ? (prevWeek.wholesale_revenue || 0) :
                    (prevWeek.total_company_revenue || 0);

        return calculatePercentChange(currentRev, prevRev);
    });

    charts.wowTrend = new Chart(ctx, {
        type: 'line',
        data: {
            labels: data.map(d => formatDate(d.week_start)),
            datasets: [{
                label: 'WoW Growth (%)',
                data: wowData,
                borderColor: '#6366f1',
                backgroundColor: 'rgba(99, 102, 241, 0.1)',
                tension: 0.4,
                fill: true
            }]
        },
        options: getChartOptions('Growth (%)')
    });
}

// YoY Trend Chart (compares each week to the same week last year)
function updateYoyTrendChart(data) {
    const ctx = document.getElementById('yoyTrendChart');

    if (charts.yoyTrend) {
        charts.yoyTrend.destroy();
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
        } else if (currentChannelFilter === 'wholesale') {
            current = d.wholesale_revenue || 0;
            previous = lastYearWeek.wholesale_revenue || 0;
        } else {
            current = d.total_company_revenue || 0;
            previous = lastYearWeek.total_company_revenue || 0;
        }

        return calculatePercentChange(current, previous);
    });

    charts.yoyTrend = new Chart(ctx, {
        type: 'line',
        data: {
            labels: data.map(d => formatDate(d.week_start)),
            datasets: [{
                label: 'YoY Growth (%)',
                data: yoyData,
                borderColor: '#10b981',
                backgroundColor: 'rgba(16, 185, 129, 0.1)',
                tension: 0.4,
                fill: true
            }]
        },
        options: getChartOptions('Growth (%)')
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

// New vs Returning Customer Trend (Bonsai Only)
function updateCustomerTypeTrendChart(data) {
    const ctx = document.getElementById('customerTypeTrendChart');

    if (charts.customerTypeTrend) {
        charts.customerTypeTrend.destroy();
    }

    charts.customerTypeTrend = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: data.map(d => formatDate(d.week_start)),
            datasets: [
                {
                    label: 'New Customers',
                    data: data.map(d => d.bonsai_new_customers || 0),
                    backgroundColor: '#10b981'
                },
                {
                    label: 'Returning Customers',
                    data: data.map(d => d.bonsai_returning_customers || 0),
                    backgroundColor: '#3b82f6'
                }
            ]
        },
        options: {
            ...getChartOptions('Customers'),
            scales: {
                ...getChartOptions('Customers').scales,
                x: { ...getChartOptions('Customers').scales.x, stacked: true },
                y: { ...getChartOptions('Customers').scales.y, stacked: true }
            }
        }
    });
}

// GA Traffic Trend Chart
function updateGATrafficChart(data) {
    const ctx = document.getElementById('gaTrafficTrendChart');

    if (charts.gaTrafficTrend) {
        charts.gaTrafficTrend.destroy();
    }

    charts.gaTrafficTrend = new Chart(ctx, {
        type: 'line',
        data: {
            labels: data.map(d => formatDate(d.week_start)),
            datasets: [
                {
                    label: 'Sessions',
                    data: data.map(d => d.bonsai_sessions || 0),
                    borderColor: '#10b981',
                    backgroundColor: 'rgba(16, 185, 129, 0.1)',
                    tension: 0.4,
                    fill: true
                },
                {
                    label: 'Users',
                    data: data.map(d => d.bonsai_users || 0),
                    borderColor: '#6366f1',
                    backgroundColor: 'rgba(99, 102, 241, 0.1)',
                    tension: 0.4,
                    fill: true
                }
            ]
        },
        options: getChartOptions('Count')
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

// Render Wholesale Customers Table
function renderWholesaleCustomers(customers) {
    const tbody = document.getElementById('wholesaleCustomersTableBody');
    tbody.innerHTML = '';

    if (!customers || customers.length === 0) {
        tbody.innerHTML = '<tr><td colspan="3" style="padding: 1rem; text-align: center;">No data available</td></tr>';
        return;
    }

    customers.forEach(customer => {
        const tr = document.createElement('tr');
        tr.style.borderBottom = '1px solid var(--border-color)';
        tr.style.transition = 'background-color 0.2s';

        // Hover effect helper
        tr.onmouseover = () => tr.style.backgroundColor = 'rgba(55, 65, 81, 0.3)';
        tr.onmouseout = () => tr.style.backgroundColor = 'transparent';

        tr.innerHTML = `
            <td style="padding: 1rem 1.5rem; color: #fff; font-weight: 500;">${customer.company_name || 'N/A'}</td>
            <td style="padding: 1rem 1.5rem;">${customer.customer_name || 'N/A'}</td>
            <td style="padding: 1rem 1.5rem;">${formatNumber(customer.total_orders)}</td>
            <td style="padding: 1rem 1.5rem; text-align: right; color: #10b981; font-weight: 500;">${formatCurrency(customer.total_revenue)}</td>
        `;
        tbody.appendChild(tr);
    });
}
