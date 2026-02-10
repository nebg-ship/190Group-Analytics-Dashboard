// Executive Overview Dashboard JS

const API_URL = 'http://localhost:5000/api/dashboard';
const TOP_SKU_URL = 'http://localhost:5000/api/top-sku';
const NOTES_STORAGE_KEY = 'executive_notes_v1';

let dashboardData = [];
let charts = {};
let currentAlerts = [];
let latestMetrics = null;
let topSkuData = null;
let topSkuRequestKey = null;
let wholesaleCustomersData = [];
const topSkuCache = new Map();

const uiState = {
    dateRange: 'ytd',
    comparisonMode: 'prior-period',
    channel: 'all',
    customerType: 'all',
    category: 'all',
    device: 'all',
    advancedOpen: false,
    panelOpen: false,
    panelTab: 'channel',
    activeMetric: 'net_revenue',
    customRange: {
        start: '',
        end: ''
    }
};

const KPI_CONFIG = {
    total_revenue: { valueId: 'kpiTotalRevenue', metaId: 'kpiTotalRevenueMeta', format: 'currency' },
    net_revenue: { valueId: 'kpiNetRevenue', metaId: 'kpiNetRevenueMeta', format: 'currency' },
    future_revenue: { valueId: 'kpiFutureRevenue', metaId: 'kpiFutureRevenueMeta', format: 'currency' },
    gross_profit: { valueId: 'kpiGrossProfit', metaId: 'kpiGrossProfitMeta', format: 'currency' },
    contribution_margin: { valueId: 'kpiContributionMargin', metaId: 'kpiContributionMarginMeta', format: 'percent', deltaFormat: 'pp' },
    orders: { valueId: 'kpiOrders', metaId: 'kpiOrdersMeta', format: 'number' },
    aov: { valueId: 'kpiAov', metaId: 'kpiAovMeta', format: 'currency' },
    units: { valueId: 'kpiUnits', metaId: 'kpiUnitsMeta', format: 'number' },
    marketing_spend: { valueId: 'kpiMarketingSpend', metaId: 'kpiMarketingSpendMeta', format: 'currency' },
    inventory_cover: { valueId: 'kpiInventoryCover', metaId: 'kpiInventoryCoverMeta', format: 'text', placeholder: 'Not Connected' }
};

document.addEventListener('DOMContentLoaded', () => {
    setupEventListeners();
    loadDashboardData();
    toggleCustomerTypeVisibility(uiState.channel);
});

function setupEventListeners() {
    document.querySelectorAll('.segment-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('.segment-btn').forEach(el => el.classList.remove('active'));
            btn.classList.add('active');
            uiState.dateRange = btn.dataset.range;
            toggleCustomRange(uiState.dateRange === 'custom');
            updateDashboard();
        });
    });

    const comparisonSelect = document.getElementById('comparisonSelect');
    comparisonSelect.addEventListener('change', (event) => {
        uiState.comparisonMode = event.target.value;
        updateDashboard();
    });

    bindSelect('filterChannel', (value) => {
        uiState.channel = value;
        toggleCustomerTypeVisibility(value);
        updateDashboard();
    });
    bindSelect('filterCustomerType', (value) => { uiState.customerType = value; });
    bindSelect('filterCategory', (value) => { uiState.category = value; });
    bindSelect('filterDevice', (value) => { uiState.device = value; });

    const advancedBtn = document.getElementById('advancedBtn');
    const advancedClose = document.getElementById('advancedClose');
    advancedBtn.addEventListener('click', () => toggleDrawer('advancedDrawer', true));
    advancedClose.addEventListener('click', () => toggleDrawer('advancedDrawer', false));

    const alertsBtn = document.getElementById('alertsBtn');
    const alertsClose = document.getElementById('alertsClose');
    alertsBtn.addEventListener('click', () => toggleDrawer('alertsDrawer', true));
    alertsClose.addEventListener('click', () => toggleDrawer('alertsDrawer', false));

    const notesBtn = document.getElementById('notesBtn');
    notesBtn.addEventListener('click', () => openPanel('notes'));

    const panelClose = document.getElementById('panelClose');
    panelClose.addEventListener('click', closePanel);

    const overlay = document.getElementById('overlay');
    overlay.addEventListener('click', () => {
        toggleDrawer('advancedDrawer', false);
        toggleDrawer('alertsDrawer', false);
        closePanel();
    });

    document.querySelectorAll('.kpi-tile').forEach(tile => {
        tile.addEventListener('click', () => {
            const metric = tile.dataset.metric;
            uiState.activeMetric = metric;
            openPanel('channel');
        });
    });

    document.querySelectorAll('.tab-btn').forEach(tab => {
        tab.addEventListener('click', () => {
            switchPanelTab(tab.dataset.tab);
        });
    });

    document.getElementById('addNoteBtn').addEventListener('click', addNote);

    const applyCustomRange = document.getElementById('applyCustomRange');
    applyCustomRange.addEventListener('click', () => {
        uiState.customRange.start = document.getElementById('customStart').value;
        uiState.customRange.end = document.getElementById('customEnd').value;
        updateDashboard();
    });

    document.getElementById('positiveDrivers').addEventListener('click', handleDriverClick);
    document.getElementById('negativeDrivers').addEventListener('click', handleDriverClick);
    document.getElementById('risksList').addEventListener('click', handleAlertClick);
    document.getElementById('alertsList').addEventListener('click', handleAlertClick);

    // SKU Clicks for variations
    document.getElementById('topBonsaiSkusList').addEventListener('click', handleSkuClick);
}

function bindSelect(id, onChange) {
    const el = document.getElementById(id);
    el.addEventListener('change', (event) => onChange(event.target.value));
}

function toggleCustomerTypeVisibility(channel) {
    const customerTypeFilter = document.getElementById('customerTypeFilter');
    if (customerTypeFilter) {
        customerTypeFilter.classList.toggle('hidden', channel !== 'bonsai');
    }
}

function toggleCustomRange(show) {
    const customRange = document.getElementById('customRange');
    customRange.classList.toggle('hidden', !show);
}

function toggleDrawer(id, open) {
    const drawer = document.getElementById(id);
    if (open) {
        drawer.classList.add('open');
    } else {
        drawer.classList.remove('open');
    }
    updateOverlay();
}

function openPanel(tab) {
    uiState.panelOpen = true;
    uiState.panelTab = tab;
    const panel = document.getElementById('explainPanel');
    panel.classList.add('open');
    switchPanelTab(tab);
    if (latestMetrics) {
        renderPanelBreakdown(latestMetrics);
    }
    updateOverlay();
}

function closePanel() {
    uiState.panelOpen = false;
    const panel = document.getElementById('explainPanel');
    panel.classList.remove('open');
    updateOverlay();
}

function updateOverlay() {
    const overlay = document.getElementById('overlay');
    const panelOpen = document.getElementById('explainPanel').classList.contains('open');
    const drawerOpen = document.getElementById('advancedDrawer').classList.contains('open')
        || document.getElementById('alertsDrawer').classList.contains('open');
    overlay.classList.toggle('hidden', !(panelOpen || drawerOpen));
}

function switchPanelTab(tab) {
    uiState.panelTab = tab;
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.tab === tab);
    });
    document.querySelectorAll('.panel-tab').forEach(panelTab => {
        panelTab.classList.toggle('active', panelTab.id === `panelTab-${tab}`);
    });
    if (tab === 'notes') {
        renderNotes();
    }
    if (tab === 'variations') {
        // Handled by handleSkuClick, but can re-render if we have active product
    }
}

async function loadDashboardData() {
    const loadingOverlay = document.getElementById('loadingOverlay');
    const errorMessage = document.getElementById('errorMessage');

    try {
        loadingOverlay.style.display = 'flex';
        errorMessage.style.display = 'none';

        const response = await fetch(API_URL);
        const result = await response.json();

        if (result.success && Array.isArray(result.data) && result.data.length > 0) {
            dashboardData = result.data;
            wholesaleCustomersData = result.wholesale_customers || [];
            updateLastUpdated(result.timestamp);
            updateDashboard();
            loadingOverlay.style.display = 'none';
        } else {
            throw new Error(result.error || 'No data returned from API');
        }
    } catch (error) {
        console.error('Error loading dashboard data:', error);
        errorMessage.style.display = 'block';
        errorMessage.querySelector('p').textContent = `Failed to load dashboard data: ${error.message}`;
        loadingOverlay.style.display = 'none';
    }
}

function updateLastUpdated(timestamp) {
    const lastUpdated = document.getElementById('lastUpdated');
    const date = new Date(timestamp);
    lastUpdated.textContent = `Data last refreshed: ${date.toLocaleString()}`;
}

function updateDashboard() {
    if (!dashboardData.length) return;

    const filteredData = filterDataByRange(dashboardData, uiState);
    const comparisonData = getComparisonData(filteredData, dashboardData, uiState);
    const metrics = computeMetrics(filteredData, comparisonData, uiState);
    latestMetrics = metrics;

    renderKpis(metrics, uiState);
    renderChannelMixChart(filteredData, uiState);
    renderWaterfallChart(metrics);
    renderDecompositionChart(metrics);
    renderChannelEfficiencyTable(metrics);
    renderDrivers(metrics, topSkuData);
    updateTopSkuData(filteredData, comparisonData);
    updateTopSkusChannelData(filteredData);
    renderAlerts(metrics);
    renderPanelBreakdown(metrics);
    renderWholesaleCustomers(wholesaleCustomersData);
}

function updateTopSkusChannelData(filteredData) {
    const range = getWeekRange(filteredData);
    if (!range) return;

    fetchTopSkusChannel(range);
}

async function fetchTopSkusChannel(range) {
    const params = new URLSearchParams({
        start: range.start,
        end: range.end
    });

    try {
        const response = await fetch(`http://localhost:5000/api/top-skus-channel?${params.toString()}`);
        const result = await response.json();
        if (result.success) {
            renderTopSkusByChannel(result.data);
        }
    } catch (error) {
        console.error('Error fetching top SKUs by channel:', error);
    }
}

function renderTopSkusByChannel(data) {
    renderSkuList('topAmazonSkusList', data.amazon);
    renderSkuList('topBonsaiSkusList', data.bonsai);
}

function renderSkuList(containerId, items) {
    const container = document.getElementById(containerId);
    if (!container) return;

    if (!items || items.length === 0) {
        container.innerHTML = '<div class="panel-placeholder">No data for this period</div>';
        return;
    }

    container.innerHTML = items.map((item, index) => {
        const primaryText = item.product_name || item.sku;
        const secondaryText = item.sku !== primaryText ? item.sku : '';
        const revenue = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 }).format(item.revenue);
        const units = new Intl.NumberFormat('en-US').format(item.units);

        return `
            <div class="sku-item ${containerId === 'topBonsaiSkusList' ? 'clickable' : ''}" 
                 ${item.product_id ? `data-product-id="${item.product_id}"` : ''}
                 ${item.product_name ? `data-product-name="${item.product_name}"` : ''}>
                <div class="sku-rank">#${index + 1}</div>
                <div class="sku-info">
                    <div class="sku-name">${primaryText} ${containerId === 'topBonsaiSkusList' ? '<span class="variation-hint">(Click for variations)</span>' : ''}</div>
                    ${secondaryText ? `<div class="sku-code">${secondaryText}</div>` : ''}
                </div>
                <div class="sku-stats">
                    <div class="sku-rev">${revenue}</div>
                    <div class="sku-units">${units} units</div>
                </div>
            </div>
        `;
    }).join('');
}

function renderWholesaleCustomers(customers) {
    const tbody = document.getElementById('wholesaleCustomersBody');
    if (!tbody) return;

    if (!customers || customers.length === 0) {
        tbody.innerHTML = '<tr><td colspan="5" class="panel-placeholder">No wholesale data for this period</td></tr>';
        return;
    }

    tbody.innerHTML = customers.map(c => {
        const confirmed = c.total_revenue || 0;
        const future = c.future_revenue || 0;
        const total = confirmed + future;

        return `
            <tr>
                <td><strong>${c.company_name || 'Individual'}</strong><br><small>${c.customer_name}</small></td>
                <td>${c.total_orders}</td>
                <td class="positive">${formatCurrency(confirmed)}</td>
                <td class="info">${formatCurrency(future)}</td>
                <td style="font-weight: bold">${formatCurrency(total)}</td>
            </tr>
        `;
    }).join('');
}

function updateTopSkuData(filteredData, comparisonData) {
    const currentRange = getWeekRange(filteredData);
    if (!currentRange) return;
    const comparisonRange = getWeekRange(comparisonData);

    const requestKey = [
        currentRange.start,
        currentRange.end,
        comparisonRange ? comparisonRange.start : '',
        comparisonRange ? comparisonRange.end : ''
    ].join('|');

    if (topSkuCache.has(requestKey)) {
        topSkuData = topSkuCache.get(requestKey);
        renderDrivers(latestMetrics, topSkuData);
        return;
    }

    if (topSkuRequestKey === requestKey) return;
    topSkuRequestKey = requestKey;
    topSkuData = null;
    renderDrivers(latestMetrics, topSkuData);

    fetchTopSku(currentRange, comparisonRange, requestKey);
}

async function fetchTopSku(currentRange, comparisonRange, requestKey) {
    const params = new URLSearchParams({
        start: currentRange.start,
        end: currentRange.end
    });
    if (comparisonRange && comparisonRange.start && comparisonRange.end) {
        params.set('compare_start', comparisonRange.start);
        params.set('compare_end', comparisonRange.end);
    }

    try {
        const response = await fetch(`${TOP_SKU_URL}?${params.toString()}`);
        const result = await response.json();
        if (!result.success) {
            throw new Error(result.error || 'Failed to fetch top SKU');
        }
        if (topSkuRequestKey !== requestKey) return;
        topSkuData = result.top_sku || null;
        topSkuCache.set(requestKey, topSkuData);
        renderDrivers(latestMetrics, topSkuData);
    } catch (error) {
        console.error('Error loading top SKU:', error);
        if (topSkuRequestKey === requestKey) {
            topSkuData = null;
            renderDrivers(latestMetrics, topSkuData);
        }
    }
}

function getWeekRange(data) {
    if (!data || !data.length) return null;
    const sorted = sortByDateDesc(data);
    const latest = sorted[0];
    const earliest = sorted[sorted.length - 1];
    const start = earliest.week_start;
    const end = addDays(latest.week_start, 6);
    return { start, end };
}

function addDays(dateString, days) {
    const date = new Date(`${dateString}T00:00:00Z`);
    date.setUTCDate(date.getUTCDate() + days);
    return date.toISOString().slice(0, 10);
}

function filterDataByRange(data, state) {
    const sorted = sortByDateDesc(data);
    const now = new Date();
    const currentYear = now.getFullYear();
    const currentMonth = now.getMonth();

    switch (state.dateRange) {
        case 'today':
            return sorted.slice(0, 1);
        case 'last7':
            return sorted.slice(0, 1);
        case 'last30':
            return sorted.slice(0, 4);
        case 'mtd':
            return sorted.filter(item => {
                const date = new Date(item.week_start + 'T00:00:00Z');
                return date.getUTCFullYear() === currentYear && date.getUTCMonth() === currentMonth;
            });
        case 'qtd': {
            const currentQuarter = Math.floor(currentMonth / 3);
            return sorted.filter(item => {
                const date = new Date(item.week_start + 'T00:00:00Z');
                return date.getUTCFullYear() === currentYear && Math.floor(date.getUTCMonth() / 3) === currentQuarter;
            });
        }
        case 'custom': {
            if (!state.customRange.start || !state.customRange.end) return sorted.slice(0, 4);
            const start = new Date(state.customRange.start);
            const end = new Date(state.customRange.end);
            return sorted.filter(item => {
                const date = new Date(item.week_start + 'T00:00:00Z');
                return date >= start && date <= end;
            });
        }
        case 'ytd':
        default:
            return sorted.filter(item => Number(item.year) === currentYear);
    }
}

function getComparisonData(selectedData, allData, state) {
    if (!selectedData.length) return [];

    if (state.comparisonMode === 'prior-year') {
        return selectedData.map(item => findPriorYearWeek(allData, item.week_start)).filter(Boolean);
    }

    const sorted = sortByDateDesc(allData);
    const oldestSelected = selectedData[selectedData.length - 1];
    const oldestIndex = sorted.findIndex(item => item.week_start === oldestSelected.week_start);
    if (oldestIndex === -1) return [];
    return sorted.slice(oldestIndex + 1, oldestIndex + 1 + selectedData.length);
}

function findPriorYearWeek(data, weekStart) {
    const currentWeekDate = new Date(weekStart + 'T00:00:00Z');
    const targetDate = new Date(currentWeekDate);
    targetDate.setUTCDate(targetDate.getUTCDate() - 364);
    return data.find(item => {
        const itemDate = new Date(item.week_start + 'T00:00:00Z');
        return Math.abs(itemDate - targetDate) <= 3 * 24 * 60 * 60 * 1000;
    });
}

function sortByDateDesc(data) {
    return [...data].sort((a, b) => new Date(b.week_start) - new Date(a.week_start));
}

function computeMetrics(currentData, previousData, state) {
    const currentTotals = computeTotals(currentData);
    const previousTotals = computeTotals(previousData);
    const selected = computeSelectedMetrics(currentTotals, state.channel);
    const selectedPrev = computeSelectedMetrics(previousTotals, state.channel);

    return {
        currentTotals,
        previousTotals,
        selected,
        selectedPrev,
        comparisonLabel: state.comparisonMode === 'prior-year' ? 'vs prior year' : 'vs prior period'
    };
}

function computeTotals(data) {
    const bonsaiRevenue = sum(data, 'bonsai_revenue');
    const amazonRevenue = sum(data, 'amazon_revenue');
    const wholesaleRevenue = sum(data, 'wholesale_revenue');
    const amazonNet = sum(data, 'amazon_net_proceeds');

    const bonsaiOrders = sum(data, 'bonsai_orders');
    const amazonOrders = sum(data, 'amazon_orders');
    const wholesaleOrders = sum(data, 'wholesale_orders');
    const totalOrders = bonsaiOrders + amazonOrders + wholesaleOrders;

    const totalRevenue = sum(data, 'total_company_revenue');
    const estimatedProfit = sum(data, 'estimated_company_profit');

    const bonsaiSessions = sum(data, 'bonsai_sessions');
    const amazonSessions = sum(data, 'amazon_sessions');
    const totalAdSpend = sum(data, 'total_ad_spend');

    return {
        bonsaiRevenue,
        amazonRevenue,
        wholesaleRevenue,
        amazonNet,
        bonsaiOrders,
        amazonOrders,
        wholesaleOrders,
        totalOrders,
        totalRevenue,
        estimatedProfit,
        amazonUnits: sum(data, 'amazon_units'),
        bonsaiSessions,
        amazonSessions,
        organicSessions: sum(data, 'organic_sessions'),
        organicUsers: sum(data, 'organic_users'),
        organicRevenue: sum(data, 'organic_revenue'),
        totalAdSpend,
        amazonAdSpend: sum(data, 'amazon_ad_spend'),
        googleAdSpend: sum(data, 'google_ad_spend'),
        wholesaleFutureRevenue: sum(data, 'wholesale_future_revenue'),
        mer: totalAdSpend > 0 ? totalRevenue / totalAdSpend : 0,
        bonsaiCvr: bonsaiSessions > 0 ? (bonsaiOrders / bonsaiSessions) * 100 : 0,
        amazonCvr: amazonSessions > 0 ? (amazonOrders / amazonSessions) * 100 : 0,
        aov: totalOrders > 0 ? totalRevenue / totalOrders : 0,
        contributionMargin: totalRevenue > 0 ? (estimatedProfit / totalRevenue) * 100 : 0,
        channels: {
            bonsai: {
                name: 'Online Storefront',
                revenue: bonsaiRevenue,
                orders: bonsaiOrders,
                profit: null,
                gmPct: null
            },
            amazon: {
                name: 'Amazon',
                revenue: amazonRevenue,
                orders: amazonOrders,
                profit: amazonNet,
                gmPct: amazonRevenue > 0 ? (amazonNet / amazonRevenue) * 100 : null
            },
            wholesale: {
                name: 'Wholesale',
                revenue: wholesaleRevenue,
                orders: wholesaleOrders,
                futureRevenue: sum(data, 'wholesale_future_revenue'),
                profit: null,
                gmPct: null
            }
        }
    };
}

function computeSelectedMetrics(totals, channel) {
    switch (channel) {
        case 'amazon':
            return {
                total_revenue: null,
                net_revenue: totals.amazonRevenue,
                gross_profit: totals.amazonNet,
                contribution_margin: totals.amazonRevenue > 0 ? (totals.amazonNet / totals.amazonRevenue) * 100 : null,
                orders: totals.amazonOrders,
                aov: totals.amazonOrders > 0 ? totals.amazonRevenue / totals.amazonOrders : 0,
                units: totals.amazonUnits,
                marketing_spend: totals.totalAdSpend,
                mer: totals.mer,
                future_revenue: null
            };
        case 'bonsai':
            return {
                total_revenue: null,
                net_revenue: totals.bonsaiRevenue,
                gross_profit: null,
                contribution_margin: null,
                orders: totals.bonsaiOrders,
                aov: totals.bonsaiOrders > 0 ? totals.bonsaiRevenue / totals.bonsaiOrders : 0,
                units: null,
                future_revenue: null
            };
        case 'wholesale':
            return {
                total_revenue: (totals.wholesaleRevenue || 0) + (totals.wholesaleFutureRevenue || 0),
                net_revenue: totals.wholesaleRevenue,
                gross_profit: null,
                contribution_margin: null,
                orders: totals.wholesaleOrders,
                aov: totals.wholesaleOrders > 0 ? totals.wholesaleRevenue / totals.wholesaleOrders : 0,
                units: null,
                future_revenue: totals.wholesaleFutureRevenue
            };
        case 'retail':
            return {
                net_revenue: null,
                gross_profit: null,
                contribution_margin: null,
                orders: null,
                aov: null,
                units: null
            };
        case 'all':
        default:
            return {
                total_revenue: null,
                net_revenue: totals.totalRevenue,
                gross_profit: totals.estimatedProfit,
                contribution_margin: totals.contributionMargin,
                orders: totals.totalOrders,
                aov: totals.aov,
                units: totals.amazonUnits,
                marketing_spend: totals.totalAdSpend,
                mer: totals.mer,
                future_revenue: null
            };
    }
}

function renderKpis(metrics, state) {
    Object.keys(KPI_CONFIG).forEach(key => {
        const config = KPI_CONFIG[key];
        const valueEl = document.getElementById(config.valueId);
        const metaEl = document.getElementById(config.metaId);
        const currentValue = metrics.selected[key];
        const previousValue = metrics.selectedPrev[key];

        const tile = valueEl.closest('.kpi-tile');
        if (currentValue === null || currentValue === undefined) {
            tile.classList.add('hidden');
            valueEl.textContent = 'N/A';
            metaEl.textContent = '--';
            metaEl.className = 'kpi-meta';
            return;
        }

        tile.classList.remove('hidden');

        valueEl.textContent = formatValue(currentValue, config.format);

        if (key === 'marketing_spend') {
            const mer = metrics.selected.mer;
            metaEl.textContent = `${mer.toFixed(1)}x MER`;
            metaEl.className = 'kpi-meta info'; // Use a neutral color for MER
            return;
        }

        const delta = (currentValue || 0) - (previousValue || 0);
        const deltaText = formatDelta(delta, config);
        metaEl.textContent = `${deltaText} ${metrics.comparisonLabel}`;
        metaEl.className = `kpi-meta ${delta > 0 ? 'positive' : delta < 0 ? 'negative' : ''}`;
    });
}

function renderChannelMixChart(data, state) {
    const ctx = document.getElementById('channelMixTrendChart');
    if (!ctx) return;
    if (charts.channelMix) charts.channelMix.destroy();

    const labels = [...data].reverse().map(item => formatDate(item.week_start));
    const datasets = [];

    if (state.channel === 'all') {
        datasets.push(makeLineDataset('Online Storefront', data, 'bonsai_revenue', '#22c55e', 0));
        datasets.push(makeLineDataset('Amazon', data, 'amazon_revenue', '#f59e0b', 1));
        datasets.push(makeLineDataset('Wholesale', data, 'wholesale_revenue', '#8b5cf6', 2));
    } else {
        const mapping = {
            bonsai: { label: 'Online Storefront', key: 'bonsai_revenue', color: '#22c55e' },
            amazon: { label: 'Amazon', key: 'amazon_revenue', color: '#f59e0b' },
            wholesale: { label: 'Wholesale', key: 'wholesale_revenue', color: '#8b5cf6' }
        };
        const config = mapping[state.channel];
        if (config) {
            datasets.push(makeLineDataset(config.label, data, config.key, config.color, 0));
        }
    }

    charts.channelMix = new Chart(ctx, {
        type: 'line',
        data: {
            labels,
            datasets
        },
        options: getChartOptions('Revenue ($)', true)
    });
}

function makeLineDataset(label, data, key, color, stack) {
    return {
        label,
        data: [...data].reverse().map(item => item[key] || 0),
        borderColor: color,
        backgroundColor: hexToRgba(color, 0.2),
        tension: 0.35,
        fill: true,
        stack
    };
}

function renderWaterfallChart(metrics) {
    const ctx = document.getElementById('revenueWaterfallChart');
    if (!ctx) return;
    if (charts.waterfall) charts.waterfall.destroy();

    const current = metrics.selected;
    const previous = metrics.selectedPrev;

    const ordersDelta = (current.orders || 0) - (previous.orders || 0);
    const prevAov = previous.orders ? (previous.net_revenue || 0) / previous.orders : 0;
    const currentAov = current.orders ? (current.net_revenue || 0) / current.orders : 0;
    const volumeEffect = ordersDelta * prevAov;
    const priceEffect = (currentAov - prevAov) * (previous.orders || 0);
    const totalDelta = (current.net_revenue || 0) - (previous.net_revenue || 0);
    const mixEffect = totalDelta - volumeEffect - priceEffect;

    const values = [volumeEffect, priceEffect, mixEffect, totalDelta];
    const labels = ['Volume', 'Price/AOV', 'Mix', 'Total Change'];
    const colors = values.map(value => value >= 0 ? '#22c55e' : '#ef4444');
    colors[3] = '#22d3ee';

    charts.waterfall = new Chart(ctx, {
        type: 'bar',
        data: {
            labels,
            datasets: [{
                label: 'Revenue Change',
                data: values,
                backgroundColor: colors
            }]
        },
        options: getChartOptions('Change in Revenue ($)', false)
    });
}

function renderDecompositionChart(metrics) {
    const ctx = document.getElementById('decompositionChart');
    if (!ctx) return;
    if (charts.decomposition) charts.decomposition.destroy();

    const revenueDelta = (metrics.selected.net_revenue || 0) - (metrics.selectedPrev.net_revenue || 0);
    const profitDelta = (metrics.selected.gross_profit || 0) - (metrics.selectedPrev.gross_profit || 0);

    charts.decomposition = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: ['Delta Revenue', 'Delta Gross Profit'],
            datasets: [{
                label: 'Change',
                data: [revenueDelta, profitDelta],
                backgroundColor: ['#f59e0b', '#8b5cf6']
            }]
        },
        options: getChartOptions('Change ($)', false)
    });
}

function renderChannelEfficiencyTable(metrics) {
    const tbody = document.getElementById('channelEfficiencyBody');
    tbody.innerHTML = '';

    const channels = Object.values(metrics.currentTotals.channels);
    channels.forEach(channel => {
        const key = channel.name === 'Online Storefront' ? 'bonsai' : channel.name.toLowerCase();
        const prevChannel = metrics.previousTotals.channels[key];
        const revenueChange = channel.revenue - (prevChannel ? prevChannel.revenue : 0);
        const orderShare = metrics.currentTotals.totalOrders > 0
            ? (channel.orders / metrics.currentTotals.totalOrders) * 100
            : 0;

        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${channel.name}</td>
            <td>${formatCurrency(channel.revenue)}</td>
            <td>${channel.gmPct === null ? 'N/A' : formatPercent(channel.gmPct)}</td>
            <td>${formatPercent(orderShare)}</td>
            <td>${formatSignedCurrency(revenueChange)}</td>
            <td>${channel.name === 'Amazon' ? formatCurrency(metrics.currentTotals.amazonAdSpend || 0) : (channel.name === 'Online Storefront' ? formatCurrency(metrics.currentTotals.googleAdSpend || 0) : 'N/A')}</td>
            <td>${channel.name === 'Amazon' ? (metrics.currentTotals.amazonAdSpend > 0 ? (channel.revenue / metrics.currentTotals.amazonAdSpend).toFixed(1) + 'x' : '0x') : (channel.name === 'Online Storefront' ? (metrics.currentTotals.googleAdSpend > 0 ? (channel.revenue / metrics.currentTotals.googleAdSpend).toFixed(1) + 'x' : '0x') : (channel.name === 'Wholesale' ? '+' + formatCurrency(channel.futureRevenue || 0) + ' Pending' : 'N/A'))}</td>
        `;
        tbody.appendChild(row);
    });
}

function renderDrivers(metrics, topSku) {
    const hasTopSku = topSku && topSku.sku;
    const topSkuDriver = hasTopSku
        ? makeTopSkuDriver(topSku)
        : makePlaceholderDriver(topSku ? 'Top SKU (No data)' : 'Top SKU');

    const drivers = [
        makeDriver('Amazon', metrics.currentTotals.channels.amazon.revenue, metrics.previousTotals.channels.amazon.revenue),
        makeDriver('Online Storefront', metrics.currentTotals.channels.bonsai.revenue, metrics.previousTotals.channels.bonsai.revenue),
        makeDriver('Wholesale', metrics.currentTotals.channels.wholesale.revenue, metrics.previousTotals.channels.wholesale.revenue),
        topSkuDriver,
        makePlaceholderDriver('Top Category')
    ];

    const positive = drivers.filter(d => d.delta >= 0).sort((a, b) => b.delta - a.delta).slice(0, 5);
    const negative = drivers.filter(d => d.delta < 0).sort((a, b) => a.delta - b.delta).slice(0, 5);

    fillDriverSlots(positive, 'No additional positive drivers');
    fillDriverSlots(negative, 'No additional negative drivers');

    renderDriverList('positiveDrivers', positive);
    renderDriverList('negativeDrivers', negative);
}

function makeDriver(label, current, previous) {
    return {
        label,
        delta: current - (previous || 0),
        metric: 'net_revenue'
    };
}

function makeTopSkuDriver(topSku) {
    const labelValue = topSku.name || topSku.sku;
    return {
        label: `Top SKU: ${labelValue}`,
        delta: (topSku.current_revenue || 0) - (topSku.previous_revenue || 0),
        metric: 'net_revenue'
    };
}

function makePlaceholderDriver(label, valueLabel = 'Not Connected') {
    return {
        label,
        delta: 0,
        metric: 'net_revenue',
        placeholder: true,
        placeholderValue: valueLabel
    };
}

function fillDriverSlots(list, label) {
    while (list.length < 5) {
        list.push(makePlaceholderDriver(label, '--'));
    }
}

function renderDriverList(id, drivers) {
    const container = document.getElementById(id);
    container.innerHTML = '';

    drivers.forEach(driver => {
        const item = document.createElement('li');
        item.className = 'driver-item';
        item.dataset.metric = driver.metric;
        item.innerHTML = `
            <div>${driver.label}</div>
            <div class="driver-value">${driver.placeholder ? driver.placeholderValue : formatCurrency(driver.delta)}</div>
        `;
        container.appendChild(item);
    });
}

function renderAlerts(metrics) {
    currentAlerts = [];

    const amazonDrop = metrics.currentTotals.amazonCvr - metrics.previousTotals.amazonCvr;
    if (amazonDrop <= -2) {
        currentAlerts.push({
            title: 'Amazon CVR down 2pp+ (vs comparison)',
            detail: `Conversion rate fell by ${formatPpDelta(amazonDrop)} to ${formatPercent(metrics.currentTotals.amazonCvr)}.`,
            severity: 'critical',
            metric: 'units'
        });
    }

    const bonsaiDrop = metrics.currentTotals.bonsaiCvr - metrics.previousTotals.bonsaiCvr;
    if (bonsaiDrop <= -1.5) {
        currentAlerts.push({
            title: 'Storefront CVR down 1.5pp+ (vs comparison)',
            detail: `Conversion rate fell by ${formatPpDelta(bonsaiDrop)} to ${formatPercent(metrics.currentTotals.bonsaiCvr)}.`,
            severity: 'warning',
            metric: 'orders'
        });
    }

    const wowAlerts = computeWowCvrAlerts();
    wowAlerts.forEach(alert => currentAlerts.push(alert));

    currentAlerts.push({
        title: 'Stockouts on top SKUs',
        detail: 'Inventory signals are not yet connected.',
        severity: 'info',
        metric: null
    });

    currentAlerts.push({
        title: 'Ads CPA above target',
        detail: 'Marketing spend data is not yet connected.',
        severity: 'info',
        metric: null
    });

    const count = currentAlerts.filter(alert => alert.severity !== 'info').length;
    document.getElementById('alertsCount').textContent = count.toString();

    renderAlertList('risksList', currentAlerts);
    renderAlertList('alertsList', currentAlerts);
}

function computeWowCvrAlerts() {
    const alerts = [];
    const sorted = sortByDateDesc(dashboardData);
    if (sorted.length < 2) return alerts;

    const latest = sorted[0];
    const previous = sorted[1];

    const latestAmazonCvr = latest.amazon_sessions ? (latest.amazon_orders / latest.amazon_sessions) * 100 : 0;
    const prevAmazonCvr = previous.amazon_sessions ? (previous.amazon_orders / previous.amazon_sessions) * 100 : 0;
    const amazonDrop = latestAmazonCvr - prevAmazonCvr;

    if (amazonDrop <= -2) {
        alerts.push({
            title: 'Amazon CVR down 2pp+ (WoW)',
            detail: `Latest week CVR is ${formatPercent(latestAmazonCvr)}.`,
            severity: 'critical',
            metric: 'units'
        });
    }

    const latestBonsaiCvr = latest.bonsai_sessions ? (latest.bonsai_orders / latest.bonsai_sessions) * 100 : 0;
    const prevBonsaiCvr = previous.bonsai_sessions ? (previous.bonsai_orders / previous.bonsai_sessions) * 100 : 0;
    const bonsaiDrop = latestBonsaiCvr - prevBonsaiCvr;

    if (bonsaiDrop <= -1.5) {
        alerts.push({
            title: 'Storefront CVR down 1.5pp+ (WoW)',
            detail: `Latest week CVR is ${formatPercent(latestBonsaiCvr)}.`,
            severity: 'warning',
            metric: 'orders'
        });
    }

    return alerts;
}

function renderAlertList(id, alerts) {
    const list = document.getElementById(id);
    list.innerHTML = '';

    alerts.forEach(alert => {
        const item = document.createElement('li');
        item.className = `alert-item ${alert.severity}`;
        if (alert.metric) {
            item.dataset.metric = alert.metric;
        }
        item.innerHTML = `
            <div class="alert-title">${alert.title}</div>
            <div class="alert-detail">${alert.detail}</div>
        `;
        list.appendChild(item);
    });
}

function renderMarketingBreakdown(metrics) {
    const list = document.getElementById('panelMarketingBreakdown');
    list.innerHTML = '';

    const marketingEntries = [
        { name: 'Organic', key: 'organic' },
        { name: 'Paid Search (Amazon)', key: 'amazon_ad' }
    ];

    marketingEntries.forEach(entry => {
        const item = document.createElement('li');
        item.className = 'panel-item';

        let revenue, detail;
        if (entry.key === 'organic') {
            revenue = metrics.currentTotals.organicRevenue || 0;
            detail = `Sessions: ${formatNumber(metrics.currentTotals.organicSessions || 0)}`;
        } else {
            // Amazon ad spend is current available in metrics
            revenue = metrics.currentTotals.amazonAdSpend || 0;
            detail = 'Attributed from Amazon SP-API';
        }

        item.innerHTML = `
            <div>
                <div>${entry.name}</div>
                <div class="panel-meta">${detail}</div>
            </div>
            <div class="driver-value">${entry.key === 'amazon_ad' ? 'Spend: ' : ''}${formatCurrency(revenue)}</div>
        `;
        list.appendChild(item);
    });
}

function renderPanelBreakdown(metrics) {
    const title = document.getElementById('panelMetricTitle');
    title.textContent = metricLabel(uiState.activeMetric);

    const breakdownList = document.getElementById('panelChannelBreakdown');
    breakdownList.innerHTML = '';

    const channelEntries = [
        { key: 'bonsai', name: 'Online Storefront' },
        { key: 'amazon', name: 'Amazon' },
        { key: 'wholesale', name: 'Wholesale' }
    ];

    channelEntries.forEach(entry => {
        const value = getChannelMetricValue(entry.key, metrics, uiState.activeMetric);
        const item = document.createElement('li');
        item.className = 'panel-item';
        item.innerHTML = `
            <div>${entry.name}</div>
            <div class="driver-value">${formatPanelValue(value, uiState.activeMetric)}</div>
        `;
        breakdownList.appendChild(item);
    });
}

function getChannelMetricValue(channelKey, metrics, metric) {
    const channel = metrics.currentTotals.channels[channelKey];
    if (!channel) return null;
    switch (metric) {
        case 'net_revenue':
            return channel.revenue;
        case 'gross_profit':
            return channel.profit;
        case 'contribution_margin':
            return channel.gmPct;
        case 'orders':
            return channel.orders;
        case 'aov':
            return channel.orders ? channel.revenue / channel.orders : 0;
        case 'units':
            return channelKey === 'amazon' ? metrics.currentTotals.amazonUnits : null;
        default:
            return null;
    }
}

function formatPanelValue(value, metric) {
    if (value === null || value === undefined) return 'N/A';
    if (metric === 'contribution_margin') {
        return formatPercent(value);
    }
    if (metric === 'orders' || metric === 'units') {
        return formatNumber(value);
    }
    if (metric === 'aov' || metric === 'net_revenue' || metric === 'gross_profit') {
        return formatCurrency(value);
    }
    return formatValue(value, 'text');
}

function renderNotes() {
    const notesList = document.getElementById('notesList');
    const notes = loadNotes();
    notesList.innerHTML = '';

    if (!notes.length) {
        const item = document.createElement('li');
        item.className = 'panel-item note-item';
        item.textContent = 'No notes yet for this range.';
        notesList.appendChild(item);
        return;
    }

    notes.forEach(note => {
        const item = document.createElement('li');
        item.className = 'panel-item note-item';
        item.innerHTML = `
            <div><strong>${note.rangeLabel}</strong></div>
            <div>${note.text}</div>
            <div class="panel-meta">${formatDateTime(note.createdAt)}</div>
        `;
        notesList.appendChild(item);
    });
}

function addNote() {
    const input = document.getElementById('noteInput');
    const text = input.value.trim();
    if (!text) return;

    const rangeLabel = getDateRangeLabel(filterDataByRange(dashboardData, uiState));
    const notes = loadNotes();
    notes.unshift({
        id: Date.now(),
        text,
        rangeLabel,
        createdAt: new Date().toISOString()
    });
    saveNotes(notes);
    input.value = '';
    renderNotes();
}

function loadNotes() {
    try {
        const stored = localStorage.getItem(NOTES_STORAGE_KEY);
        return stored ? JSON.parse(stored) : [];
    } catch (error) {
        console.warn('Failed to load notes', error);
        return [];
    }
}

function saveNotes(notes) {
    localStorage.setItem(NOTES_STORAGE_KEY, JSON.stringify(notes));
}

function handleDriverClick(event) {
    const item = event.target.closest('.driver-item');
    if (!item) return;
    uiState.activeMetric = item.dataset.metric;
    openPanel('channel');
}

async function handleSkuClick(event) {
    const item = event.target.closest('.sku-item.clickable');
    if (!item) return;

    const productId = item.dataset.productId;
    const productName = item.dataset.productName;
    if (!productId) return;

    openPanel('variations');
    const title = document.getElementById('panelMetricTitle');
    title.textContent = productName || 'Product Variations';

    const list = document.getElementById('panelVariationBreakdown');
    list.innerHTML = '<div class="panel-placeholder">Loading variation breakdown...</div>';

    const range = getWeekRange(filterDataByRange(dashboardData, uiState));
    if (!range) return;

    const params = new URLSearchParams({
        product_id: productId,
        start: range.start,
        end: range.end
    });

    try {
        const response = await fetch(`http://localhost:5000/api/sku-variations?${params.toString()}`);
        const result = await response.json();
        if (result.success) {
            renderVariationBreakdown(result.data);
        } else {
            list.innerHTML = `<div class="panel-placeholder">Error: ${result.error}</div>`;
        }
    } catch (error) {
        console.error('Error fetching variations:', error);
        list.innerHTML = '<div class="panel-placeholder">Failed to load variations</div>';
    }
}

function renderVariationBreakdown(variations) {
    const list = document.getElementById('panelVariationBreakdown');
    if (!variations || variations.length === 0) {
        list.innerHTML = '<div class="panel-placeholder">No variations found</div>';
        return;
    }

    list.innerHTML = variations.map(v => `
        <li class="panel-item">
            <div>
                <div>${v.sku}</div>
                <div class="panel-meta">${v.units} units</div>
            </div>
            <div class="driver-value">${formatCurrency(v.revenue)}</div>
        </li>
    `).join('');
}

function handleAlertClick(event) {
    const item = event.target.closest('.alert-item');
    if (!item) return;
    if (item.dataset.metric) {
        uiState.activeMetric = item.dataset.metric;
        openPanel('channel');
    }
}

function metricLabel(metric) {
    switch (metric) {
        case 'gross_profit':
            return 'Estimated Gross Profit';
        case 'contribution_margin':
            return 'Contribution Margin %';
        case 'orders':
            return 'Orders';
        case 'aov':
            return 'Average Order Value';
        case 'units':
            return 'Units (Amazon)';
        case 'marketing_spend':
            return 'Marketing Spend / MER';
        case 'inventory_cover':
            return 'Inventory Cover';
        case 'net_revenue':
        default:
            return 'Net Revenue';
    }
}

function getDateRangeLabel(data) {
    if (!data.length) return 'No data';
    const dates = data.map(item => new Date(item.week_start + 'T00:00:00Z')).sort((a, b) => a - b);
    const start = dates[0];
    const end = dates[dates.length - 1];
    return `${start.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })} - ${end.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })}`;
}

function sum(data, key) {
    return data.reduce((total, item) => total + (Number(item[key]) || 0), 0);
}

function formatValue(value, format) {
    switch (format) {
        case 'currency':
            return formatCurrency(value);
        case 'percent':
            return formatPercent(value);
        case 'number':
            return formatNumber(value);
        case 'text':
        default:
            return value;
    }
}

function formatDelta(delta, config) {
    if (config.deltaFormat === 'pp') {
        const sign = delta >= 0 ? '+' : '-';
        return `${sign}${Math.abs(delta).toFixed(1)}pp`;
    }
    if (config.format === 'currency') {
        return formatSignedCurrency(delta);
    }
    if (config.format === 'number') {
        return formatSignedNumber(delta);
    }
    return formatValue(delta, config.format);
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
    if (value === null || value === undefined || Number.isNaN(value)) return '0.0%';
    return `${value.toFixed(1)}%`;
}

function formatSignedCurrency(value) {
    const sign = value >= 0 ? '+' : '-';
    return `${sign}${formatCurrency(Math.abs(value))}`;
}

function formatSignedNumber(value) {
    const sign = value >= 0 ? '+' : '-';
    return `${sign}${formatNumber(Math.abs(value))}`;
}

function formatPpDelta(value) {
    const sign = value >= 0 ? '+' : '-';
    return `${sign}${Math.abs(value).toFixed(1)}pp`;
}

function formatDate(dateString) {
    const date = new Date(dateString);
    return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
}

function formatDateTime(dateString) {
    const date = new Date(dateString);
    return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' }) + ' ' + date.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' });
}

function getChartOptions(yAxisLabel, stacked) {
    return {
        responsive: true,
        maintainAspectRatio: true,
        plugins: {
            legend: {
                labels: {
                    color: '#9aa6bd',
                    font: { size: 12 }
                }
            },
            tooltip: {
                backgroundColor: '#0f172a',
                titleColor: '#f8fafc',
                bodyColor: '#9aa6bd',
                borderColor: 'rgba(148, 163, 184, 0.25)',
                borderWidth: 1
            }
        },
        scales: {
            x: {
                grid: { color: 'rgba(148, 163, 184, 0.12)' },
                ticks: { color: '#9aa6bd', font: { size: 11 } },
                stacked: stacked
            },
            y: {
                grid: { color: 'rgba(148, 163, 184, 0.12)' },
                ticks: { color: '#9aa6bd', font: { size: 11 } },
                title: {
                    display: true,
                    text: yAxisLabel,
                    color: '#9aa6bd'
                },
                stacked: stacked
            }
        }
    };
}

function hexToRgba(hex, alpha) {
    const parsed = hex.replace('#', '');
    const bigint = parseInt(parsed, 16);
    const r = (bigint >> 16) & 255;
    const g = (bigint >> 8) & 255;
    const b = bigint & 255;
    return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}
