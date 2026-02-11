const API_BASE = "/api/inventory";
const AUTH_STORAGE_KEY = "inventory_ops_auth_v1";
const OVERVIEW_FETCH_LIMIT = 5000;

const state = {
    locations: [],
    overviewRows: [],
    overviewPage: 1,
    overviewTruncated: false,
    queueSummary: null,
    recentEvents: [],
    selectedSku: "",
    approvals: [],
    auditRows: [],
    securityConfig: null,
    auth: {
        writeToken: "",
        adminToken: "",
        actor: ""
    }
};

document.addEventListener("DOMContentLoaded", () => {
    initializeInventoryDashboard().catch((error) => {
        console.error(error);
        setStatus(`Initial load failed: ${error.message}`, "error");
    });
});

async function initializeInventoryDashboard() {
    const today = new Date().toISOString().slice(0, 10);
    document.getElementById("transferDate").value = today;
    document.getElementById("adjustmentDate").value = today;

    loadAuthFromStorage();
    bindUiHandlers();
    await refreshAll("Loading inventory dashboard...");
}

function bindUiHandlers() {
    document.getElementById("filterForm").addEventListener("submit", async (event) => {
        event.preventDefault();
        state.overviewPage = 1;
        await refreshInventoryAndQueue("Filters applied.");
    });

    document.getElementById("refreshAllBtn").addEventListener("click", async () => {
        state.overviewPage = 1;
        await refreshAll("Refreshing all data...");
    });

    document.getElementById("transferForm").addEventListener("submit", submitTransfer);
    document.getElementById("adjustmentForm").addEventListener("submit", submitAdjustment);
    document.getElementById("locationForm").addEventListener("submit", submitLocation);
    document.getElementById("adjustmentMode").addEventListener("change", handleAdjustmentModeChange);

    document.getElementById("inventoryTableBody").addEventListener("click", handleInventoryTableClick);
    document.getElementById("eventsBody").addEventListener("click", handleEventActionClick);
    document.getElementById("approvalsBody").addEventListener("click", handleApprovalActionClick);

    document.getElementById("authForm").addEventListener("submit", handleAuthSave);
    document.getElementById("clearAuthBtn").addEventListener("click", clearAuthFields);
    document.getElementById("refreshApprovalsBtn").addEventListener("click", async () => {
        await loadApprovals();
    });
    document.getElementById("refreshAuditBtn").addEventListener("click", async () => {
        await loadAudit();
    });
    document.getElementById("rowLimit").addEventListener("change", () => {
        state.overviewPage = 1;
        renderOverviewTable();
    });
    document.getElementById("inventoryPrevPage").addEventListener("click", () => {
        if (state.overviewPage <= 1) {
            return;
        }
        state.overviewPage -= 1;
        renderOverviewTable();
    });
    document.getElementById("inventoryNextPage").addEventListener("click", () => {
        const pagination = getOverviewPaginationState();
        if (state.overviewPage >= pagination.totalPages) {
            return;
        }
        state.overviewPage += 1;
        renderOverviewTable();
    });
}

function loadAuthFromStorage() {
    const raw = localStorage.getItem(AUTH_STORAGE_KEY);
    if (!raw) {
        return;
    }
    try {
        const parsed = JSON.parse(raw);
        if (parsed && typeof parsed === "object") {
            state.auth.writeToken = String(parsed.writeToken || "");
            state.auth.adminToken = String(parsed.adminToken || "");
            state.auth.actor = String(parsed.actor || "");
        }
    } catch (error) {
        console.warn("Failed to parse stored auth settings.", error);
    }

    applyAuthToInputs();
}

function applyAuthToInputs() {
    document.getElementById("authWriteToken").value = state.auth.writeToken;
    document.getElementById("authAdminToken").value = state.auth.adminToken;
    document.getElementById("authActor").value = state.auth.actor;
}

function captureAuthFromInputs() {
    state.auth.writeToken = document.getElementById("authWriteToken").value.trim();
    state.auth.adminToken = document.getElementById("authAdminToken").value.trim();
    state.auth.actor = document.getElementById("authActor").value.trim();
}

function persistAuthIfEnabled() {
    const persist = document.getElementById("authPersist").checked;
    if (!persist) {
        localStorage.removeItem(AUTH_STORAGE_KEY);
        return;
    }
    localStorage.setItem(
        AUTH_STORAGE_KEY,
        JSON.stringify({
            writeToken: state.auth.writeToken,
            adminToken: state.auth.adminToken,
            actor: state.auth.actor
        })
    );
}

async function handleAuthSave(event) {
    event.preventDefault();
    captureAuthFromInputs();
    persistAuthIfEnabled();
    setStatus("Security headers updated for this session.", "success");
    await refreshAll("Refreshing with updated auth settings...");
}

function clearAuthFields() {
    state.auth.writeToken = "";
    state.auth.adminToken = "";
    state.auth.actor = "";
    applyAuthToInputs();
    localStorage.removeItem(AUTH_STORAGE_KEY);
    setStatus("Security headers cleared.", "success");
}

async function refreshAll(message) {
    setStatus(message, "");
    await loadSecurityConfig();
    await loadLocations();
    await refreshInventoryAndQueue();
    await loadApprovals();
    await loadAudit();
    setStatus("Inventory dashboard updated.", "success");
}

async function loadSecurityConfig() {
    try {
        state.securityConfig = await apiGet(`${API_BASE}/security-config`);
        renderSecurityConfig();
    } catch (error) {
        state.securityConfig = null;
        document.getElementById("securityConfigLine").textContent = `Security config unavailable: ${error.message}`;
    }
}

function renderSecurityConfig() {
    const line = document.getElementById("securityConfigLine");
    const config = state.securityConfig;
    if (!config) {
        line.textContent = "Security policy unavailable.";
        return;
    }

    const writeState = config.writeTokenRequired ? "write token required" : "write token optional";
    const adminState = config.adminTokenRequired ? "admin token required" : "admin token optional";
    const approvalState = config.approvalEnabled
        ? `approval enabled (threshold ${config.approvalQtyThreshold})`
        : "approval disabled";
    line.textContent = `${writeState} | ${adminState} | ${approvalState}`;
}

async function refreshInventoryAndQueue(message) {
    if (message) {
        setStatus(message, "");
    }
    await Promise.all([loadOverview(), loadQueueSummary(), loadRecentEvents()]);
    renderSummaryCards();
}

async function loadLocations() {
    const data = await apiGet(`${API_BASE}/locations?include_inactive=true`);
    state.locations = data.rows || [];
    populateLocationSelects();
}

function populateLocationSelects() {
    const selectConfigs = [
        { id: "filterLocation", includeAll: true, allLabel: "All locations" },
        { id: "transferFromLocation", includeAll: false },
        { id: "transferToLocation", includeAll: false },
        { id: "adjustmentLocation", includeAll: false }
    ];

    for (const config of selectConfigs) {
        const select = document.getElementById(config.id);
        if (!select) {
            continue;
        }

        const previousValue = select.value;
        const options = [];
        if (config.includeAll) {
            options.push(`<option value="">${config.allLabel}</option>`);
        }

        for (const location of state.locations) {
            const inactiveSuffix = location.active ? "" : " (inactive)";
            options.push(
                `<option value="${escapeHtml(location.locationId)}">${escapeHtml(location.code)} - ${escapeHtml(location.displayName)}${inactiveSuffix}</option>`
            );
        }

        select.innerHTML = options.join("");
        if (previousValue && [...select.options].some((option) => option.value === previousValue)) {
            select.value = previousValue;
        }
    }
}

async function loadOverview() {
    const params = new URLSearchParams();
    const searchValue = document.getElementById("searchInput").value.trim();
    const locationId = document.getElementById("filterLocation").value;
    const includeInactive = document.getElementById("includeInactive").checked;

    if (searchValue) {
        params.set("search", searchValue);
    }
    if (locationId) {
        params.set("location_id", locationId);
    }
    params.set("include_inactive", includeInactive ? "true" : "false");
    params.set("limit", String(OVERVIEW_FETCH_LIMIT));

    const data = await apiGet(`${API_BASE}/overview?${params.toString()}`);
    state.overviewRows = data.rows || [];
    state.overviewTruncated = state.overviewRows.length >= OVERVIEW_FETCH_LIMIT;
    renderOverviewTable();
}

function renderOverviewTable() {
    const tableBody = document.getElementById("inventoryTableBody");
    const pagination = getOverviewPaginationState();
    if (!state.overviewRows.length) {
        tableBody.innerHTML = `<tr><td colspan="7">No rows matched the current filter.</td></tr>`;
        renderOverviewPagination(pagination);
        return;
    }

    const pagedRows = state.overviewRows.slice(pagination.startIndex, pagination.endIndex);
    tableBody.innerHTML = pagedRows
        .map((row) => {
            const stockStatus = row.lowStock ? "Low Stock" : "OK";
            const rowClass = row.lowStock ? "low-stock" : "";
            return `
                <tr class="${rowClass}">
                    <td><button class="sku-btn" data-sku="${escapeHtml(row.sku)}">${escapeHtml(row.sku)}</button></td>
                    <td>${escapeHtml(row.description || "")}</td>
                    <td>${escapeHtml(row.category || "")}</td>
                    <td>${formatNumber(row.onHand)}</td>
                    <td>${formatNumber(row.available)}</td>
                    <td>${formatNumber(row.reorderPoint)}</td>
                    <td>${stockStatus}</td>
                </tr>
            `;
        })
        .join("");
    renderOverviewPagination(pagination);
}

function getOverviewPageSize() {
    const raw = Number(document.getElementById("rowLimit").value || 250);
    if (!Number.isFinite(raw) || raw < 1) {
        return 250;
    }
    return Math.floor(raw);
}

function getOverviewPaginationState() {
    const totalRows = state.overviewRows.length;
    const pageSize = getOverviewPageSize();
    const totalPages = Math.max(1, Math.ceil(totalRows / pageSize));
    state.overviewPage = Math.min(Math.max(state.overviewPage, 1), totalPages);
    const startIndex = (state.overviewPage - 1) * pageSize;
    const endIndex = Math.min(startIndex + pageSize, totalRows);

    return {
        totalRows,
        pageSize,
        totalPages,
        currentPage: state.overviewPage,
        startIndex,
        endIndex
    };
}

function renderOverviewPagination(pagination) {
    const countInfo = document.getElementById("inventoryCountInfo");
    const pageInfo = document.getElementById("inventoryPageInfo");
    const prevButton = document.getElementById("inventoryPrevPage");
    const nextButton = document.getElementById("inventoryNextPage");

    if (pagination.totalRows === 0) {
        countInfo.textContent = "Showing 0 rows.";
        pageInfo.textContent = "Page 1 of 1";
        prevButton.disabled = true;
        nextButton.disabled = true;
        return;
    }

    let countText = `Showing ${pagination.startIndex + 1}-${pagination.endIndex} of ${pagination.totalRows} loaded rows.`;
    if (state.overviewTruncated) {
        countText += ` Results are capped at ${OVERVIEW_FETCH_LIMIT}; refine filters for full coverage.`;
    }
    countInfo.textContent = countText;
    pageInfo.textContent = `Page ${pagination.currentPage} of ${pagination.totalPages}`;
    prevButton.disabled = pagination.currentPage <= 1;
    nextButton.disabled = pagination.currentPage >= pagination.totalPages;
}

async function loadQueueSummary() {
    const data = await apiGet(`${API_BASE}/queue-summary?recent_limit=20`);
    state.queueSummary = data;
    renderQueueSummary();
}

function renderQueueSummary() {
    const queueCounts = document.getElementById("queueCounts");
    const queueRecentBody = document.getElementById("queueRecentBody");
    const counts = state.queueSummary?.counts || {
        pending: 0,
        in_flight: 0,
        error: 0,
        applied: 0,
        not_ready: 0
    };

    queueCounts.innerHTML = `
        <div class="queue-pill">Pending: <strong>${counts.pending}</strong></div>
        <div class="queue-pill">In Flight: <strong>${counts.in_flight}</strong></div>
        <div class="queue-pill">Errors: <strong>${counts.error}</strong></div>
        <div class="queue-pill">Applied: <strong>${counts.applied}</strong></div>
        <div class="queue-pill">Not Ready: <strong>${counts.not_ready}</strong></div>
        <div class="queue-pill">Total: <strong>${state.queueSummary?.totalEvents || 0}</strong></div>
    `;

    const recent = state.queueSummary?.recent || [];
    if (!recent.length) {
        queueRecentBody.innerHTML = `<tr><td colspan="5">No queue events found.</td></tr>`;
        return;
    }

    queueRecentBody.innerHTML = recent
        .map((row) => {
            return `
                <tr>
                    <td title="${escapeHtml(row.eventId)}">${escapeHtml(shortId(row.eventId))}</td>
                    <td>${escapeHtml(row.eventType)}</td>
                    <td>${statusBadge(row.qbStatus)}</td>
                    <td>${formatNumber(row.retryCount)}</td>
                    <td>${escapeHtml(row.qbErrorCode || "")}</td>
                </tr>
            `;
        })
        .join("");
}

async function loadRecentEvents() {
    const data = await apiGet(`${API_BASE}/events?limit=60`);
    state.recentEvents = data.rows || [];
    renderRecentEvents();
}

function renderRecentEvents() {
    const eventsBody = document.getElementById("eventsBody");
    if (!state.recentEvents.length) {
        eventsBody.innerHTML = `<tr><td colspan="6">No recent inventory events.</td></tr>`;
        return;
    }

    eventsBody.innerHTML = state.recentEvents
        .map((row) => {
            const allowRetry = row.qbStatus === "error";
            const allowVoid = row.status === "committed" && row.qbStatus !== "in_flight" && row.qbStatus !== "applied";
            return `
                <tr>
                    <td title="${escapeHtml(row.eventId)}">${escapeHtml(shortId(row.eventId))}</td>
                    <td>${escapeHtml(row.effectiveDate)}</td>
                    <td>${escapeHtml(row.eventType)}</td>
                    <td>${statusBadge(row.qbStatus)}</td>
                    <td>${escapeHtml(row.createdBy || "-")}</td>
                    <td>
                        <div class="row-actions">
                            ${allowRetry ? `<button class="tiny-btn retry" data-action="retry" data-event-id="${escapeHtml(row.eventId)}">Retry</button>` : ""}
                            ${allowVoid ? `<button class="tiny-btn void" data-action="void" data-event-id="${escapeHtml(row.eventId)}">Void</button>` : ""}
                        </div>
                    </td>
                </tr>
            `;
        })
        .join("");
}

async function loadApprovals() {
    const body = document.getElementById("approvalsBody");
    try {
        const data = await apiGet(`${API_BASE}/approvals?status=all&limit=50`, { requiresAdmin: true });
        state.approvals = data.rows || [];
        renderApprovals();
    } catch (error) {
        state.approvals = [];
        body.innerHTML = `<tr><td colspan="6">${escapeHtml(error.message)}</td></tr>`;
    }
}

function renderApprovals() {
    const body = document.getElementById("approvalsBody");
    if (!state.approvals.length) {
        body.innerHTML = `<tr><td colspan="6">No approval requests.</td></tr>`;
        return;
    }

    body.innerHTML = state.approvals
        .map((row) => {
            const pending = row.status === "pending";
            return `
                <tr>
                    <td title="${escapeHtml(row.requestId)}">${escapeHtml(shortId(row.requestId))}</td>
                    <td>${escapeHtml(row.action || "-")}</td>
                    <td>${approvalBadge(row.status)}</td>
                    <td>${escapeHtml(row.requestedBy || "-")}</td>
                    <td>${escapeHtml(row.reason || "-")}</td>
                    <td>
                        <div class="row-actions">
                            ${pending ? `<button class="tiny-btn retry" data-approval-action="approve" data-request-id="${escapeHtml(row.requestId)}">Approve</button>` : ""}
                            ${pending ? `<button class="tiny-btn void" data-approval-action="reject" data-request-id="${escapeHtml(row.requestId)}">Reject</button>` : ""}
                        </div>
                    </td>
                </tr>
            `;
        })
        .join("");
}

async function handleApprovalActionClick(event) {
    const button = event.target.closest("button[data-approval-action]");
    if (!button) {
        return;
    }
    const requestId = button.dataset.requestId;
    const action = button.dataset.approvalAction;
    if (!requestId || !action) {
        return;
    }

    const note = window.prompt(`${action === "approve" ? "Approval" : "Rejection"} note (optional):`, "") || "";
    button.disabled = true;
    try {
        if (action === "approve") {
            await apiPost(`${API_BASE}/approvals/${encodeURIComponent(requestId)}/approve`, { note }, { requiresAdmin: true });
            setStatus(`Approval executed: ${shortId(requestId)}`, "success");
        } else {
            await apiPost(`${API_BASE}/approvals/${encodeURIComponent(requestId)}/reject`, { note }, { requiresAdmin: true });
            setStatus(`Approval rejected: ${shortId(requestId)}`, "success");
        }
        await refreshInventoryAndQueue();
        await loadApprovals();
        await loadAudit();
    } catch (error) {
        setStatus(`Approval action failed: ${error.message}`, "error");
    } finally {
        button.disabled = false;
    }
}

async function loadAudit() {
    const body = document.getElementById("auditBody");
    try {
        const data = await apiGet(`${API_BASE}/audit?limit=40`, { requiresAdmin: true });
        state.auditRows = data.rows || [];
        renderAudit();
    } catch (error) {
        state.auditRows = [];
        body.innerHTML = `<tr><td colspan="4">${escapeHtml(error.message)}</td></tr>`;
    }
}

function renderAudit() {
    const body = document.getElementById("auditBody");
    if (!state.auditRows.length) {
        body.innerHTML = `<tr><td colspan="4">No audit records.</td></tr>`;
        return;
    }
    body.innerHTML = state.auditRows
        .map((row) => {
            return `
                <tr>
                    <td>${escapeHtml(row.timestamp || "-")}</td>
                    <td>${escapeHtml(row.actor || "-")}</td>
                    <td>${escapeHtml(row.action || "-")}</td>
                    <td>${escapeHtml(row.outcome || "-")}</td>
                </tr>
            `;
        })
        .join("");
}

function renderSummaryCards() {
    const totalSkus = state.overviewRows.length;
    const lowStock = state.overviewRows.filter((row) => row.lowStock).length;
    const pending = state.queueSummary?.counts?.pending || 0;
    const errors = state.queueSummary?.counts?.error || 0;

    document.getElementById("metricSkus").textContent = formatNumber(totalSkus);
    document.getElementById("metricLowStock").textContent = formatNumber(lowStock);
    document.getElementById("metricPending").textContent = formatNumber(pending);
    document.getElementById("metricErrors").textContent = formatNumber(errors);
}

async function submitTransfer(event) {
    event.preventDefault();
    const payload = {
        effectiveDate: document.getElementById("transferDate").value,
        createdBy: document.getElementById("transferCreatedBy").value.trim(),
        memo: document.getElementById("transferMemo").value.trim(),
        lines: [
            {
                sku: document.getElementById("transferSku").value.trim(),
                qty: Number(document.getElementById("transferQty").value),
                fromLocationId: document.getElementById("transferFromLocation").value,
                toLocationId: document.getElementById("transferToLocation").value
            }
        ]
    };

    try {
        const result = await apiPost(`${API_BASE}/transfer`, payload, { requiresWrite: true });
        if (result.status === "pending_approval" && result.request) {
            setStatus(`Transfer queued for approval: ${shortId(result.request.requestId)}`, "success");
            await loadApprovals();
        } else {
            setStatus(`Transfer created: ${shortId(result.eventId)}`, "success");
        }
        document.getElementById("transferQty").value = "";
        document.getElementById("transferMemo").value = "";
        await refreshInventoryAndQueue();
        await loadAudit();
    } catch (error) {
        setStatus(`Transfer failed: ${error.message}`, "error");
    }
}

async function submitAdjustment(event) {
    event.preventDefault();
    const mode = document.getElementById("adjustmentMode").value;
    const line = {
        sku: document.getElementById("adjustmentSku").value.trim()
    };
    const amount = Number(document.getElementById("adjustmentValue").value);
    if (mode === "delta") {
        line.qty = amount;
    } else {
        line.newQty = amount;
    }

    const payload = {
        effectiveDate: document.getElementById("adjustmentDate").value,
        createdBy: document.getElementById("adjustmentCreatedBy").value.trim(),
        memo: document.getElementById("adjustmentMemo").value.trim(),
        locationId: document.getElementById("adjustmentLocation").value,
        mode,
        reasonCode: document.getElementById("adjustmentReasonCode").value.trim(),
        lines: [line]
    };

    try {
        const result = await apiPost(`${API_BASE}/adjustment`, payload, { requiresWrite: true });
        if (result.status === "pending_approval" && result.request) {
            setStatus(`Adjustment queued for approval: ${shortId(result.request.requestId)}`, "success");
            await loadApprovals();
        } else {
            setStatus(`Adjustment created: ${shortId(result.eventId)}`, "success");
        }
        document.getElementById("adjustmentValue").value = "";
        document.getElementById("adjustmentMemo").value = "";
        await refreshInventoryAndQueue();
        await loadAudit();
    } catch (error) {
        setStatus(`Adjustment failed: ${error.message}`, "error");
    }
}

async function submitLocation(event) {
    event.preventDefault();
    const payload = {
        code: document.getElementById("locationCode").value.trim(),
        displayName: document.getElementById("locationDisplayName").value.trim(),
        qbSiteFullName: document.getElementById("locationQbSiteName").value.trim(),
        qbSiteListId: document.getElementById("locationQbSiteListId").value.trim(),
        active: document.getElementById("locationActive").checked,
        isVirtual: document.getElementById("locationVirtual").checked
    };

    try {
        await apiPost(`${API_BASE}/location`, payload, { requiresAdmin: true });
        setStatus(`Location saved: ${payload.code}`, "success");
        await loadLocations();
        await loadAudit();
    } catch (error) {
        setStatus(`Location save failed: ${error.message}`, "error");
    }
}

async function handleEventActionClick(event) {
    const button = event.target.closest("button[data-action]");
    if (!button) {
        return;
    }

    const action = button.dataset.action;
    const eventId = button.dataset.eventId;
    if (!action || !eventId) {
        return;
    }

    button.disabled = true;
    try {
        if (action === "retry") {
            await apiPost(`${API_BASE}/events/${encodeURIComponent(eventId)}/retry`, {}, { requiresAdmin: true });
            setStatus(`Retry queued for ${shortId(eventId)}.`, "success");
        } else if (action === "void") {
            await apiPost(`${API_BASE}/events/${encodeURIComponent(eventId)}/void`, {}, { requiresAdmin: true });
            setStatus(`Event voided: ${shortId(eventId)}.`, "success");
        }
        await refreshInventoryAndQueue();
        await loadAudit();
    } catch (error) {
        setStatus(`Action failed: ${error.message}`, "error");
    } finally {
        button.disabled = false;
    }
}

async function handleInventoryTableClick(event) {
    const skuButton = event.target.closest("button[data-sku]");
    if (!skuButton) {
        return;
    }
    const sku = skuButton.dataset.sku;
    if (!sku) {
        return;
    }
    state.selectedSku = sku;
    await loadItemDetail(sku);
}

async function loadItemDetail(sku) {
    try {
        const data = await apiGet(`${API_BASE}/item/${encodeURIComponent(sku)}?event_limit=20`);
        renderItemDetail(data);
        document.getElementById("itemDetailCaption").textContent = `Detail for ${sku}`;
    } catch (error) {
        document.getElementById("itemDetailContent").textContent = `Failed to load item detail: ${error.message}`;
        setStatus(`Item detail failed: ${error.message}`, "error");
    }
}

function renderItemDetail(detail) {
    const container = document.getElementById("itemDetailContent");
    if (!detail) {
        container.classList.add("empty");
        container.textContent = "Item not found.";
        return;
    }

    const balanceRows = (detail.balances || [])
        .map((row) => {
            return `<tr>
                <td>${escapeHtml(row.locationCode || "-")}</td>
                <td>${formatNumber(row.onHand)}</td>
                <td>${formatNumber(row.available)}</td>
                <td>${formatDateTime(row.updatedAt)}</td>
            </tr>`;
        })
        .join("");

    const eventRows = (detail.recentEvents || [])
        .slice(0, 10)
        .map((row) => {
            return `<tr>
                <td>${formatDateTime(row.createdAt)}</td>
                <td>${escapeHtml(row.eventType)}</td>
                <td>${statusBadge(row.qbStatus)}</td>
                <td>${formatNumber(row.qty)}</td>
            </tr>`;
        })
        .join("");

    container.classList.remove("empty");
    container.innerHTML = `
        <div class="item-detail-grid">
            <div>
                <h3>${escapeHtml(detail.sku)}</h3>
                <p>${escapeHtml(detail.description || "")}</p>
                <p>Category: ${escapeHtml(detail.category || "-")}</p>
                <p>Preferred vendor: ${escapeHtml(detail.preferredVendor || "-")}</p>
            </div>
            <div>
                <h3>Commercial</h3>
                <p>Cost: ${formatNumber(detail.cost)}</p>
                <p>Price: ${formatNumber(detail.price)}</p>
                <p>Reorder point: ${formatNumber(detail.reorderPoint)}</p>
            </div>
            <div>
                <h3>Accounts</h3>
                <p>Income: ${escapeHtml(detail.account || "-")}</p>
                <p>COGS: ${escapeHtml(detail.cogsAccount || "-")}</p>
                <p>Asset: ${escapeHtml(detail.assetAccount || "-")}</p>
            </div>
        </div>
        <div class="table-wrap compact" style="margin-top: 0.8rem;">
            <table>
                <thead>
                    <tr>
                        <th>Location</th>
                        <th>On Hand</th>
                        <th>Available</th>
                        <th>Updated</th>
                    </tr>
                </thead>
                <tbody>${balanceRows || `<tr><td colspan="4">No balances found.</td></tr>`}</tbody>
            </table>
        </div>
        <div class="table-wrap compact" style="margin-top: 0.8rem;">
            <table>
                <thead>
                    <tr>
                        <th>Created</th>
                        <th>Type</th>
                        <th>QB Status</th>
                        <th>Qty</th>
                    </tr>
                </thead>
                <tbody>${eventRows || `<tr><td colspan="4">No recent events found.</td></tr>`}</tbody>
            </table>
        </div>
    `;
}

function handleAdjustmentModeChange(event) {
    const label = document.getElementById("adjustmentValueLabel");
    const input = document.getElementById("adjustmentValue");
    if (event.target.value === "set") {
        label.textContent = "New quantity";
        input.min = "0";
    } else {
        label.textContent = "Quantity delta";
        input.removeAttribute("min");
    }
}

function setStatus(message, tone) {
    const banner = document.getElementById("statusBanner");
    banner.textContent = message;
    banner.classList.remove("error", "success");
    if (tone) {
        banner.classList.add(tone);
    }
}

async function apiGet(url, options = {}) {
    return await apiRequest(url, { method: "GET", ...options });
}

async function apiPost(url, body, options = {}) {
    return await apiRequest(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
        ...options
    });
}

async function apiRequest(url, options = {}) {
    captureAuthFromInputs();

    const requiresWrite = Boolean(options.requiresWrite);
    const requiresAdmin = Boolean(options.requiresAdmin);
    const fetchOptions = { ...options };
    delete fetchOptions.requiresWrite;
    delete fetchOptions.requiresAdmin;

    const mergedHeaders = {
        ...(fetchOptions.headers || {})
    };
    if (state.auth.actor) {
        mergedHeaders["X-Inventory-User"] = state.auth.actor;
    }
    if (requiresWrite && state.auth.writeToken) {
        mergedHeaders["X-Inventory-Token"] = state.auth.writeToken;
    }
    if (requiresAdmin && state.auth.adminToken) {
        mergedHeaders["X-Inventory-Admin-Token"] = state.auth.adminToken;
    }
    fetchOptions.headers = mergedHeaders;

    const response = await fetch(url, fetchOptions);
    let payload = null;
    try {
        payload = await response.json();
    } catch (error) {
        throw new Error(`Unexpected response format (HTTP ${response.status}).`);
    }

    if (!response.ok || !payload.success) {
        throw new Error(payload.error || `HTTP ${response.status}`);
    }
    return payload.data;
}

function statusBadge(status) {
    const normalized = status || "not_ready";
    return `<span class="badge ${escapeHtml(normalized)}">${escapeHtml(normalized)}</span>`;
}

function approvalBadge(status) {
    const normalized = status || "pending";
    return `<span class="badge ${escapeHtml(normalized)}">${escapeHtml(normalized)}</span>`;
}

function formatNumber(value) {
    const number = Number(value || 0);
    return new Intl.NumberFormat("en-US", { maximumFractionDigits: 4 }).format(number);
}

function formatDateTime(value) {
    const num = Number(value);
    if (!Number.isFinite(num) || num <= 0) {
        return "-";
    }
    return new Date(num).toLocaleString();
}

function shortId(value) {
    const text = String(value || "");
    if (text.length <= 10) {
        return text;
    }
    return `${text.slice(0, 4)}...${text.slice(-4)}`;
}

function escapeHtml(value) {
    return String(value ?? "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
}
