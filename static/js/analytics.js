(function () {
  const state = window.__ANALYTICS_STATE__ || (window.__ANALYTICS_STATE__ = {
    tab: "overview",
    charts: {},
    filterOptionsLoaded: false,
    syncStatus: null,
  });
  const boot = window.__BURAQ_ANALYTICS_BOOTSTRAP__ || {};

  const el = (id) => (typeof window.analyticsEl === "function" ? window.analyticsEl(id) : document.getElementById(id));
  const esc = window.esc || ((value) => String(value || ""));

  function buildQuery(extra = {}) {
    const params = new URLSearchParams();
    const values = {
      period: el("analytics-period")?.value || "month",
      date_from: el("analytics-date-from")?.value || "",
      date_to: el("analytics-date-to")?.value || "",
      sales_plan_id: el("analytics-sales-plan")?.value || "",
      manager: el("analytics-manager")?.value || "",
      logist: el("analytics-logist")?.value || "",
      client: el("analytics-client")?.value || "",
      bl_code: el("analytics-bl-code")?.value || "",
      reys_number: el("analytics-reys-number")?.value || "",
      fura: el("analytics-fura")?.value || "",
      status: el("analytics-status")?.value || "",
      currency: el("analytics-currency")?.value || "",
      bank_or_cash: el("analytics-bank-or-cash")?.value || "",
      category: el("analytics-category")?.value || "",
      warehouse: el("analytics-warehouse")?.value || "",
      ...extra,
    };
    Object.entries(values).forEach(([key, value]) => {
      if (value !== null && value !== undefined && String(value).trim() !== "") {
        params.set(key, String(value).trim());
      }
    });
    return params.toString();
  }

  function populateSelect(id, values, placeholderLabel) {
    const select = el(id);
    if (!select) return;
    const items = Array.isArray(values) ? values.filter(Boolean) : [];
    const current = select.value || "";
    select.innerHTML = `<option value="">${esc(placeholderLabel || "Barchasi")}</option>` +
      items.map((item) => `<option value="${esc(item)}">${esc(item)}</option>`).join("");
    if (items.includes(current)) {
      select.value = current;
    }
  }

  function formatPlanTarget(plan) {
    const metric = String(plan?.target_metric || "amount_usd");
    const value = Number(plan?.target_value || plan?.target_amount_usd || 0);
    if (metric === "cbm") return `${value} m³`;
    if (metric === "bl_count") return `${value} BL`;
    return `${value} USD`;
  }

  function renderPlans(plans) {
    state.plans = Array.isArray(plans) ? plans : [];
    const tbody = el("analytics-plans-table");
    if (!tbody) return;
    if (!state.plans.length) {
      tbody.innerHTML = typeof window.analyticsTableEmpty === "function"
        ? window.analyticsTableEmpty(6, "Sales planlar hali yaratilmagan")
        : "";
      return;
    }
    tbody.innerHTML = state.plans.map((plan) => `
      <tr>
        <td>${esc(plan.name || `Plan #${plan.id}`)}</td>
        <td>${esc(plan.period_start || "—")} → ${esc(plan.period_end || "—")}</td>
        <td>${esc(plan.target_metric || "amount_usd")}</td>
        <td>${esc(formatPlanTarget(plan))}</td>
        <td>${typeof window.analyticsStatusBadge === "function"
          ? window.analyticsStatusBadge(Number(plan.is_active || 0) === 1 ? "Aktiv" : "Arxiv", Number(plan.is_active || 0) === 1 ? "success" : "muted")
          : esc(Number(plan.is_active || 0) === 1 ? "Aktiv" : "Arxiv")}</td>
        <td style="display:flex;gap:8px;flex-wrap:wrap">
          <button class="btn btn-ghost btn-sm editor-only" onclick="activateAnalyticsPlan(${Number(plan.id)})">Aktiv qilish</button>
          <button class="btn btn-red btn-sm editor-only" onclick="deleteAnalyticsPlan(${Number(plan.id)})">O‘chirish</button>
        </td>
      </tr>
    `).join("");
  }

  function hydrateFilters(options = {}, selected = {}) {
    if (Array.isArray(options.plans)) {
      state.plans = options.plans;
    }
    const planOptions = (state.plans || []).map((plan) => ({
      value: String(plan.id),
      label: plan.name || `Plan #${plan.id}`,
    }));
    const planSelect = el("analytics-sales-plan");
    if (planSelect) {
      const current = String(planSelect.value || selected.sales_plan_id || "");
      planSelect.innerHTML = `<option value="">Aktiv plan</option>` +
        planOptions.map((plan) => `<option value="${esc(plan.value)}">${esc(plan.label)}</option>`).join("");
      if (current && planOptions.some((plan) => plan.value === current)) {
        planSelect.value = current;
      }
    }

    populateSelect("analytics-manager", options.managers, "Barchasi");
    populateSelect("analytics-logist", options.logists, "Barchasi");
    populateSelect("analytics-client", options.clients, "Barchasi");
    populateSelect("analytics-bl-code", options.bl_codes, "Barchasi");
    populateSelect("analytics-reys-number", options.reys_numbers, "Barchasi");
    populateSelect("analytics-fura", options.furas, "Barchasi");
    populateSelect("analytics-status", options.statuses, "Barchasi");
    populateSelect("analytics-currency", options.currencies, "Barchasi");
    populateSelect("analytics-bank-or-cash", options.bank_or_cash, "Barchasi");
    populateSelect("analytics-category", options.categories, "Barchasi");
    populateSelect("analytics-warehouse", options.warehouses, "Barchasi");

    if (selected.period && el("analytics-period")) el("analytics-period").value = selected.period;
    if (selected.date_from && el("analytics-date-from") && typeof window.analyticsToInputDate === "function") {
      el("analytics-date-from").value = window.analyticsToInputDate(selected.date_from);
    }
    if (selected.date_to && el("analytics-date-to") && typeof window.analyticsToInputDate === "function") {
      el("analytics-date-to").value = window.analyticsToInputDate(selected.date_to);
    }

    [
      ["analytics-manager", selected.manager],
      ["analytics-logist", selected.logist],
      ["analytics-client", selected.client],
      ["analytics-bl-code", selected.bl_code],
      ["analytics-reys-number", selected.reys_number],
      ["analytics-fura", selected.fura],
      ["analytics-status", selected.status],
      ["analytics-currency", selected.currency],
      ["analytics-bank-or-cash", selected.bank_or_cash],
      ["analytics-category", selected.category],
      ["analytics-warehouse", selected.warehouse],
    ].forEach(([id, value]) => {
      if (value && el(id)) el(id).value = value;
    });
    renderPlans(state.plans);
  }

  async function ensureOptions() {
    if (state.filterOptionsLoaded && state.cachedOverview) {
      return state.cachedOverview;
    }
    const data = await window.api(`/analytics/api/overview?${buildQuery()}`);
    state.cachedOverview = data;
    hydrateFilters({ ...(data.filters || {}), plans: data.plans || [] }, data.selected_filters || {});
    state.filterOptionsLoaded = true;
    return data;
  }

  function resetFilters() {
    if (el("analytics-period")) el("analytics-period").value = "month";
    if (el("analytics-date-from")) el("analytics-date-from").value = "";
    if (el("analytics-date-to")) el("analytics-date-to").value = "";
    [
      "analytics-sales-plan",
      "analytics-manager",
      "analytics-logist",
      "analytics-client",
      "analytics-bl-code",
      "analytics-reys-number",
      "analytics-fura",
      "analytics-status",
      "analytics-currency",
      "analytics-bank-or-cash",
      "analytics-category",
      "analytics-warehouse",
    ].forEach((id) => {
      if (el(id)) el(id).value = "";
    });
    window.refreshAnalyticsActiveTab();
  }

  async function loadOverview(prefetched) {
    const data = prefetched || await window.api(`/analytics/api/overview?${buildQuery()}`);
    state.cachedOverview = data;
    hydrateFilters({ ...(data.filters || {}), plans: data.plans || [] }, data.selected_filters || {});
    if (typeof window.analyticsRenderOverview === "function") {
      window.analyticsRenderOverview(data);
    }
  }

  async function loadManagers() {
    try {
      const data = await window.api(`/analytics/api/managers?${buildQuery()}`);
      const ranking = el("analytics-manager-ranking");
      ranking.innerHTML = data.empty
        ? window.analyticsEmptyState("Menejerlar statistikasi hali yo‘q")
        : (data.ranking || []).map((item) => `<div class="analytics-insight">${esc(item)}</div>`).join("");
      const tbody = el("analytics-managers-table");
      const rows = data.table || [];
      tbody.innerHTML = rows.length ? rows.map((row) => `
        <tr>
          <td>${esc(row.manager_name)}</td>
          <td>${esc(row.sales_total || "0")}</td>
          <td>${esc(String(row.bl_count || 0))}</td>
          <td>${esc(row.paid_amount || "0")}</td>
          <td>${esc(row.debt_amount || "0")}</td>
          <td>${esc(row.average_check || "0")}</td>
          <td>${esc(row.profit || "0")}</td>
          <td>${window.analyticsStatusBadge(row.status || "—", row.status === "To'liq yopilgan" ? "success" : row.status === "Qisman to'langan" ? "warning" : "danger")}</td>
          <td>${esc(String(row.late_count || 0))}</td>
        </tr>
      `).join("") : window.analyticsTableEmpty(9, "Menejerlar KPI ma'lumotlari topilmadi");
    } catch (error) {
      window.toast(error.message, "err");
    }
  }

  async function loadLogists() {
    try {
      const data = await window.api(`/analytics/api/logists?${buildQuery()}`);
      el("analytics-logists-leaders").innerHTML = data.empty
        ? window.analyticsEmptyState("Logistlar statistikasi hali yo‘q")
        : (data.leaders || []).map((row, index) => `<div class="analytics-insight">#${index + 1} ${esc(row.logist_name)} — ${esc(row.display_value || "0")}</div>`).join("");
      el("analytics-logists-summary").innerHTML = [
        `<div class="analytics-insight">Umumiy yopilgan: ${esc(data.summary?.total_closed || "0 USD")}</div>`,
        `<div class="analytics-insight">Assigned reys: ${esc(String(data.summary?.total_reys || 0))}</div>`,
        `<div class="analytics-insight">O‘rtacha / reys: ${esc(data.summary?.avg_per_reys || "0 USD")}</div>`,
      ].join("");
      const tbody = el("analytics-logists-table");
      const rows = data.table || [];
      tbody.innerHTML = rows.length ? rows.map((row) => `
        <tr>
          <td>${esc(row.logist_name)}</td>
          <td>${esc(String(row.assigned_reys_count || 0))}</td>
          <td>${esc(row.display_value || "0")}</td>
          <td>${esc(String(row.share_percent || 0))}%</td>
          <td>${esc(row.average_per_reys || "0")}</td>
          <td>${esc(String(row.bl_count || 0))}</td>
          <td>${esc(row.warehouse_kpi || "0/0")}</td>
          <td>${esc(row.damage_kpi || "0/0")}</td>
        </tr>
      `).join("") : window.analyticsTableEmpty(8, "Logist KPI ma'lumotlari topilmadi");
    } catch (error) {
      window.toast(error.message, "err");
    }
  }

  function renderMonitorPreview(payload) {
    const wrap = el("analytics-monitor-summary");
    if (!wrap) return;
    if (!payload || payload.empty) {
      wrap.innerHTML = window.analyticsEmptyState(payload?.message || "Monitor uchun ma’lumot topilmadi");
      return;
    }
    const plan = payload.plan || {};
    const overall = payload.overall || {};
    const logists = payload.departments?.logists || {};
    const sales = payload.departments?.sales || {};
    wrap.innerHTML = [
      `<div class="analytics-insight">🎯 ${esc(plan.name || "Plan")} — ${esc(String(plan.target_value || 0))} ${esc(plan.metric_label || "")}</div>`,
      `<div class="analytics-insight">📈 Fact: ${esc(String(overall.closed_value || 0))} ${esc(plan.metric_label || "")} (${esc(String(overall.progress_percent || 0))}%)</div>`,
      `<div class="analytics-insight">🚛 Logistlar: ${esc(String(logists.closed_value || 0))} ${esc(plan.metric_label || "")}</div>`,
      `<div class="analytics-insight">💼 Savdo bo‘limi: ${esc(String(sales.closed_value || 0))} ${esc(plan.metric_label || "")}</div>`,
      `<div class="analytics-insight">🕒 Oxirgi yangilanish: ${esc(payload.last_updated || "—")}</div>`,
    ].join("");
  }

  async function loadMonitorPreview() {
    try {
      const planId = el("analytics-sales-plan")?.value || "";
      const plan = state.plans.find((item) => String(item.id) === String(planId)) || state.plans.find((item) => Number(item.is_active || 0) === 1);
      const metric = plan?.target_metric || "amount_usd";
      const query = buildQuery({ sales_plan_id: plan?.id || "", metric });
      const data = await window.api(`/analytics/api/monitor?${query}`);
      renderMonitorPreview(data);
    } catch (error) {
      window.toast(error.message, "err");
    }
  }

  function renderSyncStatus(data) {
    state.syncStatus = data || null;
    if (Array.isArray(data?.plans)) {
      state.plans = data.plans;
    }
    const connection = data?.connection || {};
    const tone = connection.connected ? (connection.mode === "api" || connection.mode === "public" ? "success" : "warning") : "warning";
    if (el("analytics-sheet-id")) el("analytics-sheet-id").value = data?.sheet_id || "";
    if (el("analytics-connection-status")) {
      el("analytics-connection-status").innerHTML = `
        <div class="analytics-status-box ${tone}">
          <strong>${esc(connection.mode === "api" ? "Google Sheets API ulangan" : connection.mode === "public" ? "Google Sheets public link rejimi tayyor" : "Google Sheets ulanmagan")}</strong><br>
          ${esc(connection.message || "")}
        </div>`;
    }
    if (el("analytics-sync-summary")) {
      el("analytics-sync-summary").innerHTML = `
        <div class="analytics-insights">
          <div class="analytics-insight">Oxirgi sync: ${esc(data?.last_sync_at || "—")}</div>
          <div class="analytics-insight">Manba: ${esc(data?.source_name || "Google Sheets / XLSX cache")}</div>
          <div class="analytics-insight">Planlar: ${esc(String((data?.plans || []).length || 0))}</div>
        </div>`;
    }
    renderPlans(data?.plans || []);
    const tbody = el("analytics-sync-logs");
    const rows = data?.logs || [];
    tbody.innerHTML = rows.length ? rows.map((row) => `
      <tr>
        <td>${esc(row.started_at || "—")}</td>
        <td>${esc(row.finished_at || "—")}</td>
        <td>${window.analyticsStatusBadge(row.status || "—", row.status === "success" ? "success" : row.status === "failed" ? "danger" : "warning")}</td>
        <td>${esc(String(row.rows_imported || 0))}</td>
        <td>${esc(String(row.rows_skipped || 0))}</td>
        <td>${esc(row.error_message || "—")}</td>
        <td>${esc(row.details_json || "—")}</td>
      </tr>
    `).join("") : window.analyticsTableEmpty(7, "Import loglari hali yo‘q");
  }

  async function loadSync() {
    try {
      const data = await window.api("/analytics/api/sync/status");
      renderSyncStatus(data);
      hydrateFilters({ plans: data.plans || state.plans }, { sales_plan_id: el("analytics-sales-plan")?.value || "" });
    } catch (error) {
      window.toast(error.message, "err");
    }
  }

  async function savePlan() {
    if (typeof window.ensureEditor === "function" && !window.ensureEditor()) return;
    try {
      const payload = {
        name: el("analytics-plan-name")?.value?.trim() || "",
        period_start: el("analytics-plan-start")?.value || "",
        period_end: el("analytics-plan-end")?.value || "",
        target_metric: el("analytics-plan-metric")?.value || "amount_usd",
        target_value: Number(el("analytics-plan-target")?.value || 0),
      };
      const res = await window.api("/analytics/api/plans", "POST", payload);
      renderPlans(res.plans || []);
      state.filterOptionsLoaded = false;
      if (el("analytics-plan-name")) el("analytics-plan-name").value = "";
      if (el("analytics-plan-start")) el("analytics-plan-start").value = "";
      if (el("analytics-plan-end")) el("analytics-plan-end").value = "";
      if (el("analytics-plan-target")) el("analytics-plan-target").value = "";
      window.toast("Sales plan saqlandi", "ok");
      await loadSync();
    } catch (error) {
      window.toast(error.message, "err");
    }
  }

  async function activatePlan(id) {
    if (typeof window.ensureEditor === "function" && !window.ensureEditor()) return;
    try {
      const res = await window.api(`/analytics/api/plans/${id}/activate`, "POST", {});
      renderPlans(res.plans || []);
      state.filterOptionsLoaded = false;
      await ensureOptions();
      window.toast("Plan aktiv qilindi", "ok");
    } catch (error) {
      window.toast(error.message, "err");
    }
  }

  async function deletePlan(id) {
    if (typeof window.ensureEditor === "function" && !window.ensureEditor()) return;
    if (!window.confirm("Sales planni o‘chirishni tasdiqlaysizmi?")) return;
    try {
      const res = await window.api(`/analytics/api/plans/${id}`, "DELETE");
      renderPlans(res.plans || []);
      state.filterOptionsLoaded = false;
      await ensureOptions();
      window.toast("Plan o‘chirildi", "ok");
    } catch (error) {
      window.toast(error.message, "err");
    }
  }

  function openMonitor() {
    const selectedPlanId = el("analytics-sales-plan")?.value || "";
    const plan = state.plans.find((item) => String(item.id) === String(selectedPlanId)) || state.plans.find((item) => Number(item.is_active || 0) === 1);
    const metric = plan?.target_metric || "amount_usd";
    const params = new URLSearchParams();
    if (plan?.id) params.set("sales_plan_id", String(plan.id));
    params.set("metric", metric);
    window.open(`/analytics/monitor?${params.toString()}`, "_blank", "noopener");
  }

  function showTab(tab, shouldLoad = true) {
    state.tab = tab;
    document.querySelectorAll(".analytics-tab-btn").forEach((btn) => {
      btn.classList.toggle("active", btn.dataset.analyticsTab === tab);
    });
    document.querySelectorAll("#page-analytics .analytics-section").forEach((section) => {
      section.classList.toggle("active", section.id === `analytics-section-${tab}`);
    });
    if (shouldLoad) {
      window.refreshAnalyticsActiveTab();
    }
  }

  async function refreshActiveTab(prefetchedOverview = null) {
    const tab = state.tab || boot.initialAnalyticsTab || "overview";
    if (tab === "overview") return loadOverview(prefetchedOverview);
    if (tab === "sales-growth") return window.loadAnalyticsSalesGrowth();
    if (tab === "cashflow") return window.loadAnalyticsCashflow();
    if (tab === "managers") return loadManagers();
    if (tab === "logists") return loadLogists();
    if (tab === "shipments") return window.loadAnalyticsShipments();
    if (tab === "debts") return window.loadAnalyticsDebts();
    if (tab === "sync") return loadSync();
    if (tab === "monitor") return loadMonitorPreview();
    return Promise.resolve();
  }

  async function loadPage(initialTab = null) {
    try {
      if (initialTab) {
        state.tab = initialTab;
      } else if (!state.tab) {
        state.tab = boot.initialAnalyticsTab || "overview";
      }
      showTab(state.tab || "overview", false);
      const prefetched = await ensureOptions();
      if ((state.tab || "overview") === "overview") {
        await loadOverview(prefetched);
      } else {
        await refreshActiveTab();
      }
    } catch (error) {
      window.toast(error.message, "err");
    }
  }

  window.analyticsQueryString = buildQuery;
  window.hydrateAnalyticsFilters = hydrateFilters;
  window.ensureAnalyticsFilterOptions = ensureOptions;
  window.resetAnalyticsFilters = resetFilters;
  window.loadAnalyticsOverview = loadOverview;
  window.loadAnalyticsManagers = loadManagers;
  window.loadAnalyticsLogists = loadLogists;
  window.renderAnalyticsPlans = renderPlans;
  window.renderAnalyticsSyncStatus = renderSyncStatus;
  window.loadAnalyticsSync = loadSync;
  window.saveAnalyticsPlan = savePlan;
  window.activateAnalyticsPlan = activatePlan;
  window.deleteAnalyticsPlan = deletePlan;
  window.loadAnalyticsMonitorPreview = loadMonitorPreview;
  window.openSalesMonitor = openMonitor;
  window.showAnalyticsTab = showTab;
  window.refreshAnalyticsActiveTab = refreshActiveTab;
  window.loadAnalyticsPage = loadPage;

  if (boot.initialView === "analytics" || document.getElementById("page-analytics")?.classList.contains("active")) {
    loadPage(state.tab || boot.initialAnalyticsTab || "overview");
  }
})();
