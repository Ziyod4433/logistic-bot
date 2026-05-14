(function () {
  const bootstrap = window.SALES_MONITOR_BOOTSTRAP || {};
  const query = new URLSearchParams(window.location.search);
  const REFRESH_SECONDS = 120; // 2 minutes

  const state = {
    plans: Array.isArray(bootstrap.salesPlans) ? bootstrap.salesPlans : [],
    planId: query.get("sales_plan_id") || bootstrap.activePlanId || "",
    metric: query.get("metric") || "cbm",
    countdownSeconds: REFRESH_SECONDS,
    countdownHandle: null,
    clockHandle: null,
    refreshHandle: null,
    deptRotationHandle: null,         // SAVDO ↔ LOGISTIKA — каждые 20 сек
    savdoViewHandle: null,            // LTL ↔ FTL внутри SAVDO — каждые 5 сек
    currentDepartment: "logists",
    savdoView: "ltl",                 // "ltl" (Ombor m³) | "ftl" (truck count)
    latestPayload: null,
  };

  const byId = (id) => document.getElementById(id);

  const els = {
    planSelect: byId("plan-select"),
    metricSelect: byId("metric-select"),
    refreshBtn: byId("refresh-btn"),
    fullscreenBtn: byId("fullscreen-btn"),
    configBtn: byId("config-btn"),
    rotationTimer: byId("rotation-timer"),
    rotationLine: byId("rotation-line"),
    clock: byId("clock"),
    planName: byId("plan-name"),
    planPeriod: byId("plan-period"),
    lastUpdated: byId("last-updated"),
    sourceName: byId("source-name"),
    progressArc: byId("progress-arc"),
    progressPercent: byId("progress-percent"),
    planTarget: byId("plan-target"),
    planClosed: byId("plan-closed"),
    planRemaining: byId("plan-remaining"),
    planBl: byId("plan-bl"),
    planBadge: byId("plan-badge"),
    monthlyMetricLabel: byId("monthly-metric-label"),
    monthlyBars: byId("monthly-bars"),
    deptMonitor: byId("dept-monitor"),
    deptTitle: byId("dept-title"),
    deptRotateNote: byId("dept-rotate-note"),
    deptTotal: byId("dept-total"),
    deptShare: byId("dept-share"),
    deptBl: byId("dept-bl"),
    deptBoard: byId("dept-board"),
    deptBody: byId("dept-body"),
    deptModeBadge: byId("dept-mode-badge"),
    deptShareRow: byId("dept-share-row"),
    shareTitle: byId("share-title"),
    logistsShareBar: byId("logists-share-bar"),
    logistsShareValue: byId("logists-share-value"),
    salesShareBar: byId("sales-share-bar"),
    salesShareValue: byId("sales-share-value"),
    main: document.querySelector(".main"),
    bottom: document.querySelector(".bottom"),
    empty: byId("monitor-empty"),
    // config modal — Ombor
    cfgOverlay: byId("cfg-overlay"),
    cfgSheetId: byId("cfg-sheet-id"),
    cfgSheetName: byId("cfg-sheet-name"),
    cfgCbmCol: byId("cfg-cbm-col"),
    cfgDateCol: byId("cfg-date-col"),
    cfgSellerCol: byId("cfg-seller-col"),
    cfgHeaderRows: byId("cfg-header-rows"),
    cfgSaveBtn: byId("cfg-save-btn"),
    cfgCancelBtn: byId("cfg-cancel-btn"),
    cfgStatus: byId("cfg-status"),
    // config modal — FTL
    cfgFtlSheetId: byId("cfg-ftl-sheet-id"),
    cfgFtlGid: byId("cfg-ftl-gid"),
    cfgFtlTypeCol: byId("cfg-ftl-type-col"),
    cfgFtlDateCol: byId("cfg-ftl-date-col"),
    cfgFtlSellerCol: byId("cfg-ftl-seller-col"),
    cfgFtlHeaderRows: byId("cfg-ftl-header-rows"),
    cfgFtlCbmPerTruck: byId("cfg-ftl-cbm-per-truck"),
  };

  const METRIC_LABELS = {
    amount_usd: "USD",
    cbm: "m³",
    bl_count: "BL",
  };

  const DEPARTMENT_META = {
    logists: {
      key: "logists",
      title: "SAVDO BO'LIMI",            // ранее SOTUVCHILAR — переименовано по запросу
      tone: "blue",
      accentClass: "ca-b",
      note: "Live · Ombor sheet",
    },
    sales: {
      key: "sales",
      title: "LOGISTIKA BO'LIMI",        // ранее SAVDO BO'LIMI — теперь это «логистика»
      tone: "purple",
      accentClass: "ca-p",
      note: "Live · Ombor sheet",
    },
  };

  // CIRCLE RADIUS — must match SVG cx/cy/r in monitor.html (r=136, stroke-width=28)
  const CIRCLE_RADIUS = 136;

  function escapeHtml(value) {
    return String(value || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function currentPlan() {
    return state.plans.find((plan) => String(plan.id) === String(state.planId)) || null;
  }

  function formatNumber(value) {
    const numeric = Number(value || 0);
    if (Math.abs(numeric - Math.round(numeric)) < 0.00001) {
      return new Intl.NumberFormat("ru-RU", { maximumFractionDigits: 0 }).format(Math.round(numeric));
    }
    return new Intl.NumberFormat("ru-RU", { maximumFractionDigits: 2 }).format(numeric);
  }

  function formatMetricValue(value, metric, label) {
    if (metric === "amount_usd") return `${formatNumber(value)} USD`;
    if (metric === "bl_count") return `${formatNumber(value)} BL`;
    return `${formatNumber(value)} ${label || "m³"}`;
  }

  function initials(name, fallback) {
    const value = String(name || "").trim();
    if (!value) return fallback;
    return value
      .split(/\s+/)
      .slice(0, 2)
      .map((part) => part.charAt(0).toUpperCase())
      .join("");
  }

  function updateClock() {
    const now = new Date();
    els.clock.textContent = [
      String(now.getHours()).padStart(2, "0"),
      String(now.getMinutes()).padStart(2, "0"),
      String(now.getSeconds()).padStart(2, "0"),
    ].join(":");
  }

  function startClock() {
    updateClock();
    clearInterval(state.clockHandle);
    state.clockHandle = window.setInterval(updateClock, 1000);
  }

  function renderCountdown() {
    const minutes = Math.floor(state.countdownSeconds / 60);
    const seconds = state.countdownSeconds % 60;
    els.rotationTimer.textContent = `${minutes}:${String(seconds).padStart(2, "0")}`;
    els.rotationLine.style.width = `${Math.max(0, Math.min(100, (state.countdownSeconds / REFRESH_SECONDS) * 100))}%`;
  }

  function restartCountdown() {
    state.countdownSeconds = REFRESH_SECONDS;
    renderCountdown();
    clearInterval(state.countdownHandle);
    state.countdownHandle = window.setInterval(() => {
      state.countdownSeconds = Math.max(0, state.countdownSeconds - 1);
      renderCountdown();
      if (state.countdownSeconds <= 0) {
        state.countdownSeconds = REFRESH_SECONDS;
      }
    }, 1000);
  }

  function populatePlans() {
    const planOptions = state.plans.length
      ? state.plans
          .map((plan) => `<option value="${escapeHtml(plan.id)}">${escapeHtml(plan.name || `Plan #${plan.id}`)}</option>`)
          .join("")
      : '<option value="">Plan tanlanmagan</option>';
    els.planSelect.innerHTML = planOptions;
    if (state.planId) {
      els.planSelect.value = String(state.planId);
    } else if (state.plans[0]) {
      state.planId = String(state.plans[0].id);
      els.planSelect.value = state.planId;
    }
    const plan = currentPlan();
    if (plan && plan.target_metric && !query.get("metric")) {
      state.metric = plan.target_metric;
    }
    els.metricSelect.value = state.metric;
  }

  function renderEmpty(message) {
    els.empty.textContent = message || "Ma'lumot topilmadi.";
    els.empty.hidden = false;
    els.empty.classList.add("active");
    els.main.classList.add("is-empty");
    els.bottom.classList.add("is-empty");
  }

  function clearEmpty() {
    els.empty.hidden = true;
    els.empty.classList.remove("active");
    els.main.classList.remove("is-empty");
    els.bottom.classList.remove("is-empty");
  }

  function renderArc(percent) {
    const circumference = 2 * Math.PI * CIRCLE_RADIUS;
    const normalized = Math.max(0, Math.min(100, Number(percent || 0)));
    const offset = circumference - (normalized / 100) * circumference;
    // V24 NEON GLASS TUBE — sync all 3 progress layers (halo glow, main neon, inner white filament)
    ["progress-arc", "progress-glow", "progress-inner"].forEach((id) => {
      const node = document.getElementById(id);
      if (!node) return;
      node.style.strokeDasharray = String(circumference);
      node.style.strokeDashoffset = String(offset);
    });
  }

  function renderMonthly(rows, metric, label) {
    if (!rows.length) {
      els.monthlyBars.innerHTML = '<div class="bar-item"><div class="bar-name">Ma\'lumot yo\'q</div></div>';
      return;
    }
    const max = Math.max(...rows.map((row) => Number(row.value || 0)), 0) || 1;
    els.monthlyBars.innerHTML = rows
      .map((row) => {
        const width = Math.max(6, (Number(row.value || 0) / max) * 100);
        return `
          <div class="bar-item">
            <div class="bar-row">
              <div class="bar-name">${escapeHtml(row.label)}</div>
              <div class="bar-value">${escapeHtml(formatMetricValue(row.value, metric, label))} • ${escapeHtml(String(row.bl_count || 0))} BL</div>
            </div>
            <div class="bar-track"><div class="bar-fill" style="width:${width}%"></div></div>
          </div>
        `;
      })
      .join("");
  }

  function leaderboardRow(item, index, tone) {
    const rankClass = index === 0 ? "r1" : index === 1 ? "r2" : index === 2 ? "r3" : "rn";
    const width = Math.max(6, Math.min(100, Number(item.share_percent || 0)));
    const valueText = escapeHtml(formatNumber(item.value || 0));
    const unitText = item.unit ? ` <span class="lscv-unit">${escapeHtml(item.unit)}</span>` : "";
    return `
      <div class="lr">
        <div class="lrank ${rankClass}">${index + 1}</div>
        <div class="lav ${tone}">${escapeHtml(item.initials || initials(item.name, tone === "blue" ? "ST" : "SM"))}</div>
        <div class="li">
          <div class="lname">${escapeHtml(item.name)}</div>
          <div class="lsub">${escapeHtml(String(item.bl_count || 0))} BL</div>
        </div>
        <div class="lsc">
          <div class="lscv">${valueText}${unitText}</div>
          <div class="lscs">${escapeHtml((Number(item.share_percent || 0)).toFixed(1))}%</div>
        </div>
        <div class="lbar"><div class="lbf ${tone}" style="width:${width}%"></div></div>
      </div>
    `;
  }

  function renderLeaders(container, rows, tone) {
    if (!rows.length) {
      container.innerHTML = '<div class="lr"><div class="lname">Ma\'lumot yo\'q</div></div>';
      return;
    }
    container.innerHTML = rows.map((row, index) => leaderboardRow(row, index, tone)).join("");
  }

  function shareTitle(metric) {
    if (metric === "cbm") return "KUB BO'YICHA REJA ULUSHI";
    if (metric === "bl_count") return "BL BO'YICHA REJA ULUSHI";
    return "DOLLAR BO'YICHA REJA ULUSHI";
  }

  function setDepartmentAccent(meta) {
    els.deptMonitor.classList.remove("ca-b", "ca-p");
    els.deptMonitor.classList.add(meta.accentClass);
    els.deptTitle.className = `ct ${meta.tone === "blue" ? "b" : "p"}`;
    els.deptTotal.className = meta.tone === "blue" ? "b" : "p";
    els.deptShare.className = meta.tone === "blue" ? "b" : "p";
    els.deptBl.className = meta.tone === "blue" ? "b" : "p";
  }

  function renderDepartment(key, payload) {
    // Backend provides:
    //   departments.logists  → SAVDO BO'LIMI    (leaders = LTL m³, .ftl = SAVDO trucks)
    //   departments.sales    → LOGISTIKA BO'LIMI (display_mode="ftl_only", trucks only)
    const meta = DEPARTMENT_META[key] || DEPARTMENT_META.logists;
    const department = payload.departments?.[key] || {};
    const plan = payload.plan || {};
    const metric = plan.metric || state.metric;
    const metricLabel = plan.metric_label || METRIC_LABELS[metric] || "m³";

    setDepartmentAccent(meta);
    els.deptTitle.textContent = meta.title;
    els.deptRotateNote.textContent = meta.note;

    const isSavdo = key === "logists";
    const isLogist = key === "sales";
    const showFtlNumbers = isSavdo && state.savdoView === "ftl" && department.ftl;
    const isFtlOnly = isLogist && department.display_mode === "ftl_only";

    // Pick header values + leaders source based on view
    let leadersToShow;

    if (isFtlOnly) {
      // LOGISTIKA BO'LIMI — trucks only, no plan share
      els.deptTotal.textContent = `${formatNumber(department.total_trucks || 0)} fura`;
      els.deptBl.textContent = formatNumber(department.total_bl || 0);
      if (els.deptShareRow) els.deptShareRow.hidden = true;
      leadersToShow = department.leaders || [];
    } else if (showFtlNumbers) {
      // SAVDO FTL sub-view — header AND leaderboard from FTL sheet
      const ftl = department.ftl || {};
      const target = Number(plan.target_value || 0);
      const ftlSharePct = target ? (Number(ftl.total_cbm || 0) / target * 100) : 0;
      els.deptTotal.textContent = `${formatNumber(ftl.total_trucks || 0)} fura`;
      els.deptShare.textContent = `${ftlSharePct.toFixed(1)}%`;
      els.deptBl.textContent = formatNumber(ftl.total_bl || 0);
      if (els.deptShareRow) els.deptShareRow.hidden = false;
      leadersToShow = ftl.leaders || [];          // ← FTL rows from FTL sheet
    } else {
      // SAVDO LTL view — header AND leaderboard from Ombor sheet
      els.deptTotal.textContent = formatMetricValue(department.closed_value || 0, metric, metricLabel);
      els.deptShare.textContent = `${Number(department.plan_share_percent || 0).toFixed(1)}%`;
      els.deptBl.textContent = formatNumber(department.bl_count || 0);
      if (els.deptShareRow) els.deptShareRow.hidden = false;
      leadersToShow = department.leaders || [];   // ← LTL rows from Ombor
    }

    // LTL/FTL pill at the end of the Jami row — only for SAVDO
    if (els.deptModeBadge) {
      if (isSavdo) {
        const mode = showFtlNumbers ? "FTL" : "LTL";
        els.deptModeBadge.hidden = false;
        els.deptModeBadge.textContent = mode;
        els.deptModeBadge.setAttribute("data-mode", mode);
      } else {
        els.deptModeBadge.hidden = true;
      }
    }

    renderLeaders(els.deptBoard, leadersToShow, meta.tone);
  }

  function animateDepartmentChange(nextKey) {
    els.deptBody.classList.add("is-animating");
    window.setTimeout(() => {
      state.currentDepartment = nextKey;
      if (state.latestPayload) {
        renderDepartment(nextKey, state.latestPayload);
      }
    }, 220);
    window.setTimeout(() => {
      els.deptBody.classList.remove("is-animating");
    }, 520);
  }

  function toggleDepartment(animated = true) {
    // Rotate between SAVDO BO'LIMI ↔ LOGISTIKA BO'LIMI.
    // On entering SAVDO, always start from LTL (so first impression = current m³ values).
    const nextKey = state.currentDepartment === "logists" ? "sales" : "logists";
    if (nextKey === "logists") state.savdoView = "ltl";

    // Cancel any pending SAVDO sub-view animation classes (race-condition safety)
    if (els.deptModeBadge) els.deptModeBadge.classList.remove("is-flip");
    if (els.deptBoard)     els.deptBoard.classList.remove("is-fading");
    // Reset SAVDO 5-sec view-rotation timer so it doesn't fire mid-book-turn
    if (typeof startSavdoViewRotation === "function") startSavdoViewRotation();

    if (animated) {
      animateDepartmentChange(nextKey);
    } else {
      state.currentDepartment = nextKey;
      if (state.latestPayload) {
        renderDepartment(nextKey, state.latestPayload);
      }
    }
  }

  function renderPayload(payload) {
    if (!payload || payload.empty) {
      renderEmpty(payload?.message || "Ma'lumot topilmadi.");
      return;
    }
    clearEmpty();
    state.latestPayload = payload;

    const isLive = payload.data_source === "ombor_live";
    const plan = payload.plan || {};
    const overall = payload.overall || {};
    const metric = plan.metric || state.metric;
    const metricLabel = plan.metric_label || METRIC_LABELS[metric] || "m³";
    const targetValue = Number(plan.target_value || 0);
    const closedValue = Number(overall.closed_value || 0);
    const remainingValue = Number(overall.remaining_value || 0);
    const progressPercent = Number(overall.progress_percent || 0);
    const totalBl = Number(overall.total_bl || 0);

    els.planName.textContent = plan.name || "—";
    els.planPeriod.textContent = plan.period_start && plan.period_end ? `${plan.period_start} → ${plan.period_end}` : "—";
    els.lastUpdated.textContent = payload.last_updated || "—";

    if (isLive) {
      els.sourceName.innerHTML = `<span class="source-live">LIVE · ${escapeHtml(payload.source_name || "Ombor")}</span>`;
    } else {
      els.sourceName.textContent = payload.source_name || "Google Sheets / XLSX cache";
    }

    els.monthlyMetricLabel.textContent = metric === "amount_usd" ? "USD" : metricLabel;
    els.shareTitle.textContent = shareTitle(metric);

    els.progressPercent.textContent = `${progressPercent.toFixed(1)}%`;
    els.planTarget.textContent = formatMetricValue(targetValue, metric, metricLabel);
    els.planClosed.textContent = formatMetricValue(closedValue, metric, metricLabel);
    els.planRemaining.textContent = formatMetricValue(remainingValue, metric, metricLabel);
    els.planBl.textContent = `${formatNumber(totalBl)} BL`;
    renderArc(progressPercent);

    if (overall.plan_completed) {
      els.planBadge.className = "plan-badge success";
      els.planBadge.textContent = Number(overall.overshoot_value || 0) > 0
        ? `Plan oshirib bajarildi: +${formatMetricValue(overall.overshoot_value || 0, metric, metricLabel)}`
        : "Plan bajarildi! 🎯";
    } else {
      els.planBadge.className = "plan-badge";
      els.planBadge.textContent = "Plan bajarilish jarayonida";
    }

    renderMonthly(payload.monthly || [], metric, metricLabel);

    const logists = payload.departments?.logists || {};
    const sales = payload.departments?.sales || {};
    const logistShare = Math.max(0, Math.min(100, Number(logists.plan_share_percent || 0)));
    const salesShare = Math.max(0, Math.min(100, Number(sales.plan_share_percent || 0)));
    els.logistsShareBar.style.width = `${logistShare}%`;
    els.logistsShareValue.textContent = `${logistShare.toFixed(1)}%`;
    els.salesShareBar.style.width = `${salesShare}%`;
    els.salesShareValue.textContent = `${salesShare.toFixed(1)}%`;

    renderDepartment(state.currentDepartment, payload);
  }

  async function fetchMonitor(resetCountdown, force) {
    const params = new URLSearchParams();
    if (state.planId) params.set("sales_plan_id", state.planId);
    if (state.metric) params.set("metric", state.metric);
    if (force) params.set("force", "1");

    const response = await fetch(`/analytics/api/monitor?${params.toString()}`, {
      headers: { Accept: "application/json" },
      credentials: "same-origin",
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.error || "Monitor ma'lumotlarini olishda xatolik.");
    }
    renderPayload(payload);
    if (resetCountdown) restartCountdown();
  }

  function scheduleRefresh() {
    clearInterval(state.refreshHandle);
    state.refreshHandle = window.setInterval(() => {
      // force=true: bypass 2-min cache so auto-refresh actually pulls fresh
      // data from Google Sheets when the rotation timer hits zero
      fetchMonitor(true, true).catch((error) => renderEmpty(error.message));
    }, REFRESH_SECONDS * 1000);
  }

  // Auto-rotate dept panel between SAVDO BO'LIMI ↔ LOGISTIKA BO'LIMI every 20 seconds.
  const DEPT_ROTATION_SECONDS = 20;
  function startDeptRotation() {
    clearInterval(state.deptRotationHandle);
    state.deptRotationHandle = window.setInterval(() => {
      if (state.latestPayload) toggleDepartment(true);
    }, DEPT_ROTATION_SECONDS * 1000);
  }

  // Inside SAVDO BO'LIMI: LTL ↔ FTL alternates every 5 seconds (header + leaderboard).
  // Important: must NOT overwrite the LOGISTIKA panel during dept-rotation race conditions.
  const SAVDO_VIEW_ROTATION_SECONDS = 5;
  function startSavdoViewRotation() {
    clearInterval(state.savdoViewHandle);
    state.savdoViewHandle = window.setInterval(() => {
      if (state.currentDepartment !== "logists") return;
      if (!state.latestPayload) return;
      state.savdoView = state.savdoView === "ltl" ? "ftl" : "ltl";
      // 3D flip on the pill + soft fade of the leaderboard cells (values change source)
      if (els.deptModeBadge) els.deptModeBadge.classList.add("is-flip");
      if (els.deptBoard) els.deptBoard.classList.add("is-fading");
      window.setTimeout(() => {
        // ⚠ Re-check inside the timeout — dept-rotation may have flipped us to
        // LOGISTIKA in the 240ms gap. Without this, we overwrite the LOGISTIKA
        // panel content with SAVDO and the user sees "no data" for LOGISTIKA.
        if (state.currentDepartment === "logists" && state.latestPayload) {
          renderDepartment("logists", state.latestPayload);
        }
        if (els.deptModeBadge) els.deptModeBadge.classList.remove("is-flip");
        if (els.deptBoard) els.deptBoard.classList.remove("is-fading");
      }, 240);
    }, SAVDO_VIEW_ROTATION_SECONDS * 1000);
  }

  function onPlanChange() {
    state.planId = els.planSelect.value;
    const selected = currentPlan();
    if (selected?.target_metric) {
      state.metric = selected.target_metric;
      els.metricSelect.value = state.metric;
    }
    const nextUrl = new URL(window.location.href);
    if (state.planId) nextUrl.searchParams.set("sales_plan_id", state.planId);
    nextUrl.searchParams.set("metric", state.metric);
    window.history.replaceState({}, "", nextUrl);
    fetchMonitor(true, false).catch((error) => renderEmpty(error.message));
  }

  function onMetricChange() {
    state.metric = els.metricSelect.value;
    const nextUrl = new URL(window.location.href);
    if (state.planId) nextUrl.searchParams.set("sales_plan_id", state.planId);
    nextUrl.searchParams.set("metric", state.metric);
    window.history.replaceState({}, "", nextUrl);
    fetchMonitor(true, false).catch((error) => renderEmpty(error.message));
  }

  // --- Config modal ---
  function openConfigModal() {
    const plan = currentPlan();
    if (!plan) return;
    // Ombor settings
    if (els.cfgSheetId) els.cfgSheetId.value = plan.ombor_sheet_id || "";
    if (els.cfgSheetName) els.cfgSheetName.value = plan.ombor_sheet_name || "Ombor";
    if (els.cfgCbmCol) els.cfgCbmCol.value = plan.ombor_cbm_col || "V";
    if (els.cfgDateCol) els.cfgDateCol.value = plan.ombor_date_col || "Z";
    if (els.cfgSellerCol) els.cfgSellerCol.value = plan.ombor_seller_col || "AG";
    if (els.cfgHeaderRows) els.cfgHeaderRows.value = plan.ombor_header_rows != null ? plan.ombor_header_rows : 2;
    // FTL settings (second sheet, full-truck sales)
    if (els.cfgFtlSheetId) els.cfgFtlSheetId.value = plan.ftl_sheet_id || "";
    if (els.cfgFtlGid) els.cfgFtlGid.value = plan.ftl_sheet_gid || "";
    if (els.cfgFtlTypeCol) els.cfgFtlTypeCol.value = plan.ftl_type_col || "J";
    if (els.cfgFtlDateCol) els.cfgFtlDateCol.value = plan.ftl_date_col || "L";
    if (els.cfgFtlSellerCol) els.cfgFtlSellerCol.value = plan.ftl_seller_col || "AB";
    if (els.cfgFtlHeaderRows) els.cfgFtlHeaderRows.value = plan.ftl_header_rows != null ? plan.ftl_header_rows : 1;
    if (els.cfgFtlCbmPerTruck) els.cfgFtlCbmPerTruck.value = plan.ftl_cbm_per_truck != null ? plan.ftl_cbm_per_truck : 10;
    if (els.cfgStatus) { els.cfgStatus.textContent = ""; els.cfgStatus.className = "cfg-status"; }
    if (els.cfgOverlay) els.cfgOverlay.hidden = false;
  }

  function closeConfigModal() {
    if (els.cfgOverlay) els.cfgOverlay.hidden = true;
  }

  async function saveConfig() {
    const plan = currentPlan();
    if (!plan || !state.planId) return;
    const sheetIdRaw = (els.cfgSheetId?.value || "").trim();
    // Extract sheet ID from full URL or use raw
    const sheetIdMatch = sheetIdRaw.match(/\/spreadsheets\/d\/([a-zA-Z0-9_-]+)/);
    const sheetId = sheetIdMatch ? sheetIdMatch[1] : sheetIdRaw;

    // Extract FTL Sheet ID from URL or use raw
    const ftlRaw = (els.cfgFtlSheetId?.value || "").trim();
    const ftlMatch = ftlRaw.match(/\/spreadsheets\/d\/([a-zA-Z0-9_-]+)/);
    const ftlId = ftlMatch ? ftlMatch[1] : ftlRaw;
    // Extract gid from URL if user pasted full URL into the ID box
    let ftlGid = (els.cfgFtlGid?.value || "").trim();
    if (!ftlGid) {
      const gidMatch = ftlRaw.match(/[?#&]gid=(\d+)/);
      if (gidMatch) ftlGid = gidMatch[1];
    }

    const body = {
      ombor_sheet_id: sheetId,
      ombor_sheet_name: (els.cfgSheetName?.value || "Ombor").trim(),
      ombor_cbm_col: (els.cfgCbmCol?.value || "V").trim().toUpperCase(),
      ombor_date_col: (els.cfgDateCol?.value || "Z").trim().toUpperCase(),
      ombor_seller_col: (els.cfgSellerCol?.value || "AG").trim().toUpperCase(),
      ombor_header_rows: parseInt(els.cfgHeaderRows?.value || "2", 10),
      // FTL — second sheet (full-truckload sales)
      ftl_sheet_id: ftlId,
      ftl_sheet_gid: ftlGid,
      ftl_type_col: (els.cfgFtlTypeCol?.value || "J").trim().toUpperCase(),
      ftl_date_col: (els.cfgFtlDateCol?.value || "L").trim().toUpperCase(),
      ftl_seller_col: (els.cfgFtlSellerCol?.value || "AB").trim().toUpperCase(),
      ftl_header_rows: parseInt(els.cfgFtlHeaderRows?.value || "1", 10),
      ftl_cbm_per_truck: parseFloat(els.cfgFtlCbmPerTruck?.value || "10"),
    };

    if (els.cfgStatus) { els.cfgStatus.textContent = "Saqlanmoqda…"; els.cfgStatus.className = "cfg-status"; }
    try {
      const resp = await fetch(`/analytics/api/plans/${state.planId}/ombor-config`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "same-origin",
        body: JSON.stringify(body),
      });
      const data = await resp.json();
      if (!resp.ok || data.error) throw new Error(data.error || "Xatolik");
      // Update local plan cache
      if (Array.isArray(data.plans)) {
        state.plans = data.plans;
        populatePlans();
      }
      if (els.cfgStatus) { els.cfgStatus.textContent = "✓ Saqlandi"; els.cfgStatus.className = "cfg-status"; }
      window.setTimeout(closeConfigModal, 800);
      fetchMonitor(true, true).catch((e) => renderEmpty(e.message));
    } catch (err) {
      if (els.cfgStatus) { els.cfgStatus.textContent = err.message; els.cfgStatus.className = "cfg-status error"; }
    }
  }

  function bindEvents() {
    els.planSelect.addEventListener("change", onPlanChange);
    els.metricSelect.addEventListener("change", onMetricChange);
    els.refreshBtn.addEventListener("click", () => {
      fetchMonitor(true, true).catch((error) => renderEmpty(error.message));
    });
    if (els.configBtn) els.configBtn.addEventListener("click", openConfigModal);
    if (els.cfgCancelBtn) els.cfgCancelBtn.addEventListener("click", closeConfigModal);
    if (els.cfgSaveBtn) els.cfgSaveBtn.addEventListener("click", saveConfig);
    if (els.cfgOverlay) {
      els.cfgOverlay.addEventListener("click", (e) => {
        if (e.target === els.cfgOverlay) closeConfigModal();
      });
    }
    els.fullscreenBtn.addEventListener("click", () => {
      if (!document.fullscreenElement) {
        document.documentElement.requestFullscreen?.();
      } else {
        document.exitFullscreen?.();
      }
    });
    document.addEventListener("fullscreenchange", () => {
      const active = !!document.fullscreenElement;
      document.body.classList.toggle("monitor-fullscreen", active);
      els.fullscreenBtn.textContent = active ? "Exit full screen" : "Full screen";
    });
  }

  function init() {
    populatePlans();
    bindEvents();
    startClock();
    renderCountdown();
    fetchMonitor(true, false).catch((error) => renderEmpty(error.message));
    scheduleRefresh();
    startDeptRotation();        // SAVDO ↔ LOGISTIKA every 20 s with book-turn animation
    startSavdoViewRotation();   // LTL ↔ FTL inside SAVDO every 5 s with soft fade
  }

  document.addEventListener("DOMContentLoaded", init);
})();
