(function () {
  const bootstrap = window.SALES_MONITOR_BOOTSTRAP || {};
  const query = new URLSearchParams(window.location.search);
  const REFRESH_SECONDS = 120;            // 2-min data refresh (background, silent)
  // Kiosk profile (login `sales` / `sales123`) — no UI to switch plans.
  // We deliberately DON'T lock to the bootstrap.activePlanId here, because
  // the TV stays open for days/weeks while the admin may activate new
  // monthly plans. By sending no sales_plan_id to the backend, the
  // server's _active_plan() resolves the current active plan every poll,
  // so the kiosk auto-follows whichever plan admin flagged as active.
  const isKiosk = !!bootstrap.isKiosk
    || (typeof document !== "undefined" && document.body && document.body.classList.contains("role-kiosk"));

  // ── Rotation segments (one full cycle = 50 sec) ──────────────────
  // The bottom progress bar tracks the CURRENT segment's duration so it
  // visually fills/empties in sync with each transition (LTL→FTL, FTL→LOGISTIKA, LOGISTIKA→LTL).
  const SEGMENTS = [
    { id: "savdo_ltl", dept: "logists", view: "ltl", seconds: 25 },
    { id: "savdo_ftl", dept: "logists", view: "ftl", seconds: 10 },
    { id: "logistika", dept: "sales",   view: null, seconds: 15 },
  ];

  const state = {
    plans: Array.isArray(bootstrap.salesPlans) ? bootstrap.salesPlans : [],
    // Kiosk: leave planId empty so /analytics/api/monitor always falls
    // through to the backend's current active plan. Admin/editor users
    // keep the URL-pinned-or-bootstrap-active behavior so they can
    // switch via the dropdown.
    planId: isKiosk
      ? ""
      : (query.get("sales_plan_id") || bootstrap.activePlanId || ""),
    metric: query.get("metric") || "cbm",
    segmentIndex: 0,                       // pointer into SEGMENTS
    segmentDurationSeconds: SEGMENTS[0].seconds,   // denominator for countdown bar
    countdownSeconds: SEGMENTS[0].seconds,
    countdownHandle: null,
    clockHandle: null,
    refreshHandle: null,
    segmentTimerId: null,             // setTimeout for next segment transition
    currentDepartment: "logists",
    savdoView: "ltl",                 // "ltl" (Ombor m³) | "ftl" (truck count)
    latestPayload: null,
    // Refresh-health tracking — used by updateStaleWarning() to flag the
    // top-of-screen source chip when Google Sheets data stops updating.
    lastSuccessfulFetchMs: 0,
    consecutiveFetchErrors: 0,
    lastFetchError: "",
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
    planStatusBanner: byId("plan-status-banner"),
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
    // Bar width = remaining fraction of the CURRENT segment's duration
    const denom = state.segmentDurationSeconds || 1;
    els.rotationLine.style.width = `${Math.max(0, Math.min(100, (state.countdownSeconds / denom) * 100))}%`;
    // Piggy-back on the per-second tick to refresh stale-state styling.
    updateStaleWarning();
  }

  function restartCountdown(seconds) {
    const total = Number(seconds) > 0 ? Number(seconds) : (state.segmentDurationSeconds || 20);
    state.segmentDurationSeconds = total;
    state.countdownSeconds = total;
    renderCountdown();
    clearInterval(state.countdownHandle);
    state.countdownHandle = window.setInterval(() => {
      state.countdownSeconds = Math.max(0, state.countdownSeconds - 1);
      renderCountdown();
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

  // Apply the segment at given index — animates the transition then arms the next.
  // SAVDO_LTL (25s) → SAVDO_FTL (10s) → LOGISTIKA (15s) → loop.
  function applySegment(idx) {
    const prev = SEGMENTS[state.segmentIndex];
    const seg  = SEGMENTS[idx];
    state.segmentIndex = idx;

    const needsDeptSwap = prev.dept !== seg.dept;

    // Clear any pending sub-view animation classes from a previous tick
    if (els.deptModeBadge) els.deptModeBadge.classList.remove("is-flip");
    if (els.deptBoard)     els.deptBoard.classList.remove("is-fading");

    if (needsDeptSwap) {
      // Cross-panel switch: book-page-turn animation
      state.savdoView = seg.view || state.savdoView;
      animateDepartmentChange(seg.dept);
    } else {
      // Same panel, different sub-view (LTL ↔ FTL) — soft flip + fade
      state.savdoView = seg.view || state.savdoView;
      if (els.deptModeBadge) els.deptModeBadge.classList.add("is-flip");
      if (els.deptBoard)     els.deptBoard.classList.add("is-fading");
      window.setTimeout(() => {
        if (state.latestPayload) renderDepartment(seg.dept, state.latestPayload);
        if (els.deptModeBadge) els.deptModeBadge.classList.remove("is-flip");
        if (els.deptBoard)     els.deptBoard.classList.remove("is-fading");
      }, 240);
    }

    // Reset countdown bar with this segment's own duration
    restartCountdown(seg.seconds);

    // Arm the next transition (with the time of THIS segment)
    clearTimeout(state.segmentTimerId);
    state.segmentTimerId = window.setTimeout(() => {
      applySegment((idx + 1) % SEGMENTS.length);
    }, seg.seconds * 1000);
  }

  function startSegmentRotation() {
    clearTimeout(state.segmentTimerId);
    // Start fresh from segment 0 (SAVDO LTL)
    state.segmentIndex = 0;
    state.currentDepartment = SEGMENTS[0].dept;
    state.savdoView = SEGMENTS[0].view;
    restartCountdown(SEGMENTS[0].seconds);
    if (state.latestPayload) renderDepartment(SEGMENTS[0].dept, state.latestPayload);
    state.segmentTimerId = window.setTimeout(() => {
      applySegment(1);
    }, SEGMENTS[0].seconds * 1000);
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

    // Plan-vs-sheet status banner inside UMUMIY PLAN card. Backend sets
    // payload.plan_data_status.state to one of:
    //   "ok"               — data present, banner hidden
    //   "empty_in_period"  — sheet has data, but none falls inside the
    //                        active plan's date range (typical when
    //                        operator activates next-month plan before
    //                        any rows are dated for that month)
    //   "sheet_empty"      — wide-range fetch returned nothing at all
    //                        (column config wrong, sheet access lost, etc.)
    if (els.planStatusBanner) {
      const status = payload.plan_data_status || {};
      const state = String(status.state || "ok");
      if (state === "empty_in_period" || state === "sheet_empty") {
        const icon = state === "sheet_empty" ? "⛔" : "⚠️";
        const hint = state === "sheet_empty"
          ? "Проверь URL таблицы, доступ (Anyone with link · Viewer) и колонки CBM / SANA / SOTUVCHI."
          : "Скорректируй период плана или внеси строки с датами в этот период.";
        els.planStatusBanner.dataset.state = state;
        els.planStatusBanner.innerHTML =
          `<span class="psb-icon">${icon}</span>` +
          `<span class="psb-body">${escapeHtml(status.message || "")}` +
          `<div class="psb-hint">${escapeHtml(hint)}</div></span>`;
        els.planStatusBanner.hidden = false;
      } else {
        els.planStatusBanner.hidden = true;
        els.planStatusBanner.innerHTML = "";
      }
    }

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
    // Cache-buster — neutralises any HTTP intermediary that ignores
    // our Cache-Control header (CDNs, ServiceWorkers, browser stale-
    // while-revalidate). Without this, /analytics/api/monitor would
    // sometimes be served from a stale cache layer.
    params.set("_t", Date.now().toString());

    const response = await fetch(`/analytics/api/monitor?${params.toString()}`, {
      headers: {
        Accept: "application/json",
        "Cache-Control": "no-cache",
        Pragma: "no-cache",
      },
      credentials: "same-origin",
      cache: "no-store",
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.error || "Monitor ma'lumotlarini olishda xatolik.");
    }
    renderPayload(payload);
    state.lastSuccessfulFetchMs = Date.now();
    state.consecutiveFetchErrors = 0;
    if (resetCountdown) restartCountdown();
  }

  function scheduleRefresh() {
    clearInterval(state.refreshHandle);
    state.refreshHandle = window.setInterval(() => {
      // force=true: bypass 2-min cache. Silent data refresh — do NOT reset the
      // visual countdown bar (that's owned by the 20-s dept-rotation cycle).
      // On failure, KEEP showing the last good payload (don't blank the screen).
      // We just bump an error counter so the diagnostic chip can warn the
      // operator that updates are stuck.
      fetchMonitor(false, true).catch((error) => {
        state.consecutiveFetchErrors = (state.consecutiveFetchErrors || 0) + 1;
        state.lastFetchError = error && error.message ? error.message : String(error);
        // Only blank the screen if we *never* had successful data AND keep
        // failing — i.e. fresh page load that never bootstrapped.
        if (!state.latestPayload) {
          renderEmpty(state.lastFetchError);
        }
        // Otherwise leave the last good numbers visible; updateStaleWarning()
        // (called on every countdown tick) will flag stale state in the UI.
      });
    }, REFRESH_SECONDS * 1000);
  }

  // Called every second from renderCountdown. If the last successful fetch
  // is more than 2× the refresh interval old, mark the source line as stale
  // so the TV operator can see something is wrong without having to scroll
  // the network log.
  function updateStaleWarning() {
    if (!els.sourceName) return;
    if (!state.lastSuccessfulFetchMs) return;
    const ageSec = Math.floor((Date.now() - state.lastSuccessfulFetchMs) / 1000);
    const staleThreshold = REFRESH_SECONDS * 2 + 10;  // ~250 sec
    if (ageSec > staleThreshold) {
      els.sourceName.dataset.stale = "1";
      els.sourceName.title = `Последнее обновление: ${ageSec} сек назад. Ошибка: ${state.lastFetchError || "?"}`;
    } else {
      delete els.sourceName.dataset.stale;
      els.sourceName.title = "";
    }
  }

  // ↑ Old startDeptRotation + startSavdoViewRotation replaced by the unified
  //   segment scheduler (applySegment / startSegmentRotation) above.

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
    startSegmentRotation();     // SAVDO_LTL 25 s → SAVDO_FTL 10 s → LOGISTIKA 15 s → loop
  }

  document.addEventListener("DOMContentLoaded", init);
})();
