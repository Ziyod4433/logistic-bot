(function () {
  const bootstrap = window.SALES_MONITOR_BOOTSTRAP || {};
  const query = new URLSearchParams(window.location.search);
  const state = {
    plans: Array.isArray(bootstrap.salesPlans) ? bootstrap.salesPlans : [],
    planId: query.get("sales_plan_id") || bootstrap.activePlanId || "",
    metric: query.get("metric") || "amount_usd",
    countdownSeconds: 300,
    countdownHandle: null,
    clockHandle: null,
    refreshHandle: null,
    currentDepartment: "logists",
    latestPayload: null,
  };

  const byId = (id) => document.getElementById(id);

  const els = {
    planSelect: byId("plan-select"),
    metricSelect: byId("metric-select"),
    refreshBtn: byId("refresh-btn"),
    fullscreenBtn: byId("fullscreen-btn"),
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
    shareTitle: byId("share-title"),
    logistsShareBar: byId("logists-share-bar"),
    logistsShareValue: byId("logists-share-value"),
    salesShareBar: byId("sales-share-bar"),
    salesShareValue: byId("sales-share-value"),
    main: document.querySelector(".main"),
    bottom: document.querySelector(".bottom"),
    empty: byId("monitor-empty"),
  };

  const METRIC_LABELS = {
    amount_usd: "USD",
    cbm: "m³",
    bl_count: "BL",
  };

  const DEPARTMENT_META = {
    logists: {
      key: "logists",
      title: "LOGISTLAR",
      tone: "blue",
      accentClass: "ca-b",
      note: "Keyingi bo'lim 5 daqiqada: Savdo bo‘limi",
    },
    sales: {
      key: "sales",
      title: "SAVDO BO‘LIMI",
      tone: "purple",
      accentClass: "ca-p",
      note: "Keyingi bo'lim 5 daqiqada: Logistlar",
    },
  };

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
    els.rotationLine.style.width = `${Math.max(0, Math.min(100, (state.countdownSeconds / 300) * 100))}%`;
  }

  function restartCountdown() {
    state.countdownSeconds = 300;
    renderCountdown();
    clearInterval(state.countdownHandle);
    state.countdownHandle = window.setInterval(() => {
      state.countdownSeconds = Math.max(0, state.countdownSeconds - 1);
      renderCountdown();
      if (state.countdownSeconds <= 0) {
        state.countdownSeconds = 300;
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
    els.empty.textContent = message || "Google Sheets ma’lumotlari hali import qilinmagan.";
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
    const radius = 78;
    const circumference = 2 * Math.PI * radius;
    const normalized = Math.max(0, Math.min(100, Number(percent || 0)));
    const offset = circumference - (normalized / 100) * circumference;
    els.progressArc.style.strokeDasharray = String(circumference);
    els.progressArc.style.strokeDashoffset = String(offset);
  }

  function renderMonthly(rows, metric, label) {
    if (!rows.length) {
      els.monthlyBars.innerHTML = '<div class="bar-item"><div class="bar-name">Ma’lumot yo‘q</div></div>';
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
    return `
      <div class="lr">
        <div class="lrank ${rankClass}">${index + 1}</div>
        <div class="lav ${tone}">${escapeHtml(item.initials || initials(item.name, tone === "blue" ? "LG" : "SM"))}</div>
        <div class="li">
          <div class="lname">${escapeHtml(item.name)}</div>
          <div class="lsub">${escapeHtml(String(item.bl_count || 0))} BL</div>
        </div>
        <div class="lsc">
          <div class="lscv">${escapeHtml(formatNumber(item.value || 0))}</div>
          <div class="lscs">${escapeHtml((Number(item.share_percent || 0)).toFixed(1))}%</div>
        </div>
        <div class="lbar"><div class="lbf ${tone}" style="width:${width}%"></div></div>
      </div>
    `;
  }

  function renderLeaders(container, rows, tone) {
    if (!rows.length) {
      container.innerHTML = '<div class="lr"><div class="lname">Ma’lumot yo‘q</div></div>';
      return;
    }
    container.innerHTML = rows.map((row, index) => leaderboardRow(row, index, tone)).join("");
  }

  function shareTitle(metric) {
    if (metric === "cbm") return "KUB BO‘YICHA REJA ULUSHI";
    if (metric === "bl_count") return "BL BO‘YICHA REJA ULUSHI";
    return "DOLLAR BO‘YICHA REJA ULUSHI";
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
    const meta = DEPARTMENT_META[key] || DEPARTMENT_META.logists;
    const department = payload.departments?.[key] || {};
    const plan = payload.plan || {};
    const metric = plan.metric || state.metric;
    const metricLabel = plan.metric_label || METRIC_LABELS[metric] || "USD";

    setDepartmentAccent(meta);
    els.deptTitle.textContent = meta.title;
    els.deptRotateNote.textContent = meta.note;
    els.deptTotal.textContent = formatMetricValue(department.closed_value || 0, metric, metricLabel);
    els.deptShare.textContent = `${Number(department.plan_share_percent || 0).toFixed(1)}%`;
    els.deptBl.textContent = formatNumber(department.bl_count || 0);
    renderLeaders(els.deptBoard, department.leaders || [], meta.tone);
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
    const nextKey = state.currentDepartment === "logists" ? "sales" : "logists";
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
      renderEmpty(payload?.message || "Google Sheets ma’lumotlari hali import qilinmagan.");
      return;
    }
    clearEmpty();
    state.latestPayload = payload;

    const plan = payload.plan || {};
    const overall = payload.overall || {};
    const metric = plan.metric || state.metric;
    const metricLabel = plan.metric_label || METRIC_LABELS[metric] || "USD";
    const targetValue = Number(plan.target_value || 0);
    const closedValue = Number(overall.closed_value || 0);
    const remainingValue = Number(overall.remaining_value || 0);
    const progressPercent = Number(overall.progress_percent || 0);
    const totalBl = Number(overall.total_bl || 0);

    els.planName.textContent = plan.name || "—";
    els.planPeriod.textContent = plan.period_start && plan.period_end ? `${plan.period_start} → ${plan.period_end}` : "—";
    els.lastUpdated.textContent = payload.last_updated || "—";
    els.sourceName.textContent = payload.source_name || "Google Sheets / XLSX cache";
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
        : "Plan bajarildi";
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

  async function fetchMonitor(resetCountdown) {
    const params = new URLSearchParams();
    if (state.planId) params.set("sales_plan_id", state.planId);
    if (state.metric) params.set("metric", state.metric);

    const response = await fetch(`/analytics/api/monitor?${params.toString()}`, {
      headers: { Accept: "application/json" },
      credentials: "same-origin",
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.error || "Monitor ma’lumotlarini olishda xatolik yuz berdi.");
    }
    renderPayload(payload);
    if (resetCountdown) restartCountdown();
  }

  function scheduleRefresh() {
    clearInterval(state.refreshHandle);
    state.refreshHandle = window.setInterval(() => {
      toggleDepartment(true);
      fetchMonitor(true).catch((error) => renderEmpty(error.message));
    }, 300000);
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
    fetchMonitor(true).catch((error) => renderEmpty(error.message));
  }

  function onMetricChange() {
    state.metric = els.metricSelect.value;
    const nextUrl = new URL(window.location.href);
    if (state.planId) nextUrl.searchParams.set("sales_plan_id", state.planId);
    nextUrl.searchParams.set("metric", state.metric);
    window.history.replaceState({}, "", nextUrl);
    fetchMonitor(true).catch((error) => renderEmpty(error.message));
  }

  function bindEvents() {
    els.planSelect.addEventListener("change", onPlanChange);
    els.metricSelect.addEventListener("change", onMetricChange);
    els.refreshBtn.addEventListener("click", () => {
      fetchMonitor(true).catch((error) => renderEmpty(error.message));
    });
    els.fullscreenBtn.addEventListener("click", () => {
      if (!document.fullscreenElement) {
        document.documentElement.requestFullscreen?.();
      } else {
        document.exitFullscreen?.();
      }
    });
  }

  function init() {
    populatePlans();
    bindEvents();
    startClock();
    renderCountdown();
    fetchMonitor(true).catch((error) => renderEmpty(error.message));
    scheduleRefresh();
  }

  document.addEventListener("DOMContentLoaded", init);
})();
