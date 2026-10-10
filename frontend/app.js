/**
 * Frontend Application Logic for Real-Time Purchase Trend Dashboard
 * Connects to FastAPI backend, manages SSE stream, renders Chart.js visualizations,
 * and handles MapReduce simulation and countdown timer.
 */

// State
let dashboardData = null;
let catalog = [];
let trendChart = null;
let topBarChart = null;
let donutChart = null;
let countdownSeconds = 300;
let eventSource = null;
let previousRanks = {};

// Department class mapping
const deptClasses = {
  produce: 'dept-produce',
  'dairy eggs': 'dept-dairy',
  beverages: 'dept-beverages',
  snacks: 'dept-snacks',
  'meat seafood': 'dept-meat'
};

// Initialize Dashboard
document.addEventListener('DOMContentLoaded', () => {
  initCharts();
  fetchCatalog();
  fetchDashboardSummary();
  initEventSource();
  initEventHandlers();
  startCountdownTicker();
});

// Setup Chart.js charts
function initCharts() {
  const isDark = true;
  const gridColor = 'rgba(255, 255, 255, 0.05)';
  const textColor = '#94A3B8';

  // 1. Trend Velocity Line Chart
  const ctxTrend = document.getElementById('trendVelocityChart').getContext('2d');
  trendChart = new Chart(ctxTrend, {
    type: 'line',
    data: {
      labels: [],
      datasets: []
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          labels: { color: textColor, font: { family: 'Inter', size: 11 } }
        },
        tooltip: {
          mode: 'index',
          intersect: false,
          backgroundColor: '#111622',
          titleColor: '#F8FAFC',
          bodyColor: '#94A3B8',
          borderColor: 'rgba(255,255,255,0.1)',
          borderWidth: 1
        }
      },
      scales: {
        x: {
          grid: { color: gridColor },
          ticks: { color: textColor, font: { family: 'JetBrains Mono', size: 10 } }
        },
        y: {
          grid: { color: gridColor },
          ticks: { color: textColor, font: { family: 'JetBrains Mono', size: 10 } }
        }
      }
    }
  });

  // 2. Top 5 Leaders Bar Chart
  const ctxBar = document.getElementById('top5BarChart').getContext('2d');
  topBarChart = new Chart(ctxBar, {
    type: 'bar',
    data: {
      labels: [],
      datasets: [{
        label: 'Units Sold',
        data: [],
        backgroundColor: [
          '#10B981', '#06B6D4', '#8B5CF6', '#F59E0B', '#F43F5E'
        ],
        borderRadius: 4
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: {
          grid: { display: false },
          ticks: { color: textColor, font: { size: 9 }, maxRotation: 20 }
        },
        y: {
          grid: { color: gridColor },
          ticks: { color: textColor, font: { size: 9 } }
        }
      }
    }
  });

  // 3. Department Share Donut Chart
  const ctxDonut = document.getElementById('departmentDonutChart').getContext('2d');
  donutChart = new Chart(ctxDonut, {
    type: 'doughnut',
    data: {
      labels: [],
      datasets: [{
        data: [],
        backgroundColor: [
          '#10B981', '#06B6D4', '#8B5CF6', '#F59E0B', '#F43F5E', '#3B82F6', '#EC4899', '#6366F1'
        ],
        borderWidth: 0
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          position: 'right',
          labels: { color: textColor, boxWidth: 10, font: { size: 9 } }
        }
      },
      cutout: '65%'
    }
  });
}

// Fetch dashboard data
async function fetchDashboardSummary() {
  try {
    const res = await fetch('/api/dashboard/summary');
    if (!res.ok) return;
    dashboardData = await res.json();
    renderDashboard(dashboardData);
    fetchTrends();
  } catch (err) {
    console.error('Error fetching dashboard summary:', err);
  }
}

// Fetch trend line data
async function fetchTrends() {
  try {
    const res = await fetch('/api/dashboard/trends?top_n=5');
    if (!res.ok) return;
    const trendData = await res.json();
    renderTrendsChart(trendData);
  } catch (err) {
    console.error('Error fetching trends:', err);
  }
}

// Fetch Product Catalog for Promo Dropdown
async function fetchCatalog() {
  try {
    const res = await fetch('/api/dashboard/catalog');
    if (!res.ok) return;
    catalog = await res.json();
    const select = document.getElementById('promoProductSelect');
    select.innerHTML = catalog.map(p => 
      `<option value="${p.product_id}">${p.product_name} (${p.department})</option>`
    ).join('');
  } catch (err) {
    console.error('Error loading catalog:', err);
  }
}

// Render Dashboard Data
function renderDashboard(data) {
  if (!data) return;

  // KPIs
  document.getElementById('kpiTotalUnits').textContent = data.metrics.total_units_sold.toLocaleString();
  document.getElementById('kpiTotalRevenue').textContent = `$${data.metrics.total_revenue.toLocaleString(undefined, { minimumFractionDigits: 2 })}`;
  document.getElementById('kpiUniqueProducts').textContent = `Aggregated across ${data.metrics.unique_active_products} active products`;
  document.getElementById('kpiKafkaRate').textContent = data.metrics.current_stream_rate_tps.toFixed(1);
  document.getElementById('kpiTotalIngested').textContent = `${data.metrics.total_transactions_ingested.toLocaleString()} total purchase events stored`;
  document.getElementById('kpiHdfsUsage').textContent = `${data.metrics.hdfs_used_mb} MB`;
  
  // Arch Node Metrics
  document.getElementById('archTpsMetric').textContent = `${data.metrics.current_stream_rate_tps.toFixed(1)} tx/s`;
  document.getElementById('archHdfsRecords').textContent = `${data.metrics.total_transactions_ingested} records`;

  // Render Top 20 Table
  renderTop20Table(data.top_products);

  // Render Top 5 Bar Chart
  const top5 = data.top_products.slice(0, 5);
  topBarChart.data.labels = top5.map(p => p.product_name.length > 14 ? p.product_name.slice(0, 14) + '...' : p.product_name);
  topBarChart.data.datasets[0].data = top5.map(p => p.units_sold);
  topBarChart.update();

  // Render Department Donut Chart
  const depts = data.department_breakdown;
  donutChart.data.labels = Object.keys(depts).map(d => d.charAt(0).toUpperCase() + d.slice(1));
  donutChart.data.datasets[0].data = Object.values(depts);
  donutChart.update();

  // Update Scheduler Countdown value
  if (data.scheduler && typeof data.scheduler.seconds_until_next_refresh === 'number') {
    countdownSeconds = data.scheduler.seconds_until_next_refresh;
  }
}

// Render Top 20 Table with Category filter & Trend badges
function renderTop20Table(products) {
  const tbody = document.getElementById('topProductsTableBody');
  const selectedDept = document.getElementById('departmentFilter').value;

  const filtered = selectedDept === 'ALL' 
    ? products 
    : products.filter(p => p.department.toLowerCase() === selectedDept.toLowerCase());

  if (filtered.length === 0) {
    tbody.innerHTML = `<tr><td colspan="7" class="loading-state">No products found in department "${selectedDept}".</td></tr>`;
    return;
  }

  tbody.innerHTML = filtered.map(p => {
    // Medal badge for top 3
    let rankBadgeClass = 'rank-norm';
    if (p.rank === 1) rankBadgeClass = 'rank-1';
    else if (p.rank === 2) rankBadgeClass = 'rank-2';
    else if (p.rank === 3) rankBadgeClass = 'rank-3';

    // Department badge class
    const deptClass = deptClasses[p.department.toLowerCase()] || '';

    // Shift trend indicator (check if product shifted rank or has high volume share)
    const prevRank = previousRanks[p.product_id];
    let trendHtml = '<span class="trend-badge trend-stable"><i class="fa-solid fa-check"></i> Steady</span>';
    if (prevRank && prevRank > p.rank) {
      trendHtml = `<span class="trend-badge trend-surging"><i class="fa-solid fa-arrow-trend-up"></i> +${prevRank - p.rank}</span>`;
    } else if (p.rank <= 3) {
      trendHtml = '<span class="trend-badge trend-surging"><i class="fa-solid fa-fire"></i> Hot</span>';
    }

    // Save rank for shift comparison
    previousRanks[p.product_id] = p.rank;

    return `
      <tr>
        <td>
          <span class="rank-badge ${rankBadgeClass}">#${p.rank}</span>
        </td>
        <td>
          <div class="product-cell">
            <span class="product-name-txt">${p.product_name}</span>
            <div class="product-sub-info">
              <span class="dept-pill ${deptClass}">${p.department}</span>
              <span class="text-muted" style="font-size: 11px;">ID: ${p.product_id} &bull; $${p.avg_price.toFixed(2)}</span>
            </div>
          </div>
        </td>
        <td class="text-right">
          <span class="units-sold-val">${p.units_sold.toLocaleString()}</span>
        </td>
        <td>
          <div class="share-bar-wrap">
            <div class="share-bar-bg">
              <div class="share-bar-fill" style="width: ${Math.min(100, p.share_pct * 3)}%;"></div>
            </div>
            <span class="share-bar-pct">${p.share_pct}%</span>
          </div>
        </td>
        <td class="text-right">
          <span class="rev-val">$${p.revenue.toLocaleString(undefined, { minimumFractionDigits: 2 })}</span>
        </td>
        <td class="text-center">
          ${trendHtml}
        </td>
        <td class="text-center">
          <button class="btn-mini-promo" onclick="triggerPromoForProduct(${p.product_id})" title="Simulate 4x promotion spike">
            <i class="fa-solid fa-bolt"></i> Promo
          </button>
        </td>
      </tr>
    `;
  }).join('');
}

// Render Trend Line Chart
function renderTrendsChart(trendData) {
  if (!trendData || !trendChart) return;
  const colors = ['#10B981', '#06B6D4', '#8B5CF6', '#F59E0B', '#F43F5E'];
  
  trendChart.data.labels = trendData.labels;
  trendChart.data.datasets = trendData.series.map((s, idx) => ({
    label: s.product_name,
    data: s.data,
    borderColor: colors[idx % colors.length],
    backgroundColor: 'transparent',
    borderWidth: 2,
    tension: 0.35,
    pointRadius: 2,
    pointHoverRadius: 5
  }));
  trendChart.update();
}

// Countdown Ticker (Visual 5-minute countdown clock)
function startCountdownTicker() {
  setInterval(() => {
    if (countdownSeconds > 0) {
      countdownSeconds--;
    } else {
      // Countdown reached 0: MapReduce runs automatically!
      fetchDashboardSummary();
      countdownSeconds = 300;
    }

    const mins = Math.floor(countdownSeconds / 60);
    const secs = countdownSeconds % 60;
    const formatted = `${String(mins).padStart(2, '0')}:${String(secs).padStart(2, '0')}`;
    const display = document.getElementById('countdownDisplay');
    if (display) display.textContent = formatted;
  }, 1000);
}

// Server-Sent Events (SSE) Live Feed
function initEventSource() {
  eventSource = new EventSource('/api/stream/events');

  eventSource.onmessage = (event) => {
    try {
      const msg = JSON.parse(event.data);
      if (msg.type === 'TRANSACTION') {
        prependTransactionTicker(msg.payload);
      } else if (msg.type === 'HEARTBEAT') {
        countdownSeconds = msg.countdown;
        document.getElementById('archTpsMetric').textContent = `${msg.tps.toFixed(1)} tx/s`;
        document.getElementById('kpiKafkaRate').textContent = msg.tps.toFixed(1);
      }
    } catch (err) {
      console.error('SSE parse error:', err);
    }
  };

  eventSource.onerror = () => {
    // Reconnect gracefully
    setTimeout(initEventSource, 3000);
  };
}

// Live Transaction Feed Ticker (Latest 8 items)
function prependTransactionTicker(tx) {
  const container = document.getElementById('liveTickerList');
  if (!container) return;

  const item = document.createElement('div');
  item.className = 'ticker-item';
  item.innerHTML = `
    <div>
      <span class="ticker-pname">${tx.product_name}</span>
      <span class="dept-pill" style="font-size: 9px; margin-left: 6px;">${tx.department}</span>
    </div>
    <span class="ticker-qty">+${tx.quantity} units</span>
  `;

  container.insertBefore(item, container.firstChild);
  if (container.children.length > 8) {
    container.removeChild(container.lastChild);
  }
}

// Execute MapReduce Job with Multi-Step Animated Progress Modal
async function triggerMapReduceNow() {
  const modal = document.getElementById('mrProgressModal');
  modal.style.display = 'block';

  const steps = [
    document.getElementById('mrStep1'),
    document.getElementById('mrStep2'),
    document.getElementById('mrStep3'),
    document.getElementById('mrStep4'),
    document.getElementById('mrStep5')
  ];

  // Reset steps
  steps.forEach(s => {
    s.className = 'mr-step-item';
  });

  // Step 1: Input split
  steps[0].classList.add('active');
  await new Promise(r => setTimeout(r, 200));
  steps[0].classList.replace('active', 'done');

  // Step 2: Mapper
  steps[1].classList.add('active');
  await new Promise(r => setTimeout(r, 250));
  steps[1].classList.replace('active', 'done');

  // Step 3: Shuffle
  steps[2].classList.add('active');
  await new Promise(r => setTimeout(r, 200));
  steps[2].classList.replace('active', 'done');

  // Step 4: Reducer
  steps[3].classList.add('active');

  try {
    const res = await fetch('/api/pipeline/mapreduce/trigger', { method: 'POST' });
    const result = await res.json();
    steps[3].classList.replace('active', 'done');

    // Step 5: Commit to HDFS
    steps[4].classList.add('active');
    await new Promise(r => setTimeout(r, 200));
    steps[4].classList.replace('active', 'done');

    // Refresh views
    await fetchDashboardSummary();
  } catch (err) {
    console.error('MapReduce trigger failed:', err);
  } finally {
    setTimeout(() => {
      modal.style.display = 'none';
    }, 1000);
  }
}

// Trigger Promotion for specific product
window.triggerPromoForProduct = async function(productId) {
  try {
    const res = await fetch('/api/pipeline/producer/surge', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ product_id: productId, multiplier: 4.0, duration_seconds: 45 })
    });
    const data = await res.json();
    
    // Show Alert Banner
    const banner = document.getElementById('surgeAlertBanner');
    const text = document.getElementById('surgeAlertText');
    const prod = catalog.find(p => p.product_id === productId);
    text.textContent = `Product #${productId} (${prod ? prod.product_name : 'Selected Item'}) surging with 4x transaction multiplier for 45s.`;
    banner.style.display = 'flex';

    // Auto trigger MapReduce after a brief interval so user can see it rise!
    setTimeout(() => {
      triggerMapReduceNow();
    }, 4000);

  } catch (err) {
    console.error('Promo trigger error:', err);
  }
};

// UI Event Handlers
function initEventHandlers() {
  // Trigger MapReduce button
  document.getElementById('btnTriggerMR').addEventListener('click', triggerMapReduceNow);

  // Department Filter
  document.getElementById('departmentFilter').addEventListener('change', () => {
    if (dashboardData) renderTop20Table(dashboardData.top_products);
  });

  // Modal open/close
  const simModal = document.getElementById('simModal');
  document.getElementById('btnToggleSimModal').addEventListener('click', () => simModal.style.display = 'flex');
  document.getElementById('btnCloseSimModal').addEventListener('click', () => simModal.style.display = 'none');

  const hdfsModal = document.getElementById('hdfsModal');
  document.getElementById('btnToggleHdfsModal').addEventListener('click', () => {
    loadHdfsAndMrLogs();
    hdfsModal.style.display = 'flex';
  });
  document.getElementById('btnCloseHdfsModal').addEventListener('click', () => hdfsModal.style.display = 'none');

  // Rate Slider
  const rateSlider = document.getElementById('rateSlider');
  const rateLabel = document.getElementById('rateValueLabel');
  rateSlider.addEventListener('input', (e) => {
    rateLabel.textContent = `${e.target.value}.0 tx/s`;
  });
  rateSlider.addEventListener('change', async (e) => {
    await fetch('/api/pipeline/producer/rate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ rate: parseFloat(e.target.value) })
    });
  });

  // Play / Pause Stream
  document.getElementById('btnPauseStream').addEventListener('click', async () => {
    await fetch('/api/pipeline/producer/pause', { method: 'POST' });
    document.getElementById('btnPauseStream').classList.add('active');
    document.getElementById('btnResumeStream').classList.remove('active');
  });

  document.getElementById('btnResumeStream').addEventListener('click', async () => {
    await fetch('/api/pipeline/producer/resume', { method: 'POST' });
    document.getElementById('btnResumeStream').classList.add('active');
    document.getElementById('btnPauseStream').classList.remove('active');
  });

  // Interval buttons
  document.getElementById('btnInterval5m').addEventListener('click', () => changeInterval(300, 'btnInterval5m'));
  document.getElementById('btnInterval30s').addEventListener('click', () => changeInterval(30, 'btnInterval30s'));
  document.getElementById('btnInterval60s').addEventListener('click', () => changeInterval(60, 'btnInterval60s'));

  // Promo Inject Button from modal
  document.getElementById('btnInjectPromo').addEventListener('click', () => {
    const select = document.getElementById('promoProductSelect');
    const pId = parseInt(select.value);
    triggerPromoForProduct(pId);
    simModal.style.display = 'none';
  });

  // Cancel Surge
  document.getElementById('btnCancelSurge').addEventListener('click', () => {
    document.getElementById('surgeAlertBanner').style.display = 'none';
  });

  // Inspector Tabs
  document.getElementById('tabBtnMR').addEventListener('click', () => {
    document.getElementById('tabBtnMR').classList.add('active');
    document.getElementById('tabBtnHDFS').classList.remove('active');
    document.getElementById('tabContentMR').style.display = 'block';
    document.getElementById('tabContentHDFS').style.display = 'none';
  });

  document.getElementById('tabBtnHDFS').addEventListener('click', () => {
    document.getElementById('tabBtnHDFS').classList.add('active');
    document.getElementById('tabBtnMR').classList.remove('active');
    document.getElementById('tabContentHDFS').style.display = 'block';
    document.getElementById('tabContentMR').style.display = 'none';
  });
}

async function changeInterval(seconds, btnId) {
  await fetch('/api/pipeline/scheduler/interval', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ interval_seconds: seconds })
  });
  countdownSeconds = seconds;
  ['btnInterval5m', 'btnInterval30s', 'btnInterval60s'].forEach(id => {
    document.getElementById(id).classList.toggle('active', id === btnId);
  });
}

// Load HDFS and MR Logs into inspector
async function loadHdfsAndMrLogs() {
  try {
    const mrRes = await fetch('/api/mapreduce/history');
    const mrData = await mrRes.json();
    if (mrData.last_job) {
      const m = mrData.last_job.metrics;
      document.getElementById('mrMapIn').textContent = m.map_input_records.toLocaleString();
      document.getElementById('mrMapOut').textContent = m.map_output_records.toLocaleString();
      document.getElementById('mrReduceGroups').textContent = m.reduce_input_groups.toLocaleString();
      document.getElementById('mrDuration').textContent = `${mrData.last_job.execution_duration_ms} ms`;
    }

    const consoleBox = document.getElementById('mrConsoleBox');
    consoleBox.innerHTML = (mrData.job_history || []).map(j => `
      <div>[${j.executed_at}] Job ${j.job_id} (${j.job_status}) | Map In: ${j.metrics.map_input_records} | Reduce Groups: ${j.metrics.reduce_input_groups} | Duration: ${j.execution_duration_ms}ms</div>
    `).reverse().join('');

    const hdfsRes = await fetch('/api/hdfs/files?limit=25');
    const hdfsFiles = await hdfsRes.json();
    const hdfsBody = document.getElementById('hdfsFileTableBody');
    hdfsBody.innerHTML = hdfsFiles.map(f => `
      <tr>
        <td style="color: #38BDF8;">${f.path}</td>
        <td>${(f.size_bytes / 1024).toFixed(1)} KB</td>
        <td>${f.replication}x</td>
        <td>${f.permission}</td>
      </tr>
    `).join('');
  } catch (err) {
    console.error('Inspector load error:', err);
  }
}
