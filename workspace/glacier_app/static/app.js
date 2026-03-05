(function () {
  const bboxInput = document.getElementById("bboxInput");
  const sourceInput = document.getElementById("sourceInput");
  const selectionModeInput = document.getElementById("selectionModeInput");
  const glacierRadiusInput = document.getElementById("glacierRadiusInput");
  const relationTextInput = document.getElementById("relationTextInput");
  const selectRelationBtn = document.getElementById("selectRelationBtn");
  const resolutionInput = document.getElementById("resolutionInput");
  const datasetInput = document.getElementById("datasetInput");
  const gridSizeInput = document.getElementById("gridSizeInput");
  const plotDownsampleInput = document.getElementById("plotDownsampleInput");
  const tau0Input = document.getElementById("tau0Input");
  const fPrimeInput = document.getElementById("fPrimeInput");
  const slopeUnitsInput = document.getElementById("slopeUnitsInput");
  const apiKeyInput = document.getElementById("apiKeyInput");
  const runFullBtn = document.getElementById("runFullBtn");
  const cancelRunBtn = document.getElementById("cancelRunBtn");
  const reanalyzeBtn = document.getElementById("reanalyzeBtn");
  const pastJobsSelect = document.getElementById("pastJobsSelect");
  const refreshJobsBtn = document.getElementById("refreshJobsBtn");
  const loadJobBtn = document.getElementById("loadJobBtn");
  const responseBox = document.getElementById("responseBox");
  const statusPill = document.getElementById("statusPill");
  const reportLink = document.getElementById("downloadReportLink");
  const apiWarning = document.getElementById("apiWarning");
  const dataSourceStatus = document.getElementById("dataSourceStatus");
  const runProgressWrap = document.getElementById("runProgressWrap");
  const runProgressLabel = document.getElementById("runProgressLabel");
  const runProgressCount = document.getElementById("runProgressCount");
  const runProgressFill = document.getElementById("runProgressFill");
  const depthLegendRange = document.getElementById("depthLegendRange");

  const demPlot = document.getElementById("demPlot");
  const slopePlot = document.getElementById("slopePlot");
  const flowPlot = document.getElementById("flowPlot");
  const depthPlot = document.getElementById("depthPlot");
  const depthTopoMapEl = document.getElementById("depthTopoMap");
  const flowTopoMapEl = document.getElementById("flowTopoMap");

  const map = L.map("map").setView([37.77, -122.44], 11);
  L.tileLayer("https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png", {
    maxZoom: 17,
    attribution:
      'Map data: &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors, ' +
      '<a href="https://viewfinderpanoramas.org">SRTM</a> | ' +
      'Map style: &copy; <a href="https://opentopomap.org">OpenTopoMap</a>'
  }).addTo(map);
  L.control.scale({ imperial: false, position: "bottomleft" }).addTo(map);

  const drawnItems = new L.FeatureGroup().addTo(map);
  const drawControl = new L.Control.Draw({
    edit: { featureGroup: drawnItems },
    draw: {
      polygon: false,
      polyline: false,
      circle: false,
      marker: false,
      circlemarker: false,
      rectangle: true
    }
  });
  map.addControl(drawControl);

  let bboxLayer = null;
  let selectedGlacierLayer = null;
  let selectedGlacierGeojson = null;
  let outlineLayer = null;
  let flowLayer = null;
  let currentJobId = "";
  let depthTopoMap = null;
  let flowTopoMap = null;
  let depthOverlayLayer = null;
  let flowOverlayLayer = null;
  let flowTopoOutlineLayer = null;
  let flowTopoFlowLayer = null;
  let runStatusTimer = null;

  function setPastJobsOptions(jobs) {
    if (!pastJobsSelect) return;
    pastJobsSelect.innerHTML = "";
    if (!jobs || jobs.length === 0) {
      const empty = document.createElement("option");
      empty.value = "";
      empty.textContent = "No jobs found";
      pastJobsSelect.appendChild(empty);
      return;
    }
    jobs.forEach((job) => {
      const opt = document.createElement("option");
      opt.value = job.job_id;
      const status = job.run_status ? String(job.run_status) : "unknown";
      const updated = job.updated_at ? String(job.updated_at).replace("T", " ").slice(0, 19) : "";
      opt.textContent = `${job.job_id} (${status}${updated ? `, ${updated}` : ""})`;
      pastJobsSelect.appendChild(opt);
    });
  }

  async function refreshPastJobs() {
    if (!pastJobsSelect) return;
    try {
      const res = await fetch("/api/jobs");
      const body = await res.json();
      if (!res.ok) {
        showApiWarning(`Job list failed: ${body.error || "unknown error"}`);
        setPastJobsOptions([]);
        return;
      }
      setPastJobsOptions(body.jobs || []);
    } catch (err) {
      showApiWarning(`Job list failed: ${String(err)}`);
      setPastJobsOptions([]);
    }
  }

  async function loadSelectedJob() {
    if (!pastJobsSelect) return;
    const selectedId = (pastJobsSelect.value || "").trim();
    if (!selectedId) {
      showApiWarning("Pick a saved job first.");
      return;
    }
    setStatus(`Loading ${selectedId}...`, true);
    clearApiWarning();
    try {
      const res = await fetch(`/api/load_job/${encodeURIComponent(selectedId)}`);
      const body = await res.json();
      if (!res.ok) {
        responseBox.textContent = JSON.stringify(body, null, 2);
        showApiWarning(`Load job failed: ${body.error || "unknown error"}`);
        setStatus("Load failed", false);
        return;
      }
      currentJobId = body.job_id;
      responseBox.textContent = JSON.stringify(body, null, 2);
      updateDataSourceStatus(body.summary || {});
      if (body.summary && Number.isFinite(Number(body.summary.bulk_constant_m))) {
        tau0Input.value = String(Number(body.summary.bulk_constant_m));
      }
      if (body.summary && Number.isFinite(Number(body.summary.f_prime))) {
        fPrimeInput.value = String(Number(body.summary.f_prime));
      }
      await showResult(body);
      setStatus(`Loaded: ${body.job_id}`, false);
      reanalyzeBtn.disabled = false;
    } catch (err) {
      responseBox.textContent = String(err);
      showApiWarning(`Load job failed: ${String(err)}`);
      setStatus("Load failed", false);
    }
  }

  function updateDataSourceStatus(summary) {
    if (!dataSourceStatus) return;
    const demSource = (summary && summary.dem_source) ? String(summary.dem_source) : "";
    const outlineSource = (summary && summary.outline_source) ? String(summary.outline_source) : "";
    const isSynthetic = demSource.includes("synthetic") || outlineSource.includes("synthetic");

    dataSourceStatus.classList.remove("sourceReal", "sourceSynthetic", "sourceUnknown");
    if (!demSource && !outlineSource) {
      dataSourceStatus.classList.add("sourceUnknown");
      dataSourceStatus.textContent = "Data Source: Unknown";
      return;
    }
    if (isSynthetic) {
      dataSourceStatus.classList.add("sourceSynthetic");
      dataSourceStatus.textContent = `Data Source: Synthetic fallback (DEM: ${demSource || "n/a"}, Outline: ${outlineSource || "n/a"})`;
      return;
    }
    dataSourceStatus.classList.add("sourceReal");
    dataSourceStatus.textContent = `Data Source: Real API data (DEM: ${demSource || "n/a"}, Outline: ${outlineSource || "n/a"})`;
  }

  function showApiWarning(message) {
    if (!apiWarning) return;
    apiWarning.textContent = message;
    apiWarning.classList.remove("hidden");
  }

  function clearApiWarning() {
    if (!apiWarning) return;
    apiWarning.textContent = "";
    apiWarning.classList.add("hidden");
  }

  function setProgressHidden(hidden) {
    if (!runProgressWrap) return;
    runProgressWrap.classList.toggle("hidden", Boolean(hidden));
  }

  function updateProgress(done, total, message) {
    const safeTotal = Math.max(1, Number(total) || 1);
    const safeDone = Math.max(0, Math.min(safeTotal, Number(done) || 0));
    const pct = Math.round((safeDone / safeTotal) * 100);
    if (runProgressLabel) runProgressLabel.textContent = message || "Working...";
    if (runProgressCount) runProgressCount.textContent = `${safeDone} / ${safeTotal}`;
    if (runProgressFill) runProgressFill.style.width = `${pct}%`;
  }

  function parseBBoxText() {
    const parts = bboxInput.value.split(",").map((x) => Number(x.trim()));
    if (parts.length !== 4 || parts.some((x) => Number.isNaN(x))) {
      return null;
    }
    return parts;
  }

  function getResolutionValue() {
    const raw = Number(resolutionInput.value);
    if (!Number.isFinite(raw) || raw <= 0) {
      resolutionInput.value = "30";
      return 30;
    }
    return raw;
  }

  function getPlotDownsampleValue() {
    const raw = Number(plotDownsampleInput.value);
    if (!Number.isFinite(raw) || raw < 1) {
      plotDownsampleInput.value = "220";
      return 220;
    }
    const clamped = Math.max(1, Math.min(2000, Math.round(raw)));
    plotDownsampleInput.value = String(clamped);
    return clamped;
  }

  function formatBBox(bbox) {
    return bbox.map((n) => Number(n).toFixed(6)).join(",");
  }

  function bboxToLeafletBounds(bbox) {
    return [[bbox[1], bbox[0]], [bbox[3], bbox[2]]];
  }

  function updateBBoxRectFromInput() {
    const bbox = parseBBoxText();
    if (!bbox) return;
    if (bboxLayer) drawnItems.removeLayer(bboxLayer);
    bboxLayer = L.rectangle(bboxToLeafletBounds(bbox), { color: "#ff6b35", weight: 2 });
    drawnItems.addLayer(bboxLayer);
    map.fitBounds(bboxLayer.getBounds(), { padding: [20, 20] });
  }

  function setStatus(text, isBusy) {
    statusPill.textContent = text;
    statusPill.classList.toggle("busy", Boolean(isBusy));
  }

  function clearGeoLayers() {
    if (selectedGlacierLayer) {
      map.removeLayer(selectedGlacierLayer);
      selectedGlacierLayer = null;
    }
    if (outlineLayer) {
      map.removeLayer(outlineLayer);
      outlineLayer = null;
    }
    if (flowLayer) {
      map.removeLayer(flowLayer);
      flowLayer = null;
    }
  }

  function computeGeoJsonBounds(geojson) {
    if (!geojson || !geojson.features || geojson.features.length === 0) return null;
    let minx = Infinity;
    let miny = Infinity;
    let maxx = -Infinity;
    let maxy = -Infinity;
    geojson.features.forEach((f) => {
      const g = f.geometry || {};
      const coords = g.coordinates || [];
      const stack = [coords];
      while (stack.length) {
        const cur = stack.pop();
        if (!Array.isArray(cur) || cur.length === 0) continue;
        if (typeof cur[0] === "number" && cur.length >= 2) {
          const x = Number(cur[0]);
          const y = Number(cur[1]);
          if (Number.isFinite(x) && Number.isFinite(y)) {
            minx = Math.min(minx, x);
            miny = Math.min(miny, y);
            maxx = Math.max(maxx, x);
            maxy = Math.max(maxy, y);
          }
        } else {
          cur.forEach((child) => stack.push(child));
        }
      }
    });
    if (!Number.isFinite(minx) || !Number.isFinite(miny) || !Number.isFinite(maxx) || !Number.isFinite(maxy)) {
      return null;
    }
    return [minx, miny, maxx, maxy];
  }

  function padBBox(bbox, frac = 0.08) {
    const dx = (bbox[2] - bbox[0]) * frac;
    const dy = (bbox[3] - bbox[1]) * frac;
    return [bbox[0] - dx, bbox[1] - dy, bbox[2] + dx, bbox[3] + dy];
  }

  async function selectGlacierAtClick(latlng) {
    const radius = Math.max(100, Math.min(25000, Number(glacierRadiusInput.value) || 5000));
    const payload = { lon: latlng.lng, lat: latlng.lat, radius_m: radius };
    setStatus("Selecting glacier...", true);
    clearApiWarning();
    try {
      const res = await fetch("/api/select_glacier", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      const body = await res.json();
      if (!res.ok) {
        showApiWarning(`Glacier select failed: ${body.error || "unknown error"}`);
        setStatus("Selection failed", false);
        return;
      }
      selectedGlacierGeojson = body;
      if (selectedGlacierLayer) map.removeLayer(selectedGlacierLayer);
      selectedGlacierLayer = L.geoJSON(body, {
        style: { color: "#ff4d6d", weight: 3, fillOpacity: 0.15 }
      }).addTo(map);
      const glacierBounds = computeGeoJsonBounds(body);
      if (glacierBounds) {
        const padded = padBBox(glacierBounds);
        bboxInput.value = formatBBox(padded);
        updateBBoxRectFromInput();
      }
      setStatus("Glacier selected", false);
    } catch (err) {
      showApiWarning(`Glacier select failed: ${String(err)}`);
      setStatus("Selection failed", false);
    }
  }

  async function selectGlacierFromRelationText() {
    const relationText = relationTextInput ? relationTextInput.value.trim() : "";
    if (!relationText) {
      showApiWarning("Paste relation text or ID first.");
      return;
    }
    setStatus("Selecting relation glacier...", true);
    clearApiWarning();
    try {
      const res = await fetch("/api/select_glacier_relation", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ relation_text: relationText })
      });
      const body = await res.json();
      if (!res.ok) {
        showApiWarning(`Relation select failed: ${body.error || "unknown error"}`);
        setStatus("Selection failed", false);
        return;
      }
      selectedGlacierGeojson = body;
      if (selectedGlacierLayer) map.removeLayer(selectedGlacierLayer);
      selectedGlacierLayer = L.geoJSON(body, {
        style: { color: "#ff4d6d", weight: 3, fillOpacity: 0.15 }
      }).addTo(map);
      const glacierBounds = computeGeoJsonBounds(body);
      if (glacierBounds) {
        const padded = padBBox(glacierBounds);
        bboxInput.value = formatBBox(padded);
        updateBBoxRectFromInput();
      }
      setStatus("Relation glacier selected", false);
    } catch (err) {
      showApiWarning(`Relation select failed: ${String(err)}`);
      setStatus("Selection failed", false);
    }
  }

  function clearPlots() {
    [demPlot, slopePlot, flowPlot, depthPlot].forEach((el) => {
      Plotly.purge(el);
      el.innerHTML = "";
    });
  }

  function bindFullscreenButtons() {
    const buttons = document.querySelectorAll(".fullscreenBtn[data-fullscreen-target]");
    buttons.forEach((btn) => {
      btn.addEventListener("click", async () => {
        const targetId = btn.getAttribute("data-fullscreen-target");
        if (!targetId) return;
        const target = document.getElementById(targetId);
        if (!target) return;
        try {
          if (document.fullscreenElement === target) {
            await document.exitFullscreen();
          } else {
            await target.requestFullscreen();
          }
        } catch (_err) {
          showApiWarning("Fullscreen is not available in this browser context.");
        }
      });
    });
  }

  function ensureTopoMaps() {
    if (!depthTopoMap && depthTopoMapEl) {
      depthTopoMap = L.map(depthTopoMapEl, { zoomControl: true, attributionControl: true }).setView([37.77, -122.44], 11);
      L.tileLayer("https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png", {
        maxZoom: 17,
        attribution:
          'Map data: &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors, ' +
          '<a href="https://viewfinderpanoramas.org">SRTM</a> | ' +
          'Map style: &copy; <a href="https://opentopomap.org">OpenTopoMap</a>'
      }).addTo(depthTopoMap);
      L.control.scale({ imperial: false, position: "bottomleft" }).addTo(depthTopoMap);
    }
    if (!flowTopoMap && flowTopoMapEl) {
      flowTopoMap = L.map(flowTopoMapEl, { zoomControl: true, attributionControl: true }).setView([37.77, -122.44], 11);
      L.tileLayer("https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png", {
        maxZoom: 17,
        attribution:
          'Map data: &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors, ' +
          '<a href="https://viewfinderpanoramas.org">SRTM</a> | ' +
          'Map style: &copy; <a href="https://opentopomap.org">OpenTopoMap</a>'
      }).addTo(flowTopoMap);
      L.control.scale({ imperial: false, position: "bottomleft" }).addTo(flowTopoMap);
    }
  }

  function clearTopoLayers() {
    if (depthTopoMap && depthOverlayLayer) {
      depthTopoMap.removeLayer(depthOverlayLayer);
      depthOverlayLayer = null;
    }
    if (flowTopoMap && flowOverlayLayer) {
      flowTopoMap.removeLayer(flowOverlayLayer);
      flowOverlayLayer = null;
    }
    if (flowTopoMap && flowTopoOutlineLayer) {
      flowTopoMap.removeLayer(flowTopoOutlineLayer);
      flowTopoOutlineLayer = null;
    }
    if (flowTopoMap && flowTopoFlowLayer) {
      flowTopoMap.removeLayer(flowTopoFlowLayer);
      flowTopoFlowLayer = null;
    }
  }

  function renderRasterPlot(target, raster, outlineTraces, cfg) {
    if (!raster || !raster.z) {
      target.innerHTML = "<div class='plotEmpty'>No data yet</div>";
      return;
    }

    const traces = [
      {
        type: "heatmap",
        x: raster.x,
        y: raster.y,
        z: raster.z,
        colorscale: cfg.colorscale,
        hovertemplate:
          `${cfg.title}<br>x: %{x:.2f}<br>y: %{y:.2f}<br>value: %{z:.3f} ${cfg.units || ""}<extra></extra>`,
        colorbar: { title: cfg.units || "value" }
      }
    ];

    (outlineTraces || []).forEach((ring, idx) => {
      traces.push({
        type: "scatter",
        mode: "lines",
        x: ring.x,
        y: ring.y,
        line: { color: "#f8f36a", width: 2 },
        name: idx === 0 ? "Glacier outline" : "",
        showlegend: idx === 0,
        hoverinfo: "skip"
      });
    });

    Plotly.newPlot(
      target,
      traces,
      {
        title: cfg.title,
        margin: { l: 40, r: 20, t: 36, b: 40 },
        xaxis: { title: "X" },
        yaxis: { title: "Y", scaleanchor: "x", scaleratio: 1 },
        legend: { orientation: "h" }
      },
      { responsive: true, displaylogo: false }
    );
  }

  function renderFlowPlot(target, demRaster, outlineTraces, flowTraces, vectorField) {
    if (!demRaster || !demRaster.z) {
      target.innerHTML = "<div class='plotEmpty'>No flow data yet</div>";
      return;
    }

    const traces = [
      {
        type: "heatmap",
        x: demRaster.x,
        y: demRaster.y,
        z: demRaster.z,
        colorscale: "Gray",
        opacity: 0.75,
        hovertemplate: "Flow background<br>x: %{x:.2f}<br>y: %{y:.2f}<br>z: %{z:.2f} m<extra></extra>",
        showscale: false
      }
    ];

    (outlineTraces || []).forEach((ring, idx) => {
      traces.push({
        type: "scatter",
        mode: "lines",
        x: ring.x,
        y: ring.y,
        line: { color: "#f8f36a", width: 2 },
        name: idx === 0 ? "Glacier outline" : "",
        showlegend: idx === 0,
        hoverinfo: "skip"
      });
    });

    const vectorAnnotations = [];
    if (vectorField && vectorField.x && vectorField.x.length > 0) {
      for (let i = 0; i + 1 < vectorField.x.length; i += 3) {
        const x0 = vectorField.x[i];
        const y0 = vectorField.y[i];
        const x1 = vectorField.x[i + 1];
        const y1 = vectorField.y[i + 1];
        if (![x0, y0, x1, y1].every((v) => Number.isFinite(v))) continue;
        vectorAnnotations.push({
          x: x1,
          y: y1,
          ax: x0,
          ay: y0,
          xref: "x",
          yref: "y",
          axref: "x",
          ayref: "y",
          showarrow: true,
          arrowhead: 3,
          arrowsize: 1,
          arrowwidth: 1.2,
          arrowcolor: "#00d1ff",
          opacity: 0.9,
          text: ""
        });
      }
    }
    if (vectorAnnotations.length > 0) {
      traces.push({
        type: "scatter",
        mode: "markers",
        x: vectorField.x,
        y: vectorField.y,
        marker: { size: 0.1, color: "rgba(0,0,0,0)" },
        name: "Flow vectors",
        hoverinfo: "skip"
      });
    }

    (flowTraces || []).slice(0, 180).forEach((line, idx) => {
      traces.push({
        type: "scatter",
        mode: "lines",
        x: line.x,
        y: line.y,
        line: { color: "#16a34a", width: 1 },
        name: idx === 0 ? "Flowlines" : "",
        showlegend: idx === 0,
        hoverinfo: "skip"
      });
    });

    Plotly.newPlot(
      target,
      traces,
      {
        title: "Flow Direction Vector Field",
        margin: { l: 40, r: 20, t: 36, b: 40 },
        xaxis: { title: "X" },
        yaxis: { title: "Y", scaleanchor: "x", scaleratio: 1 },
        legend: { orientation: "h" },
        annotations: vectorAnnotations
      },
      { responsive: true, displaylogo: false }
    );
  }

  function toStaticPath(absPath, cacheBustToken = "") {
    if (!absPath) return "";
    const marker = "/glacier_app/outputs/";
    const idx = absPath.indexOf(marker);
    if (idx === -1) return "";
    const relative = absPath.slice(idx + marker.length);
    const base = `/outputs/${relative}`;
    if (!cacheBustToken) return base;
    const sep = base.includes("?") ? "&" : "?";
    return `${base}${sep}v=${encodeURIComponent(cacheBustToken)}`;
  }

  async function fetchJsonFromPath(absPath) {
    const url = toStaticPath(absPath);
    if (!url) return null;
    try {
      const res = await fetch(url);
      if (!res.ok) return null;
      return await res.json();
    } catch (_err) {
      return null;
    }
  }

  async function fetchPlotData(jobId, maxDim) {
    const safeDim = Number.isFinite(maxDim) ? maxDim : 220;
    const res = await fetch(`/api/plot_data/${jobId}?max_dim=${encodeURIComponent(safeDim)}`);
    if (!res.ok) {
      showApiWarning(`Plot data request failed (${res.status}).`);
      return null;
    }
    return await res.json();
  }

  async function showResult(result) {
    if (!result || !result.artifacts) {
      return;
    }

    if (result.report_url) {
      reportLink.href = result.report_url;
      reportLink.classList.remove("disabled");
    }

    let outlineGeojson = null;
    let flowGeojson = null;

    if (result.artifacts.outline_geojson) {
      outlineGeojson = await fetchJsonFromPath(result.artifacts.outline_geojson);
      if (outlineGeojson) {
        if (outlineLayer) map.removeLayer(outlineLayer);
        outlineLayer = L.geoJSON(outlineGeojson, {
          style: { color: "#ffd166", weight: 2, fillOpacity: 0.1 }
        }).addTo(map);
      }
    }

    if (result.artifacts.flowlines_path) {
      flowGeojson = await fetchJsonFromPath(result.artifacts.flowlines_path);
      if (flowGeojson) {
        if (flowLayer) map.removeLayer(flowLayer);
        flowLayer = L.geoJSON(flowGeojson, { style: { color: "#00d1ff", weight: 1.5 } }).addTo(map);
      }
    }

    const plotData = await fetchPlotData(result.job_id, getPlotDownsampleValue());
    if (!plotData) {
      return;
    }
    clearApiWarning();

    const outlines = plotData.outline_traces || [];
    renderRasterPlot(demPlot, plotData.rasters.dem, outlines, {
      title: "Elevation",
      units: "m",
      colorscale: "Earth"
    });
    const gridSize = plotData.params && plotData.params.grid_size_m ? plotData.params.grid_size_m : gridSizeInput.value;
    renderRasterPlot(slopePlot, plotData.rasters.avg_slope || plotData.rasters.slope, outlines, {
      title: `Average Slope (grid ${gridSize} m)`,
      units: "",
      colorscale: "Magma"
    });
    renderRasterPlot(depthPlot, plotData.rasters.depth, outlines, {
      title: "Depth",
      units: "m",
      colorscale: "Cividis"
    });
    renderFlowPlot(
      flowPlot,
      plotData.rasters.flow_background || plotData.rasters.dem,
      outlines,
      plotData.flowlines_traces,
      plotData.vector_field
    );

    const cacheBustToken = `${result.job_id || "job"}_${Date.now()}`;
    renderTopoOverlays(plotData.topomap || {}, outlineGeojson, flowGeojson, cacheBustToken);
  }

  function renderTopoOverlays(topomap, outlineGeojson, flowGeojson, cacheBustToken) {
    ensureTopoMaps();
    clearTopoLayers();

    const boundsRaw = topomap.bounds_wgs84 || null;
    if (!boundsRaw || boundsRaw.length !== 4) {
      return;
    }
    if (depthLegendRange) {
      const lg = topomap.depth_legend || {};
      if (Number.isFinite(lg.min_m) && Number.isFinite(lg.max_m)) {
        depthLegendRange.innerHTML = `<span>${Number(lg.min_m).toFixed(1)} m</span><span>${Number(lg.max_m).toFixed(1)} m</span>`;
      } else {
        depthLegendRange.innerHTML = "<span>Shallow</span><span>Deep</span>";
      }
    }
    const bounds = [[boundsRaw[0], boundsRaw[1]], [boundsRaw[2], boundsRaw[3]]];
    if (depthTopoMap) {
      const depthUrl = toStaticPath(topomap.depth_overlay_path || "", cacheBustToken);
      if (depthUrl) {
        depthOverlayLayer = L.imageOverlay(depthUrl, bounds, { opacity: 0.78 }).addTo(depthTopoMap);
      }
      depthTopoMap.fitBounds(bounds, { padding: [14, 14] });
      setTimeout(() => depthTopoMap.invalidateSize(), 50);
    }
    if (flowTopoMap) {
      const flowOverlayUrl = toStaticPath(topomap.flow_overlay_path || "", cacheBustToken);
      if (flowOverlayUrl) {
        flowOverlayLayer = L.imageOverlay(flowOverlayUrl, bounds, { opacity: 0.85 }).addTo(flowTopoMap);
      }
      if (outlineGeojson) {
        flowTopoOutlineLayer = L.geoJSON(outlineGeojson, {
          style: { color: "#ffd166", weight: 2, fillOpacity: 0.04 }
        }).addTo(flowTopoMap);
      }
      if (flowGeojson) {
        flowTopoFlowLayer = L.geoJSON(flowGeojson, {
          style: { color: "#00d1ff", weight: 1.4, opacity: 0.95 }
        }).addTo(flowTopoMap);
      }
      flowTopoMap.fitBounds(bounds, { padding: [14, 14] });
      setTimeout(() => flowTopoMap.invalidateSize(), 50);
    }
  }

  map.on(L.Draw.Event.CREATED, function (event) {
    selectedGlacierGeojson = null;
    clearGeoLayers();
    if (bboxLayer) drawnItems.removeLayer(bboxLayer);
    bboxLayer = event.layer;
    drawnItems.addLayer(bboxLayer);
    const b = bboxLayer.getBounds();
    const bbox = [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()];
    bboxInput.value = formatBBox(bbox);
  });

  map.on("click", function (event) {
    if (selectionModeInput && selectionModeInput.value === "glacier") {
      selectGlacierAtClick(event.latlng);
    }
  });

  bboxInput.addEventListener("change", updateBBoxRectFromInput);
  if (selectionModeInput) {
    selectionModeInput.addEventListener("change", () => {
      if (selectionModeInput.value === "bbox") {
        selectedGlacierGeojson = null;
        if (selectedGlacierLayer) {
          map.removeLayer(selectedGlacierLayer);
          selectedGlacierLayer = null;
        }
      }
    });
  }
  if (selectRelationBtn) {
    selectRelationBtn.addEventListener("click", () => {
      if (!selectionModeInput || selectionModeInput.value !== "relation") {
        showApiWarning("Switch Selection Mode to 'Paste OSM Relation' first.");
        return;
      }
      selectGlacierFromRelationText();
    });
  }

  runFullBtn.addEventListener("click", async function () {
    let bbox = parseBBoxText();
    if (!bbox) {
      responseBox.textContent = "Invalid bbox. Expected: minx,miny,maxx,maxy";
      showApiWarning("Invalid bbox format.");
      return;
    }
    if (
      selectionModeInput &&
      (selectionModeInput.value === "glacier" || selectionModeInput.value === "relation") &&
      selectedGlacierGeojson
    ) {
      const selectedBounds = computeGeoJsonBounds(selectedGlacierGeojson);
      if (selectedBounds) {
        // Use exact selected glacier bounds for compute requests.
        bbox = selectedBounds;
      }
    }

    const payload = {
      bbox: bbox,
      source: sourceInput.value,
      dataset: datasetInput ? datasetInput.value : "COP30",
      resolution: getResolutionValue(),
      grid_size_m: Number(gridSizeInput.value),
      plot_max_dim: getPlotDownsampleValue(),
      bulk_constant_m: Number(tau0Input.value),
      f_prime: Number(fPrimeInput.value),
      slope_units: slopeUnitsInput.value,
      api_key: apiKeyInput.value.trim(),
      async_mode: true
    };
    if (
      selectionModeInput &&
      (selectionModeInput.value === "glacier" || selectionModeInput.value === "relation") &&
      selectedGlacierGeojson
    ) {
      payload.outline_geojson_override = selectedGlacierGeojson;
    }

    clearGeoLayers();
    clearPlots();
    clearTopoLayers();
    clearApiWarning();
    setProgressHidden(false);
    updateProgress(0, 1, "Submitting run...");
    setStatus("Running pipeline...", true);
    runFullBtn.disabled = true;
    reanalyzeBtn.disabled = true;
    reportLink.classList.add("disabled");

    try {
      const res = await fetch("/api/run_full", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      const body = await res.json();
      if (!res.ok) {
        responseBox.textContent = JSON.stringify(body, null, 2);
        showApiWarning(`API run_full failed: ${body.error || "unknown error"}`);
        setStatus("Request failed", false);
        return;
      }
      currentJobId = body.job_id;
      responseBox.textContent = JSON.stringify(body, null, 2);
      setStatus(`Running: ${body.job_id}`, true);

      const pollStatus = async () => {
        try {
          const stRes = await fetch(`/api/run_status/${encodeURIComponent(currentJobId)}`);
          const stBody = await stRes.json();
          if (!stRes.ok) {
            showApiWarning(`Status polling failed: ${stBody.error || "unknown error"}`);
            return;
          }
          const progress = stBody.progress || {};
          updateProgress(progress.done || 0, progress.total || 1, progress.message || "Working...");
          if (stBody.status === "completed") {
            if (runStatusTimer) {
              clearInterval(runStatusTimer);
              runStatusTimer = null;
            }
            responseBox.textContent = JSON.stringify(stBody, null, 2);
            updateDataSourceStatus(stBody.summary || {});
            await showResult(stBody);
            setProgressHidden(true);
            setStatus(`Completed: ${stBody.job_id}`, false);
            runFullBtn.disabled = false;
            reanalyzeBtn.disabled = false;
          } else if (stBody.status === "failed" || stBody.status === "cancelled") {
            if (runStatusTimer) {
              clearInterval(runStatusTimer);
              runStatusTimer = null;
            }
            responseBox.textContent = JSON.stringify(stBody, null, 2);
            if (stBody.status === "cancelled") {
              showApiWarning(`Run cancelled: ${stBody.error || "cancelled by user"}`);
            } else {
              showApiWarning(`API run_full failed: ${stBody.error || "unknown error"}`);
            }
            setProgressHidden(true);
            setStatus(stBody.status === "cancelled" ? "Run cancelled" : "Request failed", false);
            runFullBtn.disabled = false;
            reanalyzeBtn.disabled = false;
          }
        } catch (pollErr) {
          showApiWarning(`Status polling error: ${String(pollErr)}`);
        }
      };
      if (runStatusTimer) clearInterval(runStatusTimer);
      runStatusTimer = setInterval(pollStatus, 1500);
      await pollStatus();
    } catch (err) {
      responseBox.textContent = String(err);
      showApiWarning(`Network/API error: ${String(err)}`);
      setProgressHidden(true);
      setStatus("Network error", false);
    } finally {
      if (!runStatusTimer) {
        runFullBtn.disabled = false;
        reanalyzeBtn.disabled = !currentJobId;
      }
    }
  });

  reanalyzeBtn.addEventListener("click", async function () {
    if (!currentJobId) {
      responseBox.textContent = "Run the full pipeline once before reanalyze.";
      showApiWarning("No active job to reanalyze.");
      return;
    }
    const payload = {
      job_id: currentJobId,
      grid_size_m: Number(gridSizeInput.value),
      plot_max_dim: getPlotDownsampleValue(),
      bulk_constant_m: Number(tau0Input.value),
      f_prime: Number(fPrimeInput.value),
      slope_units: slopeUnitsInput.value
    };

    setStatus(`Reanalyzing ${currentJobId}...`, true);
    runFullBtn.disabled = true;
    reanalyzeBtn.disabled = true;
    reportLink.classList.add("disabled");
    clearApiWarning();

    try {
      const res = await fetch("/api/reanalyze", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      const body = await res.json();
      if (!res.ok) {
        responseBox.textContent = JSON.stringify(body, null, 2);
        showApiWarning(`API reanalyze failed: ${body.error || "unknown error"}`);
        setStatus("Reanalyze failed", false);
        return;
      }
      responseBox.textContent = JSON.stringify(body, null, 2);
      updateDataSourceStatus(body.summary || {});
      await showResult(body);
      setStatus(`Reanalyzed: ${body.job_id}`, false);
    } catch (err) {
      responseBox.textContent = String(err);
      showApiWarning(`Network/API error: ${String(err)}`);
      setStatus("Network error", false);
    } finally {
      runFullBtn.disabled = false;
      reanalyzeBtn.disabled = false;
    }
  });

  if (cancelRunBtn) {
    cancelRunBtn.addEventListener("click", async function () {
      if (!currentJobId) {
        showApiWarning("No active job to cancel.");
        return;
      }
      try {
        const res = await fetch(`/api/run_cancel/${encodeURIComponent(currentJobId)}`, { method: "POST" });
        const body = await res.json();
        if (!res.ok) {
          showApiWarning(`Cancel failed: ${body.error || "unknown error"}`);
          return;
        }
        setStatus(`Cancelling: ${currentJobId}`, true);
        updateProgress(0, 1, "Cancellation requested");
      } catch (err) {
        showApiWarning(`Cancel request failed: ${String(err)}`);
      }
    });
  }

  if (refreshJobsBtn) {
    refreshJobsBtn.addEventListener("click", async function () {
      await refreshPastJobs();
    });
  }
  if (loadJobBtn) {
    loadJobBtn.addEventListener("click", async function () {
      await loadSelectedJob();
    });
  }

  document.addEventListener("fullscreenchange", () => {
    [demPlot, slopePlot, flowPlot, depthPlot].forEach((el) => {
      if (el && el.data) {
        Plotly.Plots.resize(el);
      }
    });
    if (map) map.invalidateSize();
    if (depthTopoMap) depthTopoMap.invalidateSize();
    if (flowTopoMap) flowTopoMap.invalidateSize();
  });

  updateBBoxRectFromInput();
  reanalyzeBtn.disabled = true;
  ensureTopoMaps();
  bindFullscreenButtons();
  clearApiWarning();
  updateDataSourceStatus(null);
  refreshPastJobs();
})();
