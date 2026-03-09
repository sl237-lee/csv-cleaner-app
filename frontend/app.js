const uploadBtn = document.getElementById("uploadBtn");
const browseBtn = document.getElementById("browseBtn");
const dropZone = document.getElementById("dropZone");
const fileInput = document.getElementById("fileInput");
const selectedFileNameEl = document.getElementById("selectedFileName");
const missingMode = document.getElementById("missingMode");
const duplicateColumnsInput = document.getElementById("duplicateColumns");
const statusDiv = document.getElementById("status");
const resultsCard = document.getElementById("resultsCard");
const downloadCsvLink = document.getElementById("downloadCsvLink");
const downloadXlsxLink = document.getElementById("downloadXlsxLink");

const originalRowsEl = document.getElementById("originalRows");
const cleanedRowsEl = document.getElementById("cleanedRows");
const duplicatesRemovedEl = document.getElementById("duplicatesRemoved");
const rowsRemovedMissingEl = document.getElementById("rowsRemovedMissing");
const missingModeResultEl = document.getElementById("missingModeResult");
const domainProfileEl = document.getElementById("domainProfile");
const normalizedDateColumnsEl = document.getElementById("normalizedDateColumns");
const normalizedNumericColumnsEl = document.getElementById("normalizedNumericColumns");
const duplicateCheckColumnsEl = document.getElementById("duplicateCheckColumns");
const finalColumnsEl = document.getElementById("finalColumns");
const appliedRulesEl = document.getElementById("appliedRules");
const missingBeforeEl = document.getElementById("missingBefore");
const missingAfterEl = document.getElementById("missingAfter");
const changedCellsTotalEl = document.getElementById("changedCellsTotal");
const changedCellsByColumnEl = document.getElementById("changedCellsByColumn");
const changedCellsPreviewEl = document.getElementById("changedCellsPreview");
const beforePreviewTableEl = document.getElementById("beforePreviewTable");
const afterPreviewTableEl = document.getElementById("afterPreviewTable");

const API_BASE = "http://127.0.0.1:8000";

function formatList(items) {
  if (!items || items.length === 0) return "None";
  return items.join(", ");
}

function formatModeLabel(mode) {
  if (mode === "strict") return "Strict clean";
  if (mode === "safe") return "Safe clean";
  if (mode === "fill") return "Fill mode";
  return mode || "-";
}

function formatDomainLabel(domain) {
  if (!domain) return "-";
  return domain
    .split("_")
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join(" ");
}

function setSelectedFileName(file) {
  selectedFileNameEl.textContent = file ? file.name : "No file selected";
}

function isSupportedFile(file) {
  if (!file) return false;
  const name = file.name.toLowerCase();
  return name.endsWith(".csv") || name.endsWith(".xlsx");
}

function showError(message) {
  statusDiv.textContent = message;
  statusDiv.className = "status error";
}

function showSuccess(message) {
  statusDiv.textContent = message;
  statusDiv.className = "status success";
}

function showNeutral(message) {
  statusDiv.textContent = message;
  statusDiv.className = "status";
}

function renderKeyValueGrid(container, obj) {
  container.innerHTML = "";

  const entries = Object.entries(obj || {});
  if (entries.length === 0) {
    container.innerHTML = `<div class="kv-empty">None</div>`;
    return;
  }

  entries.forEach(([key, value]) => {
    const item = document.createElement("div");
    item.className = "kv-item";
    item.innerHTML = `
      <span class="kv-key">${key}</span>
      <span class="kv-value">${value}</span>
    `;
    container.appendChild(item);
  });
}

function renderChipList(container, items) {
  container.innerHTML = "";

  if (!items || items.length === 0) {
    container.innerHTML = `<div class="kv-empty">No business rules applied.</div>`;
    return;
  }

  items.forEach((item) => {
    const chip = document.createElement("span");
    chip.className = "rule-chip";
    chip.textContent = item;
    container.appendChild(chip);
  });
}

function renderTable(container, rows) {
  container.innerHTML = "";

  if (!rows || rows.length === 0) {
    container.innerHTML = `<div class="table-empty">No preview available.</div>`;
    return;
  }

  const columns = Array.from(
    rows.reduce((set, row) => {
      Object.keys(row).forEach((key) => set.add(key));
      return set;
    }, new Set())
  );

  const table = document.createElement("table");
  table.className = "preview-table";

  const thead = document.createElement("thead");
  const headRow = document.createElement("tr");

  columns.forEach((column) => {
    const th = document.createElement("th");
    th.textContent = column;
    headRow.appendChild(th);
  });

  thead.appendChild(headRow);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");

  rows.forEach((row) => {
    const tr = document.createElement("tr");

    columns.forEach((column) => {
      const td = document.createElement("td");
      const value = row[column];

      if (value === null || value === undefined || value === "") {
        td.textContent = "—";
        td.classList.add("empty-cell");
      } else {
        td.textContent = value;
      }

      tr.appendChild(td);
    });

    tbody.appendChild(tr);
  });

  table.appendChild(tbody);
  container.appendChild(table);
}

function renderChangedCellsTable(container, rows) {
  container.innerHTML = "";

  if (!rows || rows.length === 0) {
    container.innerHTML = `<div class="table-empty">No changed cells detected in the preview window.</div>`;
    return;
  }

  const table = document.createElement("table");
  table.className = "preview-table";

  table.innerHTML = `
    <thead>
      <tr>
        <th>Row</th>
        <th>Column</th>
        <th>Before</th>
        <th>After</th>
      </tr>
    </thead>
  `;

  const tbody = document.createElement("tbody");

  rows.forEach((row) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${row.row_index}</td>
      <td>${row.column ?? "—"}</td>
      <td>${row.before === "" ? "—" : row.before}</td>
      <td>${row.after === "" ? "—" : row.after}</td>
    `;
    tbody.appendChild(tr);
  });

  table.appendChild(tbody);
  container.appendChild(table);
}

function renderReport(report, beforePreview, afterPreview, changedCellsPreview) {
  originalRowsEl.textContent = report.original_rows ?? "-";
  cleanedRowsEl.textContent = report.cleaned_rows ?? "-";
  duplicatesRemovedEl.textContent = report.duplicates_removed ?? "-";
  rowsRemovedMissingEl.textContent = report.rows_removed_with_missing_values ?? "-";

  missingModeResultEl.textContent = formatModeLabel(report.missing_value_mode);
  domainProfileEl.textContent = formatDomainLabel(report.inferred_domain_profile);
  normalizedDateColumnsEl.textContent = formatList(report.normalized_date_columns);
  normalizedNumericColumnsEl.textContent = formatList(report.normalized_numeric_columns);
  duplicateCheckColumnsEl.textContent = formatList(report.duplicate_check_columns);
  finalColumnsEl.textContent = formatList(report.columns);

  renderChipList(appliedRulesEl, report.applied_business_rules);

  renderKeyValueGrid(
    missingBeforeEl,
    report.missing_values_by_column_before_cleaning
  );

  renderKeyValueGrid(
    missingAfterEl,
    report.missing_values_by_column_after_cleaning
  );

  const changeSummary = report.change_summary || {};
  changedCellsTotalEl.textContent =
    changeSummary.total_changed_cells_in_preview_window ?? 0;

  renderKeyValueGrid(
    changedCellsByColumnEl,
    changeSummary.changed_cells_by_column_in_preview_window
  );

  renderChangedCellsTable(changedCellsPreviewEl, changedCellsPreview);
  renderTable(beforePreviewTableEl, beforePreview);
  renderTable(afterPreviewTableEl, afterPreview);

  resultsCard.classList.remove("hidden");
}

function resetResults() {
  resultsCard.classList.add("hidden");
  downloadCsvLink.classList.add("hidden");
  downloadXlsxLink.classList.add("hidden");

  beforePreviewTableEl.innerHTML = "";
  afterPreviewTableEl.innerHTML = "";
  changedCellsPreviewEl.innerHTML = "";
  missingBeforeEl.innerHTML = "";
  missingAfterEl.innerHTML = "";
  changedCellsByColumnEl.innerHTML = "";
  appliedRulesEl.innerHTML = "";

  originalRowsEl.textContent = "-";
  cleanedRowsEl.textContent = "-";
  duplicatesRemovedEl.textContent = "-";
  rowsRemovedMissingEl.textContent = "-";
  missingModeResultEl.textContent = "-";
  domainProfileEl.textContent = "-";
  normalizedDateColumnsEl.textContent = "-";
  normalizedNumericColumnsEl.textContent = "-";
  duplicateCheckColumnsEl.textContent = "-";
  finalColumnsEl.textContent = "-";
  changedCellsTotalEl.textContent = "-";
}

function getSelectedFile() {
  return fileInput.files && fileInput.files.length > 0 ? fileInput.files[0] : null;
}

async function uploadAndClean() {
  const file = getSelectedFile();

  if (!file) {
    showError("Please choose a file first.");
    return;
  }

  if (!isSupportedFile(file)) {
    showError("Only CSV and XLSX files are supported.");
    return;
  }

  const formData = new FormData();
  formData.append("file", file);
  formData.append("missing_value_mode", missingMode.value);

  const duplicateColumnsRaw = duplicateColumnsInput.value.trim();
  if (duplicateColumnsRaw) {
    const duplicateColumns = duplicateColumnsRaw
      .split(",")
      .map((col) => col.trim())
      .filter(Boolean);

    formData.append("duplicate_columns", JSON.stringify(duplicateColumns));
  }

  showNeutral("Uploading and cleaning...");
  resetResults();

  try {
    const response = await fetch(`${API_BASE}/upload`, {
      method: "POST",
      body: formData
    });

    const data = await response.json();

    if (!response.ok) {
      showError(data.detail || "Something went wrong.");
      return;
    }

    showSuccess("Cleaning complete.");

    renderReport(
      data.report,
      data.before_preview || [],
      data.after_preview || [],
      data.changed_cells_preview || []
    );

    downloadCsvLink.href = `${API_BASE}${data.download_csv_url}`;
    downloadXlsxLink.href = `${API_BASE}${data.download_xlsx_url}`;
    downloadCsvLink.classList.remove("hidden");
    downloadXlsxLink.classList.remove("hidden");
  } catch (error) {
    showError("Failed to connect to backend.");
  }
}

browseBtn.addEventListener("click", () => {
  fileInput.click();
});

fileInput.addEventListener("change", () => {
  const file = getSelectedFile();
  setSelectedFileName(file);

  if (file && !isSupportedFile(file)) {
    showError("Only CSV and XLSX files are supported.");
    return;
  }

  if (file) {
    showNeutral(`Selected file: ${file.name}`);
  }
});

uploadBtn.addEventListener("click", uploadAndClean);

["dragenter", "dragover"].forEach((eventName) => {
  dropZone.addEventListener(eventName, (event) => {
    event.preventDefault();
    event.stopPropagation();
    dropZone.classList.add("drag-over");
  });
});

["dragleave", "dragend"].forEach((eventName) => {
  dropZone.addEventListener(eventName, (event) => {
    event.preventDefault();
    event.stopPropagation();
    dropZone.classList.remove("drag-over");
  });
});

dropZone.addEventListener("drop", (event) => {
  event.preventDefault();
  event.stopPropagation();
  dropZone.classList.remove("drag-over");

  const files = event.dataTransfer?.files;
  if (!files || files.length === 0) return;

  const file = files[0];

  if (!isSupportedFile(file)) {
    showError("Only CSV and XLSX files are supported.");
    return;
  }

  const dataTransfer = new DataTransfer();
  dataTransfer.items.add(file);
  fileInput.files = dataTransfer.files;

  setSelectedFileName(file);
  showNeutral(`Selected file: ${file.name}`);
});

dropZone.addEventListener("click", () => {
  fileInput.click();
});

dropZone.addEventListener("keydown", (event) => {
  if (event.key === "Enter" || event.key === " ") {
    event.preventDefault();
    fileInput.click();
  }
});

setSelectedFileName(null);