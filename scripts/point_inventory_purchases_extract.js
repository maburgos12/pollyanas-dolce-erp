async ({ start, end, branchLabel = "ALMACEN", branchValue = "", maxRows = 500 }) => {
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

  const waitFor = async (predicate, timeoutMs, errorMessage) => {
    const started = Date.now();
    while (Date.now() - started < timeoutMs) {
      const result = predicate();
      if (result) {
        return result;
      }
      await sleep(250);
    }
    throw new Error(errorMessage);
  };

  const branchSelect = await waitFor(
    () => document.querySelector("#cmb_sucursales_RC"),
    30000,
    "No se encontró #cmb_sucursales_RC"
  );

  const branchOptions = await waitFor(() => {
    const options = Array.from(branchSelect.options).map((option) => ({
      value: option.value,
      label: (option.textContent || "").trim(),
    }));
    return options.filter((option) => option.value).length >= 3 ? options : null;
  }, 30000, "No cargaron las sucursales de Compras");

  const requestedValue = String(branchValue || "").trim();
  const requestedLabel = String(branchLabel || "").trim().toUpperCase();
  const targetBranch =
    branchOptions.find((option) => requestedValue && option.value === requestedValue) ||
    branchOptions.find((option) => option.label.toUpperCase().includes(requestedLabel));
  if (!targetBranch) {
    throw new Error(
      `Sucursal no encontrada en Compras: ${requestedValue || branchLabel}`
    );
  }

  $("#cmb_sucursales_RC").val(targetBranch.value).trigger("change");

  const [startYear, startMonth, startDay] = String(start).split("-").map(Number);
  const [endYear, endMonth, endDay] = String(end).split("-").map(Number);
  const startDate = new Date(startYear, startMonth - 1, startDay);
  const endDate = new Date(endYear, endMonth - 1, endDay);

  $("#startRC").datepicker("update", startDate);
  $("#endRC").datepicker("update", endDate);

  const finishSignal = `point_extract_${Date.now()}_${Math.random().toString(16).slice(2)}`;
  let requestUrl = "";

  await new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      $(document).off(`ajaxComplete.${finishSignal}`);
      reject(new Error("Timeout esperando GetCompras"));
    }, 30000);

    const onComplete = (_event, xhr, settings) => {
      const url = String(settings?.url || "");
      if (!url.includes("/InventoryPurchases/GetCompras")) {
        return;
      }
      requestUrl = url;
      clearTimeout(timeout);
      $(document).off(`ajaxComplete.${finishSignal}`);
      setTimeout(resolve, 800);
    };

    $(document).on(`ajaxComplete.${finishSignal}`, onComplete);
    getCompras();
  });

  await sleep(1500);

  const expectedMonthToken = String(start).slice(0, 7).replace("-", "");
  await waitFor(() => {
    const firstCell = document.querySelector("#ComprasGenerales tbody tr td:nth-child(4)");
    if (!firstCell) {
      return document.querySelectorAll("#ComprasGenerales tbody tr").length === 0 ? true : null;
    }
    const text = (firstCell.textContent || "").replace(/\s+/g, "");
    return text.includes(expectedMonthToken) ? true : null;
  }, 15000, `La tabla de Compras no refrescó al mes ${expectedMonthToken}`);

  const dataTable = $.fn.dataTable.isDataTable("#ComprasGenerales")
    ? $("#ComprasGenerales").DataTable()
    : null;
  if (dataTable) {
    dataTable.page.len(-1).draw(false);
    await sleep(1000);
  }

  const rows = [];
  const tableRows = Array.from(document.querySelectorAll("#ComprasGenerales tbody tr"));
  for (const row of tableRows.slice(0, maxRows)) {
    const cells = Array.from(row.querySelectorAll("td")).map((cell) =>
      (cell.textContent || "").replace(/\s+/g, " ").trim()
    );
    if (!cells.length) {
      continue;
    }
    const onclick =
      row.querySelector("[onclick*='getComprasbyID']")?.getAttribute("onclick") ||
      row.innerHTML ||
      "";
    const match = onclick.match(/getComprasbyID\((\d+)/i);
    rows.push({
      compra_id: match ? match[1] : "",
      cells,
    });
  }

  return {
    start,
    end,
    branch: targetBranch.label,
    branch_value: targetBranch.value,
    request_url: requestUrl,
    rows_count: rows.length,
    rows,
  };
}
