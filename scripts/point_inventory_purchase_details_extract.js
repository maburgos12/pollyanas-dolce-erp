async ({ purchaseIds = [], searchTerms = [] }) => {
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const terms = searchTerms
    .map((term) => String(term || "").trim().toUpperCase())
    .filter(Boolean);

  const normalize = (value) =>
    String(value || "")
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .toUpperCase();

  const results = [];

  for (const purchaseId of purchaseIds) {
    const data = await new Promise((resolve, reject) => {
      $.get(server + "/InventoryPurchases/GetComprabyId", { fkCompra: purchaseId })
        .done((raw) => {
          try {
            resolve(JSON.parse(raw));
          } catch (error) {
            reject(new Error(`JSON inválido en compra ${purchaseId}: ${error.message}`));
          }
        })
        .fail((_xhr, _status, error) => {
          reject(new Error(`Error cargando compra ${purchaseId}: ${error || "desconocido"}`));
        });
    });

    const rows = Array.isArray(data) ? data : [];
    const normalizedRows = rows.map((row) => ({
      articulo: String(row.Articulo || "").trim(),
      cantidad: row.Cantidad,
      unidad: String(row.Unidad || "").trim(),
      costo_unitario: row.Costo_unitario,
      costo_total: row.Costo_total,
      raw: row,
    }));

    const matches =
      !terms.length
        ? normalizedRows
        : normalizedRows.filter((row) => {
            const article = normalize(row.articulo);
            return terms.some((term) => article.includes(normalize(term)));
          });

    results.push({
      purchase_id: String(purchaseId),
      rows_count: normalizedRows.length,
      matches,
    });

    await sleep(150);
  }

  return results;
}
