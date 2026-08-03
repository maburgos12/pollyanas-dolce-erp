const money = new Intl.NumberFormat("es-MX", {
  style: "currency",
  currency: "MXN",
  maximumFractionDigits: 0,
});

const views = document.querySelectorAll(".view");
const switches = document.querySelectorAll("[data-switch]");
const toast = document.querySelector("#toast");
let toastTimer;

function showToast(title, message) {
  toast.querySelector("strong").textContent = title;
  toast.querySelector("p").textContent = message;
  toast.classList.add("show");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => toast.classList.remove("show"), 3200);
}

function switchView(name) {
  views.forEach((view) => view.classList.toggle("active", view.id === `view-${name}`));
  switches.forEach((button) => button.classList.toggle("active", button.dataset.switch === name));
  document.querySelector(".workspace").scrollTo({ top: 0, behavior: "smooth" });
}

switches.forEach((button) => {
  button.addEventListener("click", () => switchView(button.dataset.switch));
});

function recalculate() {
  let total = 0;
  document.querySelectorAll(".item-row").forEach((row) => {
    const quantity = Number(row.querySelector(".quantity")?.value || 0);
    const unitCost = Number(row.querySelector(".unit-cost")?.value || 0);
    const subtotal = quantity * unitCost;
    const totalCell = row.querySelector(".line-total");
    if (subtotal > 0) {
      totalCell.textContent = money.format(subtotal);
      totalCell.classList.remove("muted");
      total += subtotal;
    } else {
      totalCell.textContent = "Por cotizar";
      totalCell.classList.add("muted");
    }
  });
  document.querySelector("#grand-total").textContent = money.format(total);
}

document.querySelector("#item-rows").addEventListener("input", (event) => {
  if (event.target.matches(".quantity, .unit-cost")) recalculate();
});

document.querySelector("#add-item").addEventListener("click", () => {
  const row = document.createElement("tr");
  row.className = "item-row";
  row.innerHTML = `
    <td><button class="photo-thumb upload" aria-label="Agregar imagen"><span>＋</span><small>Agregar foto</small></button></td>
    <td class="item-copy"><input aria-label="Artículo" placeholder="¿Qué necesita tu área?"><textarea aria-label="Motivo" placeholder="Explica para qué se utilizará"></textarea><span class="category">Por clasificar</span></td>
    <td><div class="qty"><input class="quantity" type="number" value="1" min="1"><select aria-label="Unidad"><option>pieza</option><option>juego</option><option>caja</option><option>servicio</option></select></div></td>
    <td><input type="date" value="2026-09-20" aria-label="Fecha requerida"></td>
    <td><select aria-label="Prioridad"><option>Normal</option><option>Alta</option><option>Urgente</option></select></td>
    <td><div class="money-input"><span>$</span><input class="unit-cost" type="number" placeholder="Opcional" min="0" aria-label="Costo unitario estimado"></div><small class="optional">Sin estimar</small></td>
    <td><strong class="line-total muted">Por cotizar</strong></td>
    <td><button class="row-menu remove-row" aria-label="Eliminar artículo">×</button></td>`;
  document.querySelector("#item-rows").append(row);
  row.querySelector("input").focus();
  showToast("Artículo agregado", "Completa la descripción, cantidad y fecha requerida.");
});

document.querySelector("#item-rows").addEventListener("click", (event) => {
  const removeButton = event.target.closest(".remove-row");
  if (removeButton) {
    removeButton.closest("tr").remove();
    recalculate();
    showToast("Artículo retirado", "El resto de la solicitud se conserva sin cambios.");
    return;
  }
  const photoButton = event.target.closest(".photo-thumb");
  if (photoButton) {
    const input = document.querySelector("#photo-input");
    input.dataset.target = [...document.querySelectorAll(".photo-thumb")].indexOf(photoButton);
    input.click();
  }
});

document.querySelector("#photo-input").addEventListener("change", (event) => {
  const file = event.target.files[0];
  if (!file) return;
  const targets = document.querySelectorAll(".photo-thumb");
  const target = targets[Number(event.target.dataset.target)];
  if (!target) return;
  const url = URL.createObjectURL(file);
  target.style.background = `center / cover no-repeat url("${url}")`;
  target.innerHTML = "<small>Imagen agregada</small>";
  target.classList.remove("upload");
  showToast("Imagen agregada", "La referencia visual acompañará al artículo durante todo el seguimiento.");
  event.target.value = "";
});

document.querySelector("#send-request").addEventListener("click", (event) => {
  const button = event.currentTarget;
  const original = button.innerHTML;
  button.disabled = true;
  button.textContent = "Enviando…";
  setTimeout(() => {
    button.disabled = false;
    button.innerHTML = original;
    showToast("Solicitud enviada", "Compras ya puede asignarla y comenzar a cotizar.");
  }, 650);
});

document.querySelectorAll("[data-decision]").forEach((button) => {
  button.addEventListener("click", () => {
    const requiresComment = button.dataset.decision !== "Compra autorizada";
    const comment = document.querySelector("#decision-comment");
    if (requiresComment && !comment.value.trim()) {
      comment.focus();
      comment.setAttribute("aria-invalid", "true");
      showToast("Falta explicar la decisión", "Agrega un comentario para conservar el motivo en el historial.");
      return;
    }
    comment.removeAttribute("aria-invalid");
    showToast(button.dataset.decision, "La decisión quedó registrada con fecha, responsable e impacto presupuestal.");
  });
});

recalculate();
