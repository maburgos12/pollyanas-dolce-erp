(function () {
    "use strict";

    var MIN_OPTIONS = 8;
    var MAX_RENDERED_OPTIONS = 80;
    var KEYWORDS = [
        "articulo",
        "artículo",
        "cliente",
        "empleado",
        "insumo",
        "item",
        "producto",
        "proveedor",
        "receta",
        "responsable",
        "sucursal",
        "unidad"
    ];

    function normalize(value) {
        return String(value || "")
            .normalize("NFD")
            .replace(/[\u0300-\u036f]/g, "")
            .toLowerCase()
            .trim();
    }

    function getDescriptor(select) {
        return normalize([
            select.name,
            select.id,
            select.getAttribute("aria-label"),
            select.className
        ].join(" "));
    }

    function isOptedOut(select) {
        return select.matches('[data-searchable-select="false"], [data-native-select="true"], .js-native-select, .gas-native-select');
    }

    function shouldEnhance(select) {
        if (!select || select.dataset.pdSearchableReady === "true") return false;
        if (select.multiple || select.size > 1 || select.disabled) return false;
        if (isOptedOut(select) || select.closest('[data-searchable-scope="off"]')) return false;
        if (select.options.length <= 1) return false;
        if (select.dataset.searchableSelect === "true") return true;
        if (select.options.length >= MIN_OPTIONS) return true;

        var descriptor = getDescriptor(select);
        return KEYWORDS.some(function (keyword) {
            return descriptor.indexOf(normalize(keyword)) !== -1;
        });
    }

    function optionData(select) {
        return Array.prototype.slice.call(select.options).map(function (option, index) {
            return {
                index: index,
                value: option.value,
                label: option.textContent.trim(),
                disabled: option.disabled,
                empty: option.value === ""
            };
        });
    }

    function dispatchNativeChange(select) {
        select.dispatchEvent(new Event("input", { bubbles: true }));
        select.dispatchEvent(new Event("change", { bubbles: true }));
    }

    function findExactOption(options, text) {
        var normalizedText = normalize(text);
        if (!normalizedText) return null;
        for (var i = 0; i < options.length; i += 1) {
            if (!options[i].disabled && normalize(options[i].label) === normalizedText) {
                return options[i];
            }
        }
        return null;
    }

    function selectedLabel(select) {
        var option = select.options[select.selectedIndex];
        if (!option || option.value === "") return "";
        return option.textContent.trim();
    }

    function enhanceSelect(select) {
        if (!shouldEnhance(select)) return null;

        select.dataset.pdSearchableReady = "true";

        var wrapper = document.createElement("div");
        var input = document.createElement("input");
        var chevron = document.createElement("span");
        var list = document.createElement("div");
        var uid = "pd-searchable-" + Math.random().toString(36).slice(2);
        var activeIndex = -1;
        var currentOptions = [];
        var originalRequired = select.required;

        wrapper.className = "pd-searchable-select";
        input.type = "text";
        input.className = "pd-searchable-select__input";
        input.setAttribute("autocomplete", "off");
        input.setAttribute("role", "combobox");
        input.setAttribute("aria-autocomplete", "list");
        input.setAttribute("aria-expanded", "false");
        input.setAttribute("aria-controls", uid);
        input.placeholder = select.getAttribute("placeholder") || select.dataset.placeholder || "Buscar y seleccionar";
        input.required = originalRequired;
        input.value = selectedLabel(select);

        chevron.className = "pd-searchable-select__chevron";
        chevron.setAttribute("aria-hidden", "true");

        list.id = uid;
        list.className = "pd-searchable-select__list";
        list.setAttribute("role", "listbox");

        select.classList.add("pd-select-native-hidden");
        select.setAttribute("aria-hidden", "true");
        select.tabIndex = -1;
        select.required = false;
        if (originalRequired) select.dataset.pdOriginalRequired = "true";
        select.insertAdjacentElement("afterend", wrapper);
        wrapper.appendChild(input);
        wrapper.appendChild(chevron);
        wrapper.appendChild(list);

        function setOpen(open) {
            wrapper.classList.toggle("is-open", open);
            input.setAttribute("aria-expanded", open ? "true" : "false");
            if (!open) {
                activeIndex = -1;
                input.removeAttribute("aria-activedescendant");
            }
        }

        function setValidity(message) {
            input.setCustomValidity(message || "");
            wrapper.classList.toggle("is-invalid", Boolean(message));
        }

        function clearSelectionForTyping() {
            if (select.value !== "") {
                select.value = "";
            }
        }

        function choose(option) {
            if (!option || option.disabled) return;
            select.selectedIndex = option.index;
            input.value = option.empty ? "" : option.label;
            setValidity("");
            setOpen(false);
            dispatchNativeChange(select);
        }

        function validateTypedValue() {
            var typed = input.value.trim();
            var options = optionData(select);
            var exact = findExactOption(options, typed);

            if (exact) {
                choose(exact);
                return true;
            }

            if (!typed) {
                var emptyOption = options.filter(function (option) { return option.empty && !option.disabled; })[0];
                if (emptyOption && !originalRequired) {
                    choose(emptyOption);
                    return true;
                }
                if (originalRequired) {
                    setValidity("Selecciona una opcion de la lista.");
                    return false;
                }
                select.value = "";
                setValidity("");
                dispatchNativeChange(select);
                return true;
            }

            select.value = "";
            setValidity("Selecciona una opcion de la lista.");
            return false;
        }

        function render(query) {
            var normalizedQuery = normalize(query);
            var options = optionData(select).filter(function (option) {
                if (option.disabled) return false;
                if (option.empty && normalizedQuery) return false;
                return !normalizedQuery || normalize(option.label).indexOf(normalizedQuery) !== -1;
            }).slice(0, MAX_RENDERED_OPTIONS);

            currentOptions = options;
            list.innerHTML = "";
            activeIndex = options.length ? 0 : -1;

            if (!options.length) {
                var empty = document.createElement("div");
                empty.className = "pd-searchable-select__empty";
                empty.textContent = "Sin resultados";
                list.appendChild(empty);
                return;
            }

            options.forEach(function (option, index) {
                var button = document.createElement("button");
                button.type = "button";
                button.id = uid + "-option-" + index;
                button.className = "pd-searchable-select__option";
                button.setAttribute("role", "option");
                button.setAttribute("aria-selected", select.value === option.value ? "true" : "false");
                button.textContent = option.label || "Sin seleccionar";

                if (index === activeIndex) {
                    button.classList.add("is-active");
                    input.setAttribute("aria-activedescendant", button.id);
                }
                if (select.value === option.value) {
                    button.classList.add("is-selected");
                }

                button.addEventListener("mousedown", function (event) {
                    event.preventDefault();
                    choose(option);
                });

                list.appendChild(button);
            });
        }

        function moveActive(delta) {
            if (!currentOptions.length) return;
            activeIndex = (activeIndex + delta + currentOptions.length) % currentOptions.length;
            Array.prototype.slice.call(list.querySelectorAll(".pd-searchable-select__option")).forEach(function (node, index) {
                var active = index === activeIndex;
                node.classList.toggle("is-active", active);
                if (active) {
                    input.setAttribute("aria-activedescendant", node.id);
                    node.scrollIntoView({ block: "nearest" });
                }
            });
        }

        input.addEventListener("focus", function () {
            render(input.value);
            setOpen(true);
        });

        input.addEventListener("input", function () {
            clearSelectionForTyping();
            setValidity("");
            render(input.value);
            setOpen(true);
        });

        input.addEventListener("keydown", function (event) {
            if (event.key === "ArrowDown") {
                event.preventDefault();
                if (!wrapper.classList.contains("is-open")) {
                    render(input.value);
                    setOpen(true);
                } else {
                    moveActive(1);
                }
            } else if (event.key === "ArrowUp") {
                event.preventDefault();
                moveActive(-1);
            } else if (event.key === "Enter") {
                if (wrapper.classList.contains("is-open") && currentOptions[activeIndex]) {
                    event.preventDefault();
                    choose(currentOptions[activeIndex]);
                }
            } else if (event.key === "Escape") {
                input.value = selectedLabel(select);
                setValidity("");
                setOpen(false);
            }
        });

        input.addEventListener("blur", function () {
            setTimeout(function () {
                validateTypedValue();
                setOpen(false);
            }, 120);
        });

        select.addEventListener("change", function () {
            input.value = selectedLabel(select);
            setValidity("");
        });

        var form = select.form;
        if (form) {
            form.addEventListener("submit", function (event) {
                if (!validateTypedValue()) {
                    event.preventDefault();
                    input.reportValidity();
                }
            });
        }

        return wrapper;
    }

    function enhanceAll(root) {
        var scope = root || document;
        Array.prototype.slice.call(scope.querySelectorAll("select")).forEach(enhanceSelect);
    }

    function watchMutations() {
        var observer = new MutationObserver(function (mutations) {
            mutations.forEach(function (mutation) {
                Array.prototype.slice.call(mutation.addedNodes).forEach(function (node) {
                    if (!node || node.nodeType !== 1) return;
                    if (node.matches && node.matches("select")) enhanceSelect(node);
                    if (node.querySelectorAll) enhanceAll(node);
                });
            });
        });
        observer.observe(document.body, { childList: true, subtree: true });
    }

    function boot() {
        enhanceAll(document);
        watchMutations();
    }

    window.PollyanaSearchableSelects = {
        enhance: enhanceSelect,
        enhanceAll: enhanceAll
    };

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", boot);
    } else {
        boot();
    }
}());
