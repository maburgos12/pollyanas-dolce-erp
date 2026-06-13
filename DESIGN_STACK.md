# Design Stack

This file is the shared design operating guide for Pollyana's Dolce. Codex, Claude, and any other agent should use this file together with `PRODUCT.md`.

Keep `PRODUCT.md` as the product strategy. Keep this file as the skill-routing and design-execution guide. Do not duplicate the full rules into `AGENTS.md` or `CLAUDE.md`.

## Core Principle

Use one primary skill per task and at most two support skills. Do not activate every design skill at once. Pick the smallest stack that fits the surface.

The ERP is a product UI: the task wins over decoration. The website, online store, and branding surfaces are public brand UI: first impression and visual identity matter more.

## Default Routing

| Work type | Primary skill | Support skills | Use this when |
| --- | --- | --- | --- |
| ERP dashboards, forms, reports, operations | `impeccable` | `redesign-existing-projects`, `emil-design-eng`, `hallmark audit` | Building or improving internal product UI |
| Existing ERP screen redesign | `redesign-existing-projects` | `impeccable`, `emil-design-eng` | Improving current UI without rewriting the flow |
| Public website / online store | `hallmark` | `design-taste-frontend`, `high-end-visual-design`, `emil-design-eng` | Building or redesigning customer-facing pages |
| Landing page / campaign page | `design-taste-frontend` | `hallmark`, `high-end-visual-design` | A marketing page needs strong structure and no template feel |
| Premium visual direction | `high-end-visual-design` | `hallmark`, `emil-design-eng` | A surface needs agency-level polish or stronger visual hierarchy |
| Brand identity / visual world | `brandkit` | `hallmark`, `imagegen-frontend-web` | Logo concepts, brand boards, identity directions, moodboards |
| Web design references by section | `imagegen-frontend-web` | `hallmark`, `image-to-code` | Generate visual references before coding a website |
| Visual reference to implementation | `image-to-code` | `imagegen-frontend-web`, `emil-design-eng` | Convert a strong visual reference into frontend code |
| Mobile app screens | `imagegen-frontend-mobile` | `emil-design-eng` | Mobile-only concepts or flows |
| Minimal editorial UI | `minimalist-ui` | `hallmark`, `emil-design-eng` | Quiet, restrained, typography-led pages |
| Industrial / tactical UI | `industrial-brutalist-ui` | `hallmark` | Only when explicitly requested or useful for dense telemetry-style dashboards |
| Google Stitch design system | `stitch-design-taste` | `impeccable` | Creating a Stitch-ready `DESIGN.md` |
| Full files / no placeholders | `full-output-enforcement` | Any relevant skill | User asks for full implementation, complete file, or exhaustive output |
| Advanced GSAP / Awwwards motion | `gpt-taste` | `hallmark`, `emil-design-eng` | Experimental public pages, not everyday ERP workflows |
| Backward-compatible taste output | `design-taste-frontend-v1` | None by default | Only when exact v1 behavior is needed |

## ERP Stack

Default order:

1. `impeccable`: product fit, register, UX structure, accessibility, task clarity.
2. `redesign-existing-projects`: targeted improvements in the existing codebase without breaking routes or workflows.
3. `emil-design-eng`: buttons, popovers, tabs, drawers, toasts, motion, active states, focus states, and perceived responsiveness.
4. `hallmark audit`: final anti-generic review when a screen risks looking like a template.

ERP guardrails:

- Do not make ERP screens feel like Oracle, SAP, or a generic admin template.
- Do not turn dense operational screens into marketing pages.
- Keep source-of-truth boundaries visible: Point owns catalog/product/insumos truth, RRHH owns people, `auth.User` owns credentials, and `UserProfile` owns operational scope.
- Prefer labels and workflows that match bakery operations: production, sales, inventory, branches, logistics, maintenance, bonuses, reports, and exports.
- Validate important UI changes against the real visible flow when possible: route, button, modal, export, print/PWA behavior, hard refresh, and production state are separate checkpoints.

## Website And Store Stack

Default order:

1. `hallmark`: macrostructure, anti-AI-slop discipline, honest copy, and non-generic visual rhythm.
2. `design-taste-frontend` or `high-end-visual-design`: stronger landing-page, store, campaign, or premium visual execution.
3. `brandkit` when identity or brand-world decisions are needed before layout.
4. `imagegen-frontend-web` when section-level visual references should be generated before implementation.
5. `image-to-code` when implementing from a visual reference.
6. `emil-design-eng` for final interaction and motion craft.

Website/store guardrails:

- Use real product, bakery, pastry, and brand assets when available.
- If assets or metrics are missing, use honest placeholders instead of invented proof.
- Do not invent testimonials, logos, conversion metrics, customer counts, or awards.
- Avoid generic bakery templates, generic SaaS rhythms, and overused AI patterns.
- Public pages can be more expressive than the ERP, but they must still feel like Pollyana's Dolce, not a template marketplace.

## Emil Design Engineering Rules

Use `emil-design-eng` whenever the work touches interaction feel or motion.

- Animate only when it improves feedback, spatial continuity, state comprehension, or perceived responsiveness.
- Never animate keyboard-driven actions or high-frequency operator workflows.
- Keep ordinary UI motion under 300ms: buttons 100-160ms, tooltips/popovers 125-200ms, dropdowns/selects 150-250ms.
- Prefer `transform` and `opacity`; avoid animating layout properties.
- Avoid `transition: all`, `ease-in` for UI entry, and `scale(0)` entry animations.
- Buttons and pressable controls need an active feel, usually `transform: scale(0.97)`.
- Popovers should animate from their trigger origin; centered modals can stay centered.
- Gate hover motion behind `@media (hover: hover) and (pointer: fine)`.
- Always include `prefers-reduced-motion`.
- When reviewing UI code with Emil rules, use a markdown table with `Before`, `After`, and `Why` columns.

## Hallmark Rules

Use `hallmark` as a design-quality filter and structural anti-slop system.

- `hallmark audit <target>`: no-edit punch list before broad visual changes.
- `hallmark redesign <target>`: scoped redesign while preserving routes, content intent, and component ownership unless a rebuild is approved.
- `hallmark study <screenshot-or-url>`: extract design DNA from references without copying pixels.
- Before editing, state expected file changes. Deletions require explicit approval.
- For marketing/store pages, require real assets or honest placeholders.
- Do not copy template marketplace designs or another brand's pixels.

## Leon Taste Skills

The Leon skills are specialized taste modes. Use them after choosing the work type, not as a blanket default.

- `design-taste-frontend`: default Leon frontend taste skill for landing pages, portfolios, public pages, and redesigns. Not for dashboards or data-heavy ERP workflows.
- `redesign-existing-projects`: best Leon skill for upgrading an existing site or app while respecting its current stack.
- `high-end-visual-design`: use when the visual bar needs to feel premium, cinematic, or agency-grade.
- `brandkit`: use for brand systems, logo directions, visual-world boards, and identity presentations.
- `imagegen-frontend-web`: use for one image per website section before implementation.
- `image-to-code`: use after a visual reference exists and needs to become frontend code.
- `minimalist-ui`: use for restrained editorial or clean product surfaces when minimalism is explicitly wanted.
- `industrial-brutalist-ui`: use sparingly for tactical, mechanical, telemetry, or blueprint-like interfaces.
- `stitch-design-taste`: use only when creating a Google Stitch-ready `DESIGN.md`.
- `imagegen-frontend-mobile`: use for mobile app image concepts only.
- `full-output-enforcement`: use when completeness matters more than brevity.
- `gpt-taste`: use for experimental motion-heavy public pages, especially GSAP. Avoid it for normal ERP workflows.
- `design-taste-frontend-v1`: legacy fallback only.

## File Roles

- `PRODUCT.md`: product strategy, users, principles, anti-references, accessibility.
- `DESIGN_STACK.md`: skill routing and execution rules.
- `DESIGN.md`: future visual system tokens, typography, color, components, and layout rules. Do not create it until there is real app code or a confirmed visual direction to document.
- `AGENTS.md`: Codex entrypoint and ERP operating protocol. It should point to `PRODUCT.md` and `DESIGN_STACK.md` instead of duplicating the full design rules.
- `CLAUDE.md`: Claude entrypoint and ERP operating protocol. It should point to `PRODUCT.md` and `DESIGN_STACK.md` instead of duplicating the full design rules.
