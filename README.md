# Comparador de Precios UY

> **Realizado por Joaquín Moreno Antuña.** · [neverhype.com](https://neverhype.com)

A free, search-first web app that compares **monthly supermarket prices** across
Uruguayan chains, built entirely on **official open data** from the SIPC
(Sistema de Información de Precios al Consumidor, gub.uy / precios.uy). No
scraping, no paid services — 100% free.

**🔗 Ver el sitio (live):** https://joaquinmorenoa.github.io/ds-comparador-precios-uy/

---

## English

### What it does

- **Search-first UX.** The landing page is a single search bar. After a search,
  the app reveals a per-product comparison across chains.
- **Per-product comparison.** For any product: cheapest vs. most expensive
  chain, price gap, month-over-month delta, a bar chart of the latest month, and
  a line chart of the monthly price evolution per chain (each point carries its
  min/max range).
- **Basic products table.** Monthly price of a set of staple products in each
  chain, with a year/month picker and the cheapest highlighted.
- **Compare tool.** Pick a product and two months to plot its evolution across
  chains for that range.
- **About-the-data panel.** Explains the source, the monthly aggregation
  (average with min/max), and that values are in UYU.
- **INE basic basket.** A monthly per-capita basic-basket estimate: an official
  base value adjusted by the current CPI (IPC, fetched from datosuruguay.com's
  open API), clearly labeled as an estimate with its source.

### The 2 GB problem (and the approach)

The official **Precios** resource is a single ~2 GB CSV of daily prices, and its
DataStore API is **not enabled** (no server-side querying). A browser can't load
2 GB and free storage can't persist it, so the ETL does a **single streaming
pass**: it downloads the file once (with timeout + retries) and aggregates it
on the fly to **monthly average / min / max per product and chain**, without
holding raw rows in memory or in a giant database. The result is a small
`data.js` (hundreds of KB). Designed to run on **GitHub Actions** (fast
bandwidth), not locally.

```
precios_2025.csv (~2 GB)  ──stream+aggregate──►  fact_agg (SQLite, ~KB)  ──►  frontend/data.js  ──►  GitHub Pages
```

- `src/etl.py` — robust download (retries), streaming aggregation to monthly
  avg/min/max per (product, chain). Column names and date format are
  auto-detected and reported.
- `src/build_web_data.py` — reads `fact_agg`, builds the search index, the
  monthly series and the basic-products table. Emits `frontend/data.js`.
- `frontend/` — static dashboard (vanilla JS + Chart.js, vendored locally).
- `.github/workflows/update-and-deploy.yml` — runs the ETL (best-effort, with a
  time cap so it never hangs the deploy) and publishes to Pages. Re-runs monthly.

### Data source

Sistema de Información de Precios al Consumidor (SIPC), open data published by
gub.uy (precios.uy). Prices are official; this project reads, aggregates and
visualizes them. Values in Uruguayan pesos (UYU).

---

## Español

### Qué hace

- **Buscador primero.** La pantalla inicial es solo una barra de búsqueda. Al
  buscar, aparece la comparativa del producto entre cadenas.
- **Comparativa por producto.** Cadena más barata y más cara, brecha, delta
  mensual, gráfico de barras del último mes y de líneas con la evolución mensual
  por cadena (cada punto trae su rango mín/máx).
- **Tabla de productos básicos.** Precio mensual de productos básicos en cada
  cadena, con selector de año/mes y el más barato resaltado.
- **Comparador.** Elegís un producto y dos meses para graficar su evolución
  entre cadenas en ese rango.
- **Panel "Acerca de los datos".** Explica la fuente, la agregación mensual
  (promedio con mín/máx) y que los valores están en UYU.
- **Canasta básica del INE.** Estimación mensual per cápita: un valor base
  oficial del INE ajustado por el IPC actual (traído de la API abierta de
  datosuruguay.com), etiquetada como estimación con su fuente. El valor base se
  configura en `src/build_web_data.py` (`INE_BASE`).

### El problema de los 2 GB (y el enfoque)

El recurso oficial de **Precios** es un único CSV de ~2 GB de precios diarios, y
su API de DataStore **no está activa** (no se puede consultar del lado del
servidor). Un navegador no puede cargar 2 GB y el almacenamiento gratis no lo
guarda, así que el ETL hace un **único pase por streaming**: baja el archivo una
vez (con timeout y reintentos) y lo agrega sobre la marcha a **promedio / mínimo
/ máximo mensual por producto y cadena**, sin cargar las filas crudas en memoria
ni en una base gigante. El resultado es un `data.js` chico (cientos de KB).
Pensado para correr en **GitHub Actions** (buena conexión), no localmente.

```
precios_2025.csv (~2 GB)  ──stream+agrega──►  fact_agg (SQLite, ~KB)  ──►  frontend/data.js  ──►  GitHub Pages
```

- `src/etl.py` — descarga robusta (reintentos) y agregación por streaming a
  promedio/mín/máx mensual por (producto, cadena). Detecta y reporta las
  columnas y el formato de fecha.
- `src/build_web_data.py` — lee `fact_agg`, arma el índice de búsqueda, las
  series mensuales y la tabla de básicos. Genera `frontend/data.js`.
- `frontend/` — dashboard estático (JS puro + Chart.js, incluido localmente).
- `.github/workflows/update-and-deploy.yml` — corre el ETL (best-effort, con
  tope de tiempo para no colgar el deploy) y publica en Pages. Se repite mensual.

### Fuente de datos

Sistema de Información de Precios al Consumidor (SIPC), datos abiertos de gub.uy
(precios.uy). Los precios son oficiales; este proyecto los lee, agrega y
visualiza. Valores en pesos uruguayos (UYU).

---

> **Realizado por Joaquín Moreno Antuña.** · [NeverHype](https://neverhype.com)
