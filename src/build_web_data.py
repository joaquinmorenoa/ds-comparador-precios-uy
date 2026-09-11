"""
Genera frontend/data.js a partir del warehouse SQLite (tabla fact_agg).

fact_agg ya viene agregada por (producto, cadena, mes) con promedio, minimo y
maximo. Este paso arma el indice de busqueda, las series mensuales por cadena
(promedio + rango min/max) y la tabla de productos basicos. El payload pesa
unos KB, apto para un sitio estatico.
"""

import json
import os
import sqlite3
from datetime import date

DB_PATH = os.path.join("data", "warehouse.db")
OUT_PATH = os.path.join("frontend", "data.js")

# --------------------------------------------------------------------------- #
# Canasta basica (INE): valor base oficial + reconstruccion mensual por IPC.
# El IPC se trae automatico de datosuruguay.com (API abierta, actual).
# REEMPLAZAR el valor base con la cifra oficial exacta del INE cuando se tenga.
# --------------------------------------------------------------------------- #
IPC_API = "https://datosuruguay.com/api/v1/inflation"
IPC_SERIE = "index_total_country"  # indice IPC total pais
INE_BASE = {
    "valor": 17718,                # UYU per capita/mes (PROVISORIO - a confirmar)
    "mes": "2024-06",              # mes del valor base
    "tipo": "Canasta Básica per cápita (línea de pobreza, promedio país)",
    "fuente": "INE (líneas de pobreza 2024) · IPC vía datosuruguay.com (CC-BY-4.0)",
    "provisional": True,
}

# Productos comparables minimos por cadena para entrar al indice.
MIN_CADENAS = 2
# Tope de productos para acotar el tamano del payload estatico.
MAX_PRODUCTOS = 1200

# Productos basicos para la tabla: categoria -> patrones sobre el nombre.
BASICOS = [
    ("Arroz", ("arroz",)),
    ("Aceite", ("aceite",)),
    ("Leche", ("leche",)),
    ("Pan", ("pan ", "pan de", "pan flauta")),
    ("Fideos", ("fideo", "pasta")),
    ("Azucar", ("azucar",)),
    ("Yerba", ("yerba",)),
    ("Harina", ("harina",)),
    ("Huevos", ("huevo",)),
    ("Cafe", ("cafe",)),
    ("Carne picada", ("carne picada", "picada")),
    ("Pollo", ("pollo",)),
]


def _month_order(meses):
    """Ordena etiquetas AAAA-MM cronologicamente."""
    return sorted(m for m in meses if m and str(m)[0].isdigit())


def build_series(conn):
    """series[id][cadena][mes] = [promedio, minimo, maximo]."""
    cur = conn.execute(
        "SELECT id_producto, cadena, mes, prom, minp, maxp FROM fact_agg"
    )
    series = {}
    for pid, cadena, mes, prom, minp, maxp in cur:
        if prom is None:
            continue
        series.setdefault(str(pid), {}).setdefault(cadena, {})[mes] = [
            round(prom, 2),
            round(minp, 2) if minp is not None else round(prom, 2),
            round(maxp, 2) if maxp is not None else round(prom, 2),
        ]
    return series


def build_index(conn, series):
    """Indice de busqueda para productos comparables (>= MIN_CADENAS)."""
    meta = {
        r[0]: (r[1], r[2])
        for r in conn.execute("SELECT id_producto, producto, marca FROM dim_producto")
    }
    comparables = []
    for pid, per_cadena in series.items():
        if len(per_cadena) < MIN_CADENAS:
            continue
        nombre, marca = meta.get(pid, ("", ""))
        obs = sum(len(v) for v in per_cadena.values())
        comparables.append((obs, pid, str(nombre or ""), str(marca or "")))

    comparables.sort(reverse=True)
    comparables = comparables[:MAX_PRODUCTOS]

    productos = [
        {"id": pid, "n": nombre, "m": marca}
        for _, pid, nombre, marca in comparables
    ]
    kept = {pid for _, pid, _, _ in comparables}
    precios = {pid: series[pid] for pid in kept}
    return productos, precios, meta


def build_basicos(series, meta):
    """Serie mensual de productos basicos, por cadena (para la tabla)."""
    nombres = {pid: str(n or "").lower() for pid, (n, _m) in meta.items()}
    obs = {pid: sum(len(v) for v in per.values()) for pid, per in series.items()}
    all_meses = _month_order({m for per in series.values() for c in per.values() for m in c})
    if not all_meses:
        return None

    productos, precios = [], {}
    for categoria, patrones in BASICOS:
        candidatos = [
            pid for pid, low in nombres.items()
            if pid in series and any(p in low for p in patrones)
        ]
        if not candidatos:
            continue
        rep = max(candidatos, key=lambda pid: obs.get(pid, 0))
        productos.append({"cat": categoria, "id": rep, "n": meta[rep][0]})
        precios[rep] = series[rep]
    return {"meses": all_meses, "productos": productos, "precios": precios}


def fetch_ipc():
    """Indice IPC mensual (total pais) desde la API abierta de datosuruguay."""
    import requests

    r = requests.get(IPC_API, timeout=(20, 45),
                     headers={"User-Agent": "NeverHype-ETL/1.0"})
    r.raise_for_status()
    js = r.json()
    registros = js.get("data", js) if isinstance(js, dict) else js
    ipc = {}
    for rec in registros:
        if rec.get("series") == IPC_SERIE and rec.get("value") is not None:
            ipc[str(rec.get("date", ""))[:7]] = float(rec["value"])
    return ipc


def build_ine(ipc):
    """Reconstruye la canasta mensual = valor base oficial * (IPC[m]/IPC[base])."""
    if not ipc:
        return None
    base_mes, base_val = INE_BASE["mes"], INE_BASE["valor"]
    base_idx = ipc.get(base_mes)
    if base_idx is None:
        previos = sorted(m for m in ipc if m <= base_mes)
        if not previos:
            return None
        base_idx = ipc[previos[-1]]
    # Solo años recientes (moneda estable); el IPC llega mucho mas atras.
    serie = [[m, round(base_val * ipc[m] / base_idx)]
             for m in sorted(ipc) if ipc[m] and m >= "2015-01"]
    if not serie:
        return None
    return {
        "sub": "Estimación: valor base oficial del INE ajustado por el IPC mensual",
        "tipo": INE_BASE["tipo"],
        "fuente": INE_BASE["fuente"],
        "provisional": INE_BASE.get("provisional", False),
        "serie": serie,
    }


def main():
    conn = sqlite3.connect(DB_PATH)
    try:
        series = build_series(conn)
        productos, precios, meta = build_index(conn, series)
        basicos = build_basicos(series, meta)
    finally:
        conn.close()

    ine = None
    try:
        ine = build_ine(fetch_ipc())
        print(f"canasta INE: {'ok, ' + str(len(ine['serie'])) + ' meses' if ine else 'sin datos'}")
    except Exception as exc:  # noqa: BLE001
        print(f"canasta INE no disponible ({exc}); se omite")

    meses = _month_order({m for p in precios.values() for c in p.values() for m in c})
    cadenas = sorted({c for p in precios.values() for c in p})

    payload = {
        "meta": {
            "generado": date.today().isoformat(),
            "fuente": "Sistema de Informacion de Precios al Consumidor (SIPC) - datos abiertos gub.uy",
            "meses": meses,
            "cadenas": cadenas,
            "totalProductos": len(productos),
        },
        "productos": productos,
        "precios": precios,
        "basicos": basicos,
    }
    if ine:
        payload["ine"] = ine

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as fh:
        fh.write("window.NH_DATA = ")
        json.dump(payload, fh, ensure_ascii=False, separators=(",", ":"))
        fh.write(";\n")

    kb = os.path.getsize(OUT_PATH) // 1024
    n_bas = len(basicos["productos"]) if basicos else 0
    print(f"data.js -> {OUT_PATH} ({kb} KB)")
    print(f"  productos={len(productos)}  cadenas={len(cadenas)}  "
          f"meses={len(meses)}  basicos={n_bas}")


if __name__ == "__main__":
    main()
