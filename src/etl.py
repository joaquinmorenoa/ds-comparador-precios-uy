"""
ETL del comparador de precios (datos oficiales SIPC).

El recurso de precios es un CSV de ~2 GB sin API de consulta (datastore
inactivo), asi que se procesa en un unico pase por streaming: se baja una vez
con reintentos y se agrega en el momento a PROMEDIO por (producto, cadena,
trimestre), sin cargar las filas crudas en memoria ni en una base gigante.
El resultado (tabla fact_agg) pesa unos KB. Pensado para correr en GitHub
Actions (buena conexion), no localmente.
"""

import os
import re
import socket
import sqlite3
import unicodedata
import warnings
from datetime import datetime

import pandas as pd

warnings.filterwarnings("ignore", category=UserWarning)

# Corta una descarga colgada en vez de esperar indefinidamente.
socket.setdefaulttimeout(120)

# Formatos de fecha candidatos (se detecta uno y se aplica vectorizado).
DATE_FORMATS = (
    "%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y", "%m/%d/%Y",
    "%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S", "%Y-%m-%dT%H:%M:%S",
)

BASE = "https://catalogodatos.gub.uy/dataset/35d8f45e-2aa7-48b5-98dd-f973b05cf8ba/resource"
FILES = {
    "precios_2025.csv": "36c62bab-b7e4-4c9e-ad9b-4c1182090a22",
    "productos.csv": "ed042b97-12ce-46ff-a169-b2594337a6e4",
    "establecimiento.csv": "5fbdd7e8-97fa-44db-b978-4381670c8933",
}

RAW_DIR = os.path.join("data", "raw")
DB_PATH = os.path.join("data", "warehouse.db")
CHUNK_ROWS = 300_000
COLLAPSE_EVERY = 20  # colapsa parciales cada N chunks para acotar memoria


# --------------------------------------------------------------------------- #
# Utilidades
# --------------------------------------------------------------------------- #
def _slug(text):
    text = str(text).strip().lower()
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "", text)


def _read_encoded(path, **kw):
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return pd.read_csv(path, sep=";", encoding=enc, **kw)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path, sep=";", encoding="latin-1", engine="python", **kw)


def _detect_encoding(path):
    """Encoding que funciona para el CSV grande (se fija con una muestra)."""
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            pd.read_csv(path, sep=";", encoding=enc, nrows=50)
            return enc
        except UnicodeDecodeError:
            continue
    return "latin-1"


def _pick(columns, *candidates):
    slugs = {col: _slug(col) for col in columns}
    # 1) match exacto de slug (evita que "producto" agarre "id.producto")
    for pattern in candidates:
        for col, slug in slugs.items():
            if slug == pattern:
                return col
    # 2) match por substring
    for pattern in candidates:
        for col, slug in slugs.items():
            if pattern in slug:
                return col
    return None


def _to_number(series):
    cleaned = (
        series.astype(str)
        .str.replace(r"[^\d,.\-]", "", regex=True)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    return pd.to_numeric(cleaned, errors="coerce")


def _detect_date_format(series):
    muestra = series.dropna().astype(str).str.strip().head(80)
    if muestra.empty:
        return None
    for fmt in DATE_FORMATS:
        ok = 0
        for valor in muestra:
            try:
                datetime.strptime(valor, fmt)
                ok += 1
            except ValueError:
                pass
        if ok >= max(1, int(len(muestra) * 0.8)):
            return fmt
    return None


def _to_month(series, fmt):
    """Deriva la etiqueta de mes (AAAA-MM) desde una fecha."""
    if fmt:
        dt = pd.to_datetime(series, errors="coerce", format=fmt)
    else:
        dt = pd.to_datetime(series, errors="coerce", dayfirst=True)
    per = dt.dt.strftime("%Y-%m")
    return per.where(dt.notna(), other=pd.NA)


# --------------------------------------------------------------------------- #
# Extract (descarga robusta con reintentos)
# --------------------------------------------------------------------------- #
def _download(url, dest, attempts=6):
    import time

    import requests

    headers = {"User-Agent": "Mozilla/5.0 (compatible; NeverHype-ETL/1.0)"}
    ultimo = None
    for intento in range(1, attempts + 1):
        try:
            with requests.get(url, headers=headers, stream=True,
                              timeout=(30, 90)) as resp:
                resp.raise_for_status()
                total = int(resp.headers.get("Content-Length", 0))
                tmp = dest + ".part"
                bajado = 0
                marca = 25 << 20  # avisar cada 25 MB
                with open(tmp, "wb") as fh:
                    for chunk in resp.iter_content(chunk_size=1 << 20):
                        if chunk:
                            fh.write(chunk)
                            bajado += len(chunk)
                            if bajado >= marca:
                                tot = f" / {total // (1<<20)} MB" if total else ""
                                print(f"    {bajado // (1<<20)} MB{tot}...", flush=True)
                                marca += 25 << 20
                os.replace(tmp, dest)
            print(f"  ok -> {dest} ({bajado // (1<<20)} MB)", flush=True)
            return
        except Exception as exc:  # noqa: BLE001
            ultimo = exc
            espera = min(30, 5 * intento)
            print(f"  intento {intento}/{attempts} fallo ({exc}); "
                  f"reintento en {espera}s", flush=True)
            time.sleep(espera)
    raise SystemExit(f"No se pudo descargar {os.path.basename(dest)}: {ultimo}")


def extract():
    os.makedirs(RAW_DIR, exist_ok=True)
    for name, rid in FILES.items():
        dest = os.path.join(RAW_DIR, name)
        if os.path.exists(dest) and os.path.getsize(dest) > 0:
            print(f"(ya estaba) {name} ({os.path.getsize(dest) // 1024} KB)", flush=True)
            continue
        url = f"{BASE}/{rid}/download/{name}"
        print(f"Descargando {name} ...", flush=True)
        _download(url, dest)


# --------------------------------------------------------------------------- #
# Dimensiones
# --------------------------------------------------------------------------- #
def load_dim_producto(conn):
    df = _read_encoded(os.path.join(RAW_DIR, "productos.csv"))
    pid = _pick(df.columns, "idproducto", "producto") or df.columns[0]
    out = pd.DataFrame(
        {
            "id_producto": df[pid].astype(str),
            "producto": df.get(_pick(df.columns, "producto"), ""),
            "marca": df.get(_pick(df.columns, "marca"), ""),
            "especificacion": df.get(_pick(df.columns, "especificacion", "especif"), ""),
            "nombre": df.get(_pick(df.columns, "nombre"), ""),
        }
    ).drop_duplicates(subset="id_producto")
    out.to_sql("dim_producto", conn, if_exists="replace", index=False)
    print(f"dim_producto: {len(out)} filas (clave: '{pid}')", flush=True)


def establecimiento_map():
    """id_establecimiento -> cadena (solo locales con cadena real)."""
    df = _read_encoded(os.path.join(RAW_DIR, "establecimiento.csv"))
    eid = _pick(df.columns, "idestablecimiento", "establecimiento") or df.columns[0]
    cad = _pick(df.columns, "cadena")
    mapa = {}
    if not cad:
        return mapa
    for ident, cadena in zip(df[eid].astype(str), df[cad].astype(str)):
        cadena = (cadena or "").strip()
        if cadena and cadena.lower() not in ("nan", "sin cadena", "s/c", ""):
            mapa[ident] = cadena
    return mapa


# --------------------------------------------------------------------------- #
# Agregacion por streaming del CSV de precios (~2 GB)
# --------------------------------------------------------------------------- #
def _collapse(partials):
    return (
        pd.concat(partials, ignore_index=True)
        .groupby(["id_producto", "cadena", "mes"], as_index=False)
        .agg(suma=("suma", "sum"), conteo=("conteo", "sum"),
             minp=("minp", "min"), maxp=("maxp", "max"))
    )


def aggregate_precios(conn, est_map):
    path = os.path.join(RAW_DIR, "precios_2025.csv")
    header = _read_encoded(path, nrows=200)
    cols = list(header.columns)

    col_prod = _pick(cols, "idproducto", "producto")
    col_est = _pick(cols, "idestablecimiento", "establecimiento")
    col_precio = _pick(cols, "precio", "importe", "valor")
    col_fecha = _pick(cols, "fecha", "periodo", "relevamiento")
    date_fmt = _detect_date_format(header[col_fecha]) if col_fecha else None

    print("Deteccion de columnas en precios_2025.csv:", flush=True)
    print(f"  producto={col_prod}  establecimiento={col_est}  "
          f"precio={col_precio}  fecha={col_fecha}  formato_fecha={date_fmt}",
          flush=True)
    if not (col_prod and col_est and col_precio):
        raise SystemExit(f"No se detectaron columnas clave. Columnas: {cols}")

    usecols = [c for c in (col_prod, col_est, col_precio, col_fecha) if c]
    enc = _detect_encoding(path)
    reader = pd.read_csv(path, sep=";", encoding=enc, usecols=usecols,
                         chunksize=CHUNK_ROWS)

    partials, total = [], 0
    for chunk in reader:
        frame = pd.DataFrame({
            "id_producto": chunk[col_prod].astype(str),
            "cadena": chunk[col_est].astype(str).map(est_map),
            "mes": _to_month(chunk[col_fecha], date_fmt) if col_fecha else pd.NA,
            "precio": _to_number(chunk[col_precio]),
        })
        frame = frame.dropna(subset=["precio", "cadena", "mes"])
        frame = frame[frame["precio"] > 0]
        if not frame.empty:
            grp = (frame.groupby(["id_producto", "cadena", "mes"],
                                 as_index=False)["precio"]
                   .agg(suma="sum", conteo="count", minp="min", maxp="max"))
            partials.append(grp)
        total += len(chunk)
        if len(partials) >= COLLAPSE_EVERY:
            partials = [_collapse(partials)]
        print(f"  procesadas {total} filas...", flush=True)

    if not partials:
        raise SystemExit("No se agregaron filas de precio validas.")

    final = _collapse(partials)
    final["prom"] = (final["suma"] / final["conteo"]).round(2)
    final["minp"] = final["minp"].round(2)
    final["maxp"] = final["maxp"].round(2)
    out = final[["id_producto", "cadena", "mes", "prom", "minp", "maxp"]]

    conn.execute("DROP TABLE IF EXISTS fact_agg")
    out.to_sql("fact_agg", conn, index=False)
    conn.execute("CREATE INDEX ix_agg_prod ON fact_agg(id_producto)")
    conn.commit()
    print(f"fact_agg: {len(out)} combinaciones (producto x cadena x mes) "
          f"sobre {total} filas leidas", flush=True)


# --------------------------------------------------------------------------- #
# Orquestacion
# --------------------------------------------------------------------------- #
def main():
    extract()
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    try:
        load_dim_producto(conn)
        est = establecimiento_map()
        print(f"establecimientos con cadena: {len(est)} locales, "
              f"{len(set(est.values()))} cadenas", flush=True)
        aggregate_precios(conn, est)
    finally:
        conn.close()
    print(f"\nWarehouse listo -> {DB_PATH}", flush=True)


if __name__ == "__main__":
    main()
