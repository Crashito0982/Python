# -*- coding: utf-8 -*-
"""
BRITIMP - Consolidador único (.py) con LOG detallado y CSV con ';'
-------------------------------------------------------------------------------
Cambios respecto a la versión anterior:
- LOG detallado a archivo y consola: por cada archivo se informa FOUND, GATE ITAU, SIN MOVIMIENTOS,
  TIPO detectado, PARSER usado, REGISTROS, SKIP y MOTIVO, y MOVIMIENTO a PROCESADO.
  El log se escribe en CONSOLIDADO/YYYY-MM-DD/BRITIMP_log.txt
- CSV ahora se exportan con separador ';' (sep=';') para Excel (config ES).
- INVENTARIO PDF: si el nombre no indica ATM/BCO pero es 'INV + BILLETE(S)',
  se abre el texto del PDF y se infiere ATM o BANCO por contenido (ASU/OVD PDFs cubiertos).
-------------------------------------------------------------------------------
"""

from __future__ import annotations
import os, re, sys, shutil, unicodedata, logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

try:
    from pypdf import PdfReader  # lectura de PDFs (texto)
except Exception:  # entorno minimalista
    PdfReader = None  # type: ignore

# ------------------------------ ROOT & PATHS ------------------------------

def resolve_root() -> Path:
    here = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()
    root = here
    if (here / "PENDIENTES").exists() or (here.name.upper() == "BRITIMP"):
        root = here
    elif (here / "BRITIMP").exists():
        root = here / "BRITIMP"
    return root

ROOT = resolve_root()
PENDIENTES = ROOT / "PENDIENTES"
PROCESADO  = ROOT / "PROCESADO"
CONSOLIDADO = ROOT / "CONSOLIDADO"

AGENCIES = ["ASU","CDE","ENC","OVD","CON"]  # CONCEPCION = CON

def ensure_dirs() -> None:
    for d in [PENDIENTES, PROCESADO, CONSOLIDADO]:
        d.mkdir(parents=True, exist_ok=True)
    for a in AGENCIES:
        (PENDIENTES/a).mkdir(parents=True, exist_ok=True)
        (PROCESADO/a).mkdir(parents=True, exist_ok=True)

ensure_dirs()

# ------------------------------ LOGGING ------------------------------

RUN_WRITTEN: set[str] = set()
_LOGGER: Optional[logging.Logger] = None
_LOG_PATH: Optional[Path] = None

def today_folder() -> Path:
    today = datetime.now().strftime("%Y-%m-%d")
    outdir = CONSOLIDADO / today
    outdir.mkdir(parents=True, exist_ok=True)
    return outdir

def setup_logger() -> logging.Logger:
    global _LOGGER, _LOG_PATH
    if _LOGGER is not None:
        return _LOGGER
    log_dir = today_folder()
    _LOG_PATH = log_dir / "BRITIMP_log.txt"
    logger = logging.getLogger("BRITIMP")
    logger.setLevel(logging.INFO)
    logger.handlers = []
    fmt = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh = logging.FileHandler(_LOG_PATH, encoding='utf-8', mode='a')
    fh.setFormatter(fmt)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.info("========== NUEVA EJECUCIÓN ==========")
    logger.info(f"Root: {ROOT}")
    _LOGGER = logger
    return logger

def log_info(msg: str) -> None:
    logger = setup_logger()
    logger.info(msg)

def log_warn(msg: str) -> None:
    logger = setup_logger()
    logger.warning(msg)

def log_error(msg: str) -> None:
    logger = setup_logger()
    logger.error(msg)

# ------------------------------ HELPERS ------------------------------

DATE_RE = re.compile(r"^\s*(\d{2}/\d{2}/\d{4})\s*$", re.IGNORECASE)

def excel_serial_to_ddmmyyyy(val: float) -> Optional[str]:
    try:
        if isinstance(val, (int, float)) and not pd.isna(val):
            base = datetime(1899, 12, 30)
            return (base + timedelta(days=float(val))).date().strftime("%d/%m/%Y")
    except Exception:
        pass
    return None

def to_ddmmyyyy(val: Any) -> Optional[str]:
    if isinstance(val, datetime):
        return val.strftime("%d/%m/%Y")
    if isinstance(val, (int, float)) and not pd.isna(val):
        d = excel_serial_to_ddmmyyyy(val)
        if d: return d
    if isinstance(val, str):
        s = val.strip()
        if DATE_RE.match(s):
            return s
        for fmt in ("%Y-%m-%d","%d-%m-%Y","%d/%m/%y"):
            try:
                return datetime.strptime(s, fmt).strftime("%d/%m/%Y")
            except Exception:
                pass
    return None

NUM_SANITIZER = re.compile(r"[^\d,.\-()\u00A0 ]")

def parse_numeric(val: Any) -> Optional[float]:
    if pd.isna(val): return None
    if isinstance(val, (int, float)): return float(val)
    if isinstance(val, str):
        s = NUM_SANITIZER.sub("", val).replace("\u00A0"," ").strip()
        if not s: return None
        neg = s.startswith("(") and s.endswith(")")
        if neg: s = s[1:-1].strip()
        s = s.replace(" ","")
        if "," in s and "." in s:
            if s.rfind(".") > s.rfind(","):
                s = s.replace(",","")
            else:
                s = s.replace(".","").replace(",",".")
        elif "," in s:
            parts = s.split(",")
            if len(parts)>=2 and len(parts[-1])<=2:
                s = s.replace(",",".")
            else:
                s = s.replace(",","")
        try:
            x = float(s)
            return -x if neg else x
        except Exception:
            return None
    return None

def clean_digits(val: Any) -> str:
    return "" if pd.isna(val) else re.sub(r"\D", "", str(val))

def collapse_spaced_letters(s: str) -> str:
    tokens = s.split()
    return "".join(tokens) if tokens and all(len(t)==1 for t in tokens) else s

def unir_letras_separadas(texto: str) -> str:
    if not texto: return texto
    return re.sub(r"(?:\b[A-ZÁÉÍÓÚÑ](?:\s+[A-ZÁÉÍÓÚÑ])+\b)", lambda m: m.group(0).replace(" ",""), texto)

def remove_accents(s: str) -> str:
    return "".join(ch for ch in unicodedata.normalize("NFD", s) if not unicodedata.combining(ch))

def name_matches(fname: str, required_groups: List[List[str]]) -> bool:
    if not required_groups:
        return True
    target = remove_accents(fname).upper()
    for group in required_groups:
        if not any(remove_accents(opt).upper() in target for opt in group):
            return False
    return True

# --------------- Agencia / Divisa normalización ----------------

AGENCIA_PATTERNS: Dict[str, List[str]] = {
    "ASU": [r"\bCASA\s+MATRIZ\b", r"\bASUNCION\b", r"\bASUNCIÓN\b", r"\bASU\b"],
    "CDE": [r"\bCIUDAD\s+DEL\s+ESTE\b", r"\bCDE\b"],
    "ENC": [r"\bENCARNACION\b", r"\bENCARNACIÓN\b", r"\bENC\b"],
    "OVD": [r"\bCNEL\.?\s+OVIEDO\b", r"\bCORONEL\s+OVIEDO\b", r"\bOVIEDO\b", r"\bOVD\b"],
    "CON": [r"\bCONCEPCION\b", r"\bCONCEPCIÓN\b", r"\bCON\b"],
}

def normalize_agencia_to_cod(value: Any) -> str:
    if value is None: return ""
    raw = str(value).strip()
    if raw == "": return ""
    u = remove_accents(raw.upper())
    for cod, patterns in AGENCIA_PATTERNS.items():
        for pat in patterns:
            if re.search(pat, u):
                return cod
    return ""

def infer_agencia_from_filename(fname: str) -> str:
    up = fname.upper().strip()
    if up.startswith(("01_0","01-0","01 ")): return "ASU"
    if up.startswith(("02_0","02-0","02 ")): return "CDE"
    if up.startswith(("03_0","03-0","03 ")): return "ENC"
    if up.startswith(("04_0","04-0","04 ")): return "OVD"
    return ""

def normalize_divisa_to_iso(value: Any) -> str:
    if value is None: return ""
    u = remove_accents(str(value).strip().upper())
    u = u.replace("₲","GS").replace("US$","USD").replace("U$S","USD").replace("U$D","USD")
    canon = re.sub(r"[^A-Z0-9]","",u)
    if canon.startswith("GUAR") or canon in {"PYG","PYGS","GS","GUARANI","GUARANIES"}:
        return "PYG"
    if canon.startswith("DOL") or "USD" in canon or canon in {"US","USS"}:
        return "USD"
    return ""

# ------------------------------ GATE ITAU & Scanners ------------------------------

NEGATIVE_BANKS = ["CONTINENTAL","BBVA","GNB","REGIONAL","BASA","VISION","ATLAS","SUDAMERIS","FAMILIAR","ITAPUA","AMAMBAY"]

def file_text_preview(path: Path, max_rows_excel: int = 40) -> str:
    p = Path(path)
    if p.suffix.lower() in (".xlsx",".xls"):
        try:
            xl = pd.ExcelFile(p, engine=None)
        except Exception:
            engine = "openpyxl" if p.suffix.lower()==".xlsx" else "xlrd"
            xl = pd.ExcelFile(p, engine=engine)
        parts: List[str] = []
        for sh in xl.sheet_names:
            df = pd.read_excel(xl, sheet_name=sh, header=None, nrows=max_rows_excel)
            if not df.empty:
                line = df.fillna("").astype(str).agg(lambda r: " ".join([c for c in r if str(c).strip()]), axis=1).tolist()
                parts.extend(line)
        return "\n".join(parts)
    if p.suffix.lower() == ".pdf" and PdfReader is not None:
        try:
            reader = PdfReader(str(p))
            txt = ""
            for page in reader.pages:
                txt += (page.extract_text() or "") + "\n"
            return txt
        except Exception:
            return ""
    try:
        with open(p, "rb") as f:
            data = f.read(4096)
        return str(data)
    except Exception:
        return ""

def detect_cliente_itau(path: Path, filename_tokens_hint: Optional[List[List[str]]] = None) -> bool:
    txt = unir_letras_separadas(file_text_preview(path)).upper()
    if any(b in txt for b in NEGATIVE_BANKS):
        log_info(f"[SKIP] {path.name} → Otro banco detectado en texto")
        return False
    if re.search(r"CLIENTE[^A-Z0-9]{0,10}ITAU", txt): return True
    if "BANCO ITAU" in txt: return True
    if "CLIENTE: BANCO ITAU" in txt: return True
    up = remove_accents(path.name).upper()
    pipeline_groups = [["INV","EC","CTA","ESTADO","PLANILLA"], ["ATM","BCO","BANCO","BULTO","EFECT"], ["BILLETE","BILLETES","EFECTIVO"]]
    if filename_tokens_hint:
        pipeline_groups = filename_tokens_hint
    looks_pipeline = name_matches(up, pipeline_groups)
    mentions_others = any(b in up for b in NEGATIVE_BANKS)
    return looks_pipeline and (not mentions_others)

def detect_sin_movimientos(path: Path) -> bool:
    txt = unir_letras_separadas(file_text_preview(path)).upper()
    return re.search(r"SIN\s+MOVIMIENTOS", txt, flags=re.IGNORECASE) is not None

def parse_agencia_from_text(text: str) -> str:
    m = re.search(r"SUC:\s*(.+?)\s*(?:[\)\]\-]|$)", text, flags=re.IGNORECASE)
    if m:
        cod = normalize_agencia_to_cod(m.group(1))
        return cod if cod else m.group(1).strip()
    return ""

# ------------------------------ DISPATCHER ------------------------------


def dispatch_tipo(fname: str) -> Optional[str]:
    up = remove_accents(fname).upper()
    # Inventario: nombres comunes en PDF
    if "INV" in up and "ATM" in up:
        return "INV_BILLETES_ATM"
    if "INV" in up and any(tok in up for tok in ["BANCO", "BCO", "DOLAR", "DÓLAR", "USD"]):
        return "INV_BILLETES_BCO"
    # EC | Efectivo Banco
    if name_matches(up, [["EC","CTA","ESTADO"], ["EFECT","EFECTIVO"], ["BCO","BANCO"]]):
        return "EC_EFECT_BCO"
    # EC | Efectivo ATM
    if name_matches(up, [["EC","CTA","ESTADO"], ["EFECT","EFECTIVO"], ["ATM"]]):
        return "EC_EFECT_ATM"
    # EC | Bulto ATM
    if name_matches(up, [["EC","CTA","ESTADO"], ["BULTO","BULTOS"], ["ATM"]]):
        return "EC_BULTO_ATM"
    # INV | Inventario ATM (fallback)
    if name_matches(up, [["INV"], ["BILLETE","BILLETES"], ["ATM"]]):
        return "INV_BILLETES_ATM"
    # INV | Inventario BANCO (fallback)
    if name_matches(up, [["INV"], ["BILLETE","BILLETES"], ["BCO","BANCO","DOLAR","DÓLAR"]]):
        return "INV_BILLETES_BCO"
    # INV genérico para inferencia posterior por contenido PDF
    if name_matches(up, [["INV"]]):
        return "INV_BILLETES_UNKNOWN"
    return None

# ------------------------------ PARSERS ------------------------------

def parse_ec_bultos_atm_xlsx(path: Path, DEBUG: bool=False) -> pd.DataFrame:
    try:
        xl = pd.ExcelFile(path)
    except Exception:
        xl = pd.ExcelFile(path, engine="openpyxl")
    registros: List[dict] = []
    def row_text(row) -> str:
        return " ".join([str(x) for x in row if (not pd.isna(x)) and str(x).strip() != ""]).strip()
    def parse_sheet(df_raw: pd.DataFrame) -> None:
        full_text = "\n".join(
            df_raw.fillna("").astype(str).agg(lambda r: " ".join([c for c in r if str(c).strip()]), axis=1).tolist()
        )
        agencia = parse_agencia_from_text(full_text)
        fecha_archivo = None
        m = re.search(r"ESTADO\s+DE\s+CUENTA.*DEL\s*:\s*(\d{2}/\d{2}/\d{4})", full_text, flags=re.IGNORECASE)
        if m: fecha_archivo = m.group(1)

        section: Optional[str] = None
        motivo: Optional[str] = None
        started = False

        for ridx in range(len(df_raw)):
            row = df_raw.iloc[ridx].tolist()
            txt = row_text(row).upper()

            if "SALDO ANTERIOR" in txt:
                section = None; motivo = None; started = False; continue
            if "INGRESOS" in txt and "EGRESOS" not in txt:
                section = "INGRESOS"; motivo = None; started = True; continue
            if "EGRESOS" in txt:
                section = "EGRESOS"; motivo = None; started = True; continue
            if "INFORME DE PROCESOS" in txt or "INFORME DE ERRORES" in txt:
                break
            if not started or section not in ("INGRESOS","EGRESOS"):
                continue
            if "TOTAL" in txt:
                has_date = any(to_ddmmyyyy(c) for c in row)
                if not has_date:
                    motivo = None
                    continue

            nonempty = [x for x in row if str(x).strip() and not pd.isna(x)]
            if len(nonempty)==1 and not DATE_RE.match(str(nonempty[0]).strip()):
                motivo = str(nonempty[0]).strip()
                continue

            fecha: Optional[str] = None; sucursal=""; recibo=""; nums: List[float]=[]
            idx=0
            while idx < len(row):
                f = to_ddmmyyyy(row[idx])
                if f: fecha=f; idx+=1; break
                idx+=1
            if not fecha: continue

            while idx < len(row):
                sval = str(row[idx]).strip()
                if sval: sucursal = sval; idx+=1; break
                idx+=1
            if not sucursal: continue

            while idx < len(row):
                digits = clean_digits(row[idx])
                if len(digits)>=5: recibo = digits; idx+=1; break
                idx+=1
            if not recibo: continue

            while idx < len(row):
                n = parse_numeric(row[idx])
                if n is not None: nums.append(n)
                idx+=1
            while len(nums)<4: nums.append(0.0)
            bgs, mgs, busd, musd = nums[0], nums[1], nums[2], nums[3]

            def add_row(moneda: str, bultos: Optional[int], monto: float):
                registros.append({
                    "FECHA_OPERACION": fecha,
                    "SUCURSAL": sucursal,
                    "RECIBO": recibo,
                    "BULTOS": int(bultos) if (bultos is not None and float(bultos).is_integer()) else None,
                    "MONTO": float(monto) if monto is not None else 0.0,
                    "MONEDA": moneda,
                    "ING_EGR": "IN" if section=="INGRESOS" else "OUT",
                    "CLASIFICACION": "ATM",
                    "FECHA_ARCHIVO": fecha_archivo,
                    "MOTIVO_MOVIMIENTO": (motivo or section),
                    "AGENCIA": agencia,
                    "ARCHIVO_ORIGEN": os.path.basename(path),
                })
            has_gs  = (bgs != 0) or (mgs != 0)
            has_usd = (busd != 0) or (musd != 0)
            if has_gs and not has_usd:
                add_row("PYG", bgs if bgs else None, mgs if mgs else 0.0)
            elif has_usd and not has_gs:
                add_row("USD", busd if busd else None, musd if musd else 0.0)
            elif has_gs and has_usd:
                add_row("PYG", bgs if bgs else None, mgs if mgs else 0.0)
                add_row("USD", busd if busd else None, musd if musd else 0.0)
            else:
                pass

    try:
        for sh in xl.sheet_names:
            df_raw = pd.read_excel(xl, sheet_name=sh, header=None)
            parse_sheet(df_raw)
    except Exception as e:
        log_warn(f"[WARN] Error parseando BULTOS ATM {path.name}: {e}")

    cols = ["FECHA_OPERACION","SUCURSAL","RECIBO","BULTOS","MONTO","MONEDA",
            "ING_EGR","CLASIFICACION","FECHA_ARCHIVO","MOTIVO_MOVIMIENTO",
            "AGENCIA","ARCHIVO_ORIGEN"]
    return pd.DataFrame(registros, columns=cols) if registros else pd.DataFrame(columns=cols)

def parse_inv_billetes_xlsx_atm(path: Path, DEBUG: bool=False) -> pd.DataFrame:
    try:
        xl = pd.ExcelFile(path)
    except Exception:
        xl = pd.ExcelFile(path, engine="openpyxl")

    def df_to_text(df: pd.DataFrame, rows:int=12) -> str:
        parts: List[str] = []
        for i in range(min(rows, len(df))):
            row = df.iloc[i].tolist()
            line = "".join([str(x) for x in row if not (isinstance(x, float) and pd.isna(x)) and str(x).strip() != ""]).strip()
            if line: parts.append(line)
        return "\n".join(parts)

    registros: List[dict] = []
    for sh in xl.sheet_names:
        raw = pd.read_excel(xl, sheet_name=sh, header=None)
        head_text = df_to_text(raw, rows=12)
        agencia_txt = parse_agencia_from_text(head_text)
        agencia_cod = normalize_agencia_to_cod(agencia_txt)
        agencia_out = agencia_cod if agencia_cod else agencia_txt
        m = re.search(r"PLANILLA\s+DE\s+INVENTARIO\s+DE\s+BILLETES\s+DE\s+ATM\s+AL:\s*(\d{1,2}/\d{1,2}/\d{4})", head_text, flags=re.IGNORECASE)
        fecha_inv = m.group(1) if m else None

        for ridx in range(len(raw)):
            row = raw.iloc[ridx].tolist()
            iso_div = normalize_divisa_to_iso(row[0])
            if iso_div not in {"PYG","USD"}: 
                continue
            agrup = re.sub(r"\.+$","", str(row[1]).strip()) if row[1] is not None else ""
            tipo  = collapse_spaced_letters(str(row[2]).strip()) if row[2] is not None else ""

            def i_or_zero(x: Any) -> int:
                n = parse_numeric(x)
                return int(round(n)) if n is not None else 0

            denom = parse_numeric(row[3]) if row[3] is not None else None
            if denom is None: 
                continue
            denom = int(round(denom))

            calidad_dep = i_or_zero(row[4])
            cje_dep     = i_or_zero(row[5])
            calidad_canje = i_or_zero(row[6])
            moneda_qty  = i_or_zero(row[7])

            imp = parse_numeric(row[8]) if len(row)>8 else None
            if imp is None: 
                continue

            registros.append({
                "FECHA_INVENTARIO": fecha_inv,
                "DIVISA": iso_div,
                "AGENCIA": agencia_out,
                "AGRUPACION_EFECTIVO": agrup,
                "TIPO_VALOR": tipo,
                "DENOMINACION": denom,
                "CALIDAD_DEPOSITO": calidad_dep,
                "CJE_DEP": cje_dep,
                "CALIDAD_CANJE": calidad_canje,
                "MONEDA": moneda_qty,
                "IMPORTE_TOTAL": float(imp),
                "ARCHIVO_ORIGEN": os.path.basename(path),
            })

    cols = ["FECHA_INVENTARIO","DIVISA","AGENCIA","AGRUPACION_EFECTIVO","TIPO_VALOR",
            "DENOMINACION","CALIDAD_DEPOSITO","CJE_DEP","CALIDAD_CANJE","MONEDA","IMPORTE_TOTAL","ARCHIVO_ORIGEN"]
    return pd.DataFrame(registros, columns=cols) if registros else pd.DataFrame(columns=cols)

def parse_inv_billetes_pdf_common(path: Path, DEBUG: bool=False) -> pd.DataFrame:
    if PdfReader is None:
        return pd.DataFrame(columns=[
            "FECHA_INVENTARIO","DIVISA","AGENCIA","AGRUPACION_EFECTIVO","TIPO_VALOR",
            "DENOMINACION","CALIDAD_DEPOSITO","CALIDAD_CD","CALIDAD_CANJE","MONEDA","IMPORTE_TOTAL","ARCHIVO_ORIGEN"
        ])
    try:
        reader = PdfReader(str(path))
    except Exception:
        log_warn(f"[WARN] No se pudo abrir PDF: {path.name}")
        return pd.DataFrame(columns=[
            "FECHA_INVENTARIO","DIVISA","AGENCIA","AGRUPACION_EFECTIVO","TIPO_VALOR",
            "DENOMINACION","CALIDAD_DEPOSITO","CALIDAD_CD","CALIDAD_CANJE","MONEDA","IMPORTE_TOTAL","ARCHIVO_ORIGEN"
        ])
    texto = ""
    for pg in reader.pages:
        texto += (pg.extract_text() or "") + "\n"
    texto = unir_letras_separadas(texto)
    lineas = [ln.strip() for ln in texto.splitlines() if ln.strip()]

    m_fecha = re.search(r"(PLANILLA|SALDO)\s+DE\s+INVENTARIO\s+DE\s+BILLETES.*?:\s*(\d{2}[-/]\d{2}[-/]\d{4})", texto, flags=re.IGNORECASE)
    fecha_inventario = m_fecha.group(2) if m_fecha else None
    m_suc = re.search(r"SUC:\s*([A-ZÁÉÍÓÚÑ \-\.]+)", texto, flags=re.IGNORECASE)
    agencia_raw = m_suc.group(1).strip() if m_suc else ""
    agencia = normalize_agencia_to_cod(agencia_raw) or agencia_raw

    divisa = "USD" if re.search(r"\bUSD|\bMDA\.?\s*EXT\.?", texto, flags=re.IGNORECASE) else "PYG"

    RE_NUM = re.compile(r"\d{1,3}(?:\.\d{3})*")
    RE_IGNORAR = re.compile(r"\b(SUB[-\s]?TOTAL|TOTAL\s+DEPOSITO|TOTAL\s+MONEDA|TOTAL)\b", re.IGNORECASE)

    datos = []
    agrupacion = None
    tipo_valor = None

    for linea in lineas:
        u = linea.upper()
        if RE_IGNORAR.search(u): 
            continue
        if re.match(r"^\s*(TESORO|TESOSO|PICOS|FAJOS)\b.*\b(ATM|BANCO)\b", u):
            agrupacion = linea.strip()
            divisa = "USD" if "USD" in u or ("MDA" in u and "EXT" in u) else "PYG"
        if re.match(r"^\s*(BILLETES|MONEDAS)\b", u):
            tipo_valor = linea.strip()
        nums = RE_NUM.findall(u)
        if len(nums) == 6 and fecha_inventario and agrupacion and tipo_valor:
            d, dep, cje, canje, moneda_qty, total = nums
            datos.append([fecha_inventario, divisa, agencia, agrupacion, tipo_valor, d, dep, cje, canje, moneda_qty, total])

    cols = [
        "FECHA_INVENTARIO","DIVISA","AGENCIA","AGRUPACION_EFECTIVO","TIPO_VALOR",
        "DENOMINACION","CALIDAD_DEPOSITO","CALIDAD_CD","CALIDAD_CANJE","MONEDA","IMPORTE_TOTAL"
    ]
    df = pd.DataFrame(datos, columns=cols) if datos else pd.DataFrame(columns=cols)
    if not df.empty:
        df["ARCHIVO_ORIGEN"] = os.path.basename(path)
    return df

def parse_ec_efect_xlsx_generic(path: Path, clasificacion: str, DEBUG: bool=False) -> pd.DataFrame:
    try:
        xl = pd.ExcelFile(path)
    except Exception:
        xl = pd.ExcelFile(path, engine="openpyxl")
    registros: List[dict] = []
    def row_text(row) -> str:
        return " ".join([str(x) for x in row if (not pd.isna(x)) and str(x).strip() != ""]).strip()
    for sh in xl.sheet_names:
        df_raw = pd.read_excel(xl, sheet_name=sh, header=None)
        full_text = "\n".join(
            df_raw.fillna("").astype(str).agg(lambda r: " ".join([c for c in r if str(c).strip()]), axis=1).tolist()
        )
        agencia = parse_agencia_from_text(full_text)
        fecha_archivo = None
        m = re.search(r"ESTADO\s+DE\s+CUENTA.*DEL\s*:\s*(\d{2}/\d{2}/\d{4})", full_text, flags=re.IGNORECASE)
        if m: fecha_archivo = m.group(1)

        section: Optional[str] = None
        motivo: Optional[str] = None
        started = False
        for ridx in range(len(df_raw)):
            row = df_raw.iloc[ridx].tolist()
            txt = row_text(row).upper()

            if "SALDO ANTERIOR" in txt:
                section = None; motivo = None; started = False; continue
            if "INGRESOS" in txt and "EGRESOS" not in txt:
                section = "INGRESOS"; motivo = None; started = True; continue
            if "EGRESOS" in txt:
                section = "EGRESOS"; motivo = None; started = True; continue
            if "INFORME DE PROCESOS" in txt or "INFORME DE ERRORES" in txt:
                break
            if not started or section not in ("INGRESOS","EGRESOS"):
                continue
            if "TOTAL" in txt:
                has_date = any(to_ddmmyyyy(c) for c in row)
                if not has_date:
                    motivo = None
                    continue

            fecha=None; sucursal=""; recibo=""; nums: List[float]=[]; moneda_guess="PYG"
            idx=0
            while idx < len(row):
                f = to_ddmmyyyy(row[idx])
                if f: fecha=f; idx+=1; break
                idx+=1
            if not fecha: continue

            while idx < len(row):
                sval = str(row[idx]).strip()
                if sval: sucursal = sval; idx+=1; break
                idx+=1
            if not sucursal: continue

            while idx < len(row):
                digits = clean_digits(row[idx])
                if len(digits)>=5: recibo=digits; idx+=1; break
                idx+=1
            if not recibo: continue

            while idx < len(row):
                n = parse_numeric(row[idx])
                if n is not None: nums.append(n)
                if isinstance(row[idx], str):
                    iso = normalize_divisa_to_iso(row[idx])
                    if iso in {"PYG","USD"}: moneda_guess = iso
                idx+=1

            monto = nums[-1] if nums else 0.0
            bultos = None
            if len(nums) >= 2:
                bn = nums[-2]
                if bn is not None and float(bn).is_integer():
                    bultos = int(bn)

            registros.append({
                "FECHA_OPERACION": fecha,
                "SUCURSAL": sucursal,
                "RECIBO": recibo,
                "BULTOS": bultos,
                "MONTO": float(monto) if monto is not None else 0.0,
                "MONEDA": moneda_guess,
                "ING_EGR": "IN" if section=="INGRESOS" else "OUT",
                "CLASIFICACION": clasificacion,
                "FECHA_ARCHIVO": fecha_archivo,
                "MOTIVO_MOVIMIENTO": (motivo or section),
                "AGENCIA": agencia,
                "ARCHIVO_ORIGEN": os.path.basename(path),
            })

    cols = ["FECHA_OPERACION","SUCURSAL","RECIBO","BULTOS","MONTO","MONEDA","ING_EGR",
            "CLASIFICACION","FECHA_ARCHIVO","MOTIVO_MOVIMIENTO","AGENCIA","ARCHIVO_ORIGEN"]
    return pd.DataFrame(registros, columns=cols) if registros else pd.DataFrame(columns=cols)

# ------------------------------ ORQUESTADOR POR ARCHIVO ------------------------------

OUTPUT_FILES = {
    "EC_EFECT_BCO": "BRITIMP_EFECTBANCO.csv",
    "EC_EFECT_ATM": "BRITIMP_EFECTATM.csv",
    "INV_BILLETES_BCO": "BRITIMP_INVENTARIO_BANCO.csv",
    "INV_BILLETES_ATM": "BRITIMP_INVENTARIO_ATM.csv",
    "EC_BULTO_ATM": "BRITIMP_BULTOS_ATM.csv",
}

def process_file(path: Path, parent_agency_hint: Optional[str]=None, DEBUG: bool=False) -> Tuple[pd.DataFrame, Optional[str], str, bool]:
    fname = path.name
    log_info(f"[FOUND] {fname} (carpeta: {parent_agency_hint or '¿?'}), ext={path.suffix.lower()}")
    tipo = dispatch_tipo(fname)
    is_itau = detect_cliente_itau(path)
    if not is_itau:
        log_info(f"[SKIP] {fname} → NO ITAU")
        return pd.DataFrame(), None, (parent_agency_hint or ""), False

    if detect_sin_movimientos(path):
        ag = parse_agencia_from_text(unir_letras_separadas(file_text_preview(path))) or infer_agencia_from_filename(fname) or (parent_agency_hint or "")
        log_info(f"[SKIP] {fname} → SIN MOVIMIENTOS (agencia={ag})")
        return pd.DataFrame(), tipo, ag, True

    text_preview = unir_letras_separadas(file_text_preview(path))
    agencia_from_text = parse_agencia_from_text(text_preview)
    agencia_norm = normalize_agencia_to_cod(agencia_from_text) or infer_agencia_from_filename(fname) or (parent_agency_hint or agencia_from_text or "")
    log_info(f"[INFO] {fname} → Agencia inferida: '{agencia_norm or '¿?'}'")

    ext = path.suffix.lower()
    if (tipo in (None, "INV_BILLETES_UNKNOWN")) and ext == ".pdf":
        up_text = text_preview.upper()
        if re.search(r"INVENTARIO\s+DE\s+BILLETES.*ATM", up_text) or re.search(r"\bATM\b", up_text):
            tipo = "INV_BILLETES_ATM"
        elif re.search(r"INVENTARIO\s+DE\s+BILLETES.*BANCO", up_text) or re.search(r"\bBANCO\b", up_text):
            tipo = "INV_BILLETES_BCO"
        else:
            tipo = "INV_BILLETES_BCO"
        log_info(f"[INFO] {fname} → Tipo inferido por PDF: {tipo}")

    df = pd.DataFrame()
    try:
        if tipo == "EC_BULTO_ATM" and ext in (".xlsx",".xls"):
            log_info(f"[PARSER] {fname} → EC_BULTO_ATM (xlsx/xls)")
            df = parse_ec_bultos_atm_xlsx(path, DEBUG=DEBUG)
        elif tipo == "INV_BILLETES_ATM":
            if ext in (".xlsx",".xls"):
                log_info(f"[PARSER] {fname} → INV_BILLETES_ATM (xlsx/xls)")
                df = parse_inv_billetes_xlsx_atm(path, DEBUG=DEBUG)
            elif ext == ".pdf":
                log_info(f"[PARSER] {fname} → INV_BILLETES_ATM (pdf)")
                df = parse_inv_billetes_pdf_common(path, DEBUG=DEBUG)
        elif tipo == "INV_BILLETES_BCO":
            if ext == ".pdf":
                log_info(f"[PARSER] {fname} → INV_BILLETES_BCO (pdf)")
                df = parse_inv_billetes_pdf_common(path, DEBUG=DEBUG)
            elif ext in (".xlsx",".xls"):
                log_info(f"[PARSER] {fname} → INV_BILLETES_BCO (xlsx/xls)")
                df = parse_inv_billetes_xlsx_atm(path, DEBUG=DEBUG)
        elif tipo == "EC_EFECT_ATM" and ext in (".xlsx",".xls"):
            log_info(f"[PARSER] {fname} → EC_EFECT_ATM (xlsx/xls)")
            df = parse_ec_efect_xlsx_generic(path, clasificacion="ATM", DEBUG=DEBUG)
        elif tipo == "EC_EFECT_BCO" and ext in (".xlsx",".xls"):
            log_info(f"[PARSER] {fname} → EC_EFECT_BCO (xlsx/xls)")
            df = parse_ec_efect_xlsx_generic(path, clasificacion="BANCO", DEBUG=DEBUG)
        else:
            log_info(f"[SKIP] {fname} → Tipo no reconocido/soportado (tipo={tipo}, ext={ext})")
            df = pd.DataFrame()
    except Exception as e:
        log_warn(f"[WARN] Error al parsear {fname}: {e}")
        df = pd.DataFrame()

    if not df.empty:
        if "AGENCIA" in df.columns:
            df["AGENCIA"] = df["AGENCIA"].apply(lambda x: normalize_agencia_to_cod(x) or x)
            if agencia_norm:
                df["AGENCIA"] = df["AGENCIA"].where(df["AGENCIA"].astype(str).str.strip()!="", agencia_norm)
        else:
            df["AGENCIA"] = agencia_norm
        log_info(f"[OK] {fname} → Registros procesados: {len(df)} (tipo={tipo})")
    else:
        log_info(f"[INFO] {fname} → Sin registros obtenidos")

    return df, tipo, (agencia_norm or parent_agency_hint or ""), True

# ------------------------------ ESCRITURA & MOVIMIENTOS ------------------------------

def write_consolidated(tipo: str, df: pd.DataFrame) -> Optional[Path]:
    if tipo not in OUTPUT_FILES or df.empty:
        return None
    outdir = today_folder()
    outpath = outdir / OUTPUT_FILES[tipo]
    mode = "a"
    header = False
    if tipo not in RUN_WRITTEN:
        if outpath.exists():
            outpath.unlink()
        RUN_WRITTEN.add(tipo)
        mode = "w"
        header = True
        log_info(f"[WRITE] Iniciando archivo del día → {outpath.name} (truncate)")
    wanted_cols_by_tipo: Dict[str, List[str]] = {
        "EC_EFECT_BCO": ["FECHA_OPERACION","SUCURSAL","RECIBO","BULTOS","MONTO","MONEDA","ING_EGR","CLASIFICACION","FECHA_ARCHIVO","MOTIVO_MOVIMIENTO","AGENCIA","ARCHIVO_ORIGEN"],
        "EC_EFECT_ATM": ["FECHA_OPERACION","SUCURSAL","RECIBO","BULTOS","MONTO","MONEDA","ING_EGR","CLASIFICACION","FECHA_ARCHIVO","MOTIVO_MOVIMIENTO","AGENCIA","ARCHIVO_ORIGEN"],
        "INV_BILLETES_BCO": ["FECHA_INVENTARIO","DIVISA","AGENCIA","AGRUPACION_EFECTIVO","TIPO_VALOR","DENOMINACION","CALIDAD_DEPOSITO","CJE_DEP","CALIDAD_CANJE","MONEDA","IMPORTE_TOTAL","ARCHIVO_ORIGEN"],
        "INV_BILLETES_ATM": ["FECHA_INVENTARIO","DIVISA","AGENCIA","AGRUPACION_EFECTIVO","TIPO_VALOR","DENOMINACION","CALIDAD_DEPOSITO","CJE_DEP","CALIDAD_CANJE","MONEDA","IMPORTE_TOTAL","ARCHIVO_ORIGEN"],
        "EC_BULTO_ATM": ["FECHA_OPERACION","SUCURSAL","RECIBO","BULTOS","MONTO","MONEDA","ING_EGR","CLASIFICACION","FECHA_ARCHIVO","MOTIVO_MOVIMIENTO","AGENCIA","ARCHIVO_ORIGEN"],
    }
    wanted = wanted_cols_by_tipo[tipo]
    cols = [c for c in wanted if c in df.columns] + [c for c in df.columns if c not in wanted]
    df[cols].to_csv(outpath, index=False, mode=mode, header=header, encoding="utf-8-sig", sep=';')
    log_info(f"[APPEND] {outpath.name} ← +{len(df)} filas (sep=';')")
    return outpath

def safe_move(src: Path, dst_dir: Path, rename_processed: bool=False) -> Path:
    dst_dir.mkdir(parents=True, exist_ok=True)
    base = src.name
    if rename_processed:
        stem = src.stem + " PROCESADO"
        base = stem + src.suffix
    candidate = dst_dir / base
    if not candidate.exists():
        moved = shutil.move(str(src), str(candidate))
        log_info(f"[MOVE] {Path(src).name} → {candidate}")
        return Path(moved)
    i = 1
    while True:
        cand = dst_dir / f"{Path(base).stem} ({i}){Path(base).suffix}"
        if not cand.exists():
            moved = shutil.move(str(src), str(cand))
            log_info(f"[MOVE] {Path(src).name} → {cand}")
            return Path(moved)
        i += 1

def move_original(path: Path, agencia: str, is_itau: bool, had_records: bool, tipo_conocido: bool) -> None:
    agencia = (agencia or "").strip().upper() or "ASU"
    dst = PROCESADO / agencia
    if is_itau and had_records:
        safe_move(path, dst, rename_processed=False)
    elif is_itau and (not had_records) and tipo_conocido:
        safe_move(path, dst, rename_processed=True)
    else:
        safe_move(path, dst, rename_processed=False)

# ------------------------------ SCANNER PRINCIPAL ------------------------------

def collect_pending_files() -> List[Tuple[Path, Optional[str]]]:
    results: List[Tuple[Path, Optional[str]]] = []
    for agencia in AGENCIES:
        base = PENDIENTES / agencia
        if not base.exists():
            continue
        for root, _, files in os.walk(base):
            for f in files:
                if f.lower().endswith((".xlsx",".xls",".pdf")):
                    results.append((Path(root)/f, agencia))
    return results

# ------------------------------ MAIN ------------------------------

def run(DEBUG: bool=False) -> Dict[str, int]:
    setup_logger()
    stats = {k:0 for k in OUTPUT_FILES.keys()}
    pend = collect_pending_files()
    log_info(f"[SCAN] Archivos a evaluar: {len(pend)}")
    for path, agencia_hint in pend:
        df, tipo, agencia_final, is_itau = process_file(path, parent_agency_hint=agencia_hint, DEBUG=DEBUG)
        tipo_conocido = tipo in OUTPUT_FILES
        had_records = (not df.empty)
        if df is not None and tipo and had_records:
            write_consolidated(tipo, df)
            stats[tipo] += len(df)
        move_original(path, agencia=(agencia_final or agencia_hint or ""), is_itau=is_itau, had_records=had_records, tipo_conocido=bool(tipo_conocido))
    log_info(f"[DONE] Resumen por tipo: " + ", ".join(f"{k}={v}" for k,v in stats.items()))
    return stats

if __name__ == "__main__":
    DEBUG = bool(int(os.environ.get("BRITIMP_DEBUG","0")))
    out = run(DEBUG=DEBUG)
    for k,v in out.items():
        print(f"[{k}] registros: {v}")
    print("OK")

# ------------------------------------------------------------------------------
# OPCIONAL (comentado): Append automático al finalizar la corrida
# ------------------------------------------------------------------------------
# from pathlib import Path
# import pandas as pd
#
# def _append_daily_to_master():
#     TARGET_BASE = Path(r"/ruta/a/mis/csv_maestros")  # <--- CAMBIAR
#     TARGET_BASE.mkdir(parents=True, exist_ok=True)
#     today_dir = today_folder()
#     for tipo, fname in OUTPUT_FILES.items():
#         daily = today_dir / fname
#         if not daily.exists():
#             continue
#         master = TARGET_BASE / fname
#         df_new = pd.read_csv(daily, sep=';')  # ← IMPORTANTE: leer con sep=';'
#         if master.exists():
#             df_old = pd.read_csv(master, sep=';')
#             df_out = pd.concat([df_old, df_new], axis=0, ignore_index=True).drop_duplicates()
#         else:
#             df_out = df_new
#         df_out.to_csv(master, index=False, encoding="utf-8-sig", sep=';')
#
# # _append_daily_to_master()
# ------------------------------------------------------------------------------
