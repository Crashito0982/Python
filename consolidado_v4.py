# -*- coding: utf-8 -*-
from __future__ import annotations
import os, re, sys, logging, shutil, unicodedata
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd
try:
    from pypdf import PdfReader
except Exception:
    PdfReader = None

# ---------------------------- Paths ----------------------------
def _root() -> Path:
    here = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()
    if (here / "PENDIENTES").exists() or (here.name.upper()=="BRITIMP"): return here
    if (here / "BRITIMP").exists(): return here / "BRITIMP"
    return here
ROOT = _root()
PENDIENTES = ROOT/"PENDIENTES"; PROCESADO=ROOT/"PROCESADO"; CONSOLIDADO=ROOT/"CONSOLIDADO"
AGENCIES = ["ASU","CDE","ENC","OVD","CON"]
for d in (PENDIENTES,PROCESADO,CONSOLIDADO): d.mkdir(parents=True,exist_ok=True)
for a in AGENCIES: (PENDIENTES/a).mkdir(parents=True,exist_ok=True); (PROCESADO/a).mkdir(parents=True,exist_ok=True)
def today_dir()->Path:
    p=CONSOLIDADO/datetime.now().strftime("%Y-%m-%d"); p.mkdir(parents=True,exist_ok=True); return p

# ---------------------------- Logging ----------------------------
RUN_WRITTEN:set[str]=set()
def _logger()->logging.Logger:
    lg=logging.getLogger("BRITIMP")
    if lg.handlers: return lg
    lg.setLevel(logging.INFO)
    fmt=logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh=logging.FileHandler(today_dir()/"BRITIMP_log.txt", encoding="utf-8", mode="a")
    ch=logging.StreamHandler(sys.stdout)
    fh.setFormatter(fmt); ch.setFormatter(fmt); lg.addHandler(fh); lg.addHandler(ch)
    lg.info("========== NUEVA EJECUCIÓN =========="); lg.info(f"Root: {ROOT}")
    return lg
log=_logger()

# ---------------------------- Helpers ----------------------------
DATE_RE=re.compile(r"^\s*(\d{2}/\d{2}/\d{4})\s*$")
NEGATIVE_BANKS={"CONTINENTAL","BBVA","GNB","REGIONAL","BASA","VISION","ATLAS","SUDAMERIS","FAMILIAR","ITAPUA","AMAMBAY"}
def remove_accents(s:str)->str: return "".join(ch for ch in unicodedata.normalize("NFD", s) if not unicodedata.combining(ch))
def to_ddmmyyyy(v:Any)->Optional[str]:
    if v is None or (isinstance(v,float) and pd.isna(v)): return None
    if isinstance(v, datetime): return v.strftime("%d/%m/%Y")
    if isinstance(v,(int,float)) and not pd.isna(v):
        base=datetime(1899,12,30); return (base+timedelta(days=float(v))).strftime("%d/%m/%Y")
    s=str(v).strip()
    if not s: return None
    if DATE_RE.match(s): return s
    for fmt in ("%Y-%m-%d","%d-%m-%Y","%d/%m/%y","%d/%m/%Y","%m/%d/%Y"):
        try: return datetime.strptime(s,fmt).strftime("%d/%m/%Y")
        except: pass
    return None
_NUM_SAN=re.compile(r"[^\d,\.\-()\u00A0 ]")
def parse_numeric(val:Any)->Optional[float]:
    if val is None or (isinstance(val,float) and pd.isna(val)): return None
    if isinstance(val,(int,float)): return float(val)
    s=_NUM_SAN.sub("",str(val)).replace("\u00A0"," ").strip()
    if not s: return None
    neg=s.startswith("(") and s.endswith(")")
    if neg: s=s[1:-1].strip()
    s=s.replace(" ","")
    if "," in s and "." in s:
        if s.rfind(".")>s.rfind(","): s=s.replace(",","")
        else: s=s.replace(".","").replace(",",".")
    elif "," in s:
        parts=s.split(",")
        if len(parts)>=2 and len(parts[-1])<=2: s=s.replace(",",".")
        else: s=s.replace(",","")
    try:
        x=float(s); return -x if neg else x
    except: return None
def as_int(v:Any)->int: n=parse_numeric(v); return int(round(n or 0))
def norm_divisa(v:Any)->str:
    u=remove_accents(str(v).upper()); u=u.replace("₲","GS").replace("U$S","USD").replace("US$","USD").replace("U$D","USD")
    c=re.sub(r"[^A-Z0-9]","",u)
    if c.startswith("GUAR") or c in {"PYG","PYGS","GS","GUARANI","GUARANIES"}: return "PYG"
    if c.startswith("DOL") or "USD" in c or c in {"US","USS"}: return "USD"
    return ""
def file_text_preview(p:Path, max_rows:int=40)->str:
    if p.suffix.lower() in (".xlsx",".xls"):
        try: xl=pd.ExcelFile(p, engine=None)
        except: xl=pd.ExcelFile(p, engine=("openpyxl" if p.suffix.lower()==".xlsx" else "xlrd"))
        parts=[]
        for sh in xl.sheet_names:
            df=pd.read_excel(xl, sheet_name=sh, header=None, nrows=max_rows)
            if not df.empty: parts+=df.fillna("").astype(str).agg(lambda r:" ".join(t for t in r if str(t).strip()),axis=1).tolist()
        return "\n".join(parts)
    if p.suffix.lower()==".pdf" and PdfReader:
        try:
            rd=PdfReader(str(p)); txt=""
            for pg in rd.pages: txt+=(pg.extract_text() or "")+"\n"
            return txt
        except: return ""
    try: return Path(p).read_text("utf-8", errors="ignore")[:4000]
    except: return ""
def unir_letras_separadas(t:str)->str: return re.sub(r"(?:\b[A-ZÁÉÍÓÚÑ](?:\s+[A-ZÁÉÍÓÚÑ])+\b)", lambda m:m.group(0).replace(" ",""), t or "")
def parse_agencia_from_text(t:str)->str:
    m=re.search(r"SUC:\s*([A-ZÁÉÍÓÚÑ \-\.]+)", t, re.I); return (m.group(1).strip() if m else "")
def norm_agencia(a:Any)->str:
    u=remove_accents(str(a).upper())
    if re.search(r"\bASU|ASUNCION|ASUNCIÓN|CASA\s+MATRIZ\b",u): return "ASU"
    if re.search(r"\bCDE|CIUDAD\s+DEL\s+ESTE\b",u): return "CDE"
    if re.search(r"\bENC|ENCARNACION|ENCARNACIÓN\b",u): return "ENC"
    if re.search(r"\bOVD|OVIEDO|CNEL\.?\s*OVIEDO\b",u): return "OVD"
    if re.search(r"\bCON|CONCEPCION|CONCEPCIÓN\b",u): return "CON"
    return ""
def infer_agencia_by_name(fname:str)->str:
    u=fname.upper().strip()
    if u.startswith(("01_0","01-0","01 ")): return "ASU"
    if u.startswith(("02_0","02-0","02 ")): return "CDE"
    if u.startswith(("03_0","03-0","03 ")): return "ENC"
    if u.startswith(("04_0","04-0","04 ")): return "OVD"
    return ""
def detect_itau(path:Path)->bool:
    txt=unir_letras_separadas(file_text_preview(path)).upper()
    if any(b in txt for b in NEGATIVE_BANKS): return False
    if re.search(r"CLIENTE[^A-Z0-9]{0,10}ITAU", txt): return True
    if "BANCO ITAU" in txt or "CLIENTE: BANCO ITAU" in txt: return True
    up=remove_accents(path.name).upper()
    pipe=[["INV","EC","CTA","ESTADO","PLANILLA"],["ATM","BCO","BANCO","BULTO","EFECT"],["BILLETE","BILLETES","EFECTIVO"]]
    looks=all(any(tok in up for tok in grp) for grp in pipe)
    return looks
def has_sin_mov(path:Path)->bool: return re.search(r"SIN\s+MOVIMIENTOS", unir_letras_separadas(file_text_preview(path)), re.I) is not None

# ---------------------------- Dispatcher ----------------------------
def dispatch_tipo(fname:str)->Optional[str]:
    up=remove_accents(fname).upper()
    if "INV" in up and "ATM" in up: return "INV_BILLETES_ATM"
    if "INV" in up and any(t in up for t in ["BANCO","BCO","DOLAR","DÓLAR","USD"]): return "INV_BILLETES_BCO"
    if "EC" in up and "EFECT" in up and "ATM" in up: return "EC_EFECT_ATM"
    if "EC" in up and "EFECT" in up and any(t in up for t in ["BCO","BANCO"]): return "EC_EFECT_BCO"
    if "EC" in up and "BULTO" in up and "ATM" in up: return "EC_BULTO_ATM"
    if "INV" in up: return "INV_BILLETES_UNKNOWN"
    return None

# ---------------------------- Parsers ----------------------------
def parse_inv_xlsx_generic(path:Path)->pd.DataFrame:
    try: xl=pd.ExcelFile(path)
    except: xl=pd.ExcelFile(path, engine="openpyxl")
    regs=[]
    for sh in xl.sheet_names:
        df=pd.read_excel(xl, sheet_name=sh, header=None)
        head="\n".join(df.fillna("").astype(str).agg(lambda r:" ".join(t for t in r if str(t).strip()),axis=1).tolist()[:15])
        agencia=norm_agencia(parse_agencia_from_text(head)) or ""
        m=re.search(r"(PLANILLA|SALDO)\s+DE\s+INVENTARIO\s+DE\s+BILLETES(?:\s+DE\s+(?:ATM|BANCO))?\s+AL:\s*(\d{1,2}[-/]\d{1,2}[-/]\d{4})", head, re.I)
        f_inv=to_ddmmyyyy(m.group(2)) if m else None
        for i in range(len(df)):
            row=df.iloc[i].tolist()
            div=norm_divisa(row[0])
            if div not in {"PYG","USD"}: continue
            agrup=str(row[1]).strip() if row[1] is not None else ""
            tipo=str(row[2]).strip() if row[2] is not None else ""
            denom=parse_numeric(row[3]);  denom_i = int(round(denom)) if denom is not None else None
            if denom_i is None: continue
            dep=as_int(row[4]); cje=as_int(row[5]); canje=as_int(row[6]); moneda=as_int(row[7])
            imp=as_int(row[8]) if len(row)>8 else 0
            regs.append({"FECHA_INVENTARIO":f_inv,"DIVISA":div,"AGENCIA":agencia,"AGRUPACION_EFECTIVO":agrup,"TIPO_VALOR":tipo,"DENOMINACION":denom_i,"CALIDAD_DEPOSITO":dep,"CJE_DEP":cje,"CALIDAD_CANJE":canje,"MONEDA":moneda,"IMPORTE_TOTAL":imp,"ARCHIVO_ORIGEN":os.path.basename(path)})
    cols=["FECHA_INVENTARIO","DIVISA","AGENCIA","AGRUPACION_EFECTIVO","TIPO_VALOR","DENOMINACION","CALIDAD_DEPOSITO","CJE_DEP","CALIDAD_CANJE","MONEDA","IMPORTE_TOTAL","ARCHIVO_ORIGEN"]
    return pd.DataFrame(regs, columns=cols) if regs else pd.DataFrame(columns=cols)

def parse_inv_pdf_common(path:Path)->pd.DataFrame:
    if not PdfReader: 
        return pd.DataFrame(columns=["FECHA_INVENTARIO","DIVISA","AGENCIA","AGRUPACION_EFECTIVO","TIPO_VALOR","DENOMINACION","CALIDAD_DEPOSITO","CJE_DEP","CALIDAD_CANJE","MONEDA","IMPORTE_TOTAL","ARCHIVO_ORIGEN"])
    try: reader=PdfReader(str(path))
    except: 
        log.warning(f"[WARN] No se pudo abrir PDF: {path.name}")
        return pd.DataFrame(columns=["FECHA_INVENTARIO","DIVISA","AGENCIA","AGRUPACION_EFECTIVO","TIPO_VALOR","DENOMINACION","CALIDAD_DEPOSITO","CJE_DEP","CALIDAD_CANJE","MONEDA","IMPORTE_TOTAL","ARCHIVO_ORIGEN"])
    txt="".join((pg.extract_text() or "")+"\n" for pg in reader.pages)
    txt=unir_letras_separadas(txt)
    m_f=re.search(r"(PLANILLA|SALDO)\s+DE\s+INVENTARIO\s+DE\s+BILLETES.*?:\s*(\d{1,2}[-/]\d{1,2}[-/]\d{4})", txt, re.I)
    f_inv=to_ddmmyyyy(m_f.group(2)) if m_f else None
    agencia=norm_agencia(parse_agencia_from_text(txt)) or ""
    div="USD" if re.search(r"\bUSD|MDA\.?\s*EXT", txt, re.I) else "PYG"
    RE_NUM=re.compile(r"\d{1,3}(?:\.\d{3})*(?:,\d{1,2})?")
    datos=[]; agrup=None; tipo=None
    for ln in [l.strip() for l in txt.splitlines() if l.strip()]:
        U=ln.upper()
        if re.search(r"\b(SUB[-\s]?TOTAL|TOTAL\s+DEPOSITO|TOTAL\s+MONEDA|TOTAL)\b",U): continue
        if re.match(r"^\s*(TESORO|PICOS|FAJOS)\b.*\b(ATM|BANCO)\b",U): agrup=ln.strip()
        if re.match(r"^\s*(BILLETES|MONEDAS)\b",U): tipo=ln.strip()
        nums=RE_NUM.findall(U)
        if len(nums)==6 and f_inv and agrup and tipo:
            d,dep,cje,canje,moneda,tot=[as_int(x) for x in nums]
            datos.append([f_inv,div,agencia,agrup,tipo,d,dep,cje,canje,moneda,tot])
    cols=["FECHA_INVENTARIO","DIVISA","AGENCIA","AGRUPACION_EFECTIVO","TIPO_VALOR","DENOMINACION","CALIDAD_DEPOSITO","CJE_DEP","CALIDAD_CANJE","MONEDA","IMPORTE_TOTAL"]
    df=pd.DataFrame(datos, columns=cols) if datos else pd.DataFrame(columns=cols)
    if not df.empty: df["ARCHIVO_ORIGEN"]=os.path.basename(path)
    return df

def parse_ec_xlsx_generic(path:Path, clasif:str)->pd.DataFrame:
    try: xl=pd.ExcelFile(path)
    except: xl=pd.ExcelFile(path, engine="openpyxl")
    regs=[]
    def row_text(row): return " ".join(str(x) for x in row if str(x).strip())
    for sh in xl.sheet_names:
        df=pd.read_excel(xl, sheet_name=sh, header=None)
        full="\n".join(df.fillna("").astype(str).agg(lambda r:" ".join(t for t in r if str(t).strip()),axis=1).tolist())
        agencia=norm_agencia(parse_agencia_from_text(full)) or ""
        m=re.search(r"ESTADO\s+DE\s+CUENTA.*DEL\s*:\s*(\d{2}/\d{2}/\d{4})", full, re.I)
        f_arch=m.group(1) if m else None
        section=None; started=False
        for i in range(len(df)):
            row=df.iloc[i].tolist(); txt=row_text(row).upper()
            if "SALDO ANTERIOR" in txt: section=None; started=False; continue
            if "INGRESOS" in txt and "EGRESOS" not in txt: section="INGRESOS"; started=True; continue
            if "EGRESOS" in txt: section="EGRESOS"; started=True; continue
            if not started or section not in ("INGRESOS","EGRESOS"): continue
            fecha=None; suc=""; rec=""; nums=[]; j=0
            while j<len(row):
                f=to_ddmmyyyy(row[j]); 
                if f: fecha=f; j+=1; break
                j+=1
            if not fecha: continue
            while j<len(row) and not str(row[j]).strip(): j+=1
            if j<len(row): suc=str(row[j]).strip(); j+=1
            while j<len(row):
                dig=re.sub(r"\D","",str(row[j]))
                if len(dig)>=5: rec=dig; j+=1; break
                j+=1
            while j<len(row):
                n=parse_numeric(row[j])
                if n is not None: nums.append(n)
                j+=1
            monto=as_int(nums[-1] if nums else 0)
            bultos=as_int(nums[-2]) if len(nums)>=2 else 0
            regs.append({"FECHA_OPERACION":fecha,"SUCURSAL":suc,"RECIBO":rec,"BULTOS":bultos,"MONTO":monto,"MONEDA":"PYG","ING_EGR":"IN" if section=="INGRESOS" else "OUT","CLASIFICACION":clasif,"FECHA_ARCHIVO":f_arch,"MOTIVO_MOVIMIENTO":section,"AGENCIA":agencia,"ARCHIVO_ORIGEN":os.path.basename(path)})
    cols=["FECHA_OPERACION","SUCURSAL","RECIBO","BULTOS","MONTO","MONEDA","ING_EGR","CLASIFICACION","FECHA_ARCHIVO","MOTIVO_MOVIMIENTO","AGENCIA","ARCHIVO_ORIGEN"]
    return pd.DataFrame(regs, columns=cols) if regs else pd.DataFrame(columns=cols)

# ---------------------------- Orquestador ----------------------------
OUTPUT = {
    "EC_EFECT_BCO": "BRITIMP_EFECTBANCO.csv",
    "EC_EFECT_ATM": "BRITIMP_EFECTATM.csv",
    "INV_BILLETES_BCO": "BRITIMP_INVENTARIO_BANCO.csv",
    "INV_BILLETES_ATM": "BRITIMP_INVENTARIO_ATM.csv",
    "EC_BULTO_ATM": "BRITIMP_BULTOS_ATM.csv",
}
def write_consolidated(tipo:str, df:pd.DataFrame)->Optional[Path]:
    if df.empty or tipo not in OUTPUT: return None
    out=today_dir()/OUTPUT[tipo]
    first = tipo not in RUN_WRITTEN
    if first:
        if out.exists(): out.unlink()
        RUN_WRITTEN.add(tipo); mode="w"; header=True; log.info(f"[WRITE] Iniciando archivo del día → {out.name} (truncate)")
    else:
        mode="a"; header=False
    orders={
        "INV_BILLETES_ATM": ["FECHA_INVENTARIO","DIVISA","AGENCIA","AGRUPACION_EFECTIVO","TIPO_VALOR","DENOMINACION","CALIDAD_DEPOSITO","CJE_DEP","CALIDAD_CANJE","MONEDA","IMPORTE_TOTAL","ARCHIVO_ORIGEN"],
        "INV_BILLETES_BCO": ["FECHA_INVENTARIO","DIVISA","AGENCIA","AGRUPACION_EFECTIVO","TIPO_VALOR","DENOMINACION","CALIDAD_DEPOSITO","CJE_DEP","CALIDAD_CANJE","MONEDA","IMPORTE_TOTAL","ARCHIVO_ORIGEN"],
        "EC_EFECT_ATM": ["FECHA_OPERACION","SUCURSAL","RECIBO","BULTOS","MONTO","MONEDA","ING_EGR","CLASIFICACION","FECHA_ARCHIVO","MOTIVO_MOVIMIENTO","AGENCIA","ARCHIVO_ORIGEN"],
        "EC_EFECT_BCO": ["FECHA_OPERACION","SUCURSAL","RECIBO","BULTOS","MONTO","MONEDA","ING_EGR","CLASIFICACION","FECHA_ARCHIVO","MOTIVO_MOVIMIENTO","AGENCIA","ARCHIVO_ORIGEN"],
        "EC_BULTO_ATM": ["FECHA_OPERACION","SUCURSAL","RECIBO","BULTOS","MONTO","MONEDA","ING_EGR","CLASIFICACION","FECHA_ARCHIVO","MOTIVO_MOVIMIENTO","AGENCIA","ARCHIVO_ORIGEN"],
    }
    base=orders[tipo]
    cols=[c for c in base if c in df.columns] + [c for c in df.columns if c not in base and c!="ARCHIVO_ORIGEN"] + (["ARCHIVO_ORIGEN"] if "ARCHIVO_ORIGEN" in df.columns else [])
    df[cols].to_csv(out, index=False, mode=mode, header=header, encoding="utf-8-sig", sep=';')
    log.info(f"[APPEND] {out.name} ← +{len(df)} filas (sep=';')")
    return out
def safe_move(src:Path, dst_dir:Path, rename:bool=False)->None:
    dst_dir.mkdir(parents=True, exist_ok=True)
    name=(src.stem+" PROCESADO"+src.suffix) if rename else src.name
    dst=dst_dir/name; i=1
    while dst.exists(): dst=dst_dir/f"{Path(name).stem} ({i}){Path(name).suffix}"; i+=1
    shutil.move(str(src), str(dst)); log.info(f"[MOVE] {src.name} → {dst}")
def process_file(path:Path, agency_hint:str="")->Tuple[pd.DataFrame, Optional[str], str, bool]:
    log.info(f"[FOUND] {path.name} (carpeta: {agency_hint or '¿?'}), ext={path.suffix.lower()}")
    tipo=dispatch_tipo(path.name); itau=detect_itau(path)
    if not itau: log.info(f"[SKIP] {path.name} → NO ITAU"); return pd.DataFrame(), None, agency_hint, False
    if re.search(r"SIN\s+MOVIMIENTOS", file_text_preview(path), re.I):
        ag = norm_agencia(parse_agencia_from_text(file_text_preview(path))) or infer_agencia_by_name(path.name) or agency_hint
        log.info(f"[SKIP] {path.name} → SIN MOVIMIENTOS (agencia={ag})"); return pd.DataFrame(), tipo, ag, True
    txt=unir_letras_separadas(file_text_preview(path))
    ag_text=norm_agencia(parse_agencia_from_text(txt)); ag = ag_text or infer_agencia_by_name(path.name) or agency_hint
    if (tipo in (None,"INV_BILLETES_UNKNOWN")) and path.suffix.lower()==".pdf":
        up=txt.upper()
        if re.search(r"\bATM\b", up): tipo="INV_BILLETES_ATM"
        elif re.search(r"\bBANCO\b|\bD[ÓO]LAR\b|\bUSD\b", up): tipo="INV_BILLETES_BCO"
        else: tipo="INV_BILLETES_BCO"
        log.info(f"[INFO] {path.name} → Tipo inferido por PDF: {tipo}")
    df=pd.DataFrame()
    try:
        if tipo in ("INV_BILLETES_ATM","INV_BILLETES_BCO"):
            if path.suffix.lower() in (".xlsx",".xls"): log.info(f"[PARSER] {path.name} → {tipo} (xlsx/xls)"); df=parse_inv_xlsx_generic(path)
            elif path.suffix.lower()==".pdf": log.info(f"[PARSER] {path.name} → {tipo} (pdf)"); df=parse_inv_pdf_common(path)
        elif tipo in ("EC_EFECT_ATM","EC_EFECT_BCO"):
            if path.suffix.lower() in (".xlsx",".xls"): log.info(f"[PARSER] {path.name} → {tipo} (xlsx/xls)"); df=parse_ec_xlsx_generic(path, "ATM" if tipo.endswith("ATM") else "BANCO")
        elif tipo=="EC_BULTO_ATM" and path.suffix.lower() in (".xlsx",".xls"):
            log.info(f"[PARSER] {path.name} → EC_BULTO_ATM (xlsx/xls)"); df=parse_ec_xlsx_generic(path, "ATM")
        else: log.info(f"[SKIP] {path.name} → Tipo no soportado (tipo={tipo})")
    except Exception as e:
        log.warning(f"[WARN] Error parseando {path.name}: {e}")
    if not df.empty:
        if "AGENCIA" in df.columns: df["AGENCIA"]=df["AGENCIA"].apply(lambda x: norm_agencia(x) or x)
        if ag: df["AGENCIA"]=df["AGENCIA"].where(df["AGENCIA"].astype(str).str.strip()!="", ag)
        log.info(f"[OK] {path.name} → Registros: {len(df)} (tipo={tipo})")
    else: log.info(f"[INFO] {path.name} → Sin registros")
    return df, tipo, ag, True
def run()->Dict[str,int]:
    stats={k:0 for k in OUTPUT.keys()}; pend=[]
    for a in AGENCIES:
        base=PENDIENTES/a
        for root,_,files in os.walk(base):
            for f in files:
                if f.lower().endswith((".xlsx",".xls",".pdf")): pend.append((Path(root)/f, a))
    log.info(f"[SCAN] Archivos a evaluar: {len(pend)}")
    for path, hint in pend:
        df, tipo, ag, itau = process_file(path, hint)
        known=tipo in OUTPUT; had=not df.empty
        if had and known: write_consolidated(tipo, df); stats[tipo]+=len(df)
        if itau and had: safe_move(path, PROCESADO/(ag or hint or "ASU"), rename=False)
        elif itau and (not had) and known: safe_move(path, PROCESADO/(ag or hint or "ASU"), rename=True)
        else: safe_move(path, PROCESADO/(ag or hint or "ASU"), rename=False)
    log.info("[DONE] " + ", ".join(f"{k}={v}" for k,v in stats.items()))
    return stats
if __name__=="__main__":
    out=run()
    for k,v in out.items(): print(f"[{k}] registros: {v}")
    print("OK")
