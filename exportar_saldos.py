from sqlalchemy import create_engine
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
import os, json

# 🔐 Google Sheets auth
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
cred_dict = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
creds = ServiceAccountCredentials.from_json_keyfile_dict(cred_dict, scope)
client = gspread.authorize(creds)

# 📦 PostgreSQL origen (Finnegans)
engine_origen = create_engine("postgresql+psycopg2://inpro2021nubeuser:Inproc987@infraestructura-aurora-datawarehouse-instance-zxhlvevffc1c.cijt7auhxunw.us-east-1.rds.amazonaws.com:5432/finnegansbi")

# 📦 Supabase destino
supabase_url = os.environ["db.hrjumhathzvikkmmcpya.supabase.co"]
supabase_pass = os.environ["*@q"Sc1ygpwkW$J@"]
engine_supabase = create_engine(f"postgresql+psycopg2://postgres:{supabase_pass}@{supabase_url}/postgres")

# 📁 Google Sheets
sheet_saldos = client.open_by_url("https://docs.google.com/spreadsheets/d/1oR_fdVCyn1cA8zwH4XgU5VK63cZaDC3I1i3-SWaUT20/edit")
sheet_mayor = client.open_by_url("https://docs.google.com/spreadsheets/d/1e9BuGiiOx-GhokgsM37MAaUfddxLH30T-gtYu3UtfOA/edit")

def exportar_tabla(query, hoja_nombre, sheet, tabla_supabase, columnas_decimal=[], sin_encabezado=False, rango_google="A2", max_col=None):
    df = pd.read_sql(query, engine_origen)

    # Google Sheets formato decimal
    for col in columnas_decimal:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].apply(lambda x: f"{x:.2f}".replace(".", ",") if pd.notnull(x) else "")

    # Google Sheets export
    worksheet = sheet.worksheet(hoja_nombre)
    if sin_encabezado:
        df = df.iloc[:, :max_col]
        valores = df.values.tolist()
        worksheet.batch_clear([f"A2:{chr(65+max_col)}"])  # Limpiar A2:Q por ejemplo
        worksheet.update(values=valores, range_name=rango_google)
    else:
        worksheet.clear()
        set_with_dataframe(worksheet, df)

    print(f"✅ Exportado a Google Sheet: {hoja_nombre}")

    # Supabase export
    df.to_sql(tabla_supabase, engine_supabase, if_exists="replace", index=False)
    print(f"⬆️ Subido a Supabase: {tabla_supabase}")

# ▶️ Exportaciones
exportar_tabla(
    "SELECT * FROM public.inpro2021nube_composicion_saldos_clientes_inprocil",
    "Base Saldos Clientes",
    sheet_saldos,
    "composicion_saldos",
    ["importemonedatransaccion", "importemonedaprincipal", "importemonedasecundaria"]
)

exportar_tabla(
    "SELECT * FROM public.inpro2021nube_sumas_y_saldos",
    "Base Sumas y Saldos",
    sheet_saldos,
    "sumas_y_saldos",
    ["sumadebe", "sumahaber", "saldoacumulado"]
)

exportar_tabla(
    "SELECT * FROM public.inpro2021nube_pedidos_pendientes_de_entrega",
    "Base Pendientes Entrega",
    sheet_saldos,
    "pedidos_pendientes",
    ["cantidad_pendiente"]
)

exportar_tabla(
    "SELECT * FROM public.inpro2021nube_facturacion",
    "Base Facturacion",
    sheet_saldos,
    "facturacion",
    ["preciomonedatransaccion", "importemonedatransaccion", "importemonedaprincipal", "importemonedasecundaria", "cotizacionmonedatransaccion", "cantidad"]
)

exportar_tabla(
    "SELECT * FROM public.inpro2021nube_libro_mayor",
    "Aux Libro Mayor",
    sheet_mayor,
    "libro_mayor",
    ["debe", "haber", "importemonedaprincipal", "imp__operacion_ppal_", "imp__operacion_sec_"],
    sin_encabezado=True,
    rango_google="A2",
    max_col=17
)

exportar_tabla(
    "SELECT * FROM public.inpro2021nube_ingresos_y_egresos_stock",
    "Aux Stock",
    sheet_mayor,
    "ingresos_egresos_stock",
    ["stock"],
    sin_encabezado=True,
    rango_google="A2",
    max_col=9
)
