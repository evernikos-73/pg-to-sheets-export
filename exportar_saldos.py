from sqlalchemy import create_engine
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
import os, json, time

# 🔐 Google Sheets auth
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
cred_dict = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
creds = ServiceAccountCredentials.from_json_keyfile_dict(cred_dict, scope)
client = gspread.authorize(creds)

# 📦 PostgreSQL
usuario = "inpro2021nubeuser"
contraseña = "Inproc987"
host = "infraestructura-aurora-datawarehouse-instance-zxhlvevffc1c.cijt7auhxunw.us-east-1.rds.amazonaws.com"
puerto = 5432
base = "finnegansbi"
engine = create_engine(f'postgresql+psycopg2://{usuario}:{contraseña}@{host}:{puerto}/{base}')

# 🚀 Función genérica con retry
def set_with_retry(worksheet, df, retries=3, wait=5):
    for i in range(1, retries + 1):
        try:
            set_with_dataframe(worksheet, df)
            print("✅ Exportación completada.")
            return
        except Exception as e:
            print(f"⚠️ Intento {i}/{retries} falló: {e}")
            if i < retries:
                print(f"⏳ Reintentando en {wait} segundos...")
                time.sleep(wait)
            else:
                raise

def update_with_retry(worksheet, values, range_name, retries=3, wait=5):
    for i in range(1, retries + 1):
        try:
            worksheet.update(values=values, range_name=range_name)
            print("✅ Exportación sin encabezado completada.")
            return
        except Exception as e:
            print(f"⚠️ Intento {i}/{retries} falló: {e}")
            if i < retries:
                print(f"⏳ Reintentando en {wait} segundos...")
                time.sleep(wait)
            else:
                raise

# 🧩 Función genérica para exportar a hoja completa
def exportar_tabla_completa(query, spreadsheet, hoja_nombre, columnas_decimal=[]):
    df = pd.read_sql(query, engine)
    for col in columnas_decimal:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].apply(lambda x: f"{x:.2f}".replace(".", ",") if pd.notnull(x) else "")
    worksheet = spreadsheet.worksheet(hoja_nombre)
    worksheet.clear()
    set_with_retry(worksheet, df)
    print(f"✅ Exportado: {hoja_nombre}")

# 🧩 Exportar solo A2:Q sin encabezado
def exportar_libro_mayor(query, spreadsheet, hoja_nombre, columnas_decimal=[]):
    df = pd.read_sql(query, engine)
    for col in columnas_decimal:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].apply(lambda x: f"{x:.2f}".replace(".", ",") if pd.notnull(x) else "")
    df_recortado = df.iloc[:, :17]
    valores = df_recortado.values.tolist()
    worksheet = spreadsheet.worksheet(hoja_nombre)
    worksheet.batch_clear(["A2:Q"])
    update_with_retry(worksheet, values=valores, range_name="A2")
    print("✅ Exportado sin encabezado: Aux Libro Mayor")

# 📤 Exportar A2:J sin encabezado
def exportar_stock(query, spreadsheet, hoja_nombre, columnas_decimal=[]):
    df = pd.read_sql(query, engine)
    for col in columnas_decimal:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].apply(lambda x: f"{x:.2f}".replace(".", ",") if pd.notnull(x) else "")
    df_recortado = df.iloc[:, :10]
    valores = df_recortado.values.tolist()
    worksheet = spreadsheet.worksheet(hoja_nombre)
    worksheet.batch_clear(["A2:J"])
    update_with_retry(worksheet, values=valores, range_name="A2")
    print("✅ Exportado sin encabezado: Aux Stock")

# 📤 Exportar A2:H sin encabezado
def exportar_sumas_y_saldos(query, spreadsheet, hoja_nombre, columnas_decimal=[]):
    df = pd.read_sql(query, engine)
    for col in columnas_decimal:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].apply(lambda x: f"{x:.2f}".replace(".", ",") if pd.notnull(x) else "")
    df_recortado = df.iloc[:, :10]  # 👈 solo columnas A:H
    valores = df_recortado.values.tolist()
    worksheet = spreadsheet.worksheet(hoja_nombre)
    worksheet.batch_clear(["A2:j"])
    update_with_retry(worksheet, values=valores, range_name="A2")
    print("✅ Exportado sin encabezado: Aux Sumas y Saldos")

# 📁 Spreadsheet 1
saldos_sheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1oR_fdVCyn1cA8zwH4XgU5VK63cZaDC3I1i3-SWaUT20/edit")
exportar_tabla_completa(
    "SELECT * FROM public.inpro2021nube_composicion_saldos_clientes_inprocil",
    saldos_sheet, "Base Saldos Clientes",
    ["importemonedatransaccion", "importemonedaprincipal", "importemonedasecundaria"]
)
exportar_tabla_completa(
    "SELECT * FROM public.inpro2021nube_sumas_y_saldos",
    saldos_sheet, "Base Sumas y Saldos",
    ["sumadebe", "sumahaber", "saldoacumulado"]
)
exportar_tabla_completa(
    "SELECT * FROM public.inpro2021nube_pedidos_pendientes_de_entrega",
    saldos_sheet, "Base Pendientes Entrega",
    ["cantidad_pendiente"]
)
exportar_tabla_completa(
    "SELECT * FROM public.inpro2021nube_facturacion",
    saldos_sheet, "Base Facturacion",
    ["preciomonedatransaccion", "importemonedatransaccion", "importemonedaprincipal",
     "importemonedasecundaria", "cotizacionmonedatransaccion", "cantidad"]
)
exportar_tabla_completa(
    "SELECT * FROM public.inpro2021nube_cobranzas",
    saldos_sheet, "Base Cobranza",
    ["Importe Factura"]
)

# 📁 Spreadsheet 2
libro_mayor_sheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1e9BuGiiOx-GhokgsM37MAaUfddxLH30T-gtYu3UtfOA/edit")
exportar_libro_mayor(
    "SELECT * FROM public.inpro2021nube_libro_mayor",
    libro_mayor_sheet, "Aux Libro Mayor",
    ["debe", "haber", "importemonedaprincipal", "imp__operacion_ppal_", "imp__operacion_sec_","tipo_cambio"]
)
exportar_stock(
    "SELECT * FROM public.inpro2021nube_stock_con_PUC",
    libro_mayor_sheet, "Aux Stock",
    ["stock","UltimoPrecioCompra"]
)
exportar_sumas_y_saldos(
    "SELECT * FROM public.inpro2021nube_sumas_y_saldos",
    libro_mayor_sheet, "Aux Sumas y Saldos",
    ["debe", "haber", "saldoperiodo", "saldo", "saldoinicial"]
)

# 📁 Spreadsheet 3
stock_con_puc_sheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1KQCsJbtIBDfDv86Y9n4lU6Z6e0s9SSVlPlq1MN-dF6g/edit")
exportar_stock(
    "SELECT * FROM public.inpro2021nube_stock_con_PUC",
    stock_con_puc_sheet, "Aux Stock",
    ["stock","UltimoPrecioCompra"]
)
