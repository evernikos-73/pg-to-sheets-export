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

# 📦 PostgreSQL
usuario = "inpro2021nubeuser"
contraseña = "Inproc987"
host = "infraestructura-aurora-datawarehouse-instance-zxhlvevffc1c.cijt7auhxunw.us-east-1.rds.amazonaws.com"
puerto = 5432
base = "finnegansbi"
engine = create_engine(f'postgresql+psycopg2://{usuario}:{contraseña}@{host}:{puerto}/{base}')

# 🧩 Función genérica para exportar a una hoja completa
def exportar_tabla_completa(query, spreadsheet, hoja_nombre, columnas_decimal=[]):
    df = pd.read_sql(query, engine)
    for col in columnas_decimal:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].apply(lambda x: f"{x:.2f}".replace(".", ",") if pd.notnull(x) else "")
    worksheet = spreadsheet.worksheet(hoja_nombre)
    worksheet.clear()
    set_with_dataframe(worksheet, df)
    print(f"✅ Exportado: {hoja_nombre}")

# 🧩 Función especial para exportar solo A2:Q sin encabezado
def exportar_libro_mayor(query, spreadsheet, hoja_nombre, columnas_decimal=[]):
    df = pd.read_sql(query, engine)
    for col in columnas_decimal:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].apply(lambda x: f"{x:.2f}".replace(".", ",") if pd.notnull(x) else "")
    df_recortado = df.iloc[:, :17]  # A - Q
    valores = df_recortado.values.tolist()
    worksheet = spreadsheet.worksheet(hoja_nombre)
    worksheet.batch_clear(["A2:Q"])
    worksheet.update(values=valores, range_name="A2")
    print("✅ Exportado sin encabezado: Aux Libro Mayor")

# 📁 Abrir Spreadsheet 1 (Composición de saldos)
saldos_sheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1oR_fdVCyn1cA8zwH4XgU5VK63cZaDC3I1i3-SWaUT20/edit")

# 📝 Exportaciones completas
exportar_tabla_completa(
    "SELECT * FROM public.inpro2021nube_composicion_saldos_clientes_inprocil",
    saldos_sheet,
    "Base Saldos Clientes",
    ["importemonedatransaccion", "importemonedaprincipal", "importemonedasecundaria"]
)

exportar_tabla_completa(
    "SELECT * FROM public.inpro2021nube_sumas_y_saldos",
    saldos_sheet,
    "Base Sumas y Saldos",
    ["sumadebe", "sumahaber", "saldoacumulado"]
)

exportar_tabla_completa(
    "SELECT * FROM public.inpro2021nube_pedidos_pendientes_de_entrega",
    saldos_sheet,
    "Base Pendientes Entrega",
    ["cantidad_pendiente"]
)

exportar_tabla_completa(
    "SELECT * FROM public.inpro2021nube_facturacion",
    saldos_sheet,
    "Base Facturacion",
    ["preciomonedatransaccion", "importemonedatransaccion", "importemonedaprincipal", "importemonedasecundaria", "cotizacionmonedatransaccion", "cantidad"]
)

# 📁 Abrir Spreadsheet 2 (Libro Mayor)
libro_mayor_sheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1e9BuGiiOx-GhokgsM37MAaUfddxLH30T-gtYu3UtfOA/edit")

exportar_libro_mayor(
    "SELECT * FROM public.inpro2021nube_libro_mayor",
    libro_mayor_sheet,
    "Aux Libro Mayor",
    ["debe", "haber", "importemonedaprincipal", "imp__operacion_ppal_", "imp__operacion_sec_"]
)


# 📤 Exportar a hoja "Aux Stock" solo columnas A a J sin encabezado
def exportar_stock(query, spreadsheet, hoja_nombre, columnas_decimal=[]):
    df = pd.read_sql(query, engine)
    for col in columnas_decimal:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].apply(lambda x: f"{x:.2f}".replace(".", ",") if pd.notnull(x) else "")
    df_recortado = df.iloc[:, :10]  # Columnas A - J
    valores = df_recortado.values.tolist()
    worksheet = spreadsheet.worksheet(hoja_nombre)
    worksheet.batch_clear(["A2:J"])  # Borra solo A2 a J
    worksheet.update(values=valores, range_name="A2")
    print("✅ Exportado sin encabezado: Aux Stock")

# 📤 Ejecutar la exportación
exportar_stock(
    "SELECT * FROM public.inpro2021nube_ingresos_y_egresos_stock",
    libro_mayor_sheet,
    "Aux Stock",
    ["stock"]
)
