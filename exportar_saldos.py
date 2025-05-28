from sqlalchemy import create_engine
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
import os, json

# 📦 PostgreSQL
usuario = "inpro2021nubeuser"
contraseña = "Inproc987"
host = "infraestructura-aurora-datawarehouse-instance-zxhlvevffc1c.cijt7auhxunw.us-east-1.rds.amazonaws.com"
puerto = 5432
base = "finnegansbi"
engine = create_engine(f'postgresql+psycopg2://{usuario}:{contraseña}@{host}:{puerto}/{base}')

# 📊 Función para exportar una tabla a una hoja
def exportar_tabla(query, hoja_nombre, columnas_decimal=[]):
    df = pd.read_sql(query, engine)

    # Formateo decimal (coma decimal y sin separador de miles)
    for col in columnas_decimal:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].apply(lambda x: f"{x:.2f}".replace(".", ",") if pd.notnull(x) else "")

    worksheet = spreadsheet.worksheet(hoja_nombre)
    worksheet.clear()
    set_with_dataframe(worksheet, df)
    print(f"✅ Exportado: {hoja_nombre}")

# 🔐 Google Sheets auth
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
cred_dict = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
creds = ServiceAccountCredentials.from_json_keyfile_dict(cred_dict, scope)
client = gspread.authorize(creds)

# Abrir planilla
spreadsheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1oR_fdVCyn1cA8zwH4XgU5VK63cZaDC3I1i3-SWaUT20/edit")

# 📤 Exportar las tablas con sus respectivas columnas a formatear
exportar_tabla(
    "SELECT * FROM public.inpro2021nube_composicion_saldos_clientes_inprocil",
    "Base Saldos Clientes",
    ["importemonedatransaccion", "importemonedaprincipal", "importemonedasecundaria"]
)

exportar_tabla(
    "SELECT * FROM public.inpro2021nube_sumas_y_saldos",
    "Base Sumas y Saldos",
    ["sumadebe", "sumahaber", "saldoacumulado"]
)

exportar_tabla(
    "SELECT * FROM public.inpro2021nube_pedidos_pendientes_de_entrega",
    "Base Pendientes Entrega",
    ["cantidad_pendiente"]
)

exportar_tabla(
    "SELECT * FROM public.inpro2021nube_facturacion",
    "Base Facturacion"
)
