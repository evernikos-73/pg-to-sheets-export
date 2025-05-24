from sqlalchemy import create_engine
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
import os, json

# PostgreSQL
usuario = "inpro2021nubeuser"
contraseña = "Inproc987"
host = "infraestructura-aurora-datawarehouse-instance-zxhlvevffc1c.cijt7auhxunw.us-east-1.rds.amazonaws.com"
puerto = 5432
base = "finnegansbi"
engine = create_engine(f'postgresql+psycopg2://{usuario}:{contraseña}@{host}:{puerto}/{base}')
query = "SELECT * FROM public.inpro2021nube_composicion_saldos_clientes_inprocil"
df = pd.read_sql(query, engine)

# Formateo decimal con coma
columnas_decimal = ["importemonedatransaccion", "importemonedaprincipal", "importemonedasecundaria"]
for col in columnas_decimal:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df[col] = df[col].apply(lambda x: f"{x:.2f}".replace(".", ",") if pd.notnull(x) else "")

# Google Sheets auth via secret
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
cred_dict = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
creds = ServiceAccountCredentials.from_json_keyfile_dict(cred_dict, scope)
client = gspread.authorize(creds)

# Acceder y exportar
spreadsheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1oR_fdVCyn1cA8zwH4XgU5VK63cZaDC3I1i3-SWaUT20/edit")
worksheet = spreadsheet.worksheet("Base Saldos")
worksheet.clear()
set_with_dataframe(worksheet, df)
print("✅ Exportación completada.")
