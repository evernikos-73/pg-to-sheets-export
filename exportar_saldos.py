from sqlalchemy import create_engine
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
import os, json, time
from datetime import datetime, timedelta
import numpy as np
from dateutil.relativedelta import relativedelta

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
def exportar_tabla_completa(query_or_df, spreadsheet, hoja_nombre, columnas_decimal=[]):
    if isinstance(query_or_df, str):
        df = pd.read_sql(query_or_df, engine)
    else:
        df = query_or_df
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

# 📤 Exportar A2:J sin encabezado
def exportar_sumas_y_saldos(query, spreadsheet, hoja_nombre, columnas_decimal=[]):
    df = pd.read_sql(query, engine)
    for col in columnas_decimal:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].apply(lambda x: f"{x:.2f}".replace(".", ",") if pd.notnull(x) else "")
            
    df_recortado = df.iloc[:, :10]  # 👈 solo columnas A:J (10 columnas)
        
    valores = df_recortado.values.tolist()
    worksheet = spreadsheet.worksheet(hoja_nombre)
        
    worksheet.batch_clear(["A2:J"]) # Limpia A2:J
        
    update_with_retry(worksheet, values=valores, range_name="A2")
    print("✅ Exportado sin encabezado: Aux Sumas y Saldos")

# Funciones para análisis de churn (adaptadas del notebook)
def obtener_datos_facturacion():
    """Obtiene los datos de facturación de la base de datos
    Solo incluye ventas donde cuentanombre comience con 'Ventas Merc'
    """
    query = """
SELECT 
    clientecodigo,
    clientenombre,
    fechacomprobante,
    empresacodigo,
    empresanombre,
    cuentanombre
FROM public.inpro2021nube_facturacion
WHERE cuentanombre LIKE 'Ventas Merc%%'
ORDER BY clientecodigo, fechacomprobante
"""
        
    df = pd.read_sql(query, engine)
    df['fechacomprobante'] = pd.to_datetime(df['fechacomprobante'])
    print(f"Datos cargados: {len(df)} registros (solo ventas 'Ventas Merc')")
    print(f"Rango de fechas: {df['fechacomprobante'].min()} a {df['fechacomprobante'].max()}")
    print(f"Total de clientes únicos: {df['clientecodigo'].nunique()}")
    return df

def obtener_ultima_compra_hasta_fecha(df, cliente, fecha_fin):
    """Obtiene la última compra de un cliente hasta una fecha específica"""
    compras_cliente = df[
        (df['clientecodigo'] == cliente) & 
        (df['fechacomprobante'] <= fecha_fin)
    ]
    if len(compras_cliente) == 0:
        return None
    return compras_cliente['fechacomprobante'].max()

def calcular_meses_desde_fecha(fecha_inicio, fecha_fin):
    """Calcula los meses entre dos fechas usando DATEDIFF equivalente"""
    delta = relativedelta(fecha_fin, fecha_inicio)
    return delta.years * 12 + delta.months

def calcular_status_mensual(df, cliente, primera_compra, mes_inicio, mes_fin, status_mes_anterior):
    """Calcula el status mensual de un cliente basado en la lógica DAX
    
    Lógica de churn: Si un cliente no compra durante 3 meses seguidos (<=3 meses),
    al 4to mes (después de 3 meses completos) se declara churn.
    """
    
    if pd.isna(primera_compra):
        return None
    
    ultima_compra_hasta_mes = obtener_ultima_compra_hasta_fecha(df, cliente, mes_fin)
    
    if primera_compra >= mes_inicio and primera_compra <= mes_fin:
        return "Nuevo"
    
    if ultima_compra_hasta_mes is None:
        if primera_compra < mes_inicio:
            meses_desde_primera = calcular_meses_desde_fecha(primera_compra, mes_fin)
            # Si han pasado más de 3 meses (4+ meses) desde la primera compra, es churn
            if meses_desde_primera > 3:
                # Calcular fecha de declaración de churn (fin del 4to mes después de primera compra)
                fecha_churn_declaration = primera_compra + relativedelta(months=4)
                fecha_churn_declaration = fecha_churn_declaration.replace(day=1) + relativedelta(months=1) - timedelta(days=1)
                
                if fecha_churn_declaration >= mes_inicio and fecha_churn_declaration <= mes_fin:
                    return "Churn del Mes"
                elif fecha_churn_declaration < mes_inicio:
                    if status_mes_anterior in ["Churn del Mes", "Churn Sostenido"]:
                        return "Churn Sostenido"
                    else:
                        return "Churn del Mes"
                elif meses_desde_primera <= 3:
                    return "Cliente sin compra"
        return None
    
    tiene_compra_actual = ultima_compra_hasta_mes >= mes_inicio
    
    if tiene_compra_actual:
        ultima_compra_antes = obtener_ultima_compra_hasta_fecha(df, cliente, mes_inicio - timedelta(days=1))
        
        if ultima_compra_antes is not None:
            meses_sin_compra = calcular_meses_desde_fecha(ultima_compra_antes, mes_inicio)
            # Si había pasado más de 3 meses (4+ meses) sin comprar, es Recuperado
            if meses_sin_compra > 3:
                return "Recuperado"
            else:
                return "Retenido"
        else:
            if primera_compra < mes_inicio:
                meses_desde_primera = calcular_meses_desde_fecha(primera_compra, mes_inicio)
                if meses_desde_primera > 3:
                    return "Recuperado"
            return "Retenido"
    
    # Si no tiene compra en el mes actual
    # Calcular meses desde última compra hasta el fin del mes actual
    meses_sin_compra = calcular_meses_desde_fecha(ultima_compra_hasta_mes, mes_fin)
    
    # Si tiene 3 meses o menos sin comprar, es "Cliente sin compra"
    if meses_sin_compra <= 3:
        return "Cliente sin compra"
    
    # Si tiene más de 3 meses sin comprar (4+ meses), es churn
    # Calcular la fecha de declaración de churn (fin del 4to mes después de la última compra)
    fecha_churn_declaration = ultima_compra_hasta_mes + relativedelta(months=4)
    fecha_churn_declaration = fecha_churn_declaration.replace(day=1) + relativedelta(months=1) - timedelta(days=1)
    
    # Verificar si ya estaba en churn el mes anterior
    if status_mes_anterior in ["Churn del Mes", "Churn Sostenido"]:
        return "Churn Sostenido"
    
    # Si no estaba en churn el mes anterior, verificar si es "Churn del Mes"
    # Es "Churn del Mes" si estamos en el mes donde se declara el churn (4to mes)
    if fecha_churn_declaration >= mes_inicio and fecha_churn_declaration <= mes_fin:
        return "Churn del Mes"
    elif fecha_churn_declaration < mes_inicio:
        return "Churn Sostenido"
    else:
        return "Churn del Mes"

def generar_fechas_mensuales(df):
    """Genera todas las fechas mensuales desde la primera compra hasta la última"""
    if len(df) == 0:
        return []
    
    fecha_min = df['fechacomprobante'].min()
    fecha_max = df['fechacomprobante'].max()
    
    inicio = fecha_min.replace(day=1)
    if fecha_max.month == 12:
        fin = fecha_max.replace(day=31)
    else:
        fin = (fecha_max.replace(day=1) + relativedelta(months=1)) - timedelta(days=1)
    
    fechas = []
    fecha_actual = inicio
    while fecha_actual <= fin:
        mes_inicio = fecha_actual.replace(day=1)
        if fecha_actual.month == 12:
            mes_fin = fecha_actual.replace(day=31)
        else:
            mes_fin = (fecha_actual + relativedelta(months=1)).replace(day=1) - timedelta(days=1)
        
        fechas.append((mes_inicio, mes_fin))
        fecha_actual = fecha_actual + relativedelta(months=1)
    
    return fechas

def crear_matriz_churn(df):
    """Crea la matriz completa de churn con todos los clientes y meses"""
    
    clientes = df[['clientecodigo', 'clientenombre']].drop_duplicates()
    fechas_mensuales = generar_fechas_mensuales(df)
    print(f"Períodos a procesar: {len(fechas_mensuales)} meses")
    
    primera_compra_df = df.groupby('clientecodigo')['fechacomprobante'].min().reset_index()
    primera_compra_df.columns = ['clientecodigo', 'primera_compra']
    primera_compra_dict = dict(zip(primera_compra_df['clientecodigo'], primera_compra_df['primera_compra']))
    
    resultados = []
    total_clientes = len(clientes)
    
    for idx, (_, cliente_row) in enumerate(clientes.iterrows(), 1):
        cliente = cliente_row['clientecodigo']
        cliente_nombre = cliente_row['clientenombre']
        primera_compra = primera_compra_dict.get(cliente)
        
        if idx % 100 == 0:
            print(f"Procesando cliente {idx}/{total_clientes}...")
        
        status_mes_anterior = None
        
        for mes_inicio, mes_fin in fechas_mensuales:
            if primera_compra is not None and mes_fin < primera_compra.replace(day=1):
                continue
            
            status = calcular_status_mensual(
                df, cliente, primera_compra, mes_inicio, mes_fin, status_mes_anterior
            )
            
            if status is not None:
                mes_str = mes_inicio.strftime('%Y-%m')
                resultados.append({
                    'ClienteCodigo': cliente,
                    'ClienteNombre': cliente_nombre,
                    'Mes': mes_str,
                    'Status': status
                })
            
            status_mes_anterior = status
            
    return pd.DataFrame(resultados)

# ----------------------------------------------------------------------------------
# CONFIGURACIÓN DE QUERYS ESPECÍFICAS
# ----------------------------------------------------------------------------------
# 💡 QUERY FILTRADA SOLICITADA 💡
QUERY_SALDOS_FILTRADOS = """
SELECT * FROM public.inpro2021nube_composicion_saldos_clientes_inprocil c
WHERE 
    c.empresanombre = 'INPROCIL S.A.' AND
    c.cuentacontablecodigo IN ('ANT101', 'AAP301', 'DML101')
"""

# 📁 Spreadsheet 1
saldos_sheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1oR_fdVCyn1cA8zwH4XgU5VK63cZaDC3I1i3-SWaUT20/edit")

# 💡 EXPORTACIÓN MODIFICADA PARA USAR LA QUERY FILTRADA 💡
exportar_tabla_completa(
    QUERY_SALDOS_FILTRADOS,
    saldos_sheet, "Base Saldos Clientes",
    ["importemonedatransaccion", "importemonedaprincipal", "omportemonedaaecundaria"]
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


# Análisis de churn y exportación a Spreadsheet 1, pestaña Analisis_Churn
print("\nEjecutando análisis de churn...")
df_facturacion = obtener_datos_facturacion()
matriz_churn = crear_matriz_churn(df_facturacion)
exportar_tabla_completa(
    matriz_churn,
    saldos_sheet, "Analisis_Churn",
    []  # No hay columnas decimales específicas para formatear
)

# 📁 Spreadsheet 2
libro_mayor_sheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1e9BuGiiOx-GhokgsM37MAaUfddxLH30T-gtYu3UtfOA/edit")

exportar_libro_mayor(
    "SELECT * FROM public.inpro2021nube_libro_mayor",
    libro_mayor_sheet, "Aux Libro Mayor",
    ["Debe", "Haber", "importemonedaprincipal", "Imp. operacion ppal.", "Imp. operacion sec.","Tipo Cambio"]
)

exportar_stock(
    "SELECT * FROM public.inpro2021nube_stock_con_PUC",
    libro_mayor_sheet, "Aux Stock",
    ["Stock","UltimoPrecioCompra"]
)

exportar_sumas_y_saldos(
    "SELECT * FROM public.inpro2021nube_sumas_y_saldos",
    libro_mayor_sheet, "Aux Sumas y Saldos",
    ["Debe", "Haber", "saldoperiodo", "saldo", "saldoinicial"]
)


# 📁 Spreadsheet 3
stock_con_puc_sheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1KQCsJbtIBDfDv86Y9n4lU6Z6e0s9SSVlPlq1MN-dF6g/edit")

exportar_stock(
    "SELECT * FROM public.inpro2021nube_stock_con_PUC",
    stock_con_puc_sheet, "Aux Stock",
    ["Stock","UltimoPrecioCompra"]
)
