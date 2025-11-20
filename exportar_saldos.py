# ----------------------------------------------------------------------------------
# CONFIGURACIÓN DE QUERYS ESPECÍFICAS
# ----------------------------------------------------------------------------------
# CORRECCIÓN: Se usan dobles porcentajes (%%) para que Python no crea que son variables.

QUERY_SALDOS_CLIENTES_FILTRADOS = """
SELECT * FROM public.inpro2021nube_composicion_saldos_clientes_inprocil c
WHERE 
    c.empresanombre = 'INPROCIL S.A.' AND
    c.cuentacontablecodigo IN ('ANT101', 'AAP301', 'DML101') AND
    c.clientenombre not like '%%BENVENUTO%%'  AND
    c.clientenombre not like '%%CONCEPCION%%' AND
    c.clientenombre not like '%%BUIATTI%%' AND
    c.clientenombre not like '%%CAMPUZANO HORACIO DAVID%%' AND
    c.clientenombre not like '%%CONTIN %%' AND
    c.clientenombre not like '%%COOPERATIVA DE TRABAJO%%' AND
    c.clientenombre not like '%%DOMVIL%%' AND
    c.clientenombre not like '%%GAS MOVIL%%' AND
    c.clientenombre not like '%%GNC PATAGONICA%%' AND
    c.clientenombre not like '%%GOMEZ FABIAN%%' AND
    c.clientenombre not like '%%GOMEZ GUSTAVO%%' AND
    c.clientenombre not like '%%PALLETIZATE%%' AND
    c.clientenombre not like '%%PAUSYG%%' AND
    c.clientenombre not like '%%POWER CHECK%%' AND
    c.clientenombre not like '%%RODRIGUEZ ALEJANDRO%%' AND
    c.clientenombre not like '%%VALSI GAS%%'
"""

# 💡 QUERY NUEVA: Saldos de Proveedores filtrados
QUERY_SALDOS_PROVEEDORES_FILTRADOS = """
select * from public.inpro2021nube_composicion_saldo_proveedores_inprocil c
"""

# ----------------------------------------------------------------------------------
# EXPORTACIONES PRINCIPALES
# ----------------------------------------------------------------------------------

# 📁 Spreadsheet 1 (ID: 1oR_fdVCyn1cA8zwH4XgU5VK63cZaDC3I1i3-SWaUT20)
saldos_sheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1oR_fdVCyn1cA8zwH4XgU5VK63cZaDC3I1i3-SWaUT20/edit")

# 1. EXPORTACIÓN DE SALDOS DE CLIENTES FILTRADOS
print("\nEjecutando exportación: Base Saldos Clientes...")
exportar_tabla_completa(
    QUERY_SALDOS_CLIENTES_FILTRADOS,
    saldos_sheet, "Base Saldos Clientes",
    ["importemonedatransaccion", "importemonedaprincipal", "importemonedasecundaria"]
)

# 2. 💡 EXPORTACIÓN DE SALDOS DE PROVEEDORES
print("\nEjecutando exportación: Composicion Saldo Proveedores de INPROCIL S.A.")
exportar_tabla_completa(
    QUERY_SALDOS_PROVEEDORES_FILTRADOS,
    saldos_sheet, "Composicion Saldo Proveedores",
    ["importemonedatransaccion", "importemonedaprincipal", "importemonedasecundaria"]
)

# 3. Resto de exportaciones...
print("\nEjecutando exportación: Base Sumas y Saldos...")
exportar_tabla_completa(
    "SELECT * FROM public.inpro2021nube_sumas_y_saldos",
    saldos_sheet, "Base Sumas y Saldos",
    ["sumadebe", "sumahaber", "saldoacumulado"]
)

print("\nEjecutando exportación: Base Pendientes Entrega...")
exportar_tabla_completa(
    "SELECT * FROM public.inpro2021nube_pedidos_pendientes_de_entrega",
    saldos_sheet, "Base Pendientes Entrega",
    ["cantidad_pendiente"]
)

print("\nEjecutando exportación: Base Facturacion...")
exportar_tabla_completa(
    "SELECT * FROM public.inpro2021nube_facturacion",
    saldos_sheet, "Base Facturacion",
    ["preciomonedatransaccion", "importemonedatransaccion", "importemonedaprincipal",
     "importemonedasecundaria", "cotizacionmonedatransaccion", "cantidad"]
)
