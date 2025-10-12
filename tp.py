"""
Autores: 
    
    Levigurevitz, Nicolas 1245/23 elevege02@gmail.com
    Parajo Lopez, Juan Manuel 1367/24 juancholinus@gmail.com
    Guanca, Carla Anabella 1928/21 carguanca@gmail.com

"""
# Importamos bibliotecas
import pandas as pd
import duckdb as dd
import seaborn as sns
import matplotlib.pyplot as plt

# Ubicacion de los directorios
carpeta = "/home/charly/Escritorio/ldd/tp/tp-laboratorio-de-datos/TablasOriginales/"
ruta_destino="/home/charly/Escritorio/ldd/tp/tp-laboratorio-de-datos/TablasModelo/"

#%%
""" ------------------------------------  Inicio limpieza Datos ----------------------"""

#%% 
EE  = pd.read_csv(carpeta+'/2022_padron_oficial_establecimientos_educativos.csv')
print("--------- fue exitosa la carga -----------------")
#%% 
# Establecimientos Educativos no dice nada sobre que contiene cada columna 
# Brinda información sobre los establecimientos educativos, su ubicación y ofertas educativas.
"""

Establecimiento-Localizacion: Nombre de departamento (la podemos derivar de cuenexo)
cueanexo: La clave CUEANEXO consta de 9 dígitos (columna 1): 
    2 dígitos: Identifican la jurisdicción (provincia o CABA).
    5 dígitos: Identifican al instituto o establecimiento educativo.
    2 dígitos más: Identifican los anexos o sedes del establecimiento.
nombre: nombre del instituto 
sector: Privado/Estatal
ambito : Urbano/Rural
domicilio : direccion completa
cp: codigo postal (columna 6)
codigo de area: 
telefono
codigo de localidad: (columna 9) si le sacamos los ultimos 3 digitos podriamos joinear con CODIGO AREA de poblacion
localidad 
departamento 
mail : hay mails que estan vacios 
modalidad : [Comun, especial, adultos, artistica, hospitalaria, intercultural, encierro]
               |__ despues por cada modalidad el nivel.(inicial, Nivel inicial - Jardín maternal,
                                                     Nivel inicial - Jardín de infantes	Primario,
                                                     Secundario	Secundario - INET)
)
    
"""
#%%
ESTABLECIMIENTOS_EDUCATIVOS=EE[['Cueanexo','Jurisdicción','Departamento','Nivel inicial - Jardín maternal', 
             'Nivel inicial - Jardín de infantes', 'Primario', 'Secundario', 
             'Secundario - INET', 'SNU', 'SNU - INET']]


def agregarcero(codigo):
    codigo_str = str(codigo)
    if len(codigo_str) == 8:
        return '0' + codigo_str
    return codigo_str

ESTABLECIMIENTOS_EDUCATIVOS['Cueanexo'] = ESTABLECIMIENTOS_EDUCATIVOS['Cueanexo'].astype(str)
ESTABLECIMIENTOS_EDUCATIVOS['Cueanexo'] = ESTABLECIMIENTOS_EDUCATIVOS['Cueanexo'].apply(agregarcero)
ESTABLECIMIENTOS_EDUCATIVOS['Jurisdicción'] = ESTABLECIMIENTOS_EDUCATIVOS['Jurisdicción'].replace('Ciudad de Buenos Aires', 'CABA')
acentos = {'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u',
           'Á': 'A', 'É': 'E', 'Í': 'I', 'Ó': 'O', 'Ú': 'U'}

for vocal_con_acento, vocal_sin_acento in acentos.items():
   ESTABLECIMIENTOS_EDUCATIVOS['Jurisdicción'] = ESTABLECIMIENTOS_EDUCATIVOS['Jurisdicción'].str.replace(vocal_con_acento, vocal_sin_acento, regex=False)
   ESTABLECIMIENTOS_EDUCATIVOS['Departamento']=ESTABLECIMIENTOS_EDUCATIVOS['Departamento'].str.replace(vocal_con_acento, vocal_sin_acento, regex=False)

ESTABLECIMIENTOS_EDUCATIVOS['Jurisdicción'] = ESTABLECIMIENTOS_EDUCATIVOS['Jurisdicción'].str.strip().str.upper()
ESTABLECIMIENTOS_EDUCATIVOS['Departamento'] = ESTABLECIMIENTOS_EDUCATIVOS['Departamento'].str.strip().str.upper()

ESTABLECIMIENTOS_EDUCATIVOS.rename(columns={'Jurisdicción':'provincia'}, inplace=True)
STABLECIMIENTOS_EDUCATIVOS.replace(" ",0,inplace=True) 
#%% 

ESTABLECIMIENTOS_EDUCATIVOS.to_csv( ruta_destino +"ESTABLECIMIENTOS_EDUCATIVOS.csv", index=False)
#%% 
print("---------fue exitosa la creacion del csv -----------------")
ESTABLECIMIENTOS_EDUCATIVOS.columns
#%% 
"""
columnas ESTABLECIMIENTOS_EDUCATIVOS
    cueanexo: La clave CUEANEXO consta de 9 dígitos (columna 1): 
        primeros 2 dígitos: Identifican la jurisdicción (provincia o CABA).
    provincia:
    Departamento:
    sector: Privado/Estatal:
    Nivel inicial - Jardín maternal:
    Nivel inicial - Jardín de infantes:
    Primario:
    Secundario	Secundario - INET:
"""

#%%
EP = pd.read_csv(carpeta+"Datos_por_departamento_actividad_y_sexo.csv")
print("---------fue exitosa la carga -----------------")
#%%
# Este dataset contiene la información por departamento de 
# la cantidad de empleo y establecimientos laborales por departamento según la 
# actividad realizada y la cantidad de empleados según su sexo
"""
anio: int
in_departamentos : Número entero (integer)ID del departamento según nomenclatura de INDEC 
                    |__el primer digito o los dos primeros digitos son la provincia, 
                    los siguientes tres son el departamento,
                    y el resto identifica la localidad, fracción y radio. 
departamento Texto: (string) Nombre de departamento 
provincia_id : Número entero (integer)ID de la provincia según nomenclatura de INDEC 
provincia Texto: (string)Nombre de la provincia 
clae6: Número entero (integer)Código de actividad a seis dígitos del CLAE 
letra: Texto (string)Código de actividad a nivel de letra del CLAE 
genero: Texto (string)Indica el sexo biológico de los trabajadores de la fila correspondiente 
Empleo: Número decimal (number)Indica la cantidad de puestos de trabajo para el nivel de desagregación deseado 
Establecimiento: Número decimal (number)Cantidad de establecimientos para los cruces solicitados 
empresas_exportadoras
"""
#%%
EP_filtrado = EP[['anio','in_departamentos','departamento','provincia','clae6','letra','genero','Empleo','Establecimientos','empresas_exportadoras']]
EP_filtrado = EP_filtrado[EP_filtrado['anio'] == 2022]
#%%
ESTABLECIMIENTOS_PRODUCTIVOS = EP[['in_departamentos','departamento','provincia','clae6','letra','genero','Empleo','Establecimientos','empresas_exportadoras']]


def formatear_codigo(codigo):
    codigo_str = str(codigo)
    if len(codigo_str) == 4:
        return '0' + codigo_str
    return codigo_str

ESTABLECIMIENTOS_PRODUCTIVOS['in_departamentos'] = ESTABLECIMIENTOS_PRODUCTIVOS['in_departamentos'].astype(str)
ESTABLECIMIENTOS_PRODUCTIVOS['in_departamentos'] = ESTABLECIMIENTOS_PRODUCTIVOS['in_departamentos'].apply(formatear_codigo)
print(ESTABLECIMIENTOS_PRODUCTIVOS)

ESTABLECIMIENTOS_PRODUCTIVOS['in_departamentos'] = ESTABLECIMIENTOS_PRODUCTIVOS['in_departamentos'].replace('Ciudad de Buenos Aires', 'CABA')
acentos = {'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u',
           'Á': 'A', 'É': 'E', 'Í': 'I', 'Ó': 'O', 'Ú': 'U'}

for vocal_con_acento, vocal_sin_acento in acentos.items():
   ESTABLECIMIENTOS_PRODUCTIVOS['departamento'] = ESTABLECIMIENTOS_PRODUCTIVOS['departamento'].str.replace(vocal_con_acento, vocal_sin_acento, regex=False)
   ESTABLECIMIENTOS_PRODUCTIVOS['provincia']=ESTABLECIMIENTOS_PRODUCTIVOS['provincia'].str.replace(vocal_con_acento, vocal_sin_acento, regex=False)

ESTABLECIMIENTOS_PRODUCTIVOS['departamento'] = ESTABLECIMIENTOS_PRODUCTIVOS['departamento'].str.strip().str.upper()
ESTABLECIMIENTOS_PRODUCTIVOS['provincia'] = ESTABLECIMIENTOS_PRODUCTIVOS['provincia'].str.strip().str.upper()

def formatear(codigo):
    codigo_str = str(codigo)
    if len(codigo_str) == 5:
        return '0' + codigo_str
    return codigo_str

ESTABLECIMIENTOS_PRODUCTIVOS['clae6'] = ESTABLECIMIENTOS_PRODUCTIVOS['clae6'].astype(str)
ESTABLECIMIENTOS_PRODUCTIVOS['clae6'] = ESTABLECIMIENTOS_PRODUCTIVOS['clae6'].apply(formatear)
ESTABLECIMIENTOS_PRODUCTIVOS.rename(columns={'in_departamentos':'id_departamentos'}, inplace=True)
#%%
"""
columnas ESTABLECIMIENTOS_PRODUCTIVOS
'id_departamentos': numeros de 5 digitos
'departamento': string
'provincia', 
'clae6': numeros de 6 cifras
'letra': string
'genero': Varones / Mujeres
'Empleo': int 
'Establecimientos':int
'empresas_exportadoras'; int
"""
#%%
ESTABLECIMIENTOS_PRODUCTIVOS.to_csv( ruta_destino +"ESTABLECIMIENTOS_PRODUCTIVOS.csv", index=False)
print("---------fue exitosa la creacion del csv -----------------")

#%%
codigos = pd.read_csv(carpeta+"actividades_establecimientos.csv")
print("---------fue exitosa la carga -----------------")
#%%
"""
clae6 Número entero (integer)Código de actividad a seis dígitos (CLAE6) 
clae2 Número entero (integer)Código de actividad a dos dígitos (CLAE2) 
letra Texto (string)Código de actividad a nivel de letra (CLAE Letra) 
clae6_desc Texto (string)Descriptor del clae6 (UTF-8) 
clae2_desc Texto (string)Descriptor del clae2 (UTF-8) 
letra_desc Texto (string)Descriptor de la letra (UTF-8) 

niveles de jerarqui ejemplo: 
    letra: "C"
    letra_desc: "Industria manufacturera"

    clae2: "10"
    clae2_desc: "Elaboración de productos alimenticios"

    clae6: "107200"
    clae6_desc: "Elaboración de bebidas malteadas y de malta"
    
"""
#%%
codigos['clae6'] = codigos['clae6'].astype(str)
codigos['clae6'] = codigos['clae6'].apply(formatear)
CODIGO=codigos[['clae6','letra', 'clae6_desc', 'clae2_desc', 'letra_desc']]
#%%
CODIGO.to_csv( ruta_destino +"CODIGO.csv", index=False)
print("---------fue exitosa la creacion del csv -----------------")

#%%
localidad = pd.read_excel(carpeta+"padron_poblacion.xlsX")
print("---------fue exitosa la carga -----------------")
#%%
"""
Aquí se presentan las publicaciones de resultados del Censo Nacional de Población, Hogares y 
Viviendas 2022 y sus respectivos cuadros estadísticos. La información de cada jurisdicción
se presenta a nivel de departamento, partido o comuna.
Además, se incluye un cuadro según gobierno local para las 23 provincias y 
la Ciudad Autónoma de Buenos Aires y una serie de cuadros de la Región Metropolitana de 
Buenos Aires, desagregados por comuna y partido, entidad, localidad y zona rural.

localidad/ciudad: ID DEPARTAMENTO
edad :  
casos : cantidad de personas con esa edad
% : porcentaje 
acumulado% : porcentaje acumulado hasta esa edad


rangos etarios educativos: 
    nivel inicial: 0 a 2 años
    jardin de infantes : 3 a 5 
    primario : 6 a 12 
    secundario : 12 a 18
    adulto : 18 hasta 65 
"""

#%% 
localidad_filtrado = localidad.loc[12:56594, ['Unnamed: 1','Unnamed: 2','Unnamed: 3']]
print(localidad_filtrado)
#%% 
filas_inicio_total = localidad_filtrado.loc[localidad_filtrado['Unnamed: 1'] == 'Total']
indices_inicio = list(filas_inicio_total.index)
totales = []
totales = localidad_filtrado.loc[localidad_filtrado['Unnamed: 1'] == 'Total', 'Unnamed: 2'].tolist()
print(totales)

#%% 
filas_inicio = localidad_filtrado.loc[localidad_filtrado['Unnamed: 2'] == 'Casos']
indices_inicio = list(filas_inicio.index)
indices_inicio_mas_1 = []

for i in indices_inicio:
    indice_incrementado = i + 1 
    indices_inicio_mas_1.append(indice_incrementado)
print(indices_inicio_mas_1)
#%% 
palabra_clave = 'AREA'
filas_con_palabra = localidad_filtrado.loc[localidad_filtrado['Unnamed: 1'].str.contains(palabra_clave, na=False)]

lista_areas = filas_con_palabra['Unnamed: 1'].str[-5:].tolist()

print("Cantidad de departamentos de Argentina en nuestra base de datos ", len(lista_areas))

#%% 
serie_filas = []
index = 0 
for j in indices_inicio_mas_1:
    i = j + 65  
    if i <= len(localidad_filtrado):  
        casos = localidad_filtrado.loc[j:i, 'Unnamed: 2'] 
        
        serie_filas.append([str(lista_areas[index])]+[str(totales[index])]+casos.tolist())  
        index += 1
    else:
        break

#%% 
claves = ["in_departamentos","Total_Poblacion"] 
for edad in range(0, 66) : 
    claves.append(str(edad))
    
ALUMNNOS_POR_DEPARTAMENTO = pd.DataFrame(serie_filas, columns= claves)

ALUMNNOS_POR_DEPARTAMENTO.rename(columns={'in_departamentos':'id_departamentos'}, inplace=True)

#%%
print("Cantidad de departamentos de Argentina en nuestra base de datos ", len(ALUMNNOS_POR_DEPARTAMENTO))


#%%
ALUMNNOS_POR_DEPARTAMENTO['id_departamentos'] = ALUMNNOS_POR_DEPARTAMENTO['id_departamentos'].astype(str)

"""
rangos etarios educativos: 
    nivel inicial: 0 a 2 años
    jardin de infantes : 3 a 5 
    primario : 6 a 12 
    secundario : 12 a 18
    adulto : 18 hasta 65 
"""
columnas_a_sumar=['0','1','2']
ALUMNNOS_POR_DEPARTAMENTO['nivel_inicial'] = ALUMNNOS_POR_DEPARTAMENTO[columnas_a_sumar].sum(axis=1)

columnas_a_sumar=['3','4','5']
ALUMNNOS_POR_DEPARTAMENTO['jardin_de_infantes'] = ALUMNNOS_POR_DEPARTAMENTO[columnas_a_sumar].sum(axis=1)

columnas_a_sumar=['6','7','8','9','10','11','12']
ALUMNNOS_POR_DEPARTAMENTO['primario'] = ALUMNNOS_POR_DEPARTAMENTO[columnas_a_sumar].sum(axis=1)

columnas_a_sumar=['13','14','15','16','17','18']
ALUMNNOS_POR_DEPARTAMENTO['secundario'] = ALUMNNOS_POR_DEPARTAMENTO[columnas_a_sumar].sum(axis=1)

claves=[]
for edad in range(19, 66) : 
    claves.append(str(edad))
ALUMNNOS_POR_DEPARTAMENTO['adulto'] = ALUMNNOS_POR_DEPARTAMENTO[claves].sum(axis=1)


ALUMNNOS_POR_DEPARTAMENTO= ALUMNNOS_POR_DEPARTAMENTO[['id_departamentos','nivel_inicial','jardin_de_infantes','primario','secundario','Total_Poblacion']]

print(len(ALUMNNOS_POR_DEPARTAMENTO))

#%%
ALUMNNOS_POR_DEPARTAMENTO.to_csv( ruta_destino +"LOCALIDAD.csv", index=False)
print("---------fue exitosa la creacion del csv -----------------")
#%%
"""
columnas ALUMNNOS_POR_DEPARTAMENTO:
    
'id_departamentos':
 'nivel_inicial':
 'jardin_de_infantes':
 'primario':
 'secundario':
 'Total_Poblacion':

"""
#%%
""" ------------------------------------  Empezamos a hacer las tablas de SQL ----------------------"""

#%%1
"""
Para cada departamento informar la provincia, el nombre del departamento,
la cantidad de Establecimientos Educativos (EE) de cada nivel educativo,
considerando solamente la modalidad común, y la cantidad de habitantes con
edad correspondiente al nivel educativos listado. El orden del reporte debe
ser alfabético por provincia y dentro de las provincias descendente por
cantidad de escuelas primarias
"""
consultaSQL1  = """
SELECT 
    ep.provincia,
    ep.departamento,
    SUM(COALESCE(CAST(ee."Nivel inicial - Jardín maternal" AS INTEGER), 0)) AS "Jardín Maternal",
    SUM(COALESCE(CAST(ee."Nivel inicial - Jardín de infantes" AS INTEGER), 0)) AS "Jardín Infantes",
    SUM(COALESCE(CAST(ee."Primario" AS INTEGER), 0)) AS "Primarias",
    SUM(COALESCE(CAST(ee."Secundario" AS INTEGER), 0)) AS "Secundarias",
    apd.nivel_inicial AS "Población Nivel Inicial",
    apd.jardin_de_infantes AS "Población Jardín Infantes",
    apd.primario AS "Población Primario",
    apd.secundario AS "Población Secundario"
FROM ESTABLECIMIENTOS_EDUCATIVOS ee
INNER JOIN ESTABLECIMIENTOS_PRODUCTIVOS ep 
    ON ee.provincia = ep.provincia AND ee.Departamento = ep.departamento
INNER JOIN ALUMNNOS_POR_DEPARTAMENTO apd 
    ON ep.id_departamentos = apd.id_departamentos
GROUP BY 
    ep.provincia, 
    ep.departamento,
    apd.nivel_inicial,
    apd.jardin_de_infantes,
    apd.primario,
    apd.secundario
ORDER BY 
    ep.provincia ASC,
    "Primarias" DESC
"""

DF1 = dd.sql(consultaSQL1).df()
print(len(DF1))
#%%
"""
Para cada departamento informar la provincia, el nombre del departamento y
la cantidad de empleados totales en ese departamento, para el año 2022. El
orden del reporte debe ser alfabético por provincia y, dentro de las provincias,
descendente por cantidad de empleados
"""
consultaSQL2 = """
SELECT 
    ep.provincia,
    ep.departamento,
    SUM(COALESCE(CAST(ep.Empleo AS INTEGER), 0)) AS "Cantidad total de empleados en 2022"
FROM ESTABLECIMIENTOS_PRODUCTIVOS ep
INNER JOIN (
    SELECT DISTINCT provincia, departamento 
    FROM ESTABLECIMIENTOS_PRODUCTIVOS
) ep_uniq ON ep.provincia = ep_uniq.provincia AND ep.departamento = ep_uniq.departamento
GROUP BY 
    ep.provincia, 
    ep.departamento
ORDER BY 
    ep.provincia ASC,
    "Cantidad total de empleados en 2022" DESC
"""

DF2 = dd.sql(consultaSQL2).df()
print(f"Cantidad de departamentos: {len(DF2)}")
#%%
"""
Para cada departamento, indicar provincia, nombre del departamento,
cantidad de empresas exportadoras que emplean mujeres (en 2022),
cantidad de EE (de modalidad común) y población total. Ordenar por
cantidad de EE descendente, cantidad de empresas exportadoras
descendente, nombre de provincia ascendente y nombre de departamento
ascendente. No omitir departamentos sin EE o exportadoras con empleo
femenino.
"""
consultaSQL3 = """
WITH empresas_exportadoras_mujeres AS (
    SELECT 
        provincia,
        departamento,
        SUM(COALESCE(CAST(empresas_exportadoras AS INTEGER), 0)) AS empresas_exportadoras_mujeres
    FROM ESTABLECIMIENTOS_PRODUCTIVOS
    WHERE genero = 'Mujer' AND empresas_exportadoras IS NOT NULL
    GROUP BY provincia, departamento
),
establecimientos_educativos_totales AS (
    SELECT 
        provincia,
        Departamento AS departamento,
        SUM(COALESCE(CAST("Nivel inicial - Jardín maternal" AS INTEGER), 0) +
            COALESCE(CAST("Nivel inicial - Jardín de infantes" AS INTEGER), 0) +
            COALESCE(CAST("Primario" AS INTEGER), 0) +
            COALESCE(CAST("Secundario" AS INTEGER), 0)) AS total_establecimientos
    FROM ESTABLECIMIENTOS_EDUCATIVOS
    GROUP BY provincia, Departamento
),
poblacion_total AS (
    SELECT 
        provincia,
        departamento,
        SUM(COALESCE(CAST(Total_Poblacion AS INTEGER), 0)) AS poblacion_total
    FROM ALUMNNOS_POR_DEPARTAMENTO
    GROUP BY provincia, departamento
),
departamentos_base AS (
    SELECT DISTINCT provincia, departamento FROM empresas_exportadoras_mujeres
    UNION
    SELECT DISTINCT provincia, departamento FROM establecimientos_educativos_totales
    UNION
    SELECT DISTINCT provincia, departamento FROM poblacion_total
)
SELECT 
    db.provincia,
    db.departamento,
    COALESCE(eem.empresas_exportadoras_mujeres, 0) AS "Empresas exportadoras con empleo femenino",
    COALESCE(eet.total_establecimientos, 0) AS "Cantidad Establecimientos Educativos",
    COALESCE(pt.poblacion_total, 0) AS "Población total"
FROM departamentos_base db
LEFT JOIN empresas_exportadoras_mujeres eem 
    ON db.provincia = eem.provincia AND db.departamento = eem.departamento
LEFT JOIN establecimientos_educativos_totales eet 
    ON db.provincia = eet.provincia AND db.departamento = eet.departamento
LEFT JOIN poblacion_total pt 
    ON db.provincia = pt.provincia AND db.departamento = pt.departamento
ORDER BY 
    "Cantidad Establecimientos Educativos" DESC,
    "Empresas exportadoras con empleo femenino" DESC,
    db.provincia ASC,
    db.departamento ASC
"""

DF3 = dd.sql(consultaSQL3).df()
print(f"Cantidad de departamentos: {len(DF3)}")
#%%
"""
Según los datos de 2022, para cada departamento que tenga una cantidad
de empleados mayor que el promedio de los puestos de trabajo de los
departamentos de la misma provincia, indicar: provincia, nombre del
departamento, los primeros tres dígitos del CLAE6 que más empleos genera,
(si no tiene 6 dígitos, agregar un 0 a la izquierda) y la cantidad de empleos en
ese rubro.
"""

consultaSQL4 = """
WITH empleo_por_departamento AS (
    SELECT 
        provincia,
        departamento,
        SUM(COALESCE(CAST(Empleo AS INTEGER), 0)) AS total_empleos
    FROM ESTABLECIMIENTOS_PRODUCTIVOS
    WHERE Empleo IS NOT NULL
    GROUP BY provincia, departamento
),
promedio_provincial AS (
    SELECT 
        provincia,
        AVG(total_empleos) AS promedio_empleos_provincia
    FROM empleo_por_departamento
    GROUP BY provincia
),
empleo_por_rubro AS (
    SELECT 
        provincia,
        departamento,
        -- Asegurar que clae6 tenga 6 dígitos
        LPAD(CAST(clae6 AS VARCHAR), 6, '0') AS clae6_completo,
        SUBSTRING(LPAD(CAST(clae6 AS VARCHAR), 6, '0'), 1, 3) AS clae3,
        SUM(COALESCE(CAST(Empleo AS INTEGER), 0)) AS empleos_rubro
    FROM ESTABLECIMIENTOS_PRODUCTIVOS
    WHERE Empleo IS NOT NULL
    GROUP BY provincia, departamento, clae6
),
ranking_rubros AS (
    SELECT 
        provincia,
        departamento,
        clae3,
        empleos_rubro,
        ROW_NUMBER() OVER (
            PARTITION BY provincia, departamento 
            ORDER BY empleos_rubro DESC
        ) AS ranking
    FROM empleo_por_rubro
)
SELECT 
    epr.provincia,
    epr.departamento,
    rr.clae3 AS "CLAE3 más empleos",
    rr.empleos_rubro AS "Cantidad de empleos"
FROM empleo_por_departamento epr
INNER JOIN promedio_provincial pp 
    ON epr.provincia = pp.provincia
INNER JOIN ranking_rubros rr 
    ON epr.provincia = rr.provincia AND epr.departamento = rr.departamento
WHERE 
    epr.total_empleos > pp.promedio_empleos_provincia
    AND rr.ranking = 1
ORDER BY 
    epr.provincia,
    epr.departamento
"""

DF4 = dd.sql(consultaSQL4).df()
print(f"Punto 4 - Departamentos con empleo mayor al promedio provincial: {len(DF4)}")
                     
#%%
""" ------------------------------------  Inicio graficos ----------------------"""

#%%
"""
Mostrar, utilizando herramientas de visualización, la siguiente información:
i) Cantidad de empleados por provincia, para 2022. Mostrarlos ordenados de
manera decreciente por dicha cantidad."""

empleos_provincia = ESTABLECIMIENTOS_PRODUCTIVOS.groupby('provincia')['Empleo'].sum().sort_values(ascending=True)

plt.figure(figsize=(10, 6))
empleos_provincia.plot(kind='barh')
plt.title('Empleados por Provincia - 2022')
plt.xlabel('Cantidad de Empleados')
plt.tight_layout()
plt.show()
#%%
"""
ii) Graficar la cantidad de establecimientos educativos (EE) de los
departamentos en función de la población, separando por nivel educativo y
su correspondiente grupo etario (identificándolos por colores). Se pueden
basar en la primera consulta SQL para realizar este gráfico.
"""
consulta_ee_poblacion = """
SELECT 
    ep.departamento,
    SUM(COALESCE(CAST(ee."Nivel inicial - Jardín maternal" AS INTEGER), 0) +
        COALESCE(CAST(ee."Nivel inicial - Jardín de infantes" AS INTEGER), 0)) AS ee_inicial,
    SUM(COALESCE(CAST(ee."Primario" AS INTEGER), 0)) AS ee_primario,
    SUM(COALESCE(CAST(ee."Secundario" AS INTEGER), 0)) AS ee_secundario,
    apd.nivel_inicial + apd.jardin_de_infantes AS poblacion_inicial,
    apd.primario AS poblacion_primario,
    apd.secundario AS poblacion_secundario
FROM ESTABLECIMIENTOS_EDUCATIVOS ee
INNER JOIN ESTABLECIMIENTOS_PRODUCTIVOS ep 
    ON ee.provincia = ep.provincia AND ee.Departamento = ep.departamento
INNER JOIN ALUMNNOS_POR_DEPARTAMENTO apd 
    ON ep.id_departamentos = apd.id_departamentos
GROUP BY ep.departamento, apd.nivel_inicial, apd.jardin_de_infantes, apd.primario, apd.secundario
"""

df_ee_poblacion = dd.sql(consulta_ee_poblacion).df()

# Crear el gráfico de dispersión
plt.figure(figsize=(12, 8))

# Nivel Inicial
plt.scatter(df_ee_poblacion['poblacion_inicial'], df_ee_poblacion['ee_inicial'], 
           alpha=0.6, color='red', label='Nivel Inicial', s=50)

# Primario
plt.scatter(df_ee_poblacion['poblacion_primario'], df_ee_poblacion['ee_primario'], 
           alpha=0.6, color='blue', label='Primario', s=50)

# Secundario
plt.scatter(df_ee_poblacion['poblacion_secundario'], df_ee_poblacion['ee_secundario'], 
           alpha=0.6, color='green', label='Secundario', s=50)

plt.xlabel('Población por Nivel Educativo', fontsize=12)
plt.ylabel('Cantidad de Establecimientos Educativos', fontsize=12)
plt.title('Establecimientos Educativos vs Población por Nivel', fontsize=14, fontweight='bold')
plt.legend()
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.show()

#%%
"""
iii) Realizar un boxplot por cada provincia, de la cantidad de EE por cada
departamento de la provincia. Mostrar todos los boxplots en una misma
figura, ordenados por la mediana de cada provincia.
"""
# iii) Boxplot de EE por provincia
consulta_ee_provincia = """
SELECT 
    ee.provincia,
    ee.Departamento,
    SUM(COALESCE(CAST(ee."Nivel inicial - Jardín maternal" AS INTEGER), 0) +
        COALESCE(CAST(ee."Nivel inicial - Jardín de infantes" AS INTEGER), 0) +
        COALESCE(CAST(ee."Primario" AS INTEGER), 0) +
        COALESCE(CAST(ee."Secundario" AS INTEGER), 0)) AS total_ee
FROM ESTABLECIMIENTOS_EDUCATIVOS ee
GROUP BY ee.provincia, ee.Departamento
"""

df_ee_provincia = dd.sql(consulta_ee_provincia).df()

# Ordenar provincias por mediana
provincias_ordenadas = df_ee_provincia.groupby('provincia')['total_ee'].median().sort_values(ascending=False).index

plt.figure(figsize=(14, 8))
sns.boxplot(data=df_ee_provincia, x='provincia', y='total_ee', order=provincias_ordenadas)
plt.title('Distribución de Establecimientos Educativos por Provincia', fontsize=14, fontweight='bold')
plt.xlabel('Provincia', fontsize=12)
plt.ylabel('Cantidad de EE por Departamento', fontsize=12)
plt.xticks(rotation=45, ha='right')
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.show()

print(f"Total de provincias analizadas: {len(df_ee_provincia['provincia'].unique())}")
print(f"Total de departamentos analizados: {len(df_ee_provincia)}")
#%%
"""
iv) Relación entre la cantidad de empleados cada mil habitantes (para 2022) y
de EE cada mil habitantes por departamento.
""" 
# iv) Relación empleados vs EE cada 1000 habitantes
# Calcular empleados por departamento
empleos_por_depto = ESTABLECIMIENTOS_PRODUCTIVOS.groupby('departamento')['Empleo'].sum()

# Calcular EE por departamento (sumar todos los niveles)
ee_por_depto = ESTABLECIMIENTOS_EDUCATIVOS.groupby('Departamento')[
    ['Nivel inicial - Jardín maternal', 'Nivel inicial - Jardín de infantes', 'Primario', 'Secundario']
].sum().sum(axis=1)

# Obtener población por departamento
poblacion_por_depto = LOCALIDAD.groupby('departamento')['Total'].sum()

# Unir todos los datos
datos_deptos = pd.DataFrame({
    'empleos': empleos_por_depto,
    'ee': ee_por_depto,
    'poblacion': poblacion_por_depto
}).dropna()

# Calcular ratios por 1000 habitantes
datos_deptos['empleos_por_mil'] = (datos_deptos['empleos'] / datos_deptos['poblacion']) * 1000
datos_deptos['ee_por_mil'] = (datos_deptos['ee'] / datos_deptos['poblacion']) * 1000

# Gráfico de dispersión
plt.figure(figsize=(10, 6))
plt.scatter(datos_deptos['empleos_por_mil'], datos_deptos['ee_por_mil'], alpha=0.7, s=60)
plt.xlabel('Empleados cada 1000 habitantes', fontsize=12)
plt.ylabel('EE cada 1000 habitantes', fontsize=12)
plt.title('Relación: Empleados vs EE por cada 1000 habitantes', fontsize=14, fontweight='bold')
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.show()

# Mostrar algunos datos
print(f"Departamentos analizados: {len(datos_deptos)}")
print(f"Correlación: {datos_deptos['empleos_por_mil'].corr(datos_deptos['ee_por_mil']):.3f}")
print(f"Empleos por mil habitantes - Promedio: {datos_deptos['empleos_por_mil'].mean():.1f}")
print(f"EE por mil habitantes - Promedio: {datos_deptos['ee_por_mil'].mean():.1f}")
#%%
"""
v) Las 5 actividades (CLAE6) con mayor y menor proporción (respectivamente)
de empleadas mujeres, para 2022. Incluir en el gráfico la proporción
promedio de empleo femenino.
"""

mujeres = ESTABLECIMIENTOS_PRODUCTIVOS[ESTABLECIMIENTOS_PRODUCTIVOS['genero'] == 'Mujeres']
varones = ESTABLECIMIENTOS_PRODUCTIVOS[ESTABLECIMIENTOS_PRODUCTIVOS['genero'] == 'Varones']

# Contar por actividad
actividades_mujeres = mujeres.groupby('clae6').size()
actividades_varones = varones.groupby('clae6').size()

# Juntar y calcular proporción
actividades = pd.DataFrame({
    'mujeres': actividades_mujeres,
    'varones': actividades_varones
}).fillna(0)

actividades['total'] = actividades['mujeres'] + actividades['varones']
actividades['proporcion_mujeres'] = (actividades['mujeres'] / actividades['total']) * 100

# Ordenar
actividades_ordenadas = actividades.sort_values('proporcion_mujeres', ascending=False)

# Tomar top y bottom
top_5 = actividades_ordenadas.head(5)
bottom_5 = actividades_ordenadas.tail(5)

# Gráfico simple
plt.figure(figsize=(10, 6))
plt.barh(range(5), top_5['proporcion_mujeres'], color='pink', label='Más mujeres')
plt.barh(range(5, 10), bottom_5['proporcion_mujeres'], color='lightblue', label='Más varones')
plt.ylabel('Actividades')
plt.xlabel('Porcentaje de mujeres (%)')
plt.title('Actividades con más y menos mujeres')
plt.legend()
plt.show()

print("Top 5 actividades con más mujeres:")
print(top_5[['proporcion_mujeres']])
print("\nTop 5 actividades con menos mujeres:")
print(bottom_5[['proporcion_mujeres']])