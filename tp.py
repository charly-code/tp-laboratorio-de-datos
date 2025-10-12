"""
Autores: 
    
    Levigurevitz, Nicolas 1245/23 elevege02@gmail.com
    Parajo Lopez, Juan Manuel 1367/24 juancholinus@gmail.com
    Guanca, Carla Anabella 1928/21 carguanca@gmail.com

"""
# Importamos bibliotecas
import pandas as pd
import duckdb as dd

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
#%% 

ESTABLECIMIENTOS_EDUCATIVOS.to_csv( ruta_destino +"ESTABLECIMIENTOS_EDUCATIVOS.csv", index=False)
print("---------fue exitosa la creacion del csv -----------------")

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
ESTABLECIMIENTOS_EDUCATIVOS.rename(columns={'in_departamentos':'id_departamentos'}, inplace=True)
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
    
preguntas: CLAE3 = primeros 3 dígitos de CLAE6?
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
print(ALUMNNOS_POR_DEPARTAMENTO)
#%%
ALUMNNOS_POR_DEPARTAMENTO.to_csv( ruta_destino +"LOCALIDAD.csv", index=False)
print("---------fue exitosa la creacion del csv -----------------")

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
ESTABLECIMIENTOS_EDUCATIVOS.replace(" ",0,inplace=True) #aqui, reemplazamos los contenidos vacíos por 0, ya que habia espacios en blanco, y no nos dejaba hacer la sumatoria

consultaJARDIN_MAT = """SELECT provincia,departamento, sum(CAST("Nivel Inicial - Jardín maternal" AS INT)) As "Jardín Maternal"
                        FROM ESTABLECIMIENTOS_EDUCATIVOS
                        GROUP BY provincia,departamento
                        ORDER BY provincia DESC
                        """
dfJM = dd.sql(consultaJARDIN_MAT).df()

consultaJARDIN_INFANTES = """SELECT provincia, departamento, sum(CAST("Nivel inicial - Jardín de infantes" AS INT)) As "Jardín de infantes" 
                             FROM ESTABLECIMIENTOS_EDUCATIVOS
                             GROUP BY provincia, departamento
                             ORDER BY provincia DESC"""
                             
dfJI = dd.sql(consultaJARDIN_INFANTES).df()

JardinesTotales = """SELECT dfJI.provincia As Provincia, dfJI.departamento As Depto, (dfJI."Jardín de infantes" + dfJM."Jardín Maternal") As Jardines, (poblacion_por_nivel_educativo.nivel_inicial + poblacion_por_nivel_educativo.jardin_de_infantes) As "Población Jardín"
                     FROM dfJI, dfJM, poblacion_por_nivel_educativo
                     WHERE dfJI.provincia = dfJM.provincia AND Depto = dfJM.departamento AND dfJI.provincia = poblacion_por_nivel_educativo.provincia AND Depto = poblacion_por_nivel_educativo.departamento"""
                     
dfJardinesTotales = dd.sql(JardinesTotales).df()

consultaPRIMARIA = """SELECT provincia, departamento, sum(CAST("Primario" As INT)) As "Primario"
                      FROM ESTABLECIMIENTOS_EDUCATIVOS
                      GROUP BY provincia, departamento
                      ORDER BY provincia DESC"""
                      
dfPr = dd.sql(consultaPRIMARIA).df()

primariosTotales = """SELECT p.provincia As Provincia, p.departamento As Departamento, d.Primario, p.primario As "Poblacion Primario"
                      FROM dfPr As d
                      INNER JOIN poblacion_por_nivel_educativo As p
                      ON d.provincia=p.provincia AND d.departamento=p.departamento"""
dfPrimario = dd.sql(primariosTotales).df()

consultaSECUNDARIO = """SELECT provincia, departamento, SUM(CAST("Secundario" As INT)) As "Secundario" 
                        FROM ESTABLECIMIENTOS_EDUCATIVOS
                        GROUP BY provincia, departamento
                        ORDER BY provincia DESC"""
dfSec = dd.sql(consultaSECUNDARIO).df()

secundariosTotales = """SELECT p.provincia As Provincia, p.departamento As Departamento, d.Secundario, p.Secundario As "Poblacion Secundario" 
                        FROM dfSec As d
                        INNER JOIN poblacion_por_nivel_educativo As p
                        ON d.provincia=p.provincia AND d.departamento=p.departamento"""
dfSecundario = dd.sql(secundariosTotales).df()

consultaTODOUNIDO = """SELECT j.Provincia, j.Depto, j.Jardines, j."Población Jardín", p.Primario, p."Poblacion Primario", s.Secundario, s."Poblacion Secundario"
                       FROM dfJardinesTotales As j, dfPrimario As p, dfSecundario As s 
                       WHERE j.Provincia=s.Provincia AND j.Provincia=p.Provincia AND j.Depto=s.Departamento AND j.Depto=p.Departamento
                       ORDER BY j.Provincia ASC, p.Primario DESC"""

dfTodoUnido = dd.sql(consultaTODOUNIDO).df()

#%%
"""
Para cada departamento informar la provincia, el nombre del departamento y
la cantidad de empleados totales en ese departamento, para el año 2022. El
orden del reporte debe ser alfabético por provincia y, dentro de las provincias,
descendente por cantidad de empleados
"""
consultaEMPLEOS = """SELECT provincia, departamento, sum(CAST("empleo" As INT)) As "Cantidad total de empleados en 2022"
                     FROM EP_filtrado
                     GROUP BY provincia, departamento
                     ORDER BY provincia ASC, "Cantidad total de empleados en 2022" DESC
                     """
                     
cantTotalEmpleadosPorDepartamento = dd.sql(consultaEMPLEOS).df()
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
#%%
"""
Según los datos de 2022, para cada departamento que tenga una cantidad
de empleados mayor que el promedio de los puestos de trabajo de los
departamentos de la misma provincia, indicar: provincia, nombre del
departamento, los primeros tres dígitos del CLAE6 que más empleos genera,
(si no tiene 6 dígitos, agregar un 0 a la izquierda) y la cantidad de empleos en
ese rubro.
"""


consultaITEM4 = """SELECT provincia, departamento, clae6, CASE WHEN clae6.size()"""
                     
#%%
""" ------------------------------------  Inicio graficos ----------------------"""

#%%
"""
Mostrar, utilizando herramientas de visualización, la siguiente información:
i) Cantidad de empleados por provincia, para 2022. Mostrarlos ordenados de
manera decreciente por dicha cantidad."""
#%%
"""
ii) Graficar la cantidad de establecimientos educativos (EE) de los
departamentos en función de la población, separando por nivel educativo y
su correspondiente grupo etario (identificándolos por colores). Se pueden
basar en la primera consulta SQL para realizar este gráfico.
"""
#%%
"""
iii) Realizar un boxplot por cada provincia, de la cantidad de EE por cada
departamento de la provincia. Mostrar todos los boxplots en una misma
figura, ordenados por la mediana de cada provincia.
"""
#%%
"""
iv) Relación entre la cantidad de empleados cada mil habitantes (para 2022) y
de EE cada mil habitantes por departamento.
"""
#%%
"""
v) Las 5 actividades (CLAE6) con mayor y menor proporción (respectivamente)
de empleadas mujeres, para 2022. Incluir en el gráfico la proporción
promedio de empleo femenino.
"""














