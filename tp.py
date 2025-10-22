"""
Autores: 
    
    Levigurevitz, Nicolas 1245/23 elevege02@gmail.com
    Parajo Lopez, Juan Manuel 1367/24 juancholinus@gmail.com
    Guanca, Carla Anabella 1928/21 carguanca@gmail.com

"""
# Importamos bibliotecas que vamos a utiliza 
import pandas as pd
import numpy as np
import duckdb as dd
import seaborn as sns
import matplotlib.pyplot as plt
import os

# para obtener la ruta actual
ruta = os.getcwd()

#%% 
# El dataset de Establecimientos Educativos no contiene informacion sobre las columnas 
# Brinda información sobre los establecimientos educativos, su ubicación y ofertas educativas.
ee  = pd.read_csv(ruta+'/TablasOriginales/2022_padron_oficial_establecimientos_educativos.csv')
print("--------- fue exitosa la carga -----------------")
#%% 
#veamos cuales son las columnas de nuestro dataset 
print(ee.columns)
#%% 
#nosotros trabajaremos con las columnas de modalidad comun. 
#asi que nos queda solo algunas columnas
ee_comun= ee[['Jurisdicción', 'Cueanexo', 'Nombre', 'Sector', 'Ámbito', 'Domicilio',
       'C. P.', 'Código de área', 'Teléfono', 'Código de localidad',
       'Localidad', 'Departamento', 'Mail',
       'Nivel inicial - Jardín maternal', 'Nivel inicial - Jardín de infantes',
       'Primario', 'Secundario', 'Secundario - INET', 'SNU', 'SNU - INET']]
#%% 
#colocamos ceros en los lugares vacios que se encuentran en las columnas de nivel educativo
ee_comun.replace(" ",0,inplace=True)
#%% 
#veamos que tipo de datos hay en cada columna
print (ee_comun.dtypes)

#%% 

"""
explorando las columnas del dataset tenemos:
        Jurisdicción: CABA o provincia 
        cueanexo: La clave CUEANEXO consta de 9 dígitos (columna 1): 
            2 dígitos: Identifican la jurisdicción (provincia o CABA).
            5 dígitos: Identifican al instituto o establecimiento educativo.
            2 dígitos más: Identifican los anexos o sedes del establecimiento.
        Nombre: nombre del instituto 
        Sector: Privado/Estatal
        Ámbito : Urbano/Rural
        Domicilio : direccion completa
        C. P. : codigo postal (columna 6)
        Código de área: 
        Teléfono:
        Código de localidad: Los 2 primeros dígitos corresponden al código de la provincia.
                             Los 3 siguientes corresponden al código del departamento.
                             Los 3 últimos se refieren a la localidad específica.
        Localidad: 
        Departamento :
        Mail : hay mails que estan vacios : 
        Nivel inicial - Jardín maternal:
        Nivel inicial - Jardín de infantes:
        Primario:
        Secundario:
        Secundario - INET:
        SNU:
        SNU - INET:
        
"""

#%%
# nos quedamos con las columnas que necesitamos para la creacion de nuestro DER y para la generacion de los graficos
EE_comun=ee_comun[['Cueanexo','Jurisdicción','Departamento', 'C. P.','Código de localidad','Nivel inicial - Jardín maternal', 
             'Nivel inicial - Jardín de infantes', 'Primario', 'Secundario', 
             'Secundario - INET', 'SNU', 'SNU - INET']]
#%%
# encontramos que valores se perdian porque algunos nombres estaban diferentes
EE_comun['Jurisdicción'] = EE_comun['Jurisdicción'].replace('Ciudad de Buenos Aires', 'CABA')
#%%
# hay un dataset que usamos que venia en la misma pagina de otro dataset como complemento que nos ayudo a buscar coincidencias 
# https://datos.produccion.gob.ar/dataset/distribucion-geografica-de-los-establecimientos-productivos/archivo/16f0dfc2-a9ff-4696-b2e5-79831c5c0ec4
# llamado diccionario de departamentos.csv
# en el mismo tenemos informacion que nos ayuda a conectar de mejor manera los dataset que nos dan en la catedra
codigo_provincia = pd.read_csv(ruta+"/TablasOriginales/codigo_departamento_provincia.csv")
'''
contiene: 
    
    'provincia_id': int
    'in_departamentos': int
    'departamento': string
    'provincia': string
'''
print("--------- fue exitosa la carga -----------------")

#%%
# Realizamos una limpieza de datos para poder quedarnos con la mayor cantidad de informacion 
# corregimos los errores de tipeo ya sea los acentos, mayusculas y abreviaciones en algunos casos
# Lista de vocales y sus versiones sin acento
acentos = {'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u',
           'Á': 'A', 'É': 'E', 'Í': 'I', 'Ó': 'O', 'Ú': 'U','Ü':'U'}

# Aplicar la sustitución en la columna 'Palabra'
EE_comun['Jurisdicción'] = EE_comun['Jurisdicción'].replace('Ciudad de Buenos Aires', 'CABA')
EE_comun['Departamento'] = EE_comun['Departamento'].replace('1§ DE MAYO', '1 DE MAYO')
EE_comun['Departamento'] = EE_comun['Departamento'].replace('LIBERTADOR GRL SAN MARTIN','LIBERTADOR GENERAL SAN MARTIN')


EE_comun['Jurisdicción'] = EE_comun['Jurisdicción'].str.strip().str.upper()
EE_comun['Departamento'] = EE_comun['Departamento'].str.strip().str.upper()
codigo_provincia['provincia'] = codigo_provincia['provincia'].str.strip().str.upper()
codigo_provincia['departamento'] = codigo_provincia['departamento'].str.strip().str.upper()
	
#%%
for vocal_con_acento, vocal_sin_acento in acentos.items():
   EE_comun['Jurisdicción'] = EE_comun['Jurisdicción'].str.replace(vocal_con_acento, vocal_sin_acento, regex=False)
   EE_comun['Departamento'] = EE_comun['Departamento'].str.replace(vocal_con_acento, vocal_sin_acento, regex=False)
   codigo_provincia['provincia'] = codigo_provincia['provincia'].str.replace(vocal_con_acento, vocal_sin_acento, regex=False)
   codigo_provincia['departamento'] = codigo_provincia['departamento'].str.replace(vocal_con_acento, vocal_sin_acento, regex=False)
#%%
codigo_provincia['departamento'] = codigo_provincia['departamento'].replace('DR. MANUEL BELGRANO', 'DOCTOR MANUEL BELGRANO')
codigo_provincia['departamento'] = codigo_provincia['departamento'].replace('GENERAL JUAN FACUNDO QUIROGA', 'GENERAL JUAN F QUIROGA')
codigo_provincia['departamento'] = codigo_provincia['departamento'].replace('GENERAL FELIPE VARELA', 'CORONEL FELIPE VARELA')
codigo_provincia['departamento'] = codigo_provincia['departamento'].replace('CORONEL DE MARINA LEONARDO ROSALES', 'CORONEL DE MARINA L ROSALES')
codigo_provincia['departamento'] = codigo_provincia['departamento'].replace("O'HIGGINS", 'O HIGGINS')
codigo_provincia['departamento'] = codigo_provincia['departamento'].replace('MAYOR LUIS J. FONTANA', 'MAYOR LUIS J FONTANA')
codigo_provincia['departamento'] = codigo_provincia['departamento'].replace('GENERAL ORTIZ DE OCAMPO', 'GENERAL OCAMPO')
codigo_provincia['departamento'] = codigo_provincia['departamento'].replace('1° DE MAYO', '1 DE MAYO')
codigo_provincia['departamento'] = codigo_provincia['departamento'].replace('JUAN FELIPE IBARRA','JUAN F IBARRA')
codigo_provincia['departamento'] = codigo_provincia['departamento'].replace('JUAN MARTIN DE PUEYRREDON', 'GENERAL JUAN MARTIN DE PUEYRREDON')
codigo_provincia['departamento'] = codigo_provincia['departamento'].replace('JUAN BAUTISTA ALBERDI', 'JUAN B ALBERDI')
codigo_provincia['departamento'] = codigo_provincia['departamento'].replace('ANGEL VICENTE PEÑALOZA', 'GENERAL ANGEL V PEÑALOZA')

#%%
#veamos si hay nulls en las columnas de ambos dataframes
EE_comun.isnull().sum()
codigo_provincia.isnull().sum()

#%%
# Para poder hacer el merge mas adelante , sacamos el 0 de los in_departamentos
def formatear_codigo(codigo):
    codigo_str = str(codigo)
    if codigo_str[0] == '0':
        codigo_str.pop(0)
        return codigo_str
    return codigo_str

codigo_provincia['in_departamentos'] = codigo_provincia['in_departamentos'].astype(str)
codigo_provincia['in_departamentos'] = codigo_provincia['in_departamentos'].apply(formatear_codigo)

#%%
# hago un merge con las columnas que son coincidentes las provincias y los numeros de departamento
resultadoEE_comun = EE_comun.merge(
    codigo_provincia,
    left_on=['Jurisdicción', 'Departamento'],
    right_on=['provincia', 'departamento'],
    how='left'  
)
#%%
print(resultadoEE_comun)
#%%
filas_nan = resultadoEE_comun[resultadoEE_comun['departamento'].isnull()]
print(len(filas_nan))
#LA ANTARTIDA ES LA UNICA QUE NO TIENE ID DE DEPARTAMENTO POR ESA RAZON LA EXCLUIMOS DE LA LISTA 
#%%

ESTABLECIMIENTOS_EDUCATIVOS=resultadoEE_comun[['Cueanexo', 'C. P.','in_departamentos', 'departamento',
                                      'Nivel inicial - Jardín maternal', 'Nivel inicial - Jardín de infantes', 
                                      'Primario', 'Secundario','Secundario - INET', 'SNU', 'SNU - INET']]

#%%
# creamos el csv 
ESTABLECIMIENTOS_EDUCATIVOS.to_csv( ruta +"/TablasModelo/ESTABLECIMIENTOS_EDUCATIVOS.csv", index=False)
print("---------fue exitosa la creacion del csv -----------------")
#%%
# ESTABLECIMIENTOS PRODUCTIVOS 
# Este dataset contiene la información por departamento de 
# la cantidad de empleo y establecimientos laborales por departamento según la 
# actividad realizada y la cantidad de empleados según su sexo
EP = pd.read_csv(ruta+"/TablasOriginales/Datos_por_departamento_actividad_y_sexo.csv")
print("---------fue exitosa la carga -----------------")

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
# contiene diferentes años asi que nos quedamos con el año 2022. 
EP_filtrado = EP[['anio','in_departamentos','departamento','provincia','clae6','letra','genero','Empleo','Establecimientos','empresas_exportadoras']]
EP_filtrado = EP_filtrado[EP_filtrado['anio'] == 2022]
EP_filtrado = EP[['in_departamentos','departamento','provincia','clae6','letra','genero','Empleo','Establecimientos','empresas_exportadoras']]

print(EP_filtrado)
#%%
# hay columnas que superan las empresas exportadoras en cantidad a los estableciminetos 
# la forma que encontramos para recuperar esa informacion es dar vuelta esa fila los valores de esa columna
inconsistentes = EP_filtrado[EP_filtrado['empresas_exportadoras'] > EP_filtrado['Establecimientos']]
porcentaje_inconsistentes = len(inconsistentes) / len(EP_filtrado) * 100
print(len(inconsistentes))
#%%
establecimientos = EP_filtrado['Establecimientos'].values
eexp = EP_filtrado['empresas_exportadoras'].values

est_nuevo = []
eexp_nuevo = []

for e, ex in zip(establecimientos, eexp):
    if ex > e:  
        est_nuevo.append(ex)
        eexp_nuevo.append(e)
    else:
        est_nuevo.append(e)
        eexp_nuevo.append(ex)

# Asignamos los nuevos valores (manteniendo los mismos nombres de columnas)
EP_filtrado['Establecimientos'] = est_nuevo
EP_filtrado['empresas_exportadoras'] = eexp_nuevo
#%%
#verificamos que no hay mas inconsistencias
inconsistentes = EP_filtrado[EP_filtrado['empresas_exportadoras'] > EP_filtrado['Establecimientos']]
porcentaje_inconsistentes = len(inconsistentes) / len(EP_filtrado) * 100
print(len(inconsistentes))
#%%
ESTABLECIMIENTOS_PRODUCTIVOS=EP_filtrado[['in_departamentos','departamento','provincia','clae6','letra','genero','Establecimientos','Empleo','empresas_exportadoras']]
ESTABLECIMIENTOS_PRODUCTIVOS.rename(columns={'in_departamentos': 'id_depto'})

#%%
ESTABLECIMIENTOS_PRODUCTIVOS.to_csv( ruta +"/TablasModelo/ESTABLECIMIENTOS_PRODUCTIVOS.csv", index=False)
print("---------fue exitosa la creacion del csv -----------------")
#%%
# Actividades
# Códigos de actividad junto a su descripción

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
actividades = pd.read_csv(ruta+"/TablasOriginales/actividades_establecimientos.csv")
print(actividades.columns )
print("---------fue exitosa la carga -----------------")
#%%
ACTIVIDADES = actividades[['clae6','letra', 'clae6_desc','letra_desc']]
#%%
ACTIVIDADES.to_csv( ruta +"/TablasModelo/ACTIVIDADES.csv", index=False)

#%%
LOCALIDAD = pd.read_excel(ruta+"/TablasOriginales/padron_poblacion.xlsX")
print("---------fue exitosa la carga -----------------")

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
"""

#%% 
# aca comenzamos la limpieza y reestruccturacion del dataset para quedarnos con la mayor cantidad de informacion
# primero me quedo con la parte del dataset que contiene la mayor cantidad de informacion por localidad
poblacion_filtrado = LOCALIDAD.loc[12:56594, ['Unnamed: 1','Unnamed: 2','Unnamed: 3']]
print(poblacion_filtrado)
#%% 
# notamos que las edades por  departamento comenzaba antes de la fila que contenia la palabra Total
palabra_clave = 'Total'
filas_con_palabra = poblacion_filtrado.loc[poblacion_filtrado['Unnamed: 1'].str.contains(palabra_clave, na=False)]
print(filas_con_palabra)
#%% 
# despues de hacernos una lista con las posiciones de la fila que contenia el valor que queriamos nos trajimos el conjunto de filas
# aca notamos que no todas tienen la misma longitud ya que hay departamentos con 110 años como maximo y 110 en otros 
filas_inicio = poblacion_filtrado.loc[poblacion_filtrado['Unnamed: 1'] == 'Total']
indices_inicio = list(filas_inicio.index)
indices_inicio_menos_1 = []

for i in indices_inicio:
    indice_incrementado = i - 1 
    indices_inicio_menos_1.append(indice_incrementado)
print(len(indices_inicio_menos_1))
#%% 
# notamos que las cantidades por edad de cada departamento comenzaba despues de la fila que contenia la palabra Casos

filas_inicio = poblacion_filtrado.loc[poblacion_filtrado['Unnamed: 2'] == 'Casos']
indices_inicio = list(filas_inicio.index)
indices_inicio_mas_1 = []

for i in indices_inicio:
    indice_incrementado = i + 1 
    indices_inicio_mas_1.append(indice_incrementado)
print(len(indices_inicio_mas_1))
#%% 
tamanio_anio=[]
i=0
while (i < len(indices_inicio_mas_1)):
    tamanio_anio.append(indices_inicio_menos_1[i]-indices_inicio_mas_1[i])
    i+=1
print(tamanio_anio)

#%%
# buscamos el maximo para saber que la cuando una fila llegue hasta un numero, llenamos de cero hasta el maximo asi nos queda 
# una tabla completa 
maximo_edad= max(tamanio_anio)
#%% 
# tambien notamos que las filas que contenian la palabra AREA tenian la informacion de el id_departamento
palabra_clave = 'AREA'
filas_con_palabra = poblacion_filtrado.loc[poblacion_filtrado['Unnamed: 1'].str.contains(palabra_clave, na=False)]

lista_areas = filas_con_palabra['Unnamed: 1'].str[-5:].tolist()

print("Cantidad de departamentos de Argentina en nuestra base de datos ", len(lista_areas))

#%% 
serie_filas = []
index = 0 
k=0
for j in indices_inicio_mas_1:
    i = j + tamanio_anio[k]
    if i <= len(poblacion_filtrado):  
        casos = poblacion_filtrado.loc[j:i, 'Unnamed: 2'] 
        
        serie_filas.append( [str(lista_areas[index])] + casos.tolist() )  
        index += 1
        k+=1
    else:
        break

#%% 
for row in serie_filas:
    print(row)
    if  len(row) - 1 < maximo_edad:
        diferencia = maximo_edad - len(row) +1
        row = row.extend([0]*diferencia)
#%% 
# aca empezamos a armar el dataframe con todas las filtraciones que tuvimos anteriormente
claves = ["in_departamentos"] 
for edad in range(0, 111) : 
    claves.append(str(edad))
    
cant_personas_por_id_departamento = pd.DataFrame(serie_filas, columns= claves)
print(cant_personas_por_id_departamento)
#%%
# veamos que tipos de datos nos quedo en el dataframe
print(cant_personas_por_id_departamento.dtypes)

#%%
# para poder trabajar con el id_departamento pasamos a float esas columnas
codigo_provincia['in_departamentos'] = codigo_provincia['in_departamentos'].astype(float)
cant_personas_por_id_departamento['in_departamentos']=cant_personas_por_id_departamento['in_departamentos'].astype(float)
#%%
# realizamos el merge
CENSO = pd.merge(cant_personas_por_id_departamento, codigo_provincia, on='in_departamentos', how='left')
#%%
# notamos un nan para esa columna que nos quedo asi que para los lugares asi le colocamos 0
CENSO['110'] = CENSO['110'].fillna(0)
#%%
CENSO=CENSO[['provincia_id','departamento', 'provincia']+ claves]
#%%
"""
Ahora lo estructuramos por nivel educativo de acuerdo a su edad
rangos etarios educativos que consideramos para el informe: 
    nivel inicial: 0 a 2 años
    jardin de infantes : 3 a 5 
    primario : 6 a 12 
    secundario : 12 a 18
    adulto : 18 hasta 65 
"""
nombres_columnas=['id_departamento','provincia','cant de personas en nivel_inicial',
                  'cant de personas en jardin_de_infantes','cant de personas en primario',
                  'cant de personas en secundario','cant de personas en adulto','total poblacion']

CENSO_POR_NIVEL_EDUC = pd.DataFrame( columns=nombres_columnas)
CENSO_POR_NIVEL_EDUC['provincia']=CENSO['provincia']
CENSO_POR_NIVEL_EDUC['id_departamento']=CENSO['in_departamentos']
CENSO_POR_NIVEL_EDUC['departamento']=CENSO['departamento']

columnas_a_sumar=['0','1','2']
CENSO_POR_NIVEL_EDUC['cant de personas en nivel_inicial'] = CENSO[columnas_a_sumar].sum(axis=1)

columnas_a_sumar=['3','4','5']
CENSO_POR_NIVEL_EDUC['cant de personas en jardin_de_infantes'] = CENSO[columnas_a_sumar].sum(axis=1)

columnas_a_sumar=['6','7','8','9','10','11','12']
CENSO_POR_NIVEL_EDUC['cant de personas en primario'] = CENSO[columnas_a_sumar].sum(axis=1)

columnas_a_sumar=['13','14','15','16','17','18']
CENSO_POR_NIVEL_EDUC['cant de personas en secundario'] = CENSO[columnas_a_sumar].sum(axis=1)

# la maxima edad encontrada es 100 
claves=[]
for edad in range(19, 111) : 
    claves.append(str(edad))
CENSO_POR_NIVEL_EDUC['cant de personas en adulto'] = CENSO[claves].sum(axis=1)

claves_totales = [str(edad) for edad in range(111)]
CENSO_POR_NIVEL_EDUC['total poblacion'] = CENSO[claves_totales].sum(axis=1)
#%%
CENSO_POR_NIVEL_EDUC=CENSO_POR_NIVEL_EDUC[['id_departamento','departamento', 'provincia', 'cant de personas en nivel_inicial',
       'cant de personas en jardin_de_infantes',
       'cant de personas en primario', 'cant de personas en secundario',
       'cant de personas en adulto', 'total poblacion']]

#%%
CENSO_POR_NIVEL_EDUC.to_csv( ruta +"/TablasModelo/CENSO_POR_NIVEL_EDUC.csv", index=False)
print("---------fue exitosa la creacion del csv -----------------")

#%%
""" ------------------------------------  Empezamos a hacer las tablas de SQL ------------------------------------"""

#%%
"""
CONSULTA N°1

Para cada departamento informar la provincia, el nombre del departamento,
la cantidad de Establecimientos Educativos (EE) de cada nivel educativo,
considerando solamente la modalidad común, y la cantidad de habitantes con
edad correspondiente al nivel educativos listado. El orden del reporte debe
ser alfabético por provincia y dentro de las provincias descendente por
cantidad de escuelas primarias
"""
#%%

consultaSQL = """SELECT DISTINCT c.in_departamentos AS id_departamentos, c.provincia, cne.departamento, cne."cant de personas en nivel_inicial" AS nivel_inicial, cne."cant de personas en jardin_de_infantes" AS jardin_de_infantes, cne."cant de personas en primario" AS primario, cne."cant de personas en secundario" AS secundario, cne."total poblacion" 
FROM CENSO_POR_NIVEL_EDUC AS cne
INNER JOIN CENSO AS c
ON c.departamento=cne.departamento"""

ALUMNOS_POR_DEPARTAMENTO = dd.sql(consultaSQL).df()

cantidadDeColegiosPorNivel = """
SELECT
    in_departamentos,
    departamento, 
    (sum(CAST("Nivel inicial - Jardín maternal" AS INT))) + (sum(CAST("Nivel inicial - Jardín de infantes" AS INT))) AS jardín,
    sum(CAST(Primario AS INT)) AS primario, sum(CAST(Secundario AS INT)) AS secundario
FROM ESTABLECIMIENTOS_EDUCATIVOS
GROUP BY in_departamentos, departamento
"""

dfCANT_COLEGIOS_POR_NIVEL = dd.sql(cantidadDeColegiosPorNivel).df()

consultaSQL = """
SELECT DISTINCT
a.provincia AS Provincia,
c.departamento AS Departamento,
c.jardín AS Jardines,
((a.nivel_inicial) + (a.jardin_de_infantes)) AS "Población Jardin",
c.Primario AS Primarias,
a.primario AS "Población Primaria",
c.Secundario AS Secundarios,
a.secundario AS "Población Secundaria"
FROM dfCANT_COLEGIOS_POR_NIVEL AS c
INNER JOIN ALUMNOS_POR_DEPARTAMENTO AS a
ON c.in_departamentos=a.id_departamentos AND c.departamento=a.departamento
ORDER BY a.provincia ASC, c.primario DESC
"""

DFconsulta1 = dd.sql(consultaSQL).df()
#%%
DFconsulta1.to_csv( ruta +"/MaterialComplementario/SQLconsulta1.csv", index=False)
#%%
"""
CONSULTA N°2

Para cada departamento informar la provincia, el nombre del departamento y
la cantidad de empleados totales en ese departamento, para el año 2022. El
orden del reporte debe ser alfabético por provincia y, dentro de las provincias,
descendente por cantidad de empleados
"""

#%%

consultaEMPLEADOS2022 = """
SELECT 
    provincia,
    departamento,
    sum(CAST(Empleo AS INT)) AS "Cantidad total de empleados en 2022"
FROM EP_filtrado
GROUP BY provincia, departamento
ORDER BY provincia ASC, "Cantidad total de empleados en 2022" DESC
"""

DFconsulta2 = dd.sql(consultaEMPLEADOS2022).df()
#%%
DFconsulta1.to_csv( ruta +"/MaterialComplementario/SQLconsulta2.csv", index=False)
#%%
"""
CONSULTA N°3

Para cada departamento, indicar provincia, nombre del departamento,
cantidad de empresas exportadoras que emplean mujeres (en 2022),
cantidad de EE (de modalidad común) y población total. Ordenar por
cantidad de EE descendente, cantidad de empresas exportadoras
descendente, nombre de provincia ascendente y nombre de departamento
ascendente. No omitir departamentos sin EE o exportadoras con empleo
femenino.
"""
#%%
consultaSQL = """
SELECT DISTINCT
provincia,
departamento,
genero,
sum(CAST(empresas_exportadoras AS INT)) AS empresas_exportadoras
FROM EP_filtrado
GROUP BY provincia, departamento, genero
"""

cant_emp_exp_por_genero = dd.sql(consultaSQL).df()

consultaSQL = """
SELECT DISTINCT
provincia,
departamento,
empresas_exportadoras AS "Cant Expo Mujeres"
FROM cant_emp_exp_por_genero
WHERE genero='Mujeres'
"""

cant_expo_mujeres = dd.sql(consultaSQL).df()

consultaSQL = """
SELECT DISTINCT 
p.provincia,
p.departamento,
m."Cant Expo Mujeres"
FROM codigo_provincia AS p
LEFT OUTER JOIN cant_expo_mujeres AS m
ON LOWER(p.departamento)=LOWER(m.departamento) AND LOWER(p.provincia)=LOWER(m.provincia)
"""

cant_expo_mujeres_FINAL = dd.sql(consultaSQL).df()

cant_expo_mujeres_FINAL.fillna(0, inplace=True)

consultaSQL = """
SELECT DISTINCT
m.provincia,
ee.departamento,
COUNT(*) AS "Cant_EE"
FROM ESTABLECIMIENTOS_EDUCATIVOS AS ee
INNER JOIN cant_expo_mujeres AS m
ON LOWER(ee.departamento)=LOWER(m.departamento)
GROUP BY m.provincia, ee.departamento
"""

cant_EE_por_depto = dd.sql(consultaSQL).df()

consultaSQL = """
SELECT DISTINCT 
p.provincia,
p.departamento,
ee.Cant_EE
FROM codigo_provincia AS p
LEFT OUTER JOIN cant_EE_por_depto AS ee 
ON LOWER(p.departamento)=LOWER(ee.departamento) AND LOWER(p.provincia)=LOWER(ee.provincia)
"""

cant_EE_TODOS = dd.sql(consultaSQL).df()

cant_EE_TODOS.fillna(0, inplace=True) #reemplazo valores nulos por 0

consultaSQL = """
SELECT DISTINCT 
provincia,
departamento,
"total poblacion" AS "Población"
FROM ALUMNOS_POR_DEPARTAMENTO
"""

poblacion_por_departamento = dd.sql(consultaSQL).df()

consultaSQL = """
SELECT DISTINCT
m.provincia AS Provincia,
m.departamento AS Departamento,
m."Cant Expo Mujeres" AS Cant_Expo_Mujeres,
ee.Cant_EE,
p.Población
FROM cant_expo_mujeres_FINAL AS m, cant_EE_TODOS AS ee, poblacion_por_departamento AS p
WHERE LOWER(m.provincia)=LOWER(ee.provincia) AND LOWER(m.provincia)=LOWER(p.provincia) AND LOWER(ee.provincia)=LOWER(p.provincia) AND LOWER(ee.departamento)=LOWER(p.departamento) AND LOWER(m.departamento)=LOWER(ee.departamento) AND LOWER(m.departamento)=LOWER(p.departamento)
ORDER BY Cant_EE DESC, Cant_Expo_Mujeres DESC, provincia ASC, departamento ASC
"""

DFconsulta3 = dd.sql(consultaSQL).df()
#%%

DFconsulta3.to_csv( ruta +"/MaterialComplementario/SQLconsulta3.csv", index=False)

#%%

"""
CONSULTA N°4

Según los datos de 2022, para cada departamento que tenga una cantidad
de empleados mayor que el promedio de los puestos de trabajo de los
departamentos de la misma provincia, indicar: provincia, nombre del
departamento, los primeros tres dígitos del CLAE6 que más empleos genera,
(si no tiene 6 dígitos, agregar un 0 a la izquierda) y la cantidad de empleos en
ese rubro.
"""

consultaSQL = """
SELECT DISTINCT
provincia,
departamento,
sum(CAST(Empleo AS INT)) AS cant_Empleos
FROM EP_filtrado
GROUP BY provincia, departamento
"""

cant_empleos_por_departamento = dd.sql(consultaSQL).df()

consultaSQL = """
SELECT
provincia,
AVG(Empleo) AS promedio_Empleo
FROM EP_filtrado
GROUP BY provincia, departamento
"""

promedio_empleo_por_provincia = dd.sql(consultaSQL).df()

consultaSQL = """
SELECT DISTINCT
ced.provincia,
ced.departamento,
ced.cant_Empleos
FROM cant_empleos_por_departamento AS ced
INNER JOIN promedio_empleo_por_provincia AS pep
ON ced.provincia=pep.provincia
WHERE ced.cant_Empleos > pep.promedio_Empleo
"""

empleos_mayor_al_promedio = dd.sql(consultaSQL).df()

consultaSQL = """
SELECT DISTINCT 
provincia,
departamento,
clae6,
MAX(Empleo) AS Empleo
FROM EP_filtrado
GROUP BY provincia, departamento, clae6
"""

max_clae6_empleos = dd.sql(consultaSQL).df()

consultaSQL = """
SELECT DISTINCT 
provincia,
departamento,
MAX(Empleo) AS Empleo
FROM EP_filtrado
GROUP BY provincia, departamento
"""

max_empleos = dd.sql(consultaSQL).df()

consultaSQL = """
SELECT DISTINCT 
c6.provincia,
e.departamento,
c6.clae6,
e.Empleo
FROM max_clae6_empleos AS c6
INNER JOIN max_empleos AS e
ON c6.provincia=e.provincia AND c6.departamento=e.departamento AND c6.Empleo=e.Empleo
"""

dfPRUEBA4 = dd.sql(consultaSQL).df()

consultaSQL = """
SELECT DISTINCT
d4.provincia,
d4.departamento,
d4.clae6,
d4.Empleo
FROM dfPRUEBA4 AS d4
INNER JOIN empleos_mayor_al_promedio AS e
ON d4.departamento=e.departamento AND d4.provincia=e.provincia
"""

dfUNIDO = dd.sql(consultaSQL).df()

consultaSQL = """
SELECT DISTINCT 
provincia,
departamento,
CASE WHEN LENGTH(CAST(clae6 AS VARCHAR)) = 6 THEN CAST(CAST(clae6/1000 AS INT)AS VARCHAR) ELSE CONCAT('0', CAST(CAST(clae6/1000 AS INT) AS VARCHAR)) END AS clae3,
Empleo AS "Cant. empleos"
FROM dfUNIDO
"""

dfFINALCONSULTA = dd.sql(consultaSQL).df()
#%%
dfFINALCONSULTA.to_csv( ruta +"/MaterialComplementario/SQLconsulta4.csv", index=False)
#%%
""" ------------------------------------  Inicio graficos ------------------------------------"""

#%%
"""
i) Cantidad de empleados por provincia, para 2022. Mostrarlos ordenados de
manera decreciente por dicha cantidad.

"""
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

Sabiendo que : 
    rangos etarios educativos: 
        nivel inicial: 0 a 2 años
        jardin de infantes : 3 a 5 
        primario : 6 a 12 
        secundario : 12 a 18
        adulto : 18 hasta 110 
"""
plt.figure(figsize=(12,8))
plt.title('Establecimientos educativos vs Poblacion por nivel educativo')

sns.scatterplot(data=DFconsulta1, x='Jardines', y='Población Jardin', color='blue', label='Jardines', s=80, alpha=0.7)
sns.scatterplot(data=DFconsulta1, x='Primarias', y='Población Primaria', color='yellow', label='Primario', s=80, alpha=0.7)
sns.scatterplot(data=DFconsulta1, x='Secundarios', y='Población Secundaria', color='green', label='Secundario', s=80, alpha=0.7)

plt.xlabel('Cantidad de Establecimientos Educativos')
plt.ylabel('Población de ese nivel educativo')
plt.legend()
plt.grid(True, alpha=0.3)
plt.show()

#%%
"""
iii) Realizar un boxplot por cada provincia, de la cantidad de EE por cada
departamento de la provincia. Mostrar todos los boxplots en una misma
figura, ordenados por la mediana de cada provincia.

"""
#%% 
provincias = EE_comun['Jurisdicción'].unique()

dict_provincias = {}

for provincia in provincias:
    df_provincia = EE_comun[EE_comun['Jurisdicción'] == provincia]
    
    conteo_localidades = df_provincia['Departamento'].value_counts().to_dict()
    
    dict_provincias[provincia] = conteo_localidades

# Extraemos los valores y calcular medianas
province_data = []
medians = []

for province, departments in dict_provincias.items():
    values = list(departments.values())
    province_data.append(values)
    medians.append(np.median(values))

# Ordenamos provincias por mediana
sorted_indices = np.argsort(medians)
sorted_provinces = [list(dict_provincias.keys())[i] for i in sorted_indices]
sorted_data = [province_data[i] for i in sorted_indices]

#%%
# generamos el grafico
plt.figure(figsize=(15, 10))
boxplot = plt.boxplot(sorted_data, labels=sorted_provinces, patch_artist=True)

# Personalizar el gráfico
colors = plt.cm.Set3(np.linspace(0, 1, len(sorted_provinces)))
for patch, color in zip(boxplot['boxes'], colors):
    patch.set_facecolor(color)

plt.title('Cantidad de Establecimientos Educativos por Departamento\nOrdenados por Mediana Provincial', 
          fontsize=14, fontweight='bold')
plt.xlabel('Provincia', fontsize=12)
plt.ylabel('Cantidad de EE', fontsize=12)
plt.xticks(rotation=90)
plt.grid(True, alpha=0.3)

plt.show()
#%%
"""
iv) Relación entre la cantidad de empleados cada mil habitantes (para 2022) y
de EE cada mil habitantes por departamento.

""" 
# paso a mayusculas todo para que no haya incompatibilidad en el futuro si quiero hacer algun merge
ESTABLECIMIENTOS_PRODUCTIVOS['provincia']= ESTABLECIMIENTOS_PRODUCTIVOS['provincia'].str.strip().str.upper()
ESTABLECIMIENTOS_PRODUCTIVOS['departamento']= ESTABLECIMIENTOS_PRODUCTIVOS['departamento'].str.strip().str.upper()
provincias = EE_comun['Jurisdicción'].unique()

dict_provincias_ep = {}
# aca tengo que tener cuidado que hay departamentos que aparecen en varias provincias como 25 de MAYO
# asi que agrupo los provincia y ahi por localidad asi las cuento por separado 
for provincia in provincias:
    df_provincia = ESTABLECIMIENTOS_PRODUCTIVOS[ESTABLECIMIENTOS_PRODUCTIVOS['provincia'] == provincia]
    
    conteo_localidades = df_provincia['departamento'].value_counts().to_dict()
    
    dict_provincias_ep[provincia] = conteo_localidades

#%%
CENSO_POR_NIVEL_EDUC_aux = CENSO_POR_NIVEL_EDUC[['departamento','provincia','total poblacion']]
provincias = CENSO_POR_NIVEL_EDUC_aux['provincia'].unique()

dict_provincias_censo = {}
# aca tengo que tener cuidado que hay departamentos que aparecen en varias provincias como 25 de MAYO
# asi que agrupo los provincia y ahi por localidad asi las cuento por separado 
for provincia in provincias:
    df_provincia = CENSO_POR_NIVEL_EDUC_aux[CENSO_POR_NIVEL_EDUC_aux['provincia'] == provincia]
    
    conteo = df_provincia.groupby('departamento')['total poblacion'].sum().to_dict()

    dict_provincias_censo[provincia] = conteo


#%%
claves_comunes = set(dict_provincias_ep.keys()) & set(dict_provincias.keys()) & set(dict_provincias_censo.keys())

diccionario_combinado = {}
# junto por provincia los estableciminetos educativos, los productivos y la poblacion total
for clave in claves_comunes:
    diccionario_combinado[clave] = {
        'empleo': dict_provincias_ep[clave],
        'poblacion': dict_provincias_censo[clave],
        'ee': dict_provincias.get(clave)
    }
#%%
#realizo el calculo para cada departamento cada 1000 habitantes
diccionario_combinado = {}

for provincia in claves_comunes:
    empleo_prov = dict_provincias_ep.get(provincia, {})
    poblacion_prov = dict_provincias_censo.get(provincia, {})
    ee_prov = dict_provincias.get(provincia, {})

    diccionario_combinado[provincia] = {}

    for depto in poblacion_prov.keys():
        empleo = empleo_prov.get(depto, 0)
        poblacion = poblacion_prov.get(depto, 0)
        ee = ee_prov.get(depto, 0)

        if poblacion > 0:
            empleo_por_mil = (1000 * empleo) / poblacion
            ee_por_mil = (1000 * ee) / poblacion
        else:
            empleo_por_mil = ee_por_mil = 0

        diccionario_combinado[provincia][depto] = {
            'empleo': empleo,
            'poblacion': poblacion,
            'ee': ee,
            'empleo_por_mil': empleo_por_mil,
            'ee_por_mil': ee_por_mil
        }
#%%
empleo_por_mil = []
ee_por_mil = []

for provincia in diccionario_combinado.values():
    for depto_data in provincia.values():
        empleo_por_mil.append(depto_data['empleo_por_mil'])
        ee_por_mil.append(depto_data['ee_por_mil'])

#%%#  
#generamos el grafico comparativo 
plt.figure(figsize=(10,10))
# queriamos generar un color diferente para cada localidad asi se puede analizar mejor el graico
colores = np.random.rand(len(empleo_por_mil))

plt.scatter(
    empleo_por_mil,
    ee_por_mil,
    c=colores,
    cmap='rainbow',    
    alpha=0.7
)

plt.title('Relación entre empleo y cantidad de EE por cada mil habitantes',fontsize=14)
plt.ylabel('EE por cada mil habitantes',fontsize=14)
plt.xlabel('Empleados por cada mil habitantes',fontsize=14)
plt.grid(True, linestyle='--', alpha=0.5)
plt.show()

#%%
"""
v) Las 5 actividades (CLAE6) con mayor y menor proporción (respectivamente)
de empleadas mujeres, para 2022. Incluir en el gráfico la proporción
promedio de empleo femenino.
"""

clae6=ESTABLECIMIENTOS_PRODUCTIVOS['clae6'].unique().tolist()

dicc_clae6 = {}
for c in clae6:
      dicc_clae6[c] = {'Mujeres': 0, 'Varones': 0}

for _, fila in ESTABLECIMIENTOS_PRODUCTIVOS.iterrows():
    clae = fila['clae6']
    genero = fila['genero']
    empleo = fila['Empleo']
    
    if genero == 'Mujeres':
        dicc_clae6[clae]['Mujeres'] += empleo
    elif genero == 'Varones':
        dicc_clae6[clae]['Varones'] += empleo

for clae in dicc_clae6:
    dicc_clae6[clae]=(dicc_clae6[clae]['Mujeres']/(dicc_clae6[clae]['Mujeres']+dicc_clae6[clae]['Varones']))*100
        
valores_lista=[]
valores = dicc_clae6.items()
for i in valores:
    valores_lista.append(i[1])
    valores_lista.sort()

promedio = sum(valores_lista) / len(valores_lista)

def buscar_valor(diccionario, valor_buscado):
  for clave, valor in diccionario.items():
    if valor == valor_buscado:
      return clave
  
claves_con_cero_mujeres = [clave for clave, valor in dicc_clae6.items() if valor == 0.0]
claves_con_cero_mujeres=claves_con_cero_mujeres[-5:]

claves_con_mas_mujeres=[]
for i in valores_lista[-5:]:
    claves_con_mas_mujeres.append(buscar_valor(dicc_clae6, i))

porcentajes=[]
categorias=claves_con_cero_mujeres+claves_con_mas_mujeres
for i in categorias:
    porcentajes.append(dicc_clae6[i])

#%%
# Creamos el gráfico de barras
plt.figure(figsize=(14, 8))

colores = ['lightcoral'] * 5 + ['lightgreen'] * 5

barras = plt.bar([str(c) for c in categorias], porcentajes, 
                 color=colores, alpha=0.7, edgecolor='black', linewidth=1.2)

plt.axhline(y=promedio, color='red', linestyle='--', linewidth=3, 
            label=f'Promedio general: {promedio:.2f}%')

plt.title('Las 5 Actividades (CLAE6) con Mayor y Menor Proporción de Empleadas Mujeres (2022)', 
          fontsize=16, fontweight='bold', pad=20)
plt.ylabel('Porcentaje de Mujeres (%)', fontsize=14)
plt.xlabel('Código CLAE6', fontsize=14)

for barra, porcentaje in zip(barras, porcentajes):
    plt.text(barra.get_x() + barra.get_width()/2, barra.get_height() + 0.5, 
             f'{porcentaje:.1f}%', ha='center', va='bottom', fontsize=11, fontweight='bold')

plt.xticks(fontsize=12, rotation=0)
plt.grid(axis='y', alpha=0.3, linestyle='--')

plt.text(2, max(porcentajes) * 0.8, 'MENOR PROPORCIÓN', ha='center', 
         fontsize=12, fontweight='bold',
         bbox=dict(boxstyle="round,pad=0.5", facecolor="lightcoral", alpha=0.8))
plt.text(7, max(porcentajes) * 0.8, 'MAYOR PROPORCIÓN', ha='center', 
         fontsize=12, fontweight='bold',
         bbox=dict(boxstyle="round,pad=0.5", facecolor="lightgreen", alpha=0.8))

plt.legend(loc='upper right', fontsize=12)

plt.ylim(0, max(porcentajes) * 1.1)

plt.tight_layout()

plt.show()
#FIN