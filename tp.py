"""
Autores: 
    
    Levigurevitz, Nicolas 1245/23 elevege02@gmail.com
    Parajo Lopez, Juan Manuel 1367/24 juancholinus@gmail.com
    Guanca, Carla Anabella 1928/21 carguanca@gmail.com

"""
# Importamos bibliotecas que vamos a utiliza 
import pandas as pd
import duckdb as dd

# Ubicacion de los directorios
carpeta = "/home/charly/Escritorio/ldd/tp/tp-laboratorio-de-datos/TablasOriginales/"
ruta_destino="/home/charly/Escritorio/ldd/tp/tp-laboratorio-de-datos/TablasModelo/"

#%% 
# el dataset de Establecimientos Educativos no contiene informacion sobre las columnas 
# Brinda información sobre los establecimientos educativos, su ubicación y ofertas educativas.
ee  = pd.read_csv(carpeta+'/2022_padron_oficial_establecimientos_educativos.csv')
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
#veamos que tipo de datos hay en cada columnas
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
        Código de localidad: (columna 9) si le sacamos los ultimos 3 digitos podriamos joinear con CODIGO AREA de poblacion
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
            
        cosas que encontramos, hay numeros en la columna de Telefono que no tiene el formato xxxx-xxxx sino que ees xxxx-xxxx/xxxx-xxxx
        en los mails hay tambien lo mismo, dos mails en uno. 
        en nombre de la institucion hay abreviacion que podrian llegar a un conflicto debido a que en vex por ejemplo de usar general usan gral
        
"""
#%%
#veamos si hay duplicados en las columnas
duplicados = ee_comun[ee_comun.duplicated(subset=['Jurisdicción', 'Cueanexo', 'Nombre', 'Sector', 'Ámbito', 'Domicilio',
       'C. P.', 'Código de área', 'Teléfono', 'Código de localidad',
       'Localidad', 'Departamento', 'Mail',
       'Nivel inicial - Jardín maternal', 'Nivel inicial - Jardín de infantes',
       'Primario', 'Secundario', 'Secundario - INET', 'SNU', 'SNU - INET'], keep=False)]

print(len(duplicados))
#%%
EE_comun=ee_comun[['Cueanexo','Jurisdicción','Departamento', 'C. P.','Código de localidad','Nivel inicial - Jardín maternal', 
             'Nivel inicial - Jardín de infantes', 'Primario', 'Secundario', 
             'Secundario - INET', 'SNU', 'SNU - INET']]
#%%

EE_comun['Jurisdicción'] = EE_comun['Jurisdicción'].replace('Ciudad de Buenos Aires', 'CABA')
#%%
# hay un dataset que usamos que venia en la misma pagina de otro dataset como complemento que nos ayudo a buscar coincidencias 
codigo_provincia = pd.read_csv(carpeta+"codigo_departamento_provincia.csv")
'''
contiene: 
    
    'provincia_id': int
    'in_departamentos': int
    'departamento': string
    'provincia': string
'''
print("--------- fue exitosa la carga -----------------")
#%%
print("Valores únicos en provincia:", codigo_provincia['provincia'].unique())
#%%
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

#LA ANTARTIDA ES LA UNICA QUE NO TIENE ID DE DEPARTAMENTO

#%%
#veamos si hay nulls en las columnas de ambos dataframes
EE_comun.isnull().sum()
codigo_provincia.isnull().sum()

#%%

def formatear_codigo(codigo):
    codigo_str = str(codigo)
    if codigo_str[0] == '0':
        codigo_str.pop(0)
        return codigo_str
    return codigo_str

codigo_provincia['in_departamentos'] = codigo_provincia['in_departamentos'].astype(str)
codigo_provincia['in_departamentos'] = codigo_provincia['in_departamentos'].apply(formatear_codigo)

#%%
# hago un mege con las columnas que son coincidentes las provincias y los numeros de departamento
resultadoEE_comun = EE_comun.merge(
    codigo_provincia,
    left_on=['Jurisdicción', 'Departamento'],
    right_on=['provincia', 'departamento'],
    how='left'  
)
#%%
print(resultadoEE_comun)
#%%
#vamos a ver donde no coinciden para filtrar mejor 
# ROSARIO VERA PEÑALOZA O GENERAL ANGEL V PEÑALOZA LOCALIDAD
filas_nan = resultadoEE_comun[resultadoEE_comun['departamento'].isnull()]
print(len(filas_nan))

#%%

ESTABLECIMIENTOS_EDUCATIVOS=resultadoEE_comun[['Cueanexo', 'C. P.','in_departamentos', 'departamento',
                                      'Nivel inicial - Jardín maternal', 'Nivel inicial - Jardín de infantes', 
                                      'Primario', 'Secundario','Secundario - INET', 'SNU', 'SNU - INET']]

#%%
ESTABLECIMIENTOS_EDUCATIVOS.to_csv( ruta_destino +"/ESTABLECIMIENTOS_EDUCATIVOS", index=False)
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
EP_filtrado = EP[['in_departamentos','departamento','provincia','clae6','letra','genero','Empleo','Establecimientos','empresas_exportadoras']]

print(EP_filtrado)
#%%
CONCURRE = EP[['in_departamentos','anio','clae6','letra','genero']]
CONCURRE.rename(columns={'in_departamentos': 'id_depto'})

#%%
ESTABLECIMIENTOS_PRODUCTIVOS=EP[['anio','in_departamentos','departamento','provincia','clae6','letra','genero','Establecimientos','Empleo','empresas_exportadoras']]
ESTABLECIMIENTOS_PRODUCTIVOS.rename(columns={'in_departamentos': 'id_depto'})

#%%
EE_comun.to_csv( ruta_destino +"/EP_filtrado", index=False)
print("---------fue exitosa la creacion del csv -----------------")
#%%
actividades = pd.read_csv(carpeta+"actividades_establecimientos.csv")
print(actividades.columns )
print("---------fue exitosa la carga -----------------")
#%%

ACTIVIDADES = actividades[['clae6','letra', 'clae6_desc','letra_desc']]
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
LOCALIDAD = pd.read_excel(carpeta+"padron_poblacion.xlsX")
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


rangos etarios educativos: 
    nivel inicial: 0 a 2 años
    jardin de infantes : 3 a 5 
    primario : 6 a 12 
    secundario : 12 a 18
    adulto : 18 hasta 65 
"""

#%% 
poblacion_filtrado = LOCALIDAD.loc[12:56594, ['Unnamed: 1','Unnamed: 2','Unnamed: 3']]
print(poblacion_filtrado)
#%% 
palabra_clave = 'Total'

filas_con_palabra = poblacion_filtrado.loc[poblacion_filtrado['Unnamed: 1'].str.contains(palabra_clave, na=False)]
print(filas_con_palabra)
#%% 
filas_inicio = poblacion_filtrado.loc[poblacion_filtrado['Unnamed: 1'] == 'Total']
indices_inicio = list(filas_inicio.index)
indices_inicio_menos_1 = []

for i in indices_inicio:
    indice_incrementado = i - 1 
    indices_inicio_menos_1.append(indice_incrementado)
print(len(indices_inicio_menos_1))
#%% 
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
maximo_edad= max(tamanio_anio)
#%% 
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
print(serie_filas)
#%% 

for row in serie_filas:
    print(row)
    if  len(row) - 1 < maximo_edad:
        diferencia = maximo_edad - len(row) +1
        row = row.extend([0]*diferencia)

#print(serie_filas)
#%% 
print(len(serie_filas[0]))
print(len(serie_filas[1]))

#%% 
claves = ["in_departamentos"] 
for edad in range(0, 111) : 
    claves.append(str(edad))
    
cant_personas_por_id_departamento = pd.DataFrame(serie_filas, columns= claves)
print(cant_personas_por_id_departamento)

cant_personas_por_id_departamento.dtypes

#%%
print(cant_personas_por_id_departamento.dtypes)
print(codigo_provincia.dtypes)

#%%
cant_personas_por_id_departamento['in_departamentos']=cant_personas_por_id_departamento['in_departamentos'].astype(float)
#%%
CENSO = pd.merge(cant_personas_por_id_departamento,codigo_provincia,on='in_departamentos', how='left')
#%%
CENSO['110'] = CENSO['110'].fillna(0)
#%%
CENSO=CENSO[['provincia_id','departamento', 'provincia']+ claves]
#%%-------------------------- empezamos a hacer las tablas de sql ---------------------------------

# a corregir: unir la tabla de poblacion con ee mediante el cueanexo y a eso unirlo a apdu para poder tener los nombres de los departamentos.
consultaAPDDEPTO = """
SELECT DISTINCT apd.id_departamentos, ep.departamento, apd.nivel_inicial, apd.jardin_de_infantes, apd.primario, apd.secundario, apd.Total_Poblacion
FROM ALUMNNOS_POR_DEPARTAMENTO AS apd
INNER JOIN ESTABLECIMIENTOS_PRODUCTIVOS AS ep
    ON apd.id_departamentos=ep.id_departamentos
"""

ALUMNOS_POR_DEPARTAMENTO_ULTIMO = dd.sql(consultaAPDDEPTO).df()

cantidadDeColegiosPorNivel = """
SELECT
    provincia,
    departamento, 
    (sum(CAST("Nivel inicial - Jardín maternal" AS INT))) + (sum(CAST("Nivel inicial - Jardín de infantes" AS INT))) AS Jardín,
    sum(CAST(Primario AS INT)) AS Primario, sum(CAST(Secundario AS INT)) AS Secundario
FROM ESTABLECIMIENTOS_EDUCATIVOS    
GROUP BY provincia, departamento
"""

dfCANT_COLEGIOS_POR_NIVEL = dd.sql(cantidadDeColegiosPorNivel).df()

consultaFINAL = """
SELECT DISTINCT
    ccpn.provincia,
    ccpn.Departamento,
    ccpn.Jardín AS Jardines,
    ((apdu.nivel_inicial) + (apdu.jardin_de_infantes)) AS "Poblacion Jardín",
    ccpn.Primario,
    apdu.primario AS "Población Primario",
    ccpn.Secundario,
    apdu.secundario AS "Población Secundario"
FROM dfCANT_COLEGIOS_POR_NIVEL AS ccpn, ALUMNOS_POR_DEPARTAMENTO_ULTIMO AS apdu,
WHERE ccpn.Departamento=apdu.departamento
ORDER BY ccpn.provincia ASC, ccpn.Primario DESC
"""

dfFINAL = dd.sql(consultaFINAL).df()

#%%2

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


#%%4
"""

consultaITEM4 = """SELECT provincia, departamento, clae6, CASE WHEN clae6.size()"""
                     

"""















