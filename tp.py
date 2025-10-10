"""
Autores: un judio, una peruana y un enano rubio :)

"""
# Importamos bibliotecas
import pandas as pd
import duckdb as dd
carpeta = "/home/charly/Escritorio/ldd/tp/TablasOriginales/"
#%% #===========================================================================
# Importamos los datasets que vamos a utilizar en este trabajo practico
#=============================================================================
# EE  = pd.read_excel(carpeta+"2022_padron_oficial_establecimientos_educativos.xlsx")
EE  = pd.read_csv('/home/charly/Escritorio/ldd/tp/TablasOriginales/2022_padron_oficial_establecimientos_educativos.csv')

print("--------- fue exitosa la carga -----------------")
#%%
# Establecimientos Educativos no dice nada sobre que contiene cada columna 
# Brinda información sobre los establecimientos educativos, su ubicación y ofertas educativas.
"""
Establecimiento-Localizacion: Nombre de departamento
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
mail
modalidad : [Comun, especial, adultos, artistica, hospitalaria, intercultural, encierro]
            |__ despues por cada modalidad el nivel.(inicial, Nivel inicial - Jardín maternal,
                                                     Nivel inicial - Jardín de infantes	Primario,
                                                     Secundario	Secundario - INET)
)
    
"""
EE.isnull().sum()
print(list(EE.columns))
#%%
EE_comun=EE[['Jurisdicción', 'Cueanexo','Localidad','Departamento','Nivel inicial - Jardín maternal', 'Nivel inicial - Jardín de infantes', 'Primario', 'Secundario', 'Secundario - INET', 'SNU', 'SNU - INET']]

print(EE_comun)
#%%
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
# para ver que no haya valores repetido 
def formatear_codigo(codigo):
    codigo_str = str(codigo)
    if len(codigo_str) == 4:
        return '0' + codigo_str
    return codigo_str

codigo_provincia['in_departamentos'] = codigo_provincia['in_departamentos'].apply(formatear_codigo)
print(codigo_provincia)
#%%
print("Valores únicos en provincia:", codigo_provincia['provincia'].unique())
#%%
# Lista de vocales y sus versiones sin acento
acentos = {'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u',
           'Á': 'A', 'É': 'E', 'Í': 'I', 'Ó': 'O', 'Ú': 'U'}

# Aplicar la sustitución en la columna 'Palabra'
EE_comun['Jurisdicción'] = EE_comun['Jurisdicción'].replace('Ciudad de Buenos Aires', 'CABA')
EE_comun['Departamento'] = EE_comun['Departamento'].replace('1§ DE MAYO', '1 DE MAYO')
	

for vocal_con_acento, vocal_sin_acento in acentos.items():
   EE_comun['Jurisdicción'] = EE_comun['Jurisdicción'].str.replace(vocal_con_acento, vocal_sin_acento, regex=False)
   EE_comun['Departamento']=EE_comun['Departamento'].str.replace(vocal_con_acento, vocal_sin_acento, regex=False)
   codigo_provincia['provincia']=codigo_provincia['provincia'].str.replace(vocal_con_acento, vocal_sin_acento, regex=False)
   codigo_provincia['departamento']=codigo_provincia['departamento'].str.replace(vocal_con_acento, vocal_sin_acento, regex=False)
codigo_provincia['departamento'] = codigo_provincia['departamento'].replace('DR. MANUEL BELGRANO', 'DOCTOR MANUEL BELGRANO')
codigo_provincia['departamento'] = codigo_provincia['departamento'].replace('GENERAL JUAN FACUNDO QUIROGA', 'GENERAL JUAN F QUIROGA')
codigo_provincia['departamento'] = codigo_provincia['departamento'].replace('GENERAL FELIPE VARELA', 'CORONEL FELIPE VARELA')
codigo_provincia['departamento'] = codigo_provincia['departamento'].replace('CORONEL DE MARINA LEONARDO ROSALES', 'CORONEL DE MARINA L ROSALES')
codigo_provincia['departamento'] = codigo_provincia['departamento'].replace("O'HIGGINS", 'O HIGGINS')
codigo_provincia['departamento'] = codigo_provincia['departamento'].replace('MAYOR LUIS J. FONTANA', 'MAYOR LUIS J FONTANA')
codigo_provincia['departamento'] = codigo_provincia['departamento'].replace('GENERAL GÜEMES', 'GENERAL GUEMES')
codigo_provincia['departamento'] = codigo_provincia['departamento'].replace('MAYOR LUIS J. FONTANA', 'MAYOR LUIS J FONTANA')
codigo_provincia['departamento'] = codigo_provincia['departamento'].replace('1° DE MAYO', '1 DE MAYO')


#%%
EE_comun['Jurisdicción'] = EE_comun['Jurisdicción'].str.strip().str.upper()
EE_comun['Departamento'] = EE_comun['Departamento'].str.strip().str.upper()
codigo_provincia['provincia'] = codigo_provincia['provincia'].str.strip().str.upper()
codigo_provincia['departamento'] = codigo_provincia['departamento'].str.strip().str.upper()


#%%
EE_comun.isnull().sum()
codigo_provincia.isnull().sum()
#%%
resultadoEE_comun = EE_comun.merge(
    codigo_provincia,
    left_on=['Jurisdicción', 'Departamento'],
    right_on=['provincia', 'departamento'],
    how='left'  
)
print(resultadoEE_comun.columns)
resultadoEE_comun.isnull().sum()
#%%
filas_nan = resultadoEE_comun[resultadoEE_comun['departamento'].isnull()]
print(filas_nan)
#%%
EE_comun=resultadoEE_comun[['provincia_id','in_departamentos', 'departamento',
                                   'provincia','Cueanexo','Nivel inicial - Jardín maternal',
                                   'Nivel inicial - Jardín de infantes', 'Primario', 'Secundario', 
                                   'Secundario - INET', 'SNU', 'SNU - INET']]

print(EE_comun)
#%%
ruta_destino="/home/charly/Escritorio/ldd/tp/TablasModelo/EE_comun"
EE_comun.to_csv(ruta_destino, index=False)
print("---------fue exitosa la creacion del csv -----------------")
#%%
'''
EEporComun=EE[['Nivel inicial - Jardín maternal', 'Nivel inicial - Jardín de infantes',
'Primario', 'Secundario', 'Secundario - INET', 'SNU', 'SNU - INET']]
EEporArtistica=EE[['Cueanexo','Secundario.1', 'SNU.1', 'Talleres']]
EEporEspecial=EE[['Cueanexo','Nivel inicial - Educación temprana',
'Nivel inicial - Jardín de infantes.1', 'Primario.1', 'Secundario.2',
'Integración a la modalidad común/ adultos']]
EEporAdulto=EE[['Cueanexo','Primario.2','Secundario.3', 'Alfabetización', 'Formación Profesional','Formación Profesional - INET']]
EEporHspitalaria=EE[['Cueanexo','Inicial', 'Primario.3', 'Secundario.4']]
#%%
#EEporMODALIDAD_renombrado.isnull().any()
#raro que me traiga cp vacios y no me salte como null
#%%
#ruta_destino = '/home/charly/Escritorio/ldd/tp/TablasModelo/EEporMODALIDAD_renombrado.csv'
#EEporMODALIDAD_renombrado.to_csv(ruta_destino, index=False)
#print("---------fue exitosa la creacion del csv -----------------")
'''
#%%
EP = pd.read_csv(carpeta+"Datos_por_departamento_actividad_y_sexo.csv")
EP_codigo = pd.read_csv(carpeta+"codigo_departamento_provincia.csv")
print("---------fue exitosa la carga -----------------")
EP.columns

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

'''hay que filtrar por año 2022'''
list(EP.columns)
#print(EP[['in_departamentos', 'provincia_id']])
EP_filtrado = EP[['anio','in_departamentos','departamento','provincia','clae6','letra','genero','Empleo','Establecimientos','empresas_exportadoras']]
EP_filtrado = EP_filtrado[EP_filtrado['anio'] == 2022]
print(EP_filtrado)
#%%
EP_filtrado.isnull().sum()
#%%
ruta_destino="/home/charly/Escritorio/ldd/tp/TablasModelo/EP"
EP_filtrado.to_csv(ruta_destino, index=False)
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
poblacion = pd.read_excel(carpeta+"padron_poblacion.xlsX")
print("---------fue exitosa la carga -----------------")
#%%
"""
Aquí se presentan las publicaciones de resultados del Censo Nacional de Población, Hogares y 
Viviendas 2022 y sus respectivos cuadros estadísticos. La información de cada jurisdicción
se presenta a nivel de departamento, partido o comuna.
Además, se incluye un cuadro según gobierno local para las 23 provincias y 
la Ciudad Autónoma de Buenos Aires y una serie de cuadros de la Región Metropolitana de 
Buenos Aires, desagregados por comuna y partido, entidad, localidad y zona rural.

codigo postal - localidad/ciudad
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
poblacion.columns

#%% 
poblacion_filtrado = poblacion.loc[12:56594, ['Unnamed: 1','Unnamed: 2','Unnamed: 3']]
print(poblacion_filtrado)
#%% 
filas_inicio = poblacion_filtrado.loc[poblacion_filtrado['Unnamed: 2'] == 'Casos']
indices_inicio = list(filas_inicio.index)
indices_inicio_mas_1 = []

for i in indices_inicio:
    indice_incrementado = i + 1 
    indices_inicio_mas_1.append(indice_incrementado)
print(indices_inicio_mas_1)
#%% 
palabra_clave = 'AREA'
filas_con_palabra = poblacion_filtrado.loc[poblacion_filtrado['Unnamed: 1'].str.contains(palabra_clave, na=False)]

lista_areas = filas_con_palabra['Unnamed: 1'].str[-5:].tolist()

print(len(lista_areas))

#%% 
#edades = list(range(0, 66))
serie_filas = []
#serie_filas.append(edades)
index = 0 
for j in indices_inicio_mas_1:
    i = j + 65  
    if i <= len(poblacion_filtrado):  
        casos = poblacion_filtrado.loc[j:i, 'Unnamed: 2'] 
        
        serie_filas.append([str(lista_areas[index])]+casos.tolist())  
        index += 1
    else:
        break

print(serie_filas)


#%% 
claves = ["in_departamentos"] 
for edad in range(0, 66) : 
    claves.append(str(edad))
cant_personas_por_id_departamento = pd.DataFrame(serie_filas, columns= claves)
print(cant_personas_por_id_departamento)
#print(claves)
cant_personas_por_id_departamento.dtypes
#%%
print(cant_personas_por_id_departamento)
#%%
cant_personas_por_id_departamento['in_departamentos'] = cant_personas_por_id_departamento['in_departamentos'].astype(str)
codigo_provincia['in_departamentos'] = codigo_provincia['in_departamentos'].astype(str)


"""
rangos etarios educativos: 
    nivel inicial: 0 a 2 años
    jardin de infantes : 3 a 5 
    primario : 6 a 12 
    secundario : 12 a 18
    adulto : 18 hasta 65 
"""
poblacion_por_nivel_educativo = pd.merge(cant_personas_por_id_departamento, codigo_provincia, on='in_departamentos', how='inner')
columnas_a_sumar=['0','1','2']
poblacion_por_nivel_educativo['nivel_inicial'] = poblacion_por_nivel_educativo[columnas_a_sumar].sum(axis=1)
columnas_a_sumar=['3','4','5']
poblacion_por_nivel_educativo['jardin_de_infantes'] = poblacion_por_nivel_educativo[columnas_a_sumar].sum(axis=1)
columnas_a_sumar=['6','7','8','9','10','11','12']
poblacion_por_nivel_educativo['primario'] = poblacion_por_nivel_educativo[columnas_a_sumar].sum(axis=1)
columnas_a_sumar=['13','14','15','16','17','18']
poblacion_por_nivel_educativo['secundario'] = poblacion_por_nivel_educativo[columnas_a_sumar].sum(axis=1)
#%%

claves=[]
for edad in range(19, 66) : 
    claves.append(str(edad))
poblacion_por_nivel_educativo['adulto'] = poblacion_por_nivel_educativo[claves].sum(axis=1)

print(poblacion_por_nivel_educativo.columns)
#%%
poblacion_por_nivel_educativo=poblacion_por_nivel_educativo[['in_departamentos','departamento','provincia','nivel_inicial','jardin_de_infantes',
                              'primario','secundario','adulto']]
print(poblacion_por_nivel_educativo)
#%%
ruta_destino="/home/charly/Escritorio/ldd/tp/TablasModelo/poblacion_por_nivel_educativo"
poblacion_por_nivel_educativo.to_csv(ruta_destino, index=False)
print("---------fue exitosa la creacion del csv -----------------")

#%% empezamos a hacer las tablas de sql

#%%1
EE_comun.replace(" ",0,inplace=True) #aqui, reemplazamos los contenidos vacíos por 0, ya que habia espacios en blanco, y no nos dejaba hacer la sumatoria

consultaJARDIN_MAT = """SELECT provincia,departamento, sum(CAST("Nivel Inicial - Jardín maternal" AS INT)) As "Jardín Maternal"
                        FROM EE_comun
                        GROUP BY provincia,departamento
                        ORDER BY provincia DESC
                        """
dfJM = dd.sql(consultaJARDIN_MAT).df()

consultaJARDIN_INFANTES = """SELECT provincia, departamento, sum(CAST("Nivel inicial - Jardín de infantes" AS INT)) As "Jardín de infantes" 
                             FROM EE_comun
                             GROUP BY provincia, departamento
                             ORDER BY provincia DESC"""
                             
dfJI = dd.sql(consultaJARDIN_INFANTES).df()

JardinesTotales = """SELECT dfJI.provincia As Provincia, dfJI.departamento As Depto, (dfJI."Jardín de infantes" + dfJM."Jardín Maternal") As Jardines, (poblacion_por_nivel_educativo.nivel_inicial + poblacion_por_nivel_educativo.jardin_de_infantes) As "Población Jardín"
                     FROM dfJI, dfJM, poblacion_por_nivel_educativo
                     WHERE dfJI.provincia = dfJM.provincia AND Depto = dfJM.departamento AND dfJI.provincia = poblacion_por_nivel_educativo.provincia AND Depto = poblacion_por_nivel_educativo.departamento"""
                     
dfJardinesTotales = dd.sql(JardinesTotales).df()

consultaPRIMARIA = """SELECT provincia, departamento, sum(CAST("Primario" As INT)) As "Primario"
                      FROM EE_comun
                      GROUP BY provincia, departamento
                      ORDER BY provincia DESC"""
                      
dfPr = dd.sql(consultaPRIMARIA).df()

primariosTotales = """SELECT p.provincia As Provincia, p.departamento As Departamento, d.Primario, p.primario As "Poblacion Primario"
                      FROM dfPr As d
                      INNER JOIN poblacion_por_nivel_educativo As p
                      ON d.provincia=p.provincia AND d.departamento=p.departamento"""
dfPrimario = dd.sql(primariosTotales).df()

consultaSECUNDARIO = """SELECT provincia, departamento, SUM(CAST("Secundario" As INT)) As "Secundario" 
                        FROM EE_comun
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

#%%2
consultaEMPLEOS = """SELECT provincia, departamento, sum(CAST("empleo" As INT)) As "Cantidad total de empleados en 2022"
                     FROM EP_filtrado
                     GROUP BY provincia, departamento
                     ORDER BY provincia ASC, "Cantidad total de empleados en 2022" DESC
                     """
                     
cantTotalEmpleadosPorDepartamento = dd.sql(consultaEMPLEOS).df()

#%%4

consultaITEM4 = """SELECT provincia, departamento, clae6, CASE WHEN clae6.size()"""
                     

















