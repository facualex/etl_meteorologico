from database_config import DATABASE_NAME, DATABASE_HOST, DATABASE_PASSWORD, DATABASE_PORT, DATABASE_USER
import pandas
import time
from sqlalchemy import MetaData, Table, create_engine, insert, select
from os import path

default_base_files_csv_relative_path = [
    './data/precipitaciones.csv', './data/temperaturas.csv']


def transform_coords(coordinates_string):
    direction = {'N': 1, 'S': -1, 'E': 1, 'W': -1}
    new = coordinates_string.replace(
        u'°', ' ').replace('\'', ' ').replace('"', ' ')
    new = new.split()
    new_dir = new.pop()
    new.extend([0, 0, 0])
    return (int(new[0])+int(new[1])/60.0+int(new[2])/3600.0) * direction[new_dir]


class ETLMeteorologico:
    def __init__(self, base_files_csv_relative_path=default_base_files_csv_relative_path):
        self.db_connection = None
        self.base_files_csv_relative_path = base_files_csv_relative_path
        self.__connect_database()

    def __del__(self):
        if(self.db_connection):
            self.db_connection.close()

    def __extract(self):
        self.dataframe_precipitaciones = pandas.read_csv(path.dirname(
            path.realpath(__file__)) + self.base_files_csv_relative_path[0], encoding='latin-1', delimiter=';')

        self.dataframe_temperaturas = pandas.read_csv(path.dirname(
            path.realpath(__file__)) + self.base_files_csv_relative_path[1], encoding='latin-1', delimiter=';')

    def __transform(self):
        # Corregir el símbolo de grados (°) en el campo "latitud" de ambos set de datos
        self.dataframe_temperaturas["latitud"] = self.dataframe_temperaturas['latitud'].apply(
            lambda x: x.replace('&deg', '°'))
        self.dataframe_precipitaciones["latitud"] = self.dataframe_precipitaciones['latitud'].apply(
            lambda x: x.replace('&deg', '°'))

        # Corregir y normalizar nombres de las estaciones en los CSV de temperaturas y precipitaciones
        self.__clean_station_names()

        # Agregar campo "region" al set de datos
        self.__add_regions()

        # Eliminar registros que tengan campos en "NaN"
        self.dataframe_precipitaciones.dropna(inplace=True)
        self.dataframe_temperaturas.dropna(inplace=True)

        # Limpiar campo temperatura_maxima  (se eliminan caracteres coma (,) que están en el dataset)
        self.dataframe_temperaturas["temperatura_maxima"] = self.dataframe_temperaturas['temperatura_maxima'].apply(
            lambda row: row.replace(",", ""))

        # Transformar columna temperatura_maxima a numérico
        self.dataframe_temperaturas["temperatura_maxima"] = pandas.to_numeric(
            self.dataframe_temperaturas["temperatura_maxima"])

        # Transformar formato de latitudeear
        self.dataframe_precipitaciones["latitud"] = self.dataframe_precipitaciones['latitud'].apply(
            lambda x: transform_coords(x))
        self.dataframe_temperaturas["latitud"] = self.dataframe_temperaturas['latitud'].apply(
            lambda x: transform_coords(x))

        # Resetear indices de los dataframe tras las transformaciones
        self.dataframe_precipitaciones.reset_index(drop=True, inplace=True)
        self.dataframe_temperaturas.reset_index(drop=True, inplace=True)

        # Dataframe de temperatura y precipitacion juntos por campos en común
        joined_dataframes = self.dataframe_precipitaciones.merge(
            self.dataframe_temperaturas, how='inner')
        self.joined_dataframes = joined_dataframes

        # Dataframes de temperatura y precipitacion unidos y resumidos con promedio y minmax para 'temperatura_minima', 'temperatura_maxima' y 'precipitacion'
        grouped_data = joined_dataframes.groupby(['estacion', 'mes', 'año'], as_index=False).agg({'temperatura_minima': [
            'mean', 'min', 'max'], 'temperatura_maxima': ['mean', 'min', 'max'], 'precipitacion': ['mean', 'min', 'max', 'sum']})
        self.grouped_data = grouped_data

    def __load(self):
        self.__load_regions()
        self.__load_stations()
        self.__load_periods()
        self.__load_fact_table()
        return

    def run(self):
        start_time = time.time()

        self.__extract()
        self.__transform()
        self.__load()

        execution_time = (time.time() - start_time)
        print('ETL finalizado en %s segundos.' % (execution_time))

    def __connect_database(self):
        try:
            self.db_connection = create_engine("mysql+pymysql://%s:%s@%s:%s/%s" % (
                DATABASE_USER, DATABASE_PASSWORD, DATABASE_HOST, DATABASE_PORT, DATABASE_NAME)).connect()

            if (self.db_connection):
                print('Conexión a la base de datos exitosa!')

            # Cargar tablas a atributos globales
            self.fact_table = Table('fact_temprec', MetaData(
                bind=self.db_connection), autoload=True)
            self.period_table = Table('dim_periodo', MetaData(
                bind=self.db_connection), autoload=True)
            self.station_table = Table('dim_estacion', MetaData(
                bind=self.db_connection), autoload=True)
            self.region_table = Table('dim_region', MetaData(
                bind=self.db_connection), autoload=True)

        except Exception as exception:
            self.db_connection = None
            print('Error al autenticarse con la base de datos.', exception)

    def __load_regions(self):
        unique_regions = self.joined_dataframes['region'].unique()

        print('Poblando dim_region...')
        for region in unique_regions:
            insert_region = (
                insert(self.region_table).
                values(NOMBRE_REGION=region)
            )

            self.db_connection.execute(insert_region)

    def __load_stations(self):
        stations_to_create = []

        unique_stations = self.joined_dataframes['estacion'].unique()

        for station in unique_stations:
            out = self.joined_dataframes.loc[self.joined_dataframes['estacion'] == station, [
                'estacion', 'latitud', 'altitud']]
            stations_to_create.append(
                out.drop_duplicates(['estacion']).values[0])

        print('Poblando dim_estacion...')
        for station in stations_to_create:
            nombre_estacion, latitud, altitud = station

            insert_station = (
                insert(self.station_table).
                values(NOMBRE=nombre_estacion,
                       LATITUD=latitud, ALTITUD=altitud)
            )

            self.db_connection.execute(insert_station)

    def __load_periods(self):
        periods_to_create = self.grouped_data[['mes', 'año']].values

        print('Poblando dim_periodo...')
        for month, year in periods_to_create:
            insert_period = (
                insert(self.period_table).
                values(ANNIO=year,
                       MES=month)
            )

            self.db_connection.execute(insert_period)

    def __load_fact_table(self):
        station_regions_map = self.joined_dataframes.groupby(
            ['estacion'], as_index=False).agg({'region': ['min']})

        print('Poblando tabla de hechos fact_temprec...')
        for row in self.grouped_data.values:
            nombre_estacion, month, year, promedio_temperatura_minima, minima_temperatura_minima, maxima_temperatura_minima, promedio_temperatura_maxima, minima_temperatura_maxima, maxima_temperatura_maxima, promedio_precipitacion, minima_precipitacion, maxima_precipitacion, suma_precipitacion = row
            region_estacion = station_regions_map.loc[station_regions_map['estacion']
                                                      == nombre_estacion, 'region'].values[0][0]

            select_station = select(self.station_table).where(
                self.station_table.c.NOMBRE == nombre_estacion)
            select_period = select(self.period_table).where(
                self.period_table.c.MES == month and self.period_table.c.ANNIO == year)
            select_region = select(self.region_table).where(
                self.region_table.c.NOMBRE_REGION == region_estacion)

            station = self.db_connection.execute(select_station).first()
            region = self.db_connection.execute(select_region).first()
            period = self.db_connection.execute(select_period).first()

            if (not station or not region or not period):
                continue

            id_estacion = station[0]
            id_periodo = station[0]
            id_region = region[0]

            insert_period = (
                insert(self.fact_table).
                values(
                    ID_PERIODO=id_periodo,
                    ID_ESTACION=id_estacion,
                    ID_REGION=id_region,
                    MINIMA_TEMPERATURA_MAXIMA=minima_temperatura_maxima,
                    MAXIMA_TEMPERATURA_MAXIMA=maxima_temperatura_maxima,
                    MINIMA_TEMPERATURA_MINIMA=minima_temperatura_minima,
                    MAXIMA_TEMPERATURA_MINIMA=maxima_temperatura_minima,
                    PROMEDIO_TEMPERATURA_MINIMA=promedio_temperatura_minima,
                    PROMEDIO_TEMPERATURA_MAXIMA=promedio_temperatura_maxima,
                    PROMEDIO_PRECIPITACION=promedio_precipitacion,
                    PRECIPITACION_MINIMA=minima_precipitacion,
                    PRECIPITACION_MAXIMA=maxima_precipitacion,
                    SUMA_PRECIPITACION=suma_precipitacion)
                )

            self.db_connection.execute(insert_period)

    def __clean_station_names(self):
        # Corregir nombres de estaciones provenientes del CSV de temperaturas
        self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] ==
                                        "Eulogio SÃ¡nchez, Tobalaba Ad.", "estacion"] = "Eulogio Sánchez, Tobalaba Ad."
        self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] ==
                                        "Juan FernÃ¡ndez, EstaciÃ³n MeteorolÃ³gica.", "estacion"] = "Juan Fernández, Estación Meteorológica."
        self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] ==
                                        "General Freire, CuricÃ³ Ad.", "estacion"] = "General Freire, Curicó Ad."
        self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] ==
                                        "General Bernardo O'Higgins, ChillÃ¡n Ad.", "estacion"] = "General Bernardo O'Higgins, Chillán Ad."
        self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] ==
                                        "Carriel Sur, ConcepciÃ³n Ap.", "estacion"] = "Carriel Sur, Concepción Ap."
        self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] ==
                                        "MarÃ\xada Dolores, Los Angeles Ad.", "estacion"] = "María Dolores, Los Angeles Ad."
        self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"]
                                        == "CaÃ±al Bajo,  Osorno Ad.", "estacion"] = "Cañal Bajo, Osorno Ad."
        self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"]
                                        == "FutaleufÃº Ad.", "estacion"] = "Futaleufú Ad."
        self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"]
                                        == "Puerto AysÃ©n Ad.", "estacion"] = "Puerto Aysén Ad."
        self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] ==
                                        "Carlos IbaÃ±ez, Punta Arenas Ap.", "estacion"] = "Carlos Ibañez, Punta Arenas Ap."
        self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] ==
                                        "Fuentes MartÃ\xadnez, Porvenir Ad.", "estacion"] = "Fuentes Martínez, Porvenir Ad."
        self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] ==
                                        "Guardiamarina ZaÃ±artu, Pto Williams Ad.", "estacion"] = "Guardiamarina Zañartu, Pto Williams Ad."
        self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] ==
                                        "C.M.A. Eduardo Frei Montalva, AntÃ¡rtica ", "estacion"] = "C.M.A. Eduardo Frei Montalva, Antártica."
        self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] ==
                                        "EulÃ³gio SÃ¡nchez, Tobalaba Ad.", "estacion"] = "Eulogio Sánchez, Tobalaba Ad."
        self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"]
                                        == "Carriel Sur, ConcepciÃ³n.", "estacion"] = "Carriel Sur, Concepción."
        self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] ==
                                        "Guardia Marina ZaÃ±artu, Pto Williams Ad.", "estacion"] = "Guardiamarina Zañartu, Pto Williams Ad."
        self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] ==
                                        "Desierto de Atacama, Caldera  Ad.", "estacion"] = "Desierto de Atacama, Caldera Ad."
        self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] ==
                                        "Cerro Moreno  Antofagasta  Ap.", "estacion"] = "Cerro Moreno Antofagasta Ap."
        self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] ==
                                        "Mataveri  Isla de Pascua Ap.", "estacion"] = "Mataveri Isla de Pascua Ap."
        self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"]
                                        == "Pudahuel Santiago ", "estacion"] = "Pudahuel Santiago"

        # Corregir nombres de estaciones provenientes del CSV de precipitaciones
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                           == "  Osorno Ad.", "estacion"] = "Cañal Bajo, Osorno Ad."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                           == " Ad.", "estacion"] = "Ad."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                           == " AntÃ¡rtica ", "estacion"] = "C.M.A. Eduardo Frei Montalva, Antártica."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                           == " Arica Ap.", "estacion"] = "Arica Ap."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                           == " Calama Ad.", "estacion"] = "Calama Ad."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                           == " Caldera  Ad.", "estacion"] = "Desierto de Atacama, Caldera Ad."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                           == " ChillÃ¡n Ad.", "estacion"] = "General Bernardo O'Higgins, Chillán Ad."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                           == " ConcepciÃ³n Ap.", "estacion"] = "Carriel Sur, Concepción Ap."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                           == " ConcepciÃ³n.", "estacion"] = "Carriel Sur, Concepción."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                           == " CuricÃ³ Ad.", "estacion"] = "General Freire, Curicó Ad."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] ==
                                           " EstaciÃ³n MeteorolÃ³gica.", "estacion"] = "Juan Fernández, Estación Meteorológica."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                           == " La Serena Ad.", "estacion"] = "La Florida, La Serena Ad."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                           == " Los Angeles Ad.", "estacion"] = "María Dolores, Los Angeles Ad."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                           == " Porvenir Ad.", "estacion"] = "Fuentes Martínez, Porvenir Ad."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] ==
                                           " Pto Williams Ad.", "estacion"] = "Guardiamarina Zañartu, Pto Williams Ad."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                           == " Punta Arenas Ap.", "estacion"] = "Carlos Ibañez, Punta Arenas Ap."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                           == " Temuco Ad.", "estacion"] = "Temuco Ad."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                           == " Tobalaba Ad.", "estacion"] = "Eulogio Sánchez, Tobalaba Ad."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] ==
                                           " Puerto Natales Ad.", "estacion"] = "Teniente Gallardo, Puerto Natales Ad."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                           == "Arica Ap.", "estacion"] = "Chacalluta, Arica Ap."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                           == " Valdivia Ad.", "estacion"] = "Pichoy, Valdivia Ad."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                           == " Santiago", "estacion"] = "Pudahuel Santiago"
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                           == "Calama Ad.", "estacion"] = "El Loa, Calama Ad."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                           == "Temuco Ad.", "estacion"] = "Maquehue, Temuco Ad."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                           == "Coyahique Ad.", "estacion"] = "Teniente Vidal, Coyhaique Ad."

    def __add_regions(self):
        for estacion in self.dataframe_precipitaciones["estacion"].unique():
            if (estacion == 'Cañal Bajo, Osorno Ad.'):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                                   == estacion, "region"] = "Los Lagos"
            elif (estacion == "Ad."):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                                   == estacion, "region"] = "Metropolitana"
            elif (estacion == "C.M.A. Eduardo Frei Montalva, Antártica."):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                                   == estacion, "region"] = "Magallanes y Antártica Chilena"
            elif (estacion == "Chacalluta, Arica Ap."):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                                   == estacion, "region"] = "Arica y Parinacota"
            elif (estacion == "El Loa, Calama Ad."):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                                   == estacion, "region"] = "Antofagasta"
            elif (estacion == "Desierto de Atacama, Caldera Ad."):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                                   == estacion, "region"] = "Atacama"
            elif (estacion == "General Bernardo O'Higgins, Chillán Ad."):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                                   == estacion, "region"] = "Ñuble"
            elif (estacion == "Carriel Sur, Concepción Ap."):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                                   == estacion, "region"] = "Bío Bío"
            elif (estacion == "Carriel Sur, Concepción."):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                                   == estacion, "region"] = "Bío Bío"
            elif (estacion == "Teniente Vidal, Coyhaique Ad."):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                                   == estacion, "region"] = "Aysén"
            elif (estacion == "General Freire, Curicó Ad."):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                                   == estacion, "region"] = "Maule"
            elif (estacion == "Juan Fernández, Estación Meteorológica."):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                                   == estacion, "region"] = "Valparaíso"
            elif (estacion == "La Florida, La Serena Ad."):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                                   == estacion, "region"] = "Coquimbo"
            elif (estacion == "María Dolores, Los Angeles Ad."):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                                   == estacion, "region"] = "Bío Bío"
            elif (estacion == "Fuentes Martínez, Porvenir Ad."):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                                   == estacion, "region"] = "Magallanes y Antártica Chilena"
            elif (estacion == "Guardiamarina Zañartu, Pto Williams Ad."):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                                   == estacion, "region"] = "Magallanes y Antártica Chilena"
            elif (estacion == "Teniente Gallardo, Puerto Natales Ad."):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                                   == estacion, "region"] = "Magallanes y Antártica Chilena"
            elif (estacion == "Carlos Ibañez, Punta Arenas Ap."):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                                   == estacion, "region"] = "Magallanes y Antártica Chilena"
            elif (estacion == "Pudahuel Santiago"):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                                   == estacion, "region"] = "Metropolitana"
            elif (estacion == "Maquehue, Temuco Ad."):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                                   == estacion, "region"] = "Araucanía"
            elif (estacion == "Eulogio Sánchez, Tobalaba Ad."):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                                   == estacion, "region"] = "Metropolitana"
            elif (estacion == "Pichoy, Valdivia Ad."):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"]
                                                   == estacion, "region"] = "Los Ríos"

        for estacion in self.dataframe_temperaturas["estacion"].unique():
            if (estacion == 'Chacalluta, Arica Ap.'):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"]
                                                == estacion, "region"] = "Arica y Parinacota"
            elif (estacion == 'Diego Aracena Iquique Ap.'):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"]
                                                == estacion, "region"] = "Tarapacá"
            elif (estacion == 'El Loa, Calama Ad.'):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"]
                                                == estacion, "region"] = "Antofagasta"
            elif (estacion == 'Cerro Moreno Antofagasta Ap.'):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"]
                                                == estacion, "region"] = "Antofagasta"
            elif (estacion == 'Mataveri Isla de Pascua Ap.'):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"]
                                                == estacion, "region"] = "Valparaíso"
            elif (estacion == 'Desierto de Atacama, Caldera Ad.'):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"]
                                                == estacion, "region"] = "Atacama"
            elif (estacion == 'La Florida, La Serena Ad.'):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"]
                                                == estacion, "region"] = "Coquimbo"
            elif (estacion == 'Rodelillo, Ad.'):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"]
                                                == estacion, "region"] = "Valparaíso"
            elif (estacion == 'Eulogio Sánchez, Tobalaba Ad.'):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"]
                                                == estacion, "region"] = "Metropolitana"
            elif (estacion == 'Quinta Normal, Santiago'):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"]
                                                == estacion, "region"] = "Metropolitana"
            elif (estacion == 'Pudahuel Santiago'):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"]
                                                == estacion, "region"] = "Metropolitana"
            elif (estacion == 'Santo Domingo, Ad.'):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"]
                                                == estacion, "region"] = "Valparaíso"
            elif (estacion == 'Juan Fernández, Estación Meteorológica.'):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"]
                                                == estacion, "region"] = "Valparaíso"
            elif (estacion == 'General Freire, Curicó Ad.'):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"]
                                                == estacion, "region"] = "Maule"
            elif (estacion == "General Bernardo O'Higgins, Chillán Ad."):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"]
                                                == estacion, "region"] = "Ñuble"
            elif (estacion == "Carriel Sur, Concepción Ap."):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"]
                                                == estacion, "region"] = "Bío Bío"
            elif (estacion == "María Dolores, Los Angeles Ad."):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"]
                                                == estacion, "region"] = "Bío Bío"
            elif (estacion == "Maquehue, Temuco Ad."):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"]
                                                == estacion, "region"] = "Araucanía"
            elif (estacion == "Pichoy, Valdivia Ad."):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"]
                                                == estacion, "region"] = "Los Ríos"
            elif (estacion == "Cañal Bajo, Osorno Ad."):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"]
                                                == estacion, "region"] = "Los Lagos"
            elif (estacion == "El Tepual Puerto Montt Ap."):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"]
                                                == estacion, "region"] = "Los Lagos"
            elif (estacion == "Futalfefú Ad."):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"]
                                                == estacion, "region"] = "Los Lagos"
            elif (estacion == "Alto Palena Ad."):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"]
                                                == estacion, "region"] = "Los Lagos"
            elif (estacion == "Puerto Aysén Ad."):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"]
                                                == estacion, "region"] = "Aysén"
            elif (estacion == "Teniente Vidal, Coyhaique Ad."):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"]
                                                == estacion, "region"] = "Aysén"
            elif (estacion == "Balmaceda Ad."):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"]
                                                == estacion, "region"] = "Aysén"
            elif (estacion == "Chile Chico Ad."):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"]
                                                == estacion, "region"] = "Aysén"
            elif (estacion == "Lord Cochrane Ad."):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"]
                                                == estacion, "region"] = "Aysén"
            elif (estacion == "Teniente Gallardo, Puerto Natales Ad."):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"]
                                                == estacion, "region"] = "Magallanes y Antártica Chilena"
            elif (estacion == "Carlos Ibañez, Punta Arenas Ap."):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"]
                                                == estacion, "region"] = "Magallanes y Antártica Chilena"
            elif (estacion == "Fuentes Martínez, Porvenir Ad."):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"]
                                                == estacion, "region"] = "Magallanes y Antártica Chilena"
            elif (estacion == "Guardiamarina Zañartu, Pto Williams Ad."):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"]
                                                == estacion, "region"] = "Magallanes y Antártica Chilena"
            elif (estacion == "C.M.A. Eduardo Frei Montalva, Antártica."):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"]
                                                == estacion, "region"] = "Magallanes y Antártica Chilena"
            elif (estacion == "Carriel Sur, Concepción."):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"]
                                                == estacion, "region"] = "Bío Bío"
