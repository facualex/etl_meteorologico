import pandas 
from sqlalchemy import create_engine
from os import path

default_base_files_csv_relative_path = ['./data/precipitaciones.csv', './data/temperaturas.csv'] 

from database_config import DATABASE_NAME, DATABASE_HOST, DATABASE_PASSWORD, DATABASE_PORT, DATABASE_USER

class ETLMeteorologico: 
    def __init__(self, base_files_csv_relative_path = default_base_files_csv_relative_path):
        self.db_connection = None
        self.base_files_csv_relative_path = base_files_csv_relative_path

        #self.__connect_database()
    
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
        self.dataframe_temperaturas["latitud"] = self.dataframe_temperaturas['latitud'].apply(lambda x: x.replace('&deg', '°'))
        self.dataframe_precipitaciones["latitud"] = self.dataframe_precipitaciones['latitud'].apply(lambda x: x.replace('&deg', '°'))

        # Corregir y normalizar nombres de las estaciones en los CSV de temperaturas y precipitaciones
        self.__clean_station_names()

        # Agregar campo "region" al set de datos
        self.__add_regions()

    def __load(self):
        print(self.dataframe_precipitaciones.head(10))
        print(self.dataframe_temperaturas.tail(10))
    
    def run(self):
        self.__extract()
        self.__transform()
        self.__load()

    def __connect_database(self):
        try:
            self.db_connection = create_engine("mysql+pymysql://%s:%s@%s:%s/%s" % (
                DATABASE_USER, DATABASE_PASSWORD, DATABASE_HOST, DATABASE_PORT, DATABASE_NAME)).connect()

            if (self.db_connection):
                print('Conexión a la base de datos exitosa!')
        except Exception as exception:
            self.db_connection = None
            print('Error al autenticarse con la base de datos.', exception)
    
    def __clean_station_names(self):
        # Corregir nombres de estaciones provenientes del CSV de temperaturas
        self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == "Eulogio SÃ¡nchez, Tobalaba Ad.", "estacion"] = "Eulogio Sánchez, Tobalaba Ad."
        self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == "Juan FernÃ¡ndez, EstaciÃ³n MeteorolÃ³gica.", "estacion"] = "Juan Fernández, Estación Meteorológica."
        self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == "General Freire, CuricÃ³ Ad.", "estacion"] = "General Freire, Curicó Ad."
        self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == "General Bernardo O'Higgins, ChillÃ¡n Ad.", "estacion"] = "General Bernardo O'Higgins, Chillán Ad."
        self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == "Carriel Sur, ConcepciÃ³n Ap.", "estacion"] = "Carriel Sur, Concepción Ap."
        self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == "MarÃ\xada Dolores, Los Angeles Ad.", "estacion"] = "María Dolores, Los Angeles Ad."
        self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == "CaÃ±al Bajo,  Osorno Ad.", "estacion"] = "Cañal Bajo, Osorno Ad."
        self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == "FutaleufÃº Ad.", "estacion"] = "Futalfefú Ad."
        self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == "Puerto AysÃ©n Ad.", "estacion"] = "Puerto Aysén Ad."
        self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == "Carlos IbaÃ±ez, Punta Arenas Ap.", "estacion"] = "Carlos Ibañez, Punta Arenas Ap."
        self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == "Fuentes MartÃ\xadnez, Porvenir Ad.", "estacion"] = "Fuentes Martínez, Porvenir Ad."
        self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == "Guardiamarina ZaÃ±artu, Pto Williams Ad.", "estacion"] = "Guardiamarina Zañartu, Pto Williams Ad."
        self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == "C.M.A. Eduardo Frei Montalva, AntÃ¡rtica ", "estacion"] = "C.M.A. Eduardo Frei Montalva, Antártica."
        self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == "EulÃ³gio SÃ¡nchez, Tobalaba Ad.", "estacion"] = "Eulogio Sánchez, Tobalaba Ad."
        self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == "Carriel Sur, ConcepciÃ³n.", "estacion"] = "Carriel Sur, Concepción."
        self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == "Guardia Marina ZaÃ±artu, Pto Williams Ad.", "estacion"] = "Guardiamarina Zañartu, Pto Williams Ad."
        self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == "Desierto de Atacama, Caldera  Ad.", "estacion"] = "Desierto de Atacama, Caldera Ad."
        self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == "Cerro Moreno  Antofagasta  Ap.", "estacion"] = "Cerro Moreno Antofagasta Ap."
        self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == "Mataveri  Isla de Pascua Ap.", "estacion"] = "Mataveri Isla de Pascua Ap."

        # Corregir nombres de estaciones provenientes del CSV de precipitaciones
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == "  Osorno Ad.", "estacion"] = "Osorno Ad."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == " Ad.", "estacion"] = "Ad."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == " AntÃ¡rtica ", "estacion"] = "Antártica."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == " Arica Ap.", "estacion"] = "Arica Ap."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == " Calama Ad.", "estacion"] = "Calama Ad."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == " Caldera  Ad.", "estacion"] = "Caldera Ad."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == " ChillÃ¡n Ad.", "estacion"] = "Chillán Ad."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == " ConcepciÃ³n Ap.", "estacion"] = "Concepción Ap."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == " ConcepciÃ³n.", "estacion"] = "Concepción."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == " Coyhaique Ad.", "estacion"] = "Coyahique Ad."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == " CuricÃ³ Ad.", "estacion"] = "Curicó Ad."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == " EstaciÃ³n MeteorolÃ³gica.", "estacion"] = "Estación Meteorológica."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == " La Serena Ad.", "estacion"] = "La Serena Ad."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == " Los Angeles Ad.", "estacion"] = "Los Angeles Ad."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == " Porvenir Ad.", "estacion"] = "Porvenir Ad."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == " Pto Williams Ad.", "estacion"] = "Pto Williams Ad."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == " Puerto Natales Ad.", "estacion"] = "Puerto Natales Ad."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == " Punta Arenas Ap.", "estacion"] = "Punta Arenas Ap."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == " Santiago", "estacion"] = "Santiago"
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == " Temuco Ad.", "estacion"] = "Temuco Ad."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == " Tobalaba Ad.", "estacion"] = "Tobalaba Ad."
        self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == " Valdivia Ad.", "estacion"] = "Valdivia Ad."

    def __add_regions(self):
        for estacion in self.dataframe_precipitaciones["estacion"].unique():
            if (estacion == 'Osorno Ad.'):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == estacion, "region"] = "Los Lagos"
            elif (estacion == "Ad."):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == estacion, "region"] = "Metropolitana"    
            elif (estacion == "Antártica."):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == estacion, "region"] = "Antártica Chilena"    
            elif (estacion == "Arica Ap."):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == estacion, "region"] = "Arica y Parinacota"    
            elif (estacion == "Calama Ad."):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == estacion, "region"] = "Antofagasta"    
            elif (estacion == "Caldera Ad."):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == estacion, "region"] = "Atacama"    
            elif (estacion == "Chillán Ad."):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == estacion, "region"] = "Ñuble"    
            elif (estacion == "Concepción Ap."):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == estacion, "region"] = "Bío Bío" 
            elif (estacion == "Concepción."):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == estacion, "region"] = "Bío Bío" 
            elif (estacion == "Coyahique Ad."):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == estacion, "region"] = "Aysén" 
            elif (estacion == "Curicó Ad."):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == estacion, "region"] = "Maule" 
            elif (estacion == "Estación Meteorológica."):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == estacion, "region"] = "Valparaíso" 
            elif (estacion == "La Serena Ad."):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == estacion, "region"] = "Coquimbo" 
            elif (estacion == "Los Angeles Ad."):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == estacion, "region"] = "Bío Bío"
            elif (estacion == "Porvenir Ad."):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == estacion, "region"] = "Magallanes y Antártica Chilena"
            elif (estacion == "Pto Williams Ad."):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == estacion, "region"] = "Magallanes"
            elif (estacion == "Puerto Natales Ad."):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == estacion, "region"] = "Magallanes y Antártica Chilena"     
            elif (estacion == "Punta Arenas Ap."):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == estacion, "region"] = "Magallanes y Antártica Chilena"     
            elif (estacion == "Santiago"):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == estacion, "region"] = "Metropolitana"     
            elif (estacion == "Temuco Ad."):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == estacion, "region"] = "Araucanía"     
            elif (estacion == "Tobalaba Ad."):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == estacion, "region"] = "Metropolitana"     
            elif (estacion == "Valdivia Ad."):
                self.dataframe_precipitaciones.loc[self.dataframe_precipitaciones["estacion"] == estacion, "region"] = "Los Ríos"

        for estacion in self.dataframe_temperaturas["estacion"].unique():
            if (estacion == 'Chacalluta, Arica Ap.'):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == estacion, "region"] = "Arica y Parinacota"
            elif (estacion == 'Diego Aracena Iquique Ap.'):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == estacion, "region"] = "Tarapacá"
            elif (estacion == 'El Loa, Calama Ad.'):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == estacion, "region"] = "Antofagasta"
            elif (estacion == 'Cerro Moreno Antofagasta Ap.'):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == estacion, "region"] = "Antofagasta"
            elif (estacion == 'Mataveri Isla de Pascua Ap.'):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == estacion, "region"] = "Valparaíso"
            elif (estacion == 'Desierto de Atacama, Caldera Ad.'):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == estacion, "region"] = "Atacama"
            elif (estacion == 'La Florida, La Serena Ad.'):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == estacion, "region"] = "Coquimbo"
            elif (estacion == 'Rodelillo, Ad.'):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == estacion, "region"] = "Valparaíso" 
            elif (estacion == 'Eulogio Sánchez, Tobalaba Ad.'):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == estacion, "region"] = "Metropolitana"
            elif (estacion == 'Quinta Normal, Santiago'):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == estacion, "region"] = "Metropolitana"
            elif (estacion == 'Pudahuel Santiago'):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == estacion, "region"] = "Metropolitana"    
            elif (estacion == 'Santo Domingo, Ad.'):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == estacion, "region"] = "Valparaíso"    
            elif (estacion == 'Juan Fernández, Estación Meteorológica.'):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == estacion, "region"] = "Valparaíso"    
            elif (estacion == 'General Freire, Curicó Ad.'):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == estacion, "region"] = "Maule"    
            elif (estacion == "General Bernardo O'Higgins, Chillán Ad."):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == estacion, "region"] = "Ñuble"    
            elif (estacion == "Carriel Sur, Concepción Ap."):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == estacion, "region"] = "Bío Bío"    
            elif (estacion == "María Dolores, Los Angeles Ad."):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == estacion, "region"] = "Bío Bío"    
            elif (estacion == "Maquehue, Temuco Ad."):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == estacion, "region"] = "Araucanía"    
            elif (estacion == "Pichoy, Valdivia Ad."):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == estacion, "region"] = "Los Ríos"    
            elif (estacion == "Cañal Bajo, Osorno Ad."):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == estacion, "region"] = "Los Lagos"    
            elif (estacion == "El Tepual Puerto Montt Ap."):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == estacion, "region"] = "Los Lagos"    
            elif (estacion == "Futalfefú Ad."):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == estacion, "region"] = "Los Lagos"    
            elif (estacion == "Alto Palena Ad."):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == estacion, "region"] = "Los Lagos"    
            elif (estacion == "Puerto Aysén Ad."):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == estacion, "region"] = "Aysén"    
            elif (estacion == "Teniente Vidal, Coyhaique Ad."):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == estacion, "region"] = "Aysén"    
            elif (estacion == "Balmaceda Ad."):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == estacion, "region"] = "Aysén"    
            elif (estacion == "Chile Chico Ad."):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == estacion, "region"] = "Aysén"    
            elif (estacion == "Lord Cochrane Ad."):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == estacion, "region"] = "Aysén"    
            elif (estacion == "Teniente Gallardo, Puerto Natales Ad."):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == estacion, "region"] = "Magallanes y Antártica Chilena"    
            elif (estacion == "Carlos Ibañez, Punta Arenas Ap."):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == estacion, "region"] = "Magallanes y Antártica Chilena"    
            elif (estacion == "Fuentes Martínez, Porvenir Ad."):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == estacion, "region"] = "Magallanes y Antártica Chilena"    
            elif (estacion == "Guardiamarina Zañartu, Pto Williams Ad."):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == estacion, "region"] = "Magallanes"    
            elif (estacion == "C.M.A. Eduardo Frei Montalva, Antártica."):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == estacion, "region"] = "Antártica Chilena"    
            elif (estacion == "Carriel Sur, Concepción."):
                self.dataframe_temperaturas.loc[self.dataframe_temperaturas["estacion"] == estacion, "region"] = "Bío Bío" 
            
