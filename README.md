
### Tabla de contenidos

- [1. Datos seleccionados](#1-datos-seleccionados)
- [2. Modelo de datos](#2-modelo-de-datos)
- [3. Transformaciones sobre los datos](#3-transformaciones-sobre-los-datos)
- [4. Instrucciones de ejecución](#4-instrucciones-de-ejecución)
  - [4.1. Configurar variables de acceso a la base de datos](#41-configurar-variables-de-acceso-a-la-base-de-datos)
  - [4.2. Crear entorno virtual de Python en el directorio](#42-crear-entorno-virtual-de-python-en-el-directorio)
  - [4.3. Activar entorno virtual](#43-activar-entorno-virtual)
  - [4.4. Instalar dependencias](#44-instalar-dependencias)
  - [4.5. Ejecutar programa](#45-ejecutar-programa)

# 1. Datos seleccionados

# 2. Modelo de datos

# 3. Transformaciones sobre los datos
# 4. Instrucciones de ejecución

## 4.1. Configurar variables de acceso a la base de datos
En el archivo [database_config.py](database_config.py) configurar las variables para acceder a la base de datos:

- `DATABASE_USER`
- `DATABASE_PASSWORD`
- `DATABASE_NAME`
- `DATABASE_HOST`
- `DATABASE_PORT`

## 4.2. Crear entorno virtual de Python en el directorio

**Windows**
```cmd
chdir .\etl_meteorologico

python -m venv .\
```

**Mac/Linux**
```bash
cd ./etl_meteorologico # Entrar al directorio del proyecto

python -m venv ./ # Crear entorno virtual dentro de la carpeta del proyecto
```

## 4.3. Activar entorno virtual
**Windows**
```cmd
.\Scripts\activate
```

**Mac/Linux**
```bash
source env/bin/activate
```


## 4.4. Instalar dependencias
Una vez activado el entorno virtual, ejecutar dentro de la carpeta del proyecto:
```bash
 pip install -r requirements.txt
```

## 4.5. Ejecutar programa
Finalmente, ejecutar el programa:

**Windows**
```cmd
python .\main.py
```

**Mac/Linux**
```bash
python ./main.py
```