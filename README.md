# MVP – NO COUNTRY
Repositorio del Equipo c16-111-n-data-bi


## Tema: Fintech
## Desarrollo: 
Yahoo Finance: Traernos BTC, ETH, Nasdaq, SP500 y GOLD y analizar la relación entre los mercados.
Nos conectaremos mediante Python a la API de Yahoo Finance para obtener los 5 activos mencionados, crearemos un entorno de AWS con los servicios requeridos, y como destino lo llevaremos a una pequeña base de datos SQL dentro de AWS 
Luego visualizaremos en Power BI donde podremos realizar un Análisis de los datos tomados, evaluando relaciones y analizando la evolución de los precios de cada activo.
Nos planteamos evaluar la relación en el lapso de los últimos 10 años de estos 5 activos.

## Integrantes:
• Preciado Sebastián
• Emiliano Flores
• Diego Bouzada

## Tecnologías
AWS Lambda
Power BI

Objetivos: 
•	Realizar un ETL completo utilizando un proveedor de Cloud. 
•	Visualizar en Power BI los resultados mas importantes del análisis
Hipótesis:
•	Los movimientos de las criptomonedas tienen relación con los índices más importantes del mercado mundial

------------------------------------------------------

## Proceso de Ingenieri
## Base de Datos

Hemos creado con éxito una base de datos en MySQL para nuestro proyecto, estableciendo la tabla "Moneda" con los campos fundamentales: fecha, cotización, volumen y moneda, esenciales para nuestras operaciones y análisis.

En nuestro proyecto, el servicio de AWS Secret Manager desempeña un papel esencial. Durante el proceso de despliegue, Secret Manager administra de manera segura las credenciales necesarias para establecer la conexión con la base de datos MySQL. Estas credenciales son cruciales para garantizar una conexión segura y facilitar operaciones de lectura y escritura en la base de datos.

Hemos desplegado nuestra base de datos en Railway, un servicio en la nube que nos proporciona herramientas gratuitas para crear y desplegar nuestras bases de datos mediante variables de conexión que el mismo servicio nos proporciona. Dentro de él, podemos visualizar tablas, ejecutar queries, etc. Las variables que proporciona este servicio son:

• Nombre de la base de datos
• Host
• Contraseña
• Usuario

Además, hemos explorado las capacidades de Railway, las cuales nos sorprenden al ser un servicio gratuito. Railway sirve para automatizar tareas de mantenimiento, como la gestión de índices y la optimización de consultas, mejorando así la eficiencia operativa.

Todo esto se traduce en una infraestructura escalable y eficiente. Este despliegue no solo asegura la disponibilidad de nuestros datos en MySQL, sino que también permite a la aplicación acceder de manera segura a los servicios de S3 y MySQL. Además, ayuda a alimentar nuestra herramienta de visualización, en este caso, Power BI, contribuyendo así a la protección integral de nuestros recursos.
_________________________________

## Power BI

Tomamos como origen de datos la Base de Datos MYSQL creada en Railway con la tabla de Fecha, Moneda, Cotización y Volumen y realizamos en Power Query el modelo de datos estrella de acuerdo a lo óptimo requerido por Power BI.
Para esto dividimos primeramente en dos tablas, la que contiene todas las fechas con cada moneda, cotización y volumen, y creamos además la tabla de dimensiones de moneda, asignándole a cada una un ID, y relacionándola con el campo de la tabla de hechos, reemplazando los valores por los correspondientes al IdMoneda detallado en la tabla Dim_Monedas.
La tabla Dim_Calendario la creamos con DAX desde el front de Power BI, con la dinámica de que nos tome actualizada la fecha por cada actualización incremental del set de datos.
De esta manera nos quedamos con una tabla de hechos y dos de dimensiones.

Una vez finalizado este proceso, comenzamos a realizar todas las medidas en DAX que nos permiten realizar visualizar nuestro lo que nos propusimos mostrar en nuestro Dashboard.
Este es el listado de Medidas:

•	[Max Bitcoin] = CALCULATE(MAX(Fact_Cotizaciones[Cotizacion]), Dim_Monedas[IdMoneda] = 1)

•	[Max Ethereum] = CALCULATE(MAX(Fact_Cotizaciones[Cotizacion]), Dim_Monedas[IdMoneda] = 2)

•	'Medidas'[Max S&P500] = CALCULATE(MAX(Fact_Cotizaciones[Cotizacion]), Dim_Monedas[IdMoneda] = 3)

•	'Medidas'[Max Nasdaq] = CALCULATE(MAX(Fact_Cotizaciones[Cotizacion]), Dim_Monedas[IdMoneda] = 4)

•	'Medidas'[Max Gold] = CALCULATE(MAX(Fact_Cotizaciones[Cotizacion]), Dim_Monedas[IdMoneda] = 5)

•	[Fecha_Max_Volumen] = CALCULATE (
MAX ( Dim_Calendario[Date] ),
FILTER ( Fact_Cotizaciones, Fact_Cotizaciones[volumen] = MAX ( Fact_Cotizaciones[volumen]) )
)

•	[Fecha de Actualización] = "Fecha de Actualización Informe "&FORMAT(LASTDATE('Fecha Actualización'[Fecha Actualización]), "dd/mm/yy hh:mm")

•	[Cotización Bitcoin] = VAR FechaHoy = TODAY()
RETURN
CALCULATE(
o	LASTNONBLANK('Fact_Cotizaciones'[Cotizacion], 1),
o	FILTER(
	'Fact_Cotizaciones',
	MAX('Dim_Calendario'[Date]) = FechaHoy &&
	RELATED('Dim_Monedas'[IdMoneda]) = 1
o	)
)

•	[Total Volumen] = SUM(Fact_Cotizaciones[volumen])

•	[Min Bitcoin] = CALCULATE(MIN(Fact_Cotizaciones[Cotizacion]), Dim_Monedas[IdMoneda] = 1)

•	[Min Ethereum] = CALCULATE(MIN(Fact_Cotizaciones[Cotizacion]), Dim_Monedas[IdMoneda] = 2)

•	[Min Gold] = CALCULATE(MIN(Fact_Cotizaciones[Cotizacion]), Dim_Monedas[IdMoneda] = 5)

•	[Min Nasdaq] = CALCULATE(MIN(Fact_Cotizaciones[Cotizacion]), Dim_Monedas[IdMoneda] = 4)

•	[Min S&P500] = CALCULATE(min(Fact_Cotizaciones[Cotizacion]), Dim_Monedas[IdMoneda] = 3)

El tablero cuenta con tres pestañas:
•	Resumen
•	Evoluciones temporales
•	Volumen y Composición.

A través del mismo buscamos analizar como ha evolucionado el mercado de activos financieros con la fuerte irrupción de las criptomonedas. Detectar que días son los que mas se opera, que porcentaje de volumen de compra ocupa cada activo, y en estos últimos 10 años, que porcentaje evolutivo ha ocupado cada uno y su importancia.

El tablero se realiza en Power BI Desktop, una vez realizado publicamos el mismo en la Nube de Power BI (app.powerbi.com), creamos una cuenta free de prueba que nos otorga la posibilidad de usar las funcionalidades por 60 días, y programamos diariamente una actualización del tablero para las 21:00 hs.


