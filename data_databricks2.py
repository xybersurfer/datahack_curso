df = spark.read.load("ruta de acceso",
                     format="csv", sep=";", inferSchema="true", header="true")

df.show(5, truncate = False)

df.describe().show()

	
df.groupBy("Nro Certificado").count().sort("count",ascending=True).show()
