import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object AnalisisFelicidad {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Analisis de Felicidad")
      .config("spark.master", "local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    // Cargar los datos de felicidad del 2021
    val happinessData2021 = spark.read
      .option("header", "true")
      .csv("C:/Users/SNTE/Documents/Boot cam/big-data-processing/Proyecto para entregar/world-happiness-report-2021.csv")
      .withColumn("Ladder score", col("Ladder score").cast("double"))
      .withColumn("Logged GDP per capita", col("Logged GDP per capita").cast("double"))
      .withColumn("Healthy life expectancy", col("Healthy life expectancy").cast("double"))

    // Verificar que los datos están cargados correctamente
    println("Datos de Felicidad 2021:")
    happinessData2021.show(5)

    // Cargar los datos históricos de felicidad
    val happinessDataHistorical = spark.read
      .option("header", "true")
      .csv("C:/Users/SNTE/Documents/Boot cam/big-data-processing/Proyecto para entregar/world-happiness-report.csv")
      .withColumn("Life Ladder", col("Life Ladder").cast("double"))
      .withColumn("Log GDP per capita", col("Log GDP per capita").cast("double"))
      .withColumn("Healthy life expectancy at birth", col("Healthy life expectancy at birth").cast("double"))

    // Verificar que los datos históricos están cargados correctamente
    println("Datos Históricos de Felicidad:")
    happinessDataHistorical.show(5)

    // Pregunta 1: ¿Cuál es el país más feliz del 2021 según la data?
    val happiestCountry2021 = happinessData2021.orderBy(happinessData2021("Ladder score").desc).select("Country name").first().getString(0)
    println(s"El país más feliz del 2021 es: $happiestCountry2021")

    // Crear un mapa que relacione cada "Regional indicator" con su respectivo continente
    val regionToContinent = Map(
      "Western Europe" -> "Europe",
      "North America and ANZ" -> "North America",
      "Middle East and North Africa" -> "Africa",
      "Latin America and Caribbean" -> "South America",
      "Central and Eastern Europe" -> "Europe",
      "East Asia" -> "Asia",
      "Southeast Asia" -> "Asia",
      "Commonwealth of Independent States" -> "Europe",
      "Sub-Saharan Africa" -> "Africa",
      "South Asia" -> "Asia"
    )

    // Añadir una nueva columna "Continent" al DataFrame
    val addContinent = udf((region: String) => regionToContinent.getOrElse(region, "Unknown"))
    val happinessData2021WithContinent = happinessData2021.withColumn("Continent", addContinent(col("Regional indicator")))

    // Pregunta 2: ¿Cuál es el país más feliz del 2021 por continente según la data?
    val windowSpec = Window.partitionBy("Continent").orderBy(happinessData2021WithContinent("Ladder score").desc)
    val happiestCountryByContinent = happinessData2021WithContinent.withColumn("rank", rank().over(windowSpec))
      .filter(col("rank") === 1)
      .select("Continent", "Country name", "Ladder score")

    println("El país más feliz del 2021 por continente:")
    happiestCountryByContinent.show()

    // Pregunta 3: ¿Cuál es el país que más veces ocupó el primer lugar en todos los años?
    val happiestCountryAllYears = happinessDataHistorical.groupBy("Country name")
      .agg(count("Life Ladder").alias("count"))
      .orderBy(col("count").desc)
      .select("Country name")
      .first().getString(0)

    println(s"El país que más veces ocupó el primer lugar en todos los años es: $happiestCountryAllYears")

    // Pregunta 4: ¿Qué puesto de Felicidad tiene el país con mayor GDP del 2020?
    val countryWithHighestGDP2020 = happinessDataHistorical.filter(col("year") === "2020")
      .orderBy(col("Log GDP per capita").desc)
      .select("Country name", "Life Ladder")
      .first()

    println(s"El país con mayor GDP del 2020 es: ${countryWithHighestGDP2020.getString(0)} y su puesto de felicidad es: ${countryWithHighestGDP2020.getDouble(1)}")

    // Pregunta 5: ¿En qué porcentaje ha variado a nivel mundial el GDP promedio del 2020 respecto al 2021? ¿Aumentó o disminuyó?
    val averageGDP2020 = happinessDataHistorical.filter(col("year") === "2020").agg(avg("Log GDP per capita")).first().getDouble(0)
    val averageGDP2021 = happinessData2021.agg(avg("Logged GDP per capita")).first().getDouble(0)
    val gdpVariationPercentage = ((averageGDP2021 - averageGDP2020) / averageGDP2020) * 100

    println(s"El GDP promedio ha variado en un $gdpVariationPercentage% del 2020 al 2021.")

    // Pregunta 6: ¿Cuál es el país con mayor expectativa de vida (“Healthy life expectancy at birth”)? Y ¿Cuánto tenía en ese indicador en el 2019?
    val countryWithHighestLifeExpectancy2021 = happinessData2021.orderBy(col("Healthy life expectancy").desc).select("Country name").first().getString(0)
    val lifeExpectancy2019 = happinessDataHistorical.filter(col("Country name") === countryWithHighestLifeExpectancy2021 && col("year") === "2019").select("Healthy life expectancy at birth").first().getDouble(0)

    println(s"El país con mayor expectativa de vida : $countryWithHighestLifeExpectancy2021 y su expectativa de vida en 2019 era: $lifeExpectancy2019")
  }
}
