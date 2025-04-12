package practica

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions._

object examen {


  case class Estudiante(nombre:String, edad:Option[Int], calificacion:Option[Double])

  /**Ejercicio 1: Crear un DataFrame y realizar operaciones básicas
  Pregunta: Crea un DataFrame a partir de una secuencia de tuplas que contenga información sobre
              estudiantes (nombre, edad, calificación).
            Realiza las siguientes operaciones:

            Muestra el esquema del DataFrame.
            Filtra los estudiantes con una calificación mayor a 8.
            Selecciona los nombres de los estudiantes y ordénalos por calificación de forma descendente.
   */

  object ejercicio1{

    //Muestra el esquema del DataFrame
    def mostrarEsquemaDf(estudiantes: DataFrame)(implicit spark:SparkSession): Unit = {

      estudiantes.printSchema()

    }

    //Filtra devolviendo los registros que tengan el valor de la calificación mayor a 8
    def filtrarCalificacionMayor8(estudiantes: DataFrame)(implicit spark:SparkSession): DataFrame = {

      estudiantes.filter(col("calificacion") > 8)

    }

    // Devuelve los nombres de los estudiantes ordenados por calificación de forma descendente
    def nombresEstudiantesOrdenadosCalificacionDesc(df: DataFrame): DataFrame = {

      df.select("nombre")
        .orderBy(desc("calificacion"))

    }

  }


  /**Ejercicio 2: UDF (User Defined Function)
  Pregunta: Define una función que determine si un número es par o impar.
            Aplica esta función a una columna de un DataFrame que contenga una lista de números.
   */
  //UDF para definir si un número es Par o Impar.
  val udfdefinirParImpar: UserDefinedFunction = udf((numero: Int) => if (numero % 2 == 0) "Par" else "Impar")

  //Función para agregar una nueva columna a un DataFrame donde se indica si el número de otra columna es Par o Impar por medio de una UDF
  def ejercicio2(dfNumeros: DataFrame)(implicit spark: SparkSession): DataFrame = {

    dfNumeros.withColumn("tipo", udfdefinirParImpar(col("numero")))

  }

    /**Ejercicio 3: Joins y agregaciones
    Pregunta: Dado dos DataFrames,
              uno con información de estudiantes (id, nombre)
              y otro con calificaciones (id_estudiante, asignatura, calificacion),
              realiza un join entre ellos y calcula el promedio de calificaciones por estudiante.
     ***
    */

    case class EstudianteInf(id: Int, nombre:String)

    case class Calificaciones(id_estudiante: Int, asignatura:String, calificacion:Double)

    def ejercicio3(estudiantes: DataFrame , calificaciones: DataFrame): DataFrame = {

      val estudiantesJoinCalificaciones = estudiantes.join(calificaciones, estudiantes("id") === calificaciones("id_estudiante"))

      estudiantesJoinCalificaciones.groupBy("id_estudiante", "nombre")
                                   .agg(round(functions.avg("calificacion"),2).as("promedio"))

    }


  /**Ejercicio 4: Uso de RDDs
  Pregunta: Crea un RDD a partir de una lista de palabras y cuenta la cantidad de ocurrencias de cada palabra.
  **** me hago la funcion que reciba la lista de palabra y de salida el rdd y de ahi una nueva función que cuente las palabras
  * **** debe de ser funcion por una cosa
  */

  object ejercicio4{

    def cuentaPalabras(palabrasRDD: RDD[(String,Int)]): RDD[(String,Int)] = palabrasRDD.reduceByKey(_ + _)

    def creaRDDOcurrencias(palabrasList: List[String])(implicit sc:SparkContext): RDD[(String, Int)] = {

      val rddPalabras: RDD[String] = sc.parallelize(palabrasList)

      val ocurrenciasPalabras = cuentaPalabras(rddPalabras.map(word => (word.toLowerCase(), 1)))
      ocurrenciasPalabras

    }

  }


 /**
 Ejercicio 5: Procesamiento de archivos
 Pregunta: Carga un archivo CSV que contenga información sobre
          ventas (id_venta, id_producto, cantidad, precio_unitario)
          y calcula el ingreso total (cantidad * precio_unitario) por producto.

 ****En el test hacer pruebas con 3 lineas y luego con todo
 ***
 */

 object  ejercicio5 {

   def pasarCsvDf(path: String, delimiter: String=",")(implicit spark: SparkSession): DataFrame =
     spark.read
       .options(Map(("header", "true"), ("delimiter", delimiter)))
       .csv(path)

   def calculaIngresoTotal(pathVentas: String)(implicit spark: SparkSession): DataFrame = {

     val dfVentas = pasarCsvDf(pathVentas,",")

     dfVentas.withColumn("ingreso", round(col("cantidad")*col("precio_unitario"),2))
             .groupBy("id_producto")
             .agg(sum("ingreso").as("ingreso_total"))
             .orderBy(desc("ingreso_total"))
   }
 }



}
