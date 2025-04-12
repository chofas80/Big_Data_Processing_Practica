package practica

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import practica.examen._
import practica.examen.ejercicio1._
import practica.examen.ejercicio4._
import practica.examen.ejercicio5._
import utils.TestInit

class examenTest extends TestInit{

  import spark.implicits._

    val dfEstudiantes = Seq(
      Estudiante("Samantha Gómez López", Some(17), Some(9.4)),
      Estudiante("María José Sanchéz Cortizo", Some(15), Some(8.6)),
      Estudiante("Gerardo Romero Rivas", Some(17), Some(5.8)),
      Estudiante("Laura Doval Peñalosa", Some(16), Some(6.3))
    ).toDF()


    "Ejercicio1.1" should "dfEstudiantes Schema" in {

      mostrarEsquemaDf(dfEstudiantes)

      val schemaBase = StructType(Array(
        StructField("nombre", StringType, true),
        StructField("edad", IntegerType, true),
        StructField("calificacion", DoubleType, true)
      ))
      assert(dfEstudiantes.schema === schemaBase)

    }

    "Ejercicio1.2" should "dfEstudiantes calificaciones > 8" in {

      val dfEstudiantesCalfMayor8 = filtrarCalificacionMayor8(dfEstudiantes)

      val estudiantesNotasAltas = Seq(
        ("Samantha Gómez López",17, 9.4),
        ("María José Sanchéz Cortizo", 15, 8.6)
      ).toDF("nombre", "edad", "calificacion")

      checkDfIgnoreDefault(estudiantesNotasAltas, dfEstudiantesCalfMayor8)

    }

    "Ejercicio1.3" should "dfEstudiantes Nombres ordenados" in {

      val dfEstudiantesOrdenadosCalificacionDesc = nombresEstudiantesOrdenadosCalificacionDesc(dfEstudiantes)

      val estudiantesOrdenCalfDesc = Seq(
        ("Samantha Gómez López"),
        ("María José Sanchéz Cortizo"),
        ("Laura Doval Peñalosa"),
        ("Gerardo Romero Rivas")
      ).toDF("nombre")

      checkDfIgnoreDefault(estudiantesOrdenCalfDesc, dfEstudiantesOrdenadosCalificacionDesc)
    }

    val dfNumeros = Seq(1,24,-32,4,61,74,8,45,0,100,-457,345,-788).toDF("numero")

    "Ejercicio2" should "dfNumeros UDF Par o Impar" in {

      val dfNumerosTipo = ejercicio2(dfNumeros)

      val numerosParImpar = Seq(
        (1,"Impar"),
        (24,"Par"),
        (-32, "Par"),
        (4,"Par"),
        (61,"Impar"),
        (74,"Par"),
        (8,"Par"),
        (45,"Impar"),
        (0,"Par"),
        (100,"Par"),
        (-457,"Impar"),
        (345,"Impar"),
        (-788,"Par")).toDF("numero","tipo")

      checkDfIgnoreDefault(numerosParImpar, dfNumerosTipo)
    }


  val dfEstudiantesInf = Seq(
    EstudianteInf(2,"Samantha Gómez López"),
    EstudianteInf(3,"María José Sanchéz Cortizo"),
    EstudianteInf(1,"Gerardo Romero Rivas"),
    EstudianteInf(4,"Laura Doval Peñalosa")
  ).toDF()

  val dfCalificaciones = Seq(
    Calificaciones(1,"Física", 9.2),
    Calificaciones(2,"Física", 8.4),
    Calificaciones(3,"Física", 4.6),
    Calificaciones(4,"Física", 6.1),
    Calificaciones(1,"Lengua", 8),
    Calificaciones(4,"Lengua", 7.3),
    Calificaciones(3,"Lengua", 6.7),
    Calificaciones(2,"Lengua", 9.2),
    Calificaciones(3,"Filosofía", 7.8),
    Calificaciones(1,"Filosofía", 9.5),
    Calificaciones(2,"Filosofía", 10)
  ).toDF()

  "Ejercicio3" should "Promedio calificaciones estudiantes" in {

    val dfEstudiantesPromedio: DataFrame = ejercicio3(dfEstudiantesInf, dfCalificaciones)

    val estudiantesPromedio = Seq(
      (1,"Gerardo Romero Rivas",8.9),
      (2,"Samantha Gómez López",9.2),
      (3, "María José Sanchéz Cortizo",6.37),
      (4,"Laura Doval Peñalosa",6.7)).toDF("id_estudiante","nombre","promedio")

    checkDfIgnoreDefault(estudiantesPromedio, dfEstudiantesPromedio)
  }

  "Ejercicio4" should "Contar ocurrencias de cada palabra" in {

    val sc = spark.sparkContext

    val listaPalabras = List("pez", "perro", "gato", "loro", "perro", "perico", "gato", "gato", "canario", "pez", "conejo", "perro")

    val resultado = creaRDDOcurrencias(listaPalabras)(sc)

    resultado.collect() shouldBe Seq(("perico",1), ("pez",2), ("perro",3), ("gato",3), ("conejo",1), ("loro",1), ("canario",1))
  }



  "Ejercicio5" should "Calcular el ingreso total por producto" in {

    val dfProductosIngresosTotales = calculaIngresoTotal("C:\\Users\\sofia\\Documents\\Big_Data_Processing\\Big_Data_Processing_Ejercicios_Clases\\examen\\ventas.csv")

    val productosIngresos3 = Seq(
      ("104",800.00),
      ("105",570.00),
      ("109",540.00)).toDF("id_producto","ingreso_total")

    val productosIngresosTodos = Seq(
      ("104",800.00),
      ("105",570.00),
      ("109",540.00),
      ("110",494.00),
      ("108",486.00),
      ("101",460.00),
      ("106",425.00),
      ("102",405.00),
      ("107",396.00),
      ("103",280.00)).toDF("id_producto","ingreso_total")

    checkDfIgnoreDefault(productosIngresos3, dfProductosIngresosTotales.limit(3))

    checkDfIgnoreDefault(productosIngresosTodos, dfProductosIngresosTotales)
  }

}
