package com.mmisiewicz.spark.misc

import scala.collection.mutable.ListBuffer
import doobie.imports._

import scalaz._
import Scalaz._
import scalaz.stream._
import scalaz.concurrent.Task
import doobie.postgres.imports._
import scodec.bits.ByteVector
import org.slf4j.LoggerFactory

/**
  * pgWriter is a class that allows you to quickly put a List of Case Clases into SIGINT postgres. It uses reflection
  * to figure out what the column names are, by examining the names of the member fields. Ideally it should save you
  * trouble! It also uses Scalaz streams and Doobie to make the writes nonblocking. Neat!
  * NB: if any of `pUser`, `pPassword` or `pDb` is specified, all 3 must be.
  * if I say
  * ``` case class Person(name:String, age:Int) ```
  * and I have say:
  * ``` val people = List(Person("Michael", 29)) ```
  * what will happen is `PGWriter` will do this:
  * ``` COPY into tablename (name, age) ```
  * (and then the actual data)
  * so it just looks at the definition of the class `Person` and sees the field name, and age, and uses that to construct a copy command
  *
  * @tparam T The case class of your results. Will be used to determine column names.
  * @param appName The name for your application in Postgres. (only shows up in `pg_stat_activity` for administrators)
  */
case class PGWriter[T](appName: String,
                       database: String,
                       pgAuth: String => PGAuth) {

  private var recordsIn: Array[T] = null
  private var colNames = List[String]()
  private var tableName: String = null
  private val auth = pgAuth(database)
  private val logger = LoggerFactory.getLogger(this.getClass)

  private def recordToString(record: T): String = {
    val fields = (ListBuffer[String]() /: record.getClass.getDeclaredFields) { (a, f) =>
      f.setAccessible(true)
      val fv: Any = f.get(record)
      val fStr = fv match {
        case o: Option[Any] => o match {
          case Some(ov) => ov.toString
          case None => "\\N"
        }
        case s: String => s.toString
        case z: Boolean => z.toString
        case b: Byte => b.toString
        case c: Char => c.toString
        case s: Short => s.toString
        case i: Int => i.toString
        case j: Long => j.toString
        case f: Float => f.toString
        case d: Double => d.toString
        case ar: Array[_] => "{\"" + ar.mkString("\",\"") + "\"}"
        case se: Seq[_] => "{\"" + se.mkString("\",\"") + "\"}"
        case badHombre => {
          logger.error("Your case class contains a type that is not supported by PGWriter. \n" +
            "Supported types are descendants of AnyVal/AnyRef (e.g. Ints, Floats, Longs, etc.), Strings, and Sequences (not sets) \n" +
            "Sorry! 💩 💩 💩 💩")
          logger.info(s"The bad data is: ${badHombre.toString}")
          throw new IllegalArgumentException("Your case class contains a type that is not supported by PGWriter. \n" +
            "Supported types are descendants of AnyVal/AnyRef (e.g. Ints, Floats, Longs, etc.), Strings, and Sequences (not sets) \n" +
            "Sorry! 💩 💩 💩 💩")
          ""
        }
      }
      a += fStr
    }
    fields.toList.intercalate("\t")
  }

  logger.info("Initialized pgWriter")

  // scalaz thing that recieves a task
  lazy val xa: Transactor[Task] = auth.getTransactor

  private def stringToByteVector(str: String): ByteVector = ByteVector.view(str.getBytes("UTF-8"))

  // using reflection here, will fail if there is an empty list
  private def getColNames: List[String] = {
    // return class members for case class
    val names = (ListBuffer[String]() /: recordsIn(0).getClass.getDeclaredFields) { (a, f) =>
      f.setAccessible(true)
      a += f.getName
    }
    names.toList
  }

  // set up the copy statement
  private def prog(records: Process[Task, T]): CopyManagerIO[Long] = {
    val cs = colNames.mkString(",")
    logger.debug(s"COPY $tableName ($cs) FROM STDIN DELIMITER '\t'")
    PFCM.copyIn(
      s"COPY $tableName ($cs) FROM STDIN DELIMITER '\t'",
      scalaz.stream.io.toInputStream(
        records
          .map(recordToString)
          .intersperse("\n")
          .map(stringToByteVector)
      )
    )
  }

  /**
    * This makes the copy actually happen, initiating the copy thread. Should be nonblocking.
    *
    * @param tn  Table name in postgres XL. e.g.: `user_annotation`
    * @param inp List of case class instances to insert.
    */
  def copyToTable(tn: String, inp: Array[T]) = {
    this.recordsIn = inp
    this.colNames = getColNames
    val cs = this.colNames.mkString(",")
    this.tableName = tn
    val rowProcess: Process[Task, T] =
      Process.emitAll(recordsIn)
    val task: Task[Unit] =
      PHC.pgGetCopyAPI(prog(rowProcess)).transact(xa) >>= { count =>
        Task.delay {
          logger.info(s"$count records inserted")
        }
      }
    // LET IT RIP!
    task.run
    // done
    logger.info(s"COPY $tableName ($cs) FROM STDIN DELIMITER '\t'")
    logger.info("============================================")
    this.recordsIn.take(5).map(recordToString).foreach(r => logger.info("\t" + r))
    logger.info("============================================")

  }

  def getStatement(tableName: String): String = {
    val cs = this.colNames.mkString(",")
    s"COPY $tableName ($cs) FROM STDIN DELIMITER '\t'"
  }
}
