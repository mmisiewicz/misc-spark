package com.mmisiewicz.spark.misc

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._
import scala.reflect.ClassTag
import org.slf4j.LoggerFactory

/**
  * Created by mmisiewicz on 7/28/17.
  *
  * PGReader is a class that uses Scala reflection to execute a query on PGXL and
  * give back the results as an array of case class instances. It's a poor-person's ORM.
  *
  * *WARNING!* This is untested in the scala REPL / Spark Shell. There may be problems!
  *
  * Also, It should be OK, but also watch out for nested classes. For example,
  *     class foo {
  *         case class baz (a:Int, b:Int)
  *     }
  *     // this _may_ be a problem. Better to move baz outside foo.
  *     val pgr = new PGReader[foo.baz](...)
  *
  * This may cause some issues though it should be accounted for.
  *
  * A usage example:
  *
  *     case class ClassOne(a:Int, b:Double, c:String)
  *     val pgr = new PGReader[ClassOne]("table_name","database_name",PGAuth(user="me", pass=""))
  *     val res = pgr.copyFromTable("pg_reader_test") // returns Array[ClassOne]
  *
  * @tparam S The class of the data in your table. Column names in the table must EXACTLY match
  *           the field names in the case class! Weird characters will cause undefined behavior.
  * @param appName A name for your application to be shown in `pg_stat_activity`
  * @param pgAuth An instance of PGAuth. Leave password empty if using your personal user to use
  *               kerberos. If connecting from a machine without a keytab present (e.g. from a hadoop
  *               datanode) you need to use an app user (e.g. dpaas_data_science_user) and password
  *               from the DPAAS credentials store.
  * @param queryOverride An Option to override the generated query. Useful if you want `limit` or `order`
  *                      in your results. Example: `Some("Select foo,bar from baz order by foo limit 100")`
  *
  */
case class PGReader[S <: Product](appName: String,
      database: String,
      pgAuth: String => PGAuth,
      queryOverride: Option[String] = None
    )(implicit tag: TypeTag[S]) {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private var colNames = List[String]()
  private var colTypes = List[universe.Type]()
  private var tableName: String = _
  private val auth = pgAuth(database)

  logger.info("Initialized PGReader")

  /**
   * Gets the accessor members of the class.
   */
  private def getMethods[S: TypeTag] = typeOf[S].members.collect {
    case m: MethodSymbol if m.isCaseAccessor => m
  }.toList

  /**
  * Both of these methods check for names of class fields that start with "$", which indicates
  * junk from declaration of inner classes (which we don't care about for DB purposes.)
  */
  private def getColNames: List[String] = {
    // return class members for case class
    val names = (ListBuffer[String]() /: getMethods) { (a, m) =>
      a += m.name.toString
    }
    names.filter(n => n(0) != '$').toList.reverse
  }

  /**
   * This method gets back the actual types of the case class fields. Also includes
   * some logic to watch out for inner class bullshit.
   */
  private def getColTypes: List[universe.Type] = {
    // return class members for case class
    val methods = getMethods.filter(_.name.toString()(0) != '$')
    val types = (ListBuffer[universe.Type]() /: methods) { (a, m) =>
      a += m.returnType
    }
    types.toList.reverse
  }

  /**
    * Select rows from a PG Table to an Array of Case Classes.
    *
    * @param tn  Table name in postgres XL. e.g.: `user_annotation`
    * @param whereClause Optional WHERE clause (default is None, write `Some("foo > 5")` to use one.
    */
  def copyFromTable(tn: String, whereClause : Option[String] = None)(implicit classTag: ClassTag[S]) : Array[S] = {
    this.colNames = getColNames
    this.colTypes = getColTypes
    this.tableName = tn

    val selectStatement = getStatement(tn, whereClause)
    // logger.info(selectStatement)
    logger.info("Select statement: " + selectStatement)
    val recordsOut = new ArrayBuffer[S]()
    val conn = auth.getJDBCConn
    val stmt = conn.createStatement
    val resultSet = stmt.executeQuery(selectStatement)
    var i = 0
    val clazz: Class[_] = classTag.runtimeClass
    val constructor = clazz.getConstructors()(0)
    while ( resultSet.next() ) {
      i += 1
      val args = (0 to colNames.size - 1).map { c =>
        colTypes(c) match {
          case t if t =:= typeOf[Int] => resultSet.getInt(colNames(c))
          case t if t =:= typeOf[String] => resultSet.getString(colNames(c))
          case t if t =:= typeOf[Long] => resultSet.getLong(colNames(c))
          case t if t =:= typeOf[Float] => resultSet.getFloat(colNames(c))
          case t if t =:= typeOf[Double] => resultSet.getDouble(colNames(c))
          case _ => resultSet.getString(colNames(c))
        }
      }.map(_.asInstanceOf[java.lang.Object]).toArray
      try {
        val instance = constructor.newInstance(args:_*).asInstanceOf[S]
        recordsOut += instance
      } catch {
        case e: Throwable => {
          logger.error("Ran into some issue on a line.")
          logger.error("Looking for colnames : " + colNames.mkString(" "))
          logger.error("Of types             : " + colTypes.mkString(" "))
          logger.error("Bad row was: ")
          (1 to colNames.size).foreach { i =>
            logger.error("\t" + resultSet.getString(i))
          }

          throw new Exception(e)
        }
      }
    }
    logger.info(s"Selected $i rows.")
    logger.info("============================================")
    recordsOut.take(5).foreach(r => logger.info("\t" + r.toString))
    logger.info("============================================")
    conn.close()
    recordsOut.toArray
  }

  /**
   * This function gets the statement to execute on the server by calling the reflection
   * functions.
   */
  private def getStatement(tableName: String, whereClause:Option[String]): String = {
    queryOverride match {
      case None => {
        val cs = this.colNames.mkString(",")
        whereClause match {
          case Some(w) => s"SELECT $cs FROM $tableName WHERE $w"
          case None => s"SELECT $cs FROM $tableName"
        }
      }
      case Some(q) => q
    }
  }
}
