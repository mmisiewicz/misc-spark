package com.mmisiewicz.spark.misc

import org.apache.spark._ // for Logging, since this is intended to be used in spark...

import java.util.Properties
import scala.util.Random
import scala.collection.mutable.ListBuffer

import doobie.imports._
import scalaz._
import Scalaz._
import scalaz.stream._
import scalaz.concurrent.Task
import doobie.contrib.postgresql.pgtypes._
import doobie.contrib.postgresql.imports._
import doobie.contrib.postgresql.free.copymanager.copyIn
import doobie.contrib.postgresql.hi.connection.pgGetCopyAPI
import scodec.bits.ByteVector

/**
this is a class that allows you to easily put a list of stuff into postgres. It uses introspection to 
determine what the name of the data columns should be, based on your Case class. For example: 
{{{
case class Fruit(name:String, count:Int)
val fruitData = List(Fruit("apple", 10), Fruit("banana", 50))
val pgw = new pgWriter[Fruit]("my_awesome_app") // useful for looking at pg_stat_activity
pgw.copyToTable("fruit_data", fruitData) // DB has a table called fruit data, with columns name and count.
}}}
This will automagically copy the contents of the List to the table {{fruit_data}}, filling in the columns
`name` and `count`. Exceptions are not handled, so if those columns don't exist, things blow up!
@constructor create a new PG Writer
@tparam T type parameter to know what columns should be called
@param pPort optional postgres port
@param pHosts optional list of postgres servers (will chose 1 at random - useful for clusters)
@param pUser user to use for writes
@param pPassword definitely NOT 12345 on my server. That's for sure.
@param pDb postgres DB to write to

*/
class PGWriter[T](appName:String, pPort:Option[Int] = None, pHosts:Option[List[String]] = None,
pUser:Option[String] = None, pPassword:Option[String] = None, pDb:Option[String] = None) extends org.apache.spark.Logging {
    // Read config from secrets file in the JAR if parameters not provided
    private val _pDriver = "org.postgresql.Driver"
    private val _pPort = if (pPort.isEmpty) 5432 else pPort.get
    // Muliple hosts are possible.
    private val _pHosts = if (pHosts.isEmpty) { List("localhost") } else { pHosts.get }

    private val rng = new Random()
    private val _pHost = _pHosts(rng.nextInt(_pHosts.size)) // Chose a server at random
    private val prop = new Properties()

    private val (_pPassword, _pDb, _pUser) = if (pDb.isEmpty || pPassword.isEmpty || pUser.isEmpty) {
        // This code assumes you put your secrets in a file in the resources directory (and presumably,
        // dont check it into git.)
        logInfo("Using creds from this jar")
        prop.load(getClass.getResourceAsStream("/secrets.conf"))
        (
            prop.getProperty("pg.password"),
            prop.getProperty("pg.db"),
            prop.getProperty("pg.user")
        )
    } else {
        logInfo("Override for Password, DB and User detected!")
        (pPassword.get, pDb.get, pUser.get)
    }

    private var recordsIn = List[T]()
    private var colNames = List[String]()
    private var tableName:String = null

    private def recordToString(record: T): String = {
        val fields = (ListBuffer[String]() /: record.getClass.getDeclaredFields) { (a, f) =>
            f.setAccessible(true)
            a += f.get(record).toString
        }
        fields.toList.intercalate("\t")
    }
    logInfo("Initialized pgWriter")

    lazy val xa: Transactor[Task] =
        DriverManagerTransactor(_pDriver,
        s"jdbc:postgresql://${_pHost}:${_pPort}/${_pDb}?application_name=$appName", _pUser, _pPassword)

    private def stringToByteVector(str: String): ByteVector =
        ByteVector.view(str.getBytes("UTF-8"))

    private def getColNames() : List[String] = {
        // return class members for case class
        val names = (ListBuffer[String]() /: recordsIn(0).getClass.getDeclaredFields) { (a, f) =>
                f.setAccessible(true)
                a += f.getName
        }
        names.toList
    }
    
    def prog(records: Process[Task, T]): CopyManagerIO[Long] = {
        val cs = colNames.mkString(",")
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
    the method that actually executes the copy.
    @param tn the table name (in `pDb`) to copy to.
    @param inp list to copy into postgres - type T (case classes)
    */
    def copyToTable(tn:String, inp:List[T]) = {
        recordsIn = inp
        colNames = getColNames
        val cs = colNames.mkString(",")
        tableName = tn
        logInfo(s"COPY $tableName ($cs) FROM STDIN DELIMITER '\t'")
        logInfo("============================================")
        recordsIn.take(5).map(recordToString).foreach( r => logInfo("\t" + r))
        logInfo("============================================")
        val rowProcess: Process[Task, T] =
            Process.emitAll(recordsIn)
        val task: Task[Unit] =
            PHC.pgGetCopyAPI(prog(rowProcess)).transact(xa) >>= { count =>
                Task.delay{
                    logInfo(s"$count records inserted")
                }
            }
        // LET IT RIP!
        task.run
    }
}