package com.mmisiewicz.spark.misc

import java.io.{File, FileInputStream, InputStream}
import java.sql.{Connection, DriverManager}
import java.util.Properties

import doobie.imports._

import scala.concurrent.duration._
import scalaz.concurrent.Task

case class PGAuth(host: String, database: String, user: String, password: String) {
  private val FETCH_SIZE = 50000
  val DRIVER = "org.postgresql.Driver"
  val JDBC_URL = s"jdbc:postgresql://$host/$database?defaultRowFetchSize=$FETCH_SIZE"

  // NB: Uncomment, modify and recompile if you wish to use Kerberos
  // sets the JAAS config file, which says we should attempt to use Kerberos auth.
  // In the event that the user is in the postgres `kerberos_users` group, this auth mechanism will be used,
  // and password will be ignored. If not, it will fall back to another authetnciation mechanism,
  // since the config file provided in the JAR sets kerberos to OPTIONAL.
  // private val CONFIG_FILE = "/pgjdbc_login.config"
  // val pgConfigUrl = getClass().getResource(CONFIG_FILE)
  // System.setProperty("java.security.auth.login.config", pgConfigUrl.toExternalForm)
  // System.setProperty("java.security.krb5.realm", "YOURDOMAIN.COM")
  // System.setProperty("java.security.krb5.kdc", "kadmin.yourdomain.com")

  def getJDBCConn: Connection = {
    Class.forName(DRIVER)
    val conn = DriverManager.getConnection(JDBC_URL,user,password)
    // this is required to respect fetch size. More details:
    // https://jdbc.postgresql.org/documentation/head/query.html
    // https://abhirama.wordpress.com/2009/01/07/postgresql-jdbc-and-large-result-sets/
    conn.setAutoCommit(false)
    conn
  }

  def getTransactor: Transactor[Task] = DriverManagerTransactor(DRIVER,JDBC_URL,user,password)
}

object PGAuth {
  private val SECRETS_CONF = "/secrets.conf"
  private val USER_PROP = "pg.user"
  private val PASSWORD_PROP = "pg.password"
  private val HOST_PROP = "pg.host"

  val DEFAULT_TIMEOUT = 30 minutes

  private val props = new Properties()

  /**
    * Create a [[PGAuth]] for the specified database.
    * @return [[PGAuth]] instance
    */
  def apply(host: String, user: String, password: String): String => PGAuth = (db: String) => PGAuth(host, db, user, password)
  /**
    * Create a [[PGAuth]] using a provided properties file.
    * @param propsFile [[File]] instance to access properties file
    * @return [[PGAuth]] instance
    */
  def apply(propsFile: InputStream): (String) => PGAuth = {
    (db: String) => fromProps(propsFile)(db)
  }

  private def fromProps(inputStream: InputStream) = {
    props.load(inputStream)
    (db: String) => PGAuth(props.getProperty(HOST_PROP), db, props.getProperty(USER_PROP), props.getProperty(PASSWORD_PROP))
  }
}
