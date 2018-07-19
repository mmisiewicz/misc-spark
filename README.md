# misc-spark

This package comprises a handful of utility classes I wrote for use at AppNexus. `PGWriter` is a class that dramatically simplifies database writing, making use of the Postgres high-speed copy API (and covers up a lot of functional programming complexity). `PGReader` goes the opposite way, also using Copy. Both use reflection to guess at database types.

`SimpleHttpReciever` is a single-threaded HTTP receiver  for Spark Streaming for newline-delimited JSON messages.

`PGAuth` is a class that allows you to re-use the same username and password connecting to multiple databases in a multi-tenant postgres environment. You can store your credentials in a Java properties file called "secrets.conf" in `/` of an assembly jar (e.g. stored in `src/main/resources`). The needed properties are `pg.user`, `pg.password` and `pg.host`.

Please feel free to email me if you have any questions!

# Usage Examples

## PGWriter

```scala
import com.mmisiewicz.spark.misc.{PGWriter, PGAuth}

case class Person(name:String, age:Int)
val people = Array(Person("Michael", 30), Person("John", 28))
val cnxn = PGAuth(host = "my.pg.server.com", user = "username", password = "supersecret")
val pgw = PGWriter[Person](appName = "Research app", database = "awesome_datascience_db", pgAuth = cnxn)

pgw.copyToTable("people", people)

```

## PGReader

```scala
import com.mmisiewicz.spark.misc.{PGReader, PGAuth}

case class Person(name:String, age:Int)
val cnxn = PGAuth(host = "my.pg.server.com", user = "username", password = "supersecret")
val pgr = PGReader[Person](appName = "Research app", database = "awesome_datascience_db", pgAuth = cnxn)

val people = pgr.copyFromTable("people")

```

# Unfinished business

Port the unit tests into this repository (they exist though!).

Copyright Michael Misiewicz, 2018.
