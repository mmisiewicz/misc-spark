package com.mmisiewicz.spark
/** 
==Overview==
This package contains miscellaneous stuff I have wirrten to use in spark. There are two classes currently.

1. [[pgWriter]] is a class that allows automagical writing of lists of case classes to postgres, inferring the column names. It uses doobie to be non-blocking. It is intended to be used in Spark jobs to store final answers to a postgres database. It supports clusters (such as Postgres-XL).

2. [[simpleHttpReciever]] is a class to download records for Spark Streaming from an HTTP server, one line at a time.

@author Michael Misiewicz
@version 0.1
*/
package object misc {

}