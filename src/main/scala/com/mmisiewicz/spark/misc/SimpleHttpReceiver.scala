package com.mmisiewicz.spark.misc

import scalaj.http._
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel
import org.apache.spark._
import org.apache.spark.streaming.receiver._


/**
A really simple HTTP receiver for use in Spark Streaming. I used it to receive a stream of events in JSON format! 
Try using a bunch of them if you have a bunch of data!
@constructor create a new HTTP receiver 
@param url The URL to receive
@param httpParams Optional HTTP parameters (e.g. special headers you might want)
@param proxy Proxy settings
*/
class SimpleHttpReciever(url: String, httpParams:Option[Map[Any, Any]] = None, proxy:Option[(String, Int)] = None)
    extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {
    // inspired by http://spark.apache.org/docs/latest/streaming-custom-receivers.html
    /**
    Start the thread that receives data over a connection
    */
    def onStart() {
    // Copied from template for custom recievers.
        new Thread("Socket Receiver") {
            override def run() { receive() }
        }.start()
        logInfo("Started HTTP receiver class")
    }

    /** There is nothing much to do as the thread calling receive()
    is designed to stop by itself if isStopped() returns false
    */
    def onStop() {
        logInfo("Stopping HTTP reception. Did server terminate connection?")
    }

    /** 
    connects to the URL and consumes the things! Does not decode the JSON here.
    */
    private def receive() {
        try {
            // Connect to host:port
            var httpRequest = Http(url)
            if (proxy.nonEmpty) {
                httpRequest = httpRequest.proxy(proxy.get._1, proxy.get._2)
            }
            
            if (httpParams.nonEmpty) {
                httpParams.get.map { p =>
                    httpRequest = httpRequest.param(p._1.toString, p._2.toString)
                }
            }
            
            // Until stopped or connection broken continue reading
            httpRequest.execute(parser = { inputStream =>
                val reader = new java.io.BufferedReader(new java.io.InputStreamReader(inputStream))
                var inputLine = reader.readLine()
                while(!isStopped && inputLine != null) {
                    store(inputLine)
                    inputLine = reader.readLine()
                }
                reader.close()
            })
            
            // Restart in an attempt to connect again when server is active again
            restart("Trying to connect again! Connection was terminated for some reason.")
        } catch {
        case e: java.net.ConnectException =>
            // restart if could not connect to server
            restart("Error connecting to " + url, e)
        case t: Throwable =>
            // restart if there is any other error
            restart("Error receiving data, generic exception", t)
        }
    }
}