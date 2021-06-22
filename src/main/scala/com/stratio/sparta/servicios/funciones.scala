package com.stratio.sparta.servicios

import com.azure.storage.blob.BlobServiceClientBuilder
import com.azure.storage.blob.models.BlobItem
import com.typesafe.scalalogging.slf4j.Logger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory
//import com.azure.storage.file.datalake.DataLakeServiceClientBuilder
import org.apache.hadoop.io.IOUtils

import scala.util.{Failure, Success, Try}

object funciones  extends Serializable {

  val log = Logger(LoggerFactory.getLogger(this.getClass))
  val conf: Configuration = new Configuration
  val fs: FileSystem = FileSystem.get(conf)

  def downloadFiles(nombreArchivo: String, urlSalida: String, contenedor: String, token: String, endPoint: String) {


    System.out.println("debug.ENTRO:" + nombreArchivo + " - " + endPoint + " - " + token + " - " + contenedor + " - " + urlSalida)
    log.debug("debug.ENTRO:" + nombreArchivo + " - " + endPoint + " - " + token + " - " + contenedor + " - " + urlSalida)

        log.debug("funciones.INICIA CONEXION")
        val blobServiceClient = new BlobServiceClientBuilder().endpoint(endPoint).sasToken(token).buildClient
        val blobContainerClient = blobServiceClient.getBlobContainerClient(contenedor)
        val blobs = blobContainerClient.listBlobs().iterator()


        log.debug("funciones.INICIA WHILE")
        while (blobs.hasNext) {

          log.debug("funciones.ENTRA WHILE")
          val blob: BlobItem = blobs.next()
          val dir: Array[String] = blob.getName.split("/")
          log.debug("funciones.ARCHIVO: "+ blob.getName)

          if (blob.getName.startsWith(nombreArchivo) && blob.getName.endsWith(".parquet")) {
            log.debug("funciones.ENTRA IF "+ blob.getName)

            Try(fs.create(new Path(urlSalida + dir(dir.size - 1))))
            match {
              case Failure(exception) =>
                log.debug("funciones.ERROR "+ exception.getMessage)
              case Success(value) =>
                log.debug("funciones.DOWNLOAD: " + blob.getName)
                blobContainerClient.getBlobClient(blob.getName).download(value)
//                IOUtils.copyBytes(inpStr, value, conf)
                IOUtils.closeStream(value)
//                IOUtils.closeStream(inpStr)

                log.debug("funciones.FIN: " + blob.getName)
            }
          }
        }
  }

  //
  //  def downloadFiles2(properties: Map[String, String]): Unit = {
  //
  //    val builder = new DataLakeServiceClientBuilder()
  //      .endpoint("https://extstratio.dfs.core.windows.net")
  //    val dataLakeServiceClient = builder.buildClient()
  //    val fileSystemClient = dataLakeServiceClient.listFileSystems().iterator();
  //
  //    while (fileSystemClient.hasNext) {
  //            val blob  = fileSystemClient.next()
  //            val dir = blob.getName.split("/")
  //            System.out.println("Archivo: " + blob.getName)
  //            log.debug("Here goes my debug message.")
  //          }
  //  }


}
