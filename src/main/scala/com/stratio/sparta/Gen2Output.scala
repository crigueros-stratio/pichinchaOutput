/*
 * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved.
 *
 * This software – including all its source code – contains proprietary information of Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled, without express written authorization from Stratio Big Data Inc., Sucursal en España.
 */
package com.stratio.sparta

import com.azure.core.http.rest.PagedIterable
import com.azure.storage.blob.{BlobContainerClient, BlobServiceClient, BlobServiceClientBuilder}
import com.azure.storage.blob.models.{BlobItem, BlobStorageException}
import com.stratio.sparta.sdk.lite.common.models.OutputOptions
import com.stratio.sparta.sdk.lite.xd.common._
import org.apache.spark.sql._
import org.apache.spark.sql.crossdata.XDSession

import scala.util.{Failure, Success, Try}
import com.stratio.sparta.servicios.funciones
import com.stratio.sparta.servicios.funciones.{fs, log}
import org.apache.hadoop.fs.Path

import java.io.UncheckedIOException
import scala.collection.JavaConversions._

class Gen2Output(
                  xdSession: XDSession,
                  properties: Map[String, String]
                )
  extends LiteCustomXDOutput(xdSession, properties) {

  lazy val endPoint = properties.get("endPoint") match {
    case Some(value: String) => Try(value) match {
      case Success(v) => v
      case Failure(ex) => throw new IllegalStateException(s"$value ", ex)
    }
    case None => throw new IllegalStateException("'endPoint' key must be defined in the Option properties")
  }

  lazy val token = properties.get("token") match {
    case Some(value: String) => Try(value) match {
      case Success(v) => v
      case Failure(ex) => throw new IllegalStateException(s"$value ", ex)
    }
    case None => throw new IllegalStateException("'token' key must be defined in the Option properties")
  }

  lazy val contenedor = properties.get("contenedor") match {
    case Some(value: String) => Try(value) match {
      case Success(v) => v
      case Failure(ex) => throw new IllegalStateException(s"$value ", ex)
    }
    case None => throw new IllegalStateException("'contenedor' key must be defined in the Option properties")
  }

  lazy val urlSalida = properties.get("urlSalida") match {
    case Some(value: String) => Try(value) match {
      case Success(v) => v
      case Failure(ex) => throw new IllegalStateException(s"$value ", ex)
    }
    case None => throw new IllegalStateException("'urlSalida' key must be defined in the Option properties")
  }

  lazy val nombreArchivo = properties.get("nombreArchivo") match {
    case Some(value: String) => Try(value) match {
      case Success(v) => v
      case Failure(ex) => throw new IllegalStateException(s"$value ", ex)
    }
    case None => throw new IllegalStateException("'nombreArchivo' key must be defined in the Option properties")
  }


  override def save(data: DataFrame, outputOptions: OutputOptions): Unit = {

    if (!nombreArchivo.isEmpty && !urlSalida.isEmpty && !contenedor.isEmpty && !token.isEmpty && !endPoint.isEmpty  ){
      logger.info(s"nombreArchivo : $nombreArchivo")
      logger.info(s"urlSalida : $urlSalida")
      logger.info(s"contenedor : $contenedor")
      logger.info(s"token : $token")
      logger.info(s"endPoint : $endPoint")

      Try {
        val blobServiceClient: BlobServiceClient = new BlobServiceClientBuilder().endpoint(endPoint).sasToken(token).buildClient
        val blobContainerClient: BlobContainerClient = blobServiceClient.getBlobContainerClient(contenedor)
        val blobs: PagedIterable[BlobItem] = blobContainerClient.listBlobs()

        logger.info("Archivo23: " + blobs.isEmpty )
        logger.info("Archivo24: " + blobs.isEmpty + " - " + blobs)
        for (blob <- blobs) {
          logger.info("Archivo25: " + blob.getName)
          val dir: Array[String] = blob.getName.split("/")
          logger.info("Archivo26: " + blob.getName)
//          if (blob.getName.startsWith(nombreArchivo) && blob.getName.endsWith(".parquet")) {
//            Try(fs.create(new Path(urlSalida + dir(dir.size - 1))))
//            match {
//              case Failure(exception) =>
//
//              case Success(value) =>
//                blobContainerClient.getBlobClient(blob.getName).download(value)
//                value.close()
//            }
//          }
        }
        logger.info("Archivo43: " + blobs.isEmpty )


      }
      match {
        case Failure(e: BlobStorageException) =>
            logger.debug("funciones.ERROR " + e.getMessage)
        case Failure(e: UncheckedIOException) =>
          logger.debug("funciones.ERROR " + e.getMessage)
        case Failure(e: NullPointerException) =>
          logger.debug("funciones.ERROR " + e.getMessage)

        case Success(value) =>
          logger.info("todo bien fin")

      }
    }
  }

//  override def save(data: DataFrame, outputOptions: OutputOptions): Unit = {
//    funciones.downloadFiles(properties)
//    }

    override def save(data: DataFrame, saveMode: String, saveOptions: Map[String, String]): Unit = ???
}
