///*
// * © 2017 Stratio Big Data Inc., Sucursal en España. All rights reserved.
// *
// * This software – including all its source code – contains proprietary information of Stratio Big Data Inc., Sucursal en España and may not be revealed, sold, transferred, modified, distributed or otherwise made available, licensed or sublicensed to third parties; nor reverse engineered, disassembled or decompiled, without express written authorization from Stratio Big Data Inc., Sucursal en España.
// */
//package com.stratio.sparta
//
//import com.stratio.sparta.sdk.lite.common.models.OutputOptions
//import com.stratio.sparta.sdk.lite.xd.common._
//import com.stratio.sparta.servicios.funciones
//import org.apache.spark.sql._
//import org.apache.spark.sql.crossdata.XDSession
//
//class Gen2Output2(
//                  xdSession: XDSession,
//                  properties: Map[String, String]
//                )
//  extends LiteCustomXDOutput(xdSession, properties) {
//
//  override def save(data: DataFrame, outputOptions: OutputOptions): Unit = {
//    funciones.downloadFiles2(properties)
//    }
//
//    override def save(data: DataFrame, saveMode: String, saveOptions: Map[String, String]): Unit = ???
//}
