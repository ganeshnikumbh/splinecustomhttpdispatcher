/*
 * Copyright 2020 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.abnamro.dmp.mdc.splinecustomhttpdispatcher
import org.apache.spark.internal.Logging

import org.apache.http.HttpHeaders
import scalaj.http.{HttpRequest, HttpResponse}
import za.co.absa.commons.lang.ARM.using
import za.co.absa.spline.harvester.dispatcher.httpdispatcher.HttpConstants.Encoding
import org.abnamro.dmp.mdc.splinecustomhttpdispatcher.AzureRestEndpoint._

import java.io.ByteArrayOutputStream
import java.util.zip.GZIPOutputStream
import javax.ws.rs.HttpMethod
import scalaj.http._

class AzureRestEndpoint(val request: HttpRequest,val authentication: Map[String, String]) extends Logging {

  def getAccessToken(authentication: Map[String, String]): String = {
  val clientId: String= authentication("clientId")
  val clientSecret: String= authentication("clientSecret")
  val tenantId: String= authentication("tenantId")
  val scope: String= authentication("scope")
  val tokenEndpoint = s"https://login.microsoftonline.com/$tenantId/oauth2/v2.0/token"
  val response = Http(tokenEndpoint)
    .postForm(Seq(
      "client_id" -> clientId,
      "client_secret" -> clientSecret,
      "grant_type" -> "client_credentials",
      "scope" -> scope.concat("/.default")
    )).options(HttpOptions.allowUnsafeSSL,HttpOptions.readTimeout(5000))
    .asString

  val responseJson = response.body
  import scala.util.matching.Regex
  val pattern: Regex = """"access_token"\s*:\s*"([^"]+)"""".r

  val accessToken = "Bearer ".concat(pattern.findFirstMatchIn(responseJson).map(_.group(1)).getOrElse(""))
  accessToken
}
  
  def head(): HttpResponse[String] = {
    val requestnew = request.header("Authorization", getAccessToken(authentication)).method(HttpMethod.HEAD)
    logInfo(requestnew.toString())
    requestnew.asString
  }
  

  def post(data: String, contentType: String, enableRequestCompression: Boolean): HttpResponse[String] = {
    val jsonRequest = request
      .header(HttpHeaders.CONTENT_TYPE, contentType)
      .header("Authorization", getAccessToken(authentication))

    if (enableRequestCompression && data.length > GzipCompressionLengthThreshold) {
      jsonRequest
        .header(HttpHeaders.CONTENT_ENCODING, Encoding.GZIP)
        .postData(gzipContent(data.getBytes("UTF-8")))
        .asString
    } else {
      jsonRequest
        .postData(data)
        .asString
    }
  }
}

object AzureRestEndpoint {
  private val GzipCompressionLengthThreshold: Int = 2048

  private def gzipContent(bytes: Array[Byte]): Array[Byte] = {
    val byteStream = new ByteArrayOutputStream(bytes.length)
    using(new GZIPOutputStream(byteStream))(_.write(bytes))
    byteStream.toByteArray
  }
}
