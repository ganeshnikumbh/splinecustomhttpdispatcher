/*
 * Copyright 2019 ABSA Group Limited
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

import org.apache.commons.configuration.Configuration
import org.apache.spark.internal.Logging
import scalaj.http.{Http, HttpStatusException}
import za.co.absa.commons.lang.extensions.TraversableExtension._
import za.co.absa.commons.version.Version
import za.co.absa.spline.harvester.dispatcher.ProducerApiVersion.SupportedApiRange
import za.co.absa.spline.harvester.dispatcher.httpdispatcher.HttpConstants.Encoding
import za.co.absa.spline.harvester.dispatcher.httpdispatcher._
import za.co.absa.spline.harvester.dispatcher.modelmapper.ModelMapper
import za.co.absa.spline.harvester.exception.SplineInitializationException
import za.co.absa.spline.producer.model.{ExecutionEvent, ExecutionPlan}

import javax.ws.rs.core.MediaType
import scala.util.Try
import scala.util.control.NonFatal
import za.co.absa.spline.harvester.dispatcher.LineageDispatcher
import za.co.absa.spline.harvester.dispatcher.SplineHeaders
import za.co.absa.spline.harvester.dispatcher.ProducerApiVersion

/**
 * AzureHttpLineageDispatcherConfig is responsible for sending the lineage data to spline gateway through producer API
 */
class AzureHttpLineageDispatcher(restClient: AzureRestClient, apiVersionOption: Option[Version], requestCompressionOption: Option[Boolean])
  extends LineageDispatcher
    with Logging {

  import org.abnamro.dmp.mdc.splinecustomhttpdispatcher.AzureHttpLineageDispatcher._
  import za.co.absa.spline.harvester.json.HarvesterJsonSerDe.impl._

  def this(dispatcherConfig: AzureHttpLineageDispatcherConfig) =
    this(
      AzureHttpLineageDispatcher.createDefaultRestClient(dispatcherConfig),
      dispatcherConfig.apiVersionOption,
      dispatcherConfig.requestCompressionOption
    )

  def this(configuration: Configuration) = this(AzureHttpLineageDispatcherConfig(configuration))

  private val executionPlansEndpoint = restClient.endpoint(RESTResource.ExecutionPlans)
  private val executionEventsEndpoint = restClient.endpoint(RESTResource.ExecutionEvents)

  private lazy val serverHeaders: Map[String, IndexedSeq[String]] = getServerHeaders(restClient)
  private val apiVersion: Version = apiVersionOption.getOrElse(resolveApiVersion(serverHeaders))

  logInfo(s"Using Producer API version: ${apiVersion.asString}")

  private val modelMapper = ModelMapper.forApiVersion(apiVersion)

  private val requestCompressionSupported: Boolean =
    requestCompressionOption.getOrElse(resolveRequestCompression(serverHeaders))

  override def name = "Http"

  override def send(plan: ExecutionPlan): Unit = {
    for (execPlanDTO <- modelMapper.toDTO(plan)) {
      sendJson(execPlanDTO.toJson, executionPlansEndpoint)
    }
  }

  override def send(event: ExecutionEvent): Unit = {
    for (eventDTO <- modelMapper.toDTO(event)) {
      sendJson(Seq(eventDTO).toJson, executionEventsEndpoint)
    }
  }

  private def sendJson(json: String, endpoint: AzureRestEndpoint): Unit = {
    val url = endpoint.request.url
    logTrace(s"sendJson $url : \n${json.asPrettyJson}")

    val contentType =
      if (apiVersion == ProducerApiVersion.V1) MediaType.APPLICATION_JSON
      else s"application/vnd.absa.spline.producer.v${apiVersion.asString}+json"

    try {
      endpoint
        .post(json, contentType, requestCompressionSupported)
        .throwError

    } catch {
      case HttpStatusException(code, _, body) =>
        throw new RuntimeException(createHttpErrorMessage(s"Cannot send lineage data to $url", code, body))
      case NonFatal(e) =>
        throw new RuntimeException(s"Cannot send lineage data to $url", e)
    }
  }
}

object AzureHttpLineageDispatcher extends Logging {
  private def createDefaultRestClient(config: AzureHttpLineageDispatcherConfig): AzureRestClient = {
    logInfo(s"Producer URL: ${config.producerUrl}")
    AzureRestClient(
      Http,
      config.producerUrl,
      config.connTimeout,
      config.readTimeout,
      config.disableSslValidation,
      config.headers,
      config.authentication
    )
  }

  private def getServerHeaders(restClient: AzureRestClient): Map[String, IndexedSeq[String]] = {
    val unableToConnectMsg = "Spark Agent was not able to establish connection to Spline Gateway"
    val serverHasIssuesMsg = "Connection to Spline Gateway: OK, but the Gateway is not initialized properly! Check Gateway logs"

    val statusEndpoint = restClient.endpoint(RESTResource.Status)
    Try(statusEndpoint.head())
      .map {
        case resp if resp.is2xx =>
          resp.headers
        case resp if resp.is5xx =>
          throw new SplineInitializationException(createHttpErrorMessage(serverHasIssuesMsg, resp.code, resp.body))
        case resp =>
          throw new SplineInitializationException(createHttpErrorMessage(unableToConnectMsg, resp.code, resp.body))
      }
      .recover {
        case e: SplineInitializationException => throw e
        case NonFatal(e) => throw new SplineInitializationException(unableToConnectMsg, e)
      }
      .get
      .withDefaultValue(Array.empty[String])
  }

  private def createHttpErrorMessage(msg: String, code: Int, body: String): String = {
    s"$msg. HTTP Response: $code $body"
  }

  private def resolveRequestCompression(serverHeaders: Map[String, IndexedSeq[String]]): Boolean =
    serverHeaders(SplineHeaders.AcceptRequestEncoding)
      .exists(_.toLowerCase == Encoding.GZIP)

  private def resolveApiVersion(serverHeaders: Map[String, IndexedSeq[String]]): Version = {
    val serverApiVersions =
      serverHeaders(SplineHeaders.ApiVersion)
        .map(Version.asSimple)
        .toNonEmptyOption
        .getOrElse(Seq(ProducerApiVersion.Default))

    val serverApiLTSVersions =
      serverHeaders(SplineHeaders.ApiLTSVersion)
        .map(Version.asSimple)
        .toNonEmptyOption
        .getOrElse(serverApiVersions)

    val apiCompatManager = ProducerApiCompatibilityManager(serverApiVersions, serverApiLTSVersions)

    apiCompatManager.newerServerApiVersion.foreach(ver => logWarning(s"Newer Producer API version ${ver.asString} is available on the server"))
    apiCompatManager.deprecatedApiVersion.foreach(ver => logWarning(s"UPGRADE SPLINE AGENT! Producer API version ${ver.asString} is deprecated"))

    val apiVer = apiCompatManager.highestCompatibleApiVersion.getOrElse(throw new SplineInitializationException(
      s"Spline Agent and Server versions don't match. " +
        s"Agent supports API versions ${SupportedApiRange.Min.asString} to ${SupportedApiRange.Max.asString}, " +
        s"but the server only provides: ${serverApiVersions.map(_.asString).mkString(", ")}"))

    apiVer
  }
}
