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

package io.github.lineageco.splinecustomhttpdispatcher


import org.apache.spark.internal.Logging
import scalaj.http.BaseHttp

import scala.concurrent.duration.Duration

trait AzureRestClient {
  def endpoint(url: String): AzureRestEndpoint
}

object AzureRestClient extends Logging {
  /**
   * @param baseHttp          HTTP client
   * @param baseURL           REST endpoint base URL
   * @param connectionTimeout timeout for establishing TCP connection
   * @param readTimeout       timeout for each individual TCP packet (in already established connection)
   */
  def apply(
    baseHttp: BaseHttp,
    baseURL: String,
    connectionTimeout: Duration,
    readTimeout: Duration,
    secHeader: String): AzureRestClient = {

    logDebug(s"baseURL = $baseURL")
    logDebug(s"connectionTimeout = $connectionTimeout")
    logDebug(s"readTimeout = $readTimeout")

    //noinspection ConvertExpressionToSAM
    new AzureRestClient {
      override def endpoint(resource: String): AzureRestEndpoint = new AzureRestEndpoint(
        baseHttp(s"$baseURL/$resource")
          .timeout(connectionTimeout.toMillis.toInt, readTimeout.toMillis.toInt)
          .header(AzureSplineHeaders.Timeout, readTimeout.toMillis.toString)
          .header(AzureSplineHeaders.HttpSecurityKey, secHeader)
          .compress(true))
    }
  }
}
