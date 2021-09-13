/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.credentials

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.metastore.{IMetaStoreClient, RetryingMetaStoreClient}
import org.apache.hadoop.hive.thrift.DelegationTokenIdentifier
import org.apache.hadoop.io.Text
import org.apache.hadoop.security.{Credentials, SecurityUtil}
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
import org.apache.hadoop.security.token.Token

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf

class HiveDelegationTokenProvider extends HadoopDelegationTokenProvider with Logging {

  private var clients: Map[Text, IMetaStoreClient] = Map.empty
  private var principal: String = _

  override def serviceName: String = "hive"

  override def initialize(hadoopConf: Configuration, kyuubiConf: KyuubiConf): Unit = {
    val hiveConf = new HiveConf(hadoopConf, classOf[HiveConf])
    val urisSeq = (HiveConf.getTrimmedVar(hiveConf, ConfVars.METASTOREURIS) +:
      kyuubiConf.get(KyuubiConf.CREDENTIALS_HIVE_METASTORE_URIS))
      .filter(_.nonEmpty)

    if (SecurityUtil.getAuthenticationMethod(hadoopConf) != AuthenticationMethod.SIMPLE
      && urisSeq.nonEmpty
      && hiveConf.getBoolVar(ConfVars.METASTORE_USE_THRIFT_SASL)) {

      val principalKey = ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname
      principal = hiveConf.getTrimmed(principalKey, "")
      require(principal.nonEmpty, s"Hive principal $principalKey undefined")

      clients = urisSeq.map { uris =>
        info(s"Creating HiveMetaStoreClient with metastore uris $uris")
        val conf = new HiveConf(hiveConf)
        conf.setVar(ConfVars.METASTOREURIS, uris)
        (new Text(uris), RetryingMetaStoreClient.getProxy(conf, false))
      }.toMap
    }
  }

  override def delegationTokensRequired(): Boolean = clients.nonEmpty

  override def obtainDelegationTokens(owner: String, creds: Credentials): Unit = {
    clients.foreach { case (uris, client) =>
      info(s"Getting Hive delegation token for $owner against $principal")
      val tokenStr = client.getDelegationToken(owner, principal)
      val hive2Token = new Token[DelegationTokenIdentifier]()
      hive2Token.decodeFromUrlString(tokenStr)
      debug(s"Get Token from hive metastore: ${hive2Token.toString}")
      creds.addToken(uris, hive2Token)
    }
  }

  override def close(): Unit = clients.values.foreach(_.close())
}
