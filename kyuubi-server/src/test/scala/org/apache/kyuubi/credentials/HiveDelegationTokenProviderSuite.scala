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

import java.io.{File, FileOutputStream}
import java.net.{URI, URLClassLoader}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars._
import org.apache.hadoop.hive.metastore.{HiveMetaException, HiveMetaStore}
import org.apache.hadoop.hive.thrift.{DelegationTokenIdentifier, HadoopThriftAuthBridge, HadoopThriftAuthBridge23}
import org.apache.hadoop.io.Text
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.thrift.TProcessor
import org.apache.thrift.protocol.TProtocol
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.kyuubi.{KerberizedTestHelper, Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.credentials.LocalMetaServer.defaultHiveConf

class HiveDelegationTokenProviderSuite extends KerberizedTestHelper {

  private val hadoopConfDir: File = Utils.createTempDir().toFile
  private var hiveConf: HiveConf = _
  private var extraMetaStoreUri: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    tryWithSecurityEnabled {
      UserGroupInformation.loginUserFromKeytab(testPrincipal, testKeytab)

      // HiveMetaStore creates a new hadoop `Configuration` object for each request to verify
      // whether user can impersonate others.
      // So we create a URLClassLoader with core-site.xml and set it as the thread's context
      // classloader when creating `Configuration` object.
      val conf = new Configuration(false)
      conf.set("hadoop.security.authentication", "kerberos")
      val realUser = UserGroupInformation.getCurrentUser.getShortUserName
      conf.set(s"hadoop.proxyuser.$realUser.groups", "*")
      conf.set(s"hadoop.proxyuser.$realUser.hosts", "*")

      val xml = new File(hadoopConfDir, "core-site.xml")
      val os = new FileOutputStream(xml)
      try {
        conf.writeXml(os)
      } finally {
        os.close()
      }

      val classloader =
        new URLClassLoader(
          Array(hadoopConfDir.toURI.toURL),
          classOf[Configuration].getClassLoader)

      hiveConf = LocalMetaServer.defaultHiveConf(20201)
      hiveConf.addResource(conf)
      hiveConf.setVar(METASTORE_USE_THRIFT_SASL, "true")
      hiveConf.setVar(METASTORE_KERBEROS_PRINCIPAL, testPrincipal)
      hiveConf.setVar(METASTORE_KERBEROS_KEYTAB_FILE, testKeytab)
      val metaServer = new LocalMetaServer(hiveConf, classloader)
      metaServer.start()

      val extraHiveConf = LocalMetaServer.defaultHiveConf(20202)
      extraHiveConf.addResource(conf)
      extraHiveConf.setVar(METASTORE_USE_THRIFT_SASL, "true")
      extraHiveConf.setVar(METASTORE_KERBEROS_PRINCIPAL, testPrincipal)
      extraHiveConf.setVar(METASTORE_KERBEROS_KEYTAB_FILE, testKeytab)
      val extraMetaServer = new LocalMetaServer(extraHiveConf, classloader)
      extraMetaServer.start()
      extraMetaStoreUri = extraHiveConf.getVar(METASTOREURIS)
    }
  }

  override def afterAll(): Unit = {
    FileUtils.deleteDirectory(hadoopConfDir)
  }

  test("obtain hive delegation token") {
    tryWithSecurityEnabled {
      UserGroupInformation.loginUserFromKeytab(testPrincipal, testKeytab)

      val kyuubiConf = new KyuubiConf(false)
        .set(KyuubiConf.CREDENTIALS_HIVE_METASTORE_URIS, Seq(extraMetaStoreUri))
      val provider = new HiveDelegationTokenProvider
      provider.initialize(hiveConf, kyuubiConf)
      assert(provider.delegationTokensRequired())

      val owner = "who"
      val credentials = new Credentials
      provider.obtainDelegationTokens(owner, credentials)

      val tokens = credentials.getAllTokens.asScala
        .filter(_.getKind == DelegationTokenIdentifier.HIVE_DELEGATION_KIND)
        .toSeq
      assert(tokens.size == 2)

      tokens.foreach { token =>
        val tokenIdent = token.decodeIdentifier().asInstanceOf[DelegationTokenIdentifier]
        assertResult(DelegationTokenIdentifier.HIVE_DELEGATION_KIND)(token.getKind)
        assertResult(new Text(owner))(tokenIdent.getOwner)
        val currentUserName = UserGroupInformation.getCurrentUser.getUserName
        assertResult(new Text(currentUserName))(tokenIdent.getRealUser)
      }
    }
  }
}

class LocalMetaServer(
    hiveConf: HiveConf = defaultHiveConf(20201),
    serverContextClassLoader: ClassLoader)
    extends Logging {

  def start(): Unit = {
    val startLock = new ReentrantLock
    val startCondition = startLock.newCondition
    val startedServing = new AtomicBoolean(false)
    val startFailed = new AtomicBoolean(false)

    Future {
      try {
        val uri = new URI(HiveConf.getTrimmedStringsVar(hiveConf, METASTOREURIS)(0))
        HiveMetaStore.startMetaStore(
          uri.getPort,
          new HadoopThriftAuthBridgeWithServerContextClassLoader(
            serverContextClassLoader),
          hiveConf,
          startLock,
          startCondition,
          startedServing)
      } catch {
        case t: Throwable =>
          error("Failed to start LocalMetaServer", t)
          startFailed.set(true)
      }
    }

    eventually(timeout(30.seconds), interval(100.milliseconds)) {
      assert(startedServing.get() || startFailed.get())
    }

    if (startFailed.get()) {
      throw new HiveMetaException("Failed to start LocalMetaServer")
    }
  }

  def getHiveConf: HiveConf = hiveConf
}

object LocalMetaServer {

  def defaultHiveConf(port: Int): HiveConf = {
    val hiveConf = new HiveConf()
    hiveConf.setVar(METASTOREURIS, "thrift://localhost:" + port)
    hiveConf.setVar(METASTORE_SCHEMA_VERIFICATION, "false")
    hiveConf.set("datanucleus.schema.autoCreateTables", "true")
    hiveConf
  }

}

class HadoopThriftAuthBridgeWithServerContextClassLoader(classloader: ClassLoader)
    extends HadoopThriftAuthBridge23 {

  override def createServer(
      keytabFile: String,
      principalConf: String): HadoopThriftAuthBridge.Server = {
    new Server(keytabFile, principalConf)
  }

  class Server(keytabFile: String, principalConf: String)
      extends HadoopThriftAuthBridge.Server(keytabFile, principalConf) {

    override def wrapProcessor(processor: TProcessor): TProcessor = {
      new SetThreadContextClassLoaderProcess(super.wrapProcessor(processor))
    }

  }

  class SetThreadContextClassLoaderProcess(wrapped: TProcessor) extends TProcessor {

    override def process(in: TProtocol, out: TProtocol): Boolean = {
      val origin = Thread.currentThread().getContextClassLoader
      try {
        Thread.currentThread().setContextClassLoader(classloader)
        wrapped.process(in, out)
      } finally {
        Thread.currentThread().setContextClassLoader(origin)
      }
    }

  }

}
