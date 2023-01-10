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

package org.apache.kyuubi.plugin.spark.authz.serde

import java.util.ServiceLoader

import scala.collection.JavaConverters._

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.{DataWritingCommand, DataWritingCommandExec, ExecutedCommandExec, RunnableCommand}

import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils.{getAuthzUgi, getFieldVal}
import org.apache.kyuubi.plugin.spark.authz.util.WithInternalChild

trait TableOwnerRewriter {

  def key: String = getClass.getSimpleName

  def rewritePlan(spark: SparkSession, sparkPlan: SparkPlan, table: Table): SparkPlan
}

object TableOwnerRewriter {
  val tableOwnerRewriters: Map[String, TableOwnerRewriter] = {
    ServiceLoader.load(classOf[TableOwnerRewriter])
      .iterator()
      .asScala
      .map(e => (e.key, e))
      .toMap
  }
}

class RunnableCommandTableOwnerRewriter extends TableOwnerRewriter {

  protected def shouldRewrite(sparkPlan: SparkPlan, tableExists: Boolean): Boolean = {
    !tableExists
  }

  override def rewritePlan(spark: SparkSession, sparkPlan: SparkPlan, table: Table): SparkPlan = {
    val catalog = spark.sessionState.catalog
    val tableId = TableIdentifier(table.table, table.database)
    if (shouldRewrite(sparkPlan, catalog.tableExists(tableId))) {
      val exec = sparkPlan.asInstanceOf[ExecutedCommandExec]
      val newCmd =
        RewriteTableOwnerRunnableCommand(exec.cmd, tableId)
      sparkPlan.asInstanceOf[ExecutedCommandExec].copy(cmd = newCmd)
    } else {
      sparkPlan
    }
  }
}

class CreateViewCommandTableOwnerRewriter extends RunnableCommandTableOwnerRewriter {

  override protected def shouldRewrite(sparkPlan: SparkPlan, tableExists: Boolean): Boolean = {
    val exec = sparkPlan.asInstanceOf[ExecutedCommandExec]
    val isPermView = new ViewTypeTableTypeExtractor().apply(
      getFieldVal(exec.cmd, "viewType"),
      null) == TableType.PERMANENT_VIEW
    isPermView && (getFieldVal[Boolean](exec.cmd, "replace") || !tableExists)
  }
}

case class RewriteTableOwnerRunnableCommand(child: RunnableCommand, tableId: TableIdentifier)
  extends RunnableCommand with UnaryLike[LogicalPlan] with WithInternalChild {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val ret = child.run(sparkSession)

    val catalog = sparkSession.sessionState.catalog
    val metadata = catalog.getTableMetadata(tableId)
    val authzUser = getAuthzUgi(sparkSession.sparkContext).getShortUserName
    if (metadata.owner != authzUser) {
      catalog.alterTable(metadata.copy(owner = authzUser))
    }
    ret
  }

  override def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = {
    copy(child = newChild.asInstanceOf[RunnableCommand])
  }
}

class DataWritingCommandTableOwnerRewriter extends TableOwnerRewriter {

  override def rewritePlan(spark: SparkSession, sparkPlan: SparkPlan, table: Table): SparkPlan = {
    val catalog = spark.sessionState.catalog
    val tableId = TableIdentifier(table.table, table.database)
    if (!catalog.tableExists(tableId)) {
      val exec = sparkPlan.asInstanceOf[DataWritingCommandExec]
      val newCmd =
        RewriteTableOwnerDataWritingCommand(exec.cmd, tableId)
      sparkPlan.asInstanceOf[DataWritingCommandExec].copy(cmd = newCmd)
    } else {
      sparkPlan
    }
  }
}

case class RewriteTableOwnerDataWritingCommand(
    delegate: DataWritingCommand,
    tableId: TableIdentifier)
  extends DataWritingCommand with WithInternalChild {

  override def query: LogicalPlan = delegate.query

  override def outputColumnNames: Seq[String] = delegate.outputColumnNames

  override def run(sparkSession: SparkSession, child: SparkPlan): Seq[Row] = {
    val ret = delegate.run(sparkSession, child)

    val catalog = sparkSession.sessionState.catalog
    val metadata = catalog.getTableMetadata(tableId)
    val authzUser = getAuthzUgi(sparkSession.sparkContext).getShortUserName
    if (metadata.owner != authzUser) {
      catalog.alterTable(metadata.copy(owner = authzUser))
    }
    ret
  }

  override def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = {
    copy(delegate = newChild.asInstanceOf[DataWritingCommand])
  }
}
