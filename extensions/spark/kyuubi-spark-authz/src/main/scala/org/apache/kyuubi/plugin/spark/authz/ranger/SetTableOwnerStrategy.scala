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

package org.apache.kyuubi.plugin.spark.authz.ranger

import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.SparkPlan
import org.slf4j.LoggerFactory

import org.apache.kyuubi.plugin.spark.authz.OperationType
import org.apache.kyuubi.plugin.spark.authz.serde.{CatalogTableTableExtractor, TABLE_COMMAND_SPECS, TableCommandSpec}
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils.{getAuthzUgi, getFieldVal, invokeAs, setFieldVal}

class SetTableOwnerStrategy(spark: SparkSession) extends Strategy {
  import SetTableOwnerStrategy._

  final private val LOG = LoggerFactory.getLogger(getClass)

  private val createTableOps = Set(
    OperationType.CREATETABLE.toString,
    OperationType.CREATETABLE_AS_SELECT.toString,
    OperationType.CREATEVIEW.toString)

  private val authzUser = getAuthzUgi(spark.sparkContext).getShortUserName

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    val tableCmdSpec = TABLE_COMMAND_SPECS.get(plan.getClass.getName)
    if (tableCmdSpec.isDefined
      && createTableOps.contains(tableCmdSpec.get.opType)
      && !plan.getTagValue(KYUUBI_AUTHZ_OWNER_TAG).contains(true)) {
      plan.setTagValue(KYUUBI_AUTHZ_OWNER_TAG, true)
      applyInternal(plan, tableCmdSpec.get)
    } else {
      Nil
    }
  }

  private def applyInternal(plan: LogicalPlan, spec: TableCommandSpec): Seq[SparkPlan] = {
    val outputTableDescs =
      spec.tableDescs.filterNot(desc => desc.isInput || desc.tableTypeDesc.exists(_.skip(plan)))

    outputTableDescs.flatMap { tableDesc =>
      try {
        tableDesc match {
          case desc if desc.fieldExtractor == classOf[CatalogTableTableExtractor].getSimpleName =>
            // v1 create table commands with CatalogTable field
            val table = invokeAs[CatalogTable](plan, desc.fieldName)
            setFieldVal(plan, desc.fieldName, table.copy(owner = authzUser))
            Nil

          case desc if desc.extract(plan).exists(_.catalog.isEmpty) =>
            // v1 create table commands without CatalogTable field
            desc.extract(plan, spark).map { table =>
              spark.sessionState.planner.plan(plan).map(exec =>
                SetTableOwnerExec(
                  exec,
                  TableIdentifier(table.table, table.database),
                  authzUser,
                  isReplaceTableCommand(plan))).toSeq
            }.getOrElse(Nil)

          case _ =>
            Nil
        }
      } catch {
        case e: Exception =>
          LOG.warn(tableDesc.error(plan, e))
          Nil
      }
    }
  }

  private def isReplaceTableCommand(plan: LogicalPlan): Boolean = plan.nodeName match {
    case "CreateViewCommand" =>
      getFieldVal[Boolean](plan, "replace")
    case _ =>
      false
  }
}

object SetTableOwnerStrategy {

  val KYUUBI_AUTHZ_OWNER_TAG = TreeNodeTag[Boolean]("__KYUUBI_AUTHZ_OWNER_TAG")
}
