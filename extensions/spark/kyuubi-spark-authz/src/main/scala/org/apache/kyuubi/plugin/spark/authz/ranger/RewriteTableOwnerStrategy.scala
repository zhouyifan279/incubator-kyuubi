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
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.SparkPlan
import org.slf4j.LoggerFactory

import org.apache.kyuubi.plugin.spark.authz.ranger.RewriteTableOwnerStrategy.KYUUBI_REWRITE_OWNER_TAG
import org.apache.kyuubi.plugin.spark.authz.serde.{Table, TABLE_COMMAND_SPECS, TableDesc, TableOwnerRewriter}

class RewriteTableOwnerStrategy(spark: SparkSession) extends Strategy {
  final private val LOG = LoggerFactory.getLogger(getClass)

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    val spec = TABLE_COMMAND_SPECS.get(plan.getClass.getName)
    val table = spec.flatMap(
      _.tableDescs
        .filterNot(_.isInput)
        .flatMap(desc => safeExtract(desc, plan).toSeq)
        .headOption)
    val rewriterName = spec.flatMap(_.ownerRewriter)
    if (table.isDefined
      && rewriterName.nonEmpty
      && !plan.getTagValue(KYUUBI_REWRITE_OWNER_TAG).contains(true)) {
      // Let SparkPlanner skip current strategy in `applyInternal`
      plan.setTagValue(KYUUBI_REWRITE_OWNER_TAG, true)
      val ret = applyInternal(plan, table.get, rewriterName.get)
      plan.unsetTagValue(KYUUBI_REWRITE_OWNER_TAG)
      ret
    } else {
      Nil
    }
  }

  private def safeExtract(tableDesc: TableDesc, plan: LogicalPlan): Option[Table] = {
    try {
      tableDesc.extract(plan)
    } catch {
      case e: Exception =>
        LOG.warn(tableDesc.error(plan, e))
        None
    }
  }

  private def applyInternal(
      plan: LogicalPlan,
      table: Table,
      rewriterName: String): Seq[SparkPlan] = {
    val ret = spark.sessionState.planner.plan(plan).toSeq
    TableOwnerRewriter.tableOwnerRewriters.get(rewriterName) match {
      case Some(rewriter) =>
        ret.map(exec => rewriter.rewritePlan(spark, exec, table))
      case None =>
        ret
    }
  }
}

object RewriteTableOwnerStrategy {
  val KYUUBI_REWRITE_OWNER_TAG = TreeNodeTag[Boolean]("__KYUUBI_REWRITE_OWNER_TAG")
}
