/*
 * Copyright 2022 University of California, Riverside
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
package edu.ucr.cs.bdlab.beast.io

import edu.ucr.cs.bdlab.beast.sql.ST_MBR
import edu.ucr.cs.bdlab.beast.sql.SpatialPredicates.{ST_EnvelopeIntersects, ST_Intersects}
import org.apache.spark.beast.sql.EnvelopeDataType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Literal, PredicateHelper}
import org.apache.spark.sql.catalyst.optimizer.ConstantFolding
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation

object PushDownSpatialFilter extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = {
    // 1- Apply filter-refine to add ST_EnvelopeIntersects where possible
    var intermediatePlan = plan.transform {
        case Filter(ST_Intersects(Seq(left, right)), DataSourceV2ScanRelation(relation, scan, output, kgp, ord)) if scan.isInstanceOf[SupportsSpatialFilterPushDown] =>
          Filter(ST_Intersects(Seq(left, right)), Filter(ST_EnvelopeIntersects(Seq(ST_MBR(Seq(left)), ST_MBR(Seq(right)))), DataSourceV2ScanRelation(relation, scan, output, kgp, ord)))
      }
    // 2- Apply constant folding to calculate the MBR of literals if possible
    intermediatePlan = ConstantFolding.apply(intermediatePlan)
    // 3- Apply filter push-down into the reader
    intermediatePlan.transform {
      case Filter(ST_EnvelopeIntersects(Seq(Literal(envelope, EnvelopeDataType), _)), DataSourceV2ScanRelation(relation, scan, output, kgp, ord)) if scan.isInstanceOf[SupportsSpatialFilterPushDown] =>
        scan.asInstanceOf[SupportsSpatialFilterPushDown].pushDownSpatialFilter(EnvelopeDataType.fromRow(envelope.asInstanceOf[InternalRow]))
        DataSourceV2ScanRelation(relation, scan, output, kgp, ord)
      case Filter(ST_EnvelopeIntersects(Seq(_, Literal(envelope, EnvelopeDataType))), DataSourceV2ScanRelation(relation, scan, output, kgp, ord)) if scan.isInstanceOf[SupportsSpatialFilterPushDown] =>
        scan.asInstanceOf[SupportsSpatialFilterPushDown].pushDownSpatialFilter(EnvelopeDataType.fromRow(envelope.asInstanceOf[InternalRow]))
        DataSourceV2ScanRelation(relation, scan, output, kgp, ord)
    }
  }
}
