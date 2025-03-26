/*
 * Copyright 2020 University of California, Riverside
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
package edu.ucr.cs.bdlab

import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypesMixin
import edu.ucr.cs.bdlab.beast.indexing.IndexMixin
import edu.ucr.cs.bdlab.beast.io.ReadWriteMixin
import edu.ucr.cs.bdlab.davinci.VisualizationMixin
import edu.ucr.cs.bdlab.raptor.RaptorMixin
import org.apache.spark.beast.SparkSQLRegistration
import org.apache.spark.sql.SparkSession

/**
 * Contains implicit conversions that simplify the access to spatial functions in Beast.
 * To use it, add the following command to your Scala program.
 *
 * import edu.ucr.cs.bdlab.beast._
 */
package object beast extends ReadWriteMixin
  with SpatialOperationsMixin
  with SpatialDataTypesMixin
  with IndexMixin
  with VisualizationMixin
  with RaptorMixin