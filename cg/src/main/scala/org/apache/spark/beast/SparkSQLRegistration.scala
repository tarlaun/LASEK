package org.apache.spark.beast

import edu.ucr.cs.bdlab.beast.sql._
import org.apache.spark.beast.sql.{EnvelopeDataType, GeometryDataType}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo
import org.apache.spark.sql.types.UDTRegistration
import org.locationtech.jts.geom.{Envelope, Geometry}

object SparkSQLRegistration {
  def registerUDT(): Unit = {
    // Register the Geometry user-defined data type
    if (!UDTRegistration.exists(classOf[Geometry].getName))
      UDTRegistration.register(classOf[Geometry].getName, classOf[GeometryDataType].getName)
    if (!UDTRegistration.exists(classOf[Envelope].getName))
      UDTRegistration.register(classOf[Envelope].getName, classOf[EnvelopeDataType].getName)
  }

  /**
   * Generate the SQL function name from the class
   *
   * @param function the function
   * @return the short name of the class
   */
  def getFunctionName(function: FunctionBuilder): String = function.getClass.getSimpleName.dropRight(1)

  /**Functions that are implemented as expressions due to their complexities*/
  val functions: Seq[FunctionBuilder] = Seq[FunctionBuilder](
    ST_CreateLinePolygon, ST_Extent, ST_Connect, ST_BreakLine, ST_CreateEnvelopeN, ST_CreateBox,
    SpatialPredicates.ST_Intersects, SpatialPredicates.ST_EnvelopeIntersects, ST_ToSpatialParquet, ST_FromSpatialParquet
  )

  def registerUDF(sparkSession: SparkSession): Unit = {
    functions.foreach(f => {
      val functionID = FunctionIdentifier(getFunctionName(f))
      if (!sparkSession.sessionState.functionRegistry.functionExists(functionID)) {
        val expressionInfo = new ExpressionInfo(f.getClass.getCanonicalName, functionID.database.orNull, functionID.funcName)
        sparkSession.sessionState.functionRegistry.registerFunction(functionID, expressionInfo, f)
      }
    })

    // Creation functions
    if (!sparkSession.sessionState.functionRegistry.functionExists(FunctionIdentifier("ST_CreatePoint")))
      sparkSession.udf.register("ST_CreatePoint", CreationFunctions.ST_CreatePoint)
    if (!sparkSession.sessionState.functionRegistry.functionExists(FunctionIdentifier("ST_FromWKT")))
      sparkSession.udf.register("ST_FromWKT", CreationFunctions.ST_FromWKT)
    if (!sparkSession.sessionState.functionRegistry.functionExists(FunctionIdentifier("ST_FromWKB")))
      sparkSession.udf.register("ST_FromWKB", CreationFunctions.ST_FromWKB)
    if (!sparkSession.sessionState.functionRegistry.functionExists(FunctionIdentifier("ST_ToWKB")))
      sparkSession.udf.register("ST_ToWKB", CreationFunctions.ST_ToWKB)
    if (!sparkSession.sessionState.functionRegistry.functionExists(FunctionIdentifier("ST_CreateLine")))
      sparkSession.udf.register("ST_CreateLine", CreationFunctions.ST_CreateLine)
    if (!sparkSession.sessionState.functionRegistry.functionExists(FunctionIdentifier("ST_CreatePolygon")))
      sparkSession.udf.register("ST_CreatePolygon", CreationFunctions.ST_CreatePolygon)

    // Analysis functions
    if (!sparkSession.sessionState.functionRegistry.functionExists(FunctionIdentifier("ST_ConvexHull")))
      sparkSession.udf.register("ST_ConvexHull", AnalysisFunctions.ST_ConvexHull)
    if (!sparkSession.sessionState.functionRegistry.functionExists(FunctionIdentifier("ST_Envelope")))
      sparkSession.udf.register("ST_Envelope", AnalysisFunctions.ST_Envelope)
    if (!sparkSession.sessionState.functionRegistry.functionExists(FunctionIdentifier("ST_Boundary")))
      sparkSession.udf.register("ST_Boundary", AnalysisFunctions.ST_Boundary)
    if (!sparkSession.sessionState.functionRegistry.functionExists(FunctionIdentifier("ST_Norm")))
      sparkSession.udf.register("ST_Norm", AnalysisFunctions.ST_Norm)
    if (!sparkSession.sessionState.functionRegistry.functionExists(FunctionIdentifier("ST_Reverse")))
      sparkSession.udf.register("ST_Reverse", AnalysisFunctions.ST_Reverse)
    if (!sparkSession.sessionState.functionRegistry.functionExists(FunctionIdentifier("ST_Centroid")))
      sparkSession.udf.register("ST_Centroid", AnalysisFunctions.ST_Centroid)
    if (!sparkSession.sessionState.functionRegistry.functionExists(FunctionIdentifier("ST_Union")))
      sparkSession.udf.register("ST_Union", AnalysisFunctions.ST_Union)
    if (!sparkSession.sessionState.functionRegistry.functionExists(FunctionIdentifier("ST_Difference")))
      sparkSession.udf.register("ST_Difference", AnalysisFunctions.ST_Difference)

    // Spatial predicate
    if (!sparkSession.sessionState.functionRegistry.functionExists(FunctionIdentifier("ST_IsValid")))
      sparkSession.udf.register("ST_IsValid", SpatialPredicates.ST_IsValid)
    if (!sparkSession.sessionState.functionRegistry.functionExists(FunctionIdentifier("ST_IsSimple")))
      sparkSession.udf.register("ST_IsSimple", SpatialPredicates.ST_IsSimple)
    if (!sparkSession.sessionState.functionRegistry.functionExists(FunctionIdentifier("ST_IsEmpty")))
      sparkSession.udf.register("ST_IsEmpty", SpatialPredicates.ST_IsEmpty)
    if (!sparkSession.sessionState.functionRegistry.functionExists(FunctionIdentifier("ST_IsRectangle")))
      sparkSession.udf.register("ST_IsRectangle", SpatialPredicates.ST_IsRectangle)
    if (!sparkSession.sessionState.functionRegistry.functionExists(FunctionIdentifier("ST_Intersects")))
      sparkSession.udf.register("ST_Intersects", SpatialPredicates.ST_Intersects)
    if (!sparkSession.sessionState.functionRegistry.functionExists(FunctionIdentifier("ST_Contains")))
      sparkSession.udf.register("ST_Contains", SpatialPredicates.ST_Contains)
    if (!sparkSession.sessionState.functionRegistry.functionExists(FunctionIdentifier("ST_Within")))
      sparkSession.udf.register("ST_Within", SpatialPredicates.ST_Within)
    if (!sparkSession.sessionState.functionRegistry.functionExists(FunctionIdentifier("ST_Disjoint")))
      sparkSession.udf.register("ST_Disjoint", SpatialPredicates.ST_Disjoint)
    if (!sparkSession.sessionState.functionRegistry.functionExists(FunctionIdentifier("ST_Overlaps")))
      sparkSession.udf.register("ST_Overlaps", SpatialPredicates.ST_Overlaps)
    if (!sparkSession.sessionState.functionRegistry.functionExists(FunctionIdentifier("ST_Covers")))
      sparkSession.udf.register("ST_Covers", SpatialPredicates.ST_Covers)
    if (!sparkSession.sessionState.functionRegistry.functionExists(FunctionIdentifier("ST_CoveredBy")))
      sparkSession.udf.register("ST_CoveredBy", SpatialPredicates.ST_CoveredBy)
    if (!sparkSession.sessionState.functionRegistry.functionExists(FunctionIdentifier("ST_Crosses")))
      sparkSession.udf.register("ST_Crosses", SpatialPredicates.ST_Crosses)
    if (!sparkSession.sessionState.functionRegistry.functionExists(FunctionIdentifier("ST_Touches")))
      sparkSession.udf.register("ST_Touches", SpatialPredicates.ST_Touches)
    if (!sparkSession.sessionState.functionRegistry.functionExists(FunctionIdentifier("ST_WithinDistance")))
      sparkSession.udf.register("ST_WithinDistance", SpatialPredicates.ST_WithinDistance)

    // Calculation functions
    if (!sparkSession.sessionState.functionRegistry.functionExists(FunctionIdentifier("ST_Area")))
      sparkSession.udf.register("ST_Area", CalculateFunctions.ST_Area)
    if (!sparkSession.sessionState.functionRegistry.functionExists(FunctionIdentifier("ST_Length")))
      sparkSession.udf.register("ST_Length", CalculateFunctions.ST_Length)
    if (!sparkSession.sessionState.functionRegistry.functionExists(FunctionIdentifier("ST_BoundaryDimension")))
      sparkSession.udf.register("ST_BoundaryDimension", CalculateFunctions.ST_BoundaryDimension)
    if (!sparkSession.sessionState.functionRegistry.functionExists(FunctionIdentifier("ST_Dimension")))
      sparkSession.udf.register("ST_Dimension", CalculateFunctions.ST_Dimension)
    if (!sparkSession.sessionState.functionRegistry.functionExists(FunctionIdentifier("ST_SRID")))
      sparkSession.udf.register("ST_SRID", CalculateFunctions.ST_SRID)
    if (!sparkSession.sessionState.functionRegistry.functionExists(FunctionIdentifier("ST_XMin")))
      sparkSession.udf.register("ST_XMin", CalculateFunctions.ST_XMin)
    if (!sparkSession.sessionState.functionRegistry.functionExists(FunctionIdentifier("ST_YMin")))
      sparkSession.udf.register("ST_YMin", CalculateFunctions.ST_YMin)
    if (!sparkSession.sessionState.functionRegistry.functionExists(FunctionIdentifier("ST_XMax")))
      sparkSession.udf.register("ST_XMax", CalculateFunctions.ST_XMax)
    if (!sparkSession.sessionState.functionRegistry.functionExists(FunctionIdentifier("ST_YMax")))
      sparkSession.udf.register("ST_YMax", CalculateFunctions.ST_YMax)
    if (!sparkSession.sessionState.functionRegistry.functionExists(FunctionIdentifier("ST_NumPoints")))
      sparkSession.udf.register("ST_NumPoints", CalculateFunctions.ST_NumPoints)
    if (!sparkSession.sessionState.functionRegistry.functionExists(FunctionIdentifier("ST_NumGeometries")))
      sparkSession.udf.register("ST_NumGeometries", CalculateFunctions.ST_NumGeometries)
    if (!sparkSession.sessionState.functionRegistry.functionExists(FunctionIdentifier("ST_GeometryN")))
      sparkSession.udf.register("ST_GeometryN", CalculateFunctions.ST_GeometryN)
    if (!sparkSession.sessionState.functionRegistry.functionExists(FunctionIdentifier("ST_GeometryType")))
      sparkSession.udf.register("ST_GeometryType", CalculateFunctions.ST_GeometryType)

  }
}
