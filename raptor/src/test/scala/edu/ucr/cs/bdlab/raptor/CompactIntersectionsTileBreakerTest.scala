package edu.ucr.cs.bdlab.raptor

import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CompactIntersectionsTileBreakerTest extends AnyFunSuite with ScalaSparkTest {

  test("Iterate over two intersections") {
    val intersections1 = new Intersections()
    intersections1.numIntersections = 8
    intersections1.tileID = Array[Int](1, 1, 1, 1, 2, 2, 2, 4)
    intersections1.ys = Array[Int](2, 2, 3, 3, 2, 3, 4, 3)
    intersections1.xs = Array[Int](1, 2, 4, 5, 1, 2, 4, 5, 1, 3, 1, 2, 1, 1, 3, 3)
    intersections1.polygonIndexes = Array[Int](1, 1, 2, 2, 1, 1, 2, 1)
    intersections1.featureIDs = Array[Long](1, 2, 3)

    val intersections2 = new Intersections()
    intersections2.numIntersections = 3
    intersections2.tileID = Array[Int](1, 1, 2)
    intersections2.ys = Array[Int](1, 2, 1)
    intersections2.xs = Array[Int](2, 3, 2, 3, 3, 4)
    intersections2.polygonIndexes = Array[Int](1, 2, 1)
    intersections2.featureIDs = Array[Long](1, 2, 3)

    val breaker = new CompactIntersectionsTileBreaker(Array((1, intersections1), (2, intersections2)).iterator)
    var count = 0
    for (intersections <- breaker) {
      count += 1
      if (count == 1)
        assertResult(0x100000001L)(intersections._1)
      else if (count == 2)
        assertResult(0x100000002L)(intersections._1)
      else if (count == 4)
        assertResult(0x200000001L)(intersections._1)
    }
    assertResult(5)(count)
  }
}
