/*
 * Copyright 2018 University of California, Riverside
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
package edu.ucr.cs.bdlab.beast.cg;

import edu.ucr.cs.bdlab.test.JavaSparkTest;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.impl.CoordinateArraySequenceFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SpatialJoinAlgorithmsTest extends JavaSparkTest {
  public void testPlaneSweepRectangles() throws IOException {
    double[][] coords1 = readCoordsResource("/test1.rect");
    double[][] coords2 = readCoordsResource("/test2.rect");
    double[][] minCoords1 = new double[][]{coords1[1], coords1[2]};
    double[][] maxCoords1 = new double[][]{coords1[3], coords1[4]};
    double[][] minCoords2 = new double[][]{coords2[1], coords2[2]};
    double[][] maxCoords2 = new double[][]{coords2[3], coords2[4]};
    List<String> expectedResults = Arrays.asList("1,9", "2,9", "3,10");
    int count = SpatialJoinAlgorithms.planeSweepRectangles(minCoords1, maxCoords1, minCoords2, maxCoords2, (i,j, refPoint) -> {
      String result = (int) coords1[0][i]+","+(int) coords2[0][j];
      assertTrue("Unexpected result pair "+result, expectedResults.indexOf(result) != -1);
    }, null);
    assertEquals(expectedResults.size(), count);
  }

  public void testSpatialJoinWithSimplification() throws IOException {
    int sides = 100;
    List<Geometry> r = new ArrayList<>();
    r.add(generatePolygon(sides));

    GeometryFactory factory = new GeometryFactory();
    List<Geometry> s = new ArrayList<>();
    s.add(factory.toGeometry(new Envelope(-1.0, 1.0, -1.0, 1.0)));
    s.add(factory.toGeometry(new Envelope(0.5, 1.0, 0.5, 1.0)));

    int numResults = SpatialJoinAlgorithms.spatialJoinIntersectsWithSimplification(r, s, 50, null, null);
    assertEquals(2, numResults);
  }

  public void testQuadSplit() {
    int sides = 100;
    Polygon p = generatePolygon(sides);
    List<Geometry> parts = SpatialJoinAlgorithms.quadSplit(p, 50);
    assertEquals(4, parts.size());
  }

  static Polygon generatePolygon(int sides) {
    CoordinateSequence cs = CoordinateArraySequenceFactory.instance().create(sides + 1, 2);
    for (int i = 0; i < sides; i++) {
      double angle = i * Math.PI * 2.0 / sides;
      cs.setOrdinate(i, 0, Math.cos(angle));
      cs.setOrdinate(i, 1, Math.sin(angle));
    }
    cs.setOrdinate(sides, 0, Math.cos(0));
    cs.setOrdinate(sides, 1, Math.sin(0));
    return (Polygon) (new GeometryFactory().createPolygon(cs));
  }
}