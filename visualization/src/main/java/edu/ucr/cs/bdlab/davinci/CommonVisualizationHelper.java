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
package edu.ucr.cs.bdlab.davinci;

import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.io.FeatureReader;
import edu.ucr.cs.bdlab.beast.util.IConfigurable;
import edu.ucr.cs.bdlab.beast.util.OperationParam;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import scala.Option;

import java.awt.*;
import java.lang.reflect.Field;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Some helper functions for visualization
 */
public class CommonVisualizationHelper implements IConfigurable {

  /**The minimum bounding rectangle (MBR) of the input space or the region to visualize*/
  @OperationParam(
      description = "The MBR of the input or the area to visualized"
  )
  public static final String InputMBR = "mbr";
  /**Keep aspect ratio of the input*/
  @OperationParam(
      description = "Makes the aspect ratio of the image similar to that of the input",
      defaultValue = "true"
  )
  public static final String KeepRatio = "keepratio";
  /**Flip the final image vertically*/
  @OperationParam(
      description = "Flip the final image vertically",
      defaultValue = "true"
  )
  public static final String VerticalFlip = "vflip";

  /**The configuration entry for the plotter class name*/
  public static final String PlotterClassName = "Visualization.PlotterClassName";

  /**Type of plotter to use (short user-friendly name)*/
  @OperationParam(
      description = "Type of plotter to use",
      required = true
  )
  public static final String PlotterName = "plotter";

  /**Instruct the visualization module to apply the Mercator projection on the fly*/
  @OperationParam(
      description = "Reproject the geometries to the Web Mercator projection before visualization",
      defaultValue = "false"
  )
  public static final String UseMercatorProjection = "mercator";

  /**The maximum value for latitude in radians in the Web Mercator projection*/
  public static final double MercatorMaxLatitudeRadians = 2 * Math.atan(Math.exp(Math.PI)) - Math.PI / 2;
  public static final double MercatorMaxLatitudeDegrees = Math.toDegrees(MercatorMaxLatitudeRadians);
  public static final Envelope MercatorMapBoundariesEnvelope = new Envelope(-180, +180, -MercatorMaxLatitudeDegrees, +MercatorMaxLatitudeDegrees);
  public static final Polygon MercatorMapBoundariesPolygon = (Polygon)(FeatureReader.DefaultGeometryFactory.toGeometry(MercatorMapBoundariesEnvelope));
  public static final CoordinateReferenceSystem MercatorCRS;

  static {
    try {
      MercatorCRS = CRS.decode("EPSG:3857", true);
    } catch (FactoryException e) {
      throw new RuntimeException("Could not create web mercator CRS", e);
    }
  }

  public static Color getColor(String color) {
    color = color.toLowerCase();
    try {
      Field field = Color.class.getField(color);
      if (field != null)
        return (Color) field.get(Color.class);
    } catch (NoSuchFieldException e) {
    } catch (IllegalAccessException e) {
    }
    if (color.equals("none"))
      return new Color(0, true);
    final Pattern HTMLColorRegex = Pattern.compile("#[0-9a-f]{3,8}", Pattern.CASE_INSENSITIVE);
    Matcher matcher = HTMLColorRegex.matcher(color);
    if (matcher.matches()) {
      // Handle colors written in HTML formats (e.g., #rgb or #rrggbb)
      int r,g,b, a = 255;
      if (color.length() == 4 || color.length() == 5) {
        // #rgb format
        r = Integer.parseInt(color.substring(1, 2) + color.substring(1, 2), 16);
        g = Integer.parseInt(color.substring(2, 3) + color.substring(2, 3), 16);
        b = Integer.parseInt(color.substring(3, 4) + color.substring(3, 4), 16);
        if (color.length() == 5)
          a = Integer.parseInt(color.substring(4, 5) + color.substring(4, 5), 16);
      } else if (color.length() == 7 || color.length() == 9) {
        // #rrggbb format
        r = Integer.parseInt(color.substring(1, 3), 16);
        g = Integer.parseInt(color.substring(3, 5), 16);
        b = Integer.parseInt(color.substring(5, 7), 16);
        if (color.length() == 9)
          a = Integer.parseInt(color.substring(7, 9), 16);
      } else {
        throw new RuntimeException(String.format("Cannot parse color '%s' as hexadecimal", color));
      }
      return new Color(r, g, b, a);
    }
    return null;
  }

  /**The maximum latitude (in degrees) that is supported in Web Mercator*/
  public static final double WebMercatorMaxLatitude = Math.toDegrees(2 * Math.atan(Math.exp(Math.PI)) - Math.PI / 2.0);

  @Override
  public void addDependentClasses(BeastOptions opts, Stack<Class<?>> parameterClasses) {
    Option<String> plotterName = opts.get(PlotterName);
    if (plotterName.isEmpty())
      return;
    Class<? extends Plotter> plotterClass = Plotter.plotters.get(plotterName.get());
    if (plotterClass != null)
      parameterClasses.push(plotterClass);
  }

}
