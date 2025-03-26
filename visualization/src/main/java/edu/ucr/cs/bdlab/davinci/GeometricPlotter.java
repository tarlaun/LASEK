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
package edu.ucr.cs.bdlab.davinci;

import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeND;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.geolite.GeometryHelper;
import edu.ucr.cs.bdlab.beast.util.OperationParam;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Polygon;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.geom.AffineTransform;
import java.awt.image.AffineTransformOp;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.OutputStream;

/**
 * A plotter that draws the geometry of the features (e.g., the point location or polygon boundary)
 */
@Plotter.Metadata(
    shortname = "gplot",
    imageExtension = ".png",
    description = "A plotter that draws the geometric shape of objects on a raster canvas"
)
public class GeometricPlotter extends Plotter {

  @OperationParam(
      description = "The color of the stroke of the objects. Used for points, envelopes, lines, and polygons.",
      defaultValue = "black"
  )
  public static final String StrokeColor = "stroke";

  @OperationParam(
     description = "The color of the fill of the objects. Used for envelopes and polygons",
      defaultValue = "none"
  )
  public static final String FillColor = "fill";

  @OperationParam(
     description = "The size of points in pixels when there are few points in the canvas",
      defaultValue = "7"
  )
  public static final String PointSize = "pointsize";

  @OperationParam(
     description = "Use anitaliasing for plotting",
      defaultValue = "false"
  )
  public static final String Antialiasing = "antialias";

  @OperationParam(
      description = "Use pure Java library for graphics to avoid JVM crash on some system",
      defaultValue = "false",
      showInUsage = false
  )
  public static final String UsePureJavaGraphics = "Plotter.UsePureJavaGraphics";

  /**Color for the outer stroke or for points*/
  private int strokeColor;

  /**Color for the fill of polygons or envelopes*/
  private int fillColor;

  /**Size of a point in pixels*/
  private int pointsize;

  /**Whether to use antialiasing or not*/
  private boolean antialiasing;

  /**Use Pure Java Graphics to avoid JVM crash on some systems*/
  private boolean usePureJavaGraphics;

  @Override
  public void setup(BeastOptions opts) {
    super.setup(opts);
    this.strokeColor = CommonVisualizationHelper.getColor(opts.getString(StrokeColor, "Black")).getRGB();
    this.fillColor = CommonVisualizationHelper.getColor(opts.getString(FillColor, "None")).getRGB();
    this.pointsize = opts.getInt(PointSize, 7);
    this.antialiasing = opts.getBoolean(Antialiasing, false);
    this.usePureJavaGraphics = opts.getBoolean(UsePureJavaGraphics, false);
  }

  @Override
  public int getBufferSize() {
    return pointsize;
  }

  @Override
  public Canvas createCanvas(int width, int height, Envelope mbr, long tileID) {
    ImageCanvas imageCanvas = new ImageCanvas(mbr, width, height, tileID, pointsize);
    imageCanvas.setUsePureJavaGraphics(usePureJavaGraphics);
    return imageCanvas;
  }

  @Override
  public boolean plot(Canvas canvasLayer, IFeature shape) {
    return plotGeometry(canvasLayer, shape.getGeometry());
  }

  /**
   * Plots the given geometry to the canvas. The temporary arrays xs and ys are used for converting geometry
   * objects from model space to image space.
   * @param canvas the canvas to plot the geometry to
   * @param geom the geometry to plot
   * @return {@code true} if the canvas was updated as a result of this plot operation
   */
  protected boolean plotGeometry(Canvas canvas, Geometry geom) {
    ImageCanvas iCanvas = (ImageCanvas) canvas;
    Graphics g = iCanvas.getOrCreateGraphics(antialiasing);

    if (geom.isEmpty())
      return false;
    assert GeometryHelper.getCoordinateDimension(geom) == 2 :
        String.format("Cannot work with %d dimensions", GeometryHelper.getCoordinateDimension(geom));

    boolean changed = false;
    switch (geom.getGeometryType()) {
      case "Point":
        Coordinate c = geom.getCoordinate();
        int x = (int) canvas.transformX(c.x);
        int y = (int) canvas.transformY(c.y);
        plotPoint(iCanvas, g, x, y);
        changed = true;
        break;
      case "Envelope":
        EnvelopeND env = (EnvelopeND) geom;
        int x1, y1, x2, y2;
        x1 = (int) canvas.transformX(env.getMinCoord(0));
        y1 = (int) canvas.transformY(env.getMinCoord(1));
        x2 = (int) canvas.transformX(env.getMaxCoord(0));
        y2 = (int) canvas.transformY(env.getMaxCoord(1));
        if ((fillColor & 0xFF000000) != 0) {
          g.setColor(new Color(fillColor, true));
          g.fillRect(x1, y1, x2 - x1, y2 - y1);
          changed = true;
        }
        if ((strokeColor& 0xFF000000) != 0) {
          g.setColor(new Color(strokeColor, true));
          g.drawRect(x1, y1, x2 - x1 - 1, y2 - y1 - 1);
        }
        changed = changed || x1 >= 0 || y1 >= 0 || x2 < canvas.getWidth() || y2 < canvas.getHeight();
        break;
      case "LineString":
      case "LinearRing":
        LineString lineString = (LineString) geom;
        changed = plotLineStringOrRing(canvas, g, lineString, false) || changed;
        break;
      case "Polygon":
        Polygon polygon = (Polygon) geom;
        if (polygon.isRectangle()) {
          Envelope e = polygon.getEnvelopeInternal();
          x1 = (int) canvas.transformX(e.getMinX());
          y1 = (int) canvas.transformY(e.getMinY());
          x2 = (int) canvas.transformX(e.getMaxX());
          y2 = (int) canvas.transformY(e.getMaxY());
          if ((fillColor & 0xFF000000) != 0) {
            g.setColor(new Color(fillColor, true));
            g.fillRect(x1, y1, x2 - x1, y2 - y1);
            changed = true;
          }
          if ((strokeColor & 0xFF000000) != 0) {
            g.setColor(new Color(strokeColor, true));
            g.drawRect(x1, y1, x2 - x1 - 1, y2 - y1 - 1);
          }
          changed = changed || x1 >= 0 || y1 >= 0 || x2 < canvas.getWidth() || y2 < canvas.getHeight();
        } else {
          boolean fill = (fillColor& 0xFF000000) != 0;
          if (fill && polygon.getNumInteriorRing() > 0) {
            // Special handling for filled polygons with holes
            // Plot the polygon on a separate image and clear the holes there. Then merge with this image
            BufferedImage tempImage = new BufferedImage(canvas.getWidth(), canvas.getHeight(), BufferedImage.TYPE_INT_ARGB);
            Graphics2D g2 = tempImage.createGraphics();
            // Not a filled polygon or no holes. This is easier to handle
            changed = plotLineStringOrRing(canvas, g2, polygon.getExteriorRing(), true) || changed;
            g2.setComposite(AlphaComposite.getInstance(AlphaComposite.CLEAR, 0.0f));
            for (int iRing = 0; iRing < polygon.getNumInteriorRing(); iRing++)
              changed = plotLineStringOrRing(canvas, g2, polygon.getInteriorRingN(iRing), true) || changed;
            g2.dispose();
            g.drawImage(tempImage, 0, 0, null);
          } else {
            // Not a filled polygon or no holes. This is easier to handle
            changed = plotLineStringOrRing(canvas, g, polygon.getExteriorRing(), fill) || changed;
            for (int iRing = 0; iRing < polygon.getNumInteriorRing(); iRing++)
              changed = plotLineStringOrRing(canvas, g, polygon.getInteriorRingN(iRing), fill) || changed;
          }
        }
        break;
      case "MultiLineString":
      case "MultiPolygon":
      case "MultiPoint":
      case "GeometryCollection":
        GeometryCollection geomCollection = (GeometryCollection) geom;
        for (int iGgeom = 0; iGgeom < geomCollection.getNumGeometries(); iGgeom++)
          changed = plotGeometry(canvas, (geomCollection.getGeometryN(iGgeom))) || changed;
        break;
      default:
        throw new RuntimeException(String.format("Cannot plot geometries of type '%s'", geom.getGeometryType()));
    }
    return changed;
  }

  private boolean plotLineStringOrRing(Canvas canvas, Graphics g, LineString lineString, boolean fill) {
    boolean canvasChanged = false;
    int x;
    int y;
    int arraySize = 0;
    // The mask of a point has four bits set as described below.
    // Bit #0: If set, it means that the point is outside the canvas from the right
    // Bit #1: If set, it means that the point is outside the canvas from the left
    // Bit #2: If set, it means that the point is outside the canvas from the top
    // Bit #3: If set, it means that the point is outside the canvas from the bottom
    // If two consecutive points have the same bit set (whatever the bit is), it indicates a line segment that
    // is not visible on the image.
    int prevMask = 0;
    int[] xs = new int[Math.min(1000, lineString.getNumPoints())];
    int[] ys = new int[Math.min(1000, lineString.getNumPoints())];
    for (int iPoint = 0; iPoint < lineString.getNumPoints(); iPoint++) {
      Coordinate tempPoint= lineString.getCoordinateN(iPoint);
      x = (int) canvas.transformX(tempPoint.getX());
      y = (int) canvas.transformY(tempPoint.getY());
      int mask = 0;
      if (x < 0) mask |= 1;
      if (x > canvas.getWidth()) mask |= 2;
      if (y < 0) mask |= 4;
      if (y > canvas.getHeight()) mask |= 8;
      if (!fill && (mask & prevMask) != 0) {
        // Line segment not visible. Plot whatever we have right now and only add the last point
        if (arraySize > 1) {
          drawPolyline(xs, ys, g, arraySize, fill);
          canvasChanged = true;
        }
        xs[0] = x;
        ys[0] = y;
        arraySize = 1;
      } else {
        // Segment might be visible
        xs[arraySize] = x;
        ys[arraySize] = y;
        if (arraySize == 0 || xs[arraySize] != xs[arraySize-1] || ys[arraySize] != ys[arraySize-1])
          arraySize++;
      }
      if (arraySize == xs.length) {
        // Plot the current list of points and reset the buffer
        drawPolyline(xs, ys, g, arraySize, fill);
        canvasChanged = true;
        xs[0] = xs[arraySize - 1];
        ys[0] = ys[arraySize - 1];
        arraySize = 1;
      }
      prevMask = mask;
    }
    if (arraySize > 1) {
      drawPolyline(xs, ys, g, arraySize, fill);
      canvasChanged = true;
    }
    return canvasChanged;
  }

  private void drawPolyline(int[] xs, int[] ys, Graphics g, int numPoints, boolean fill) {
    if (fill) {
      g.setColor(new Color(fillColor, true));
      g.fillPolygon(xs, ys, numPoints);
    }
    g.setColor(new Color(strokeColor, true));
    g.drawPolyline(xs, ys, numPoints);
  }

  @Override
  public Class<? extends Canvas> getCanvasClass() {
    return ImageCanvas.class;
  }

  @Override
  public Canvas merge(Canvas finalLayer, Canvas intermediateLayer) {
    ImageCanvas finalLayer1 = (ImageCanvas) finalLayer;
    ImageCanvas intermediateLayer1 = (ImageCanvas) intermediateLayer;
    finalLayer1.mergeWith(intermediateLayer1);
    return finalLayer;
  }

  protected void plotPoint(ImageCanvas canvas, Graphics g, int x, int y) {
    // If pointsize is zero, then just plot it as a single pixel
    if (pointsize == 0) {
      g.setColor(new Color(strokeColor, true));
      g.fillRect(x, y, 1, 1);
      return;
    }
    // Mark the corresponding pixel in the bitmap as occupied
    canvas.setPixelOccupied1(x, y);
  }

  protected void plotAllRemainingPoints(ImageCanvas canvas, Graphics g) {
    // Consider only pixel locations that have exactly one point. Plot a circle centered at that point depending
    // on how much space is available around it.
    g.setColor(new Color(strokeColor, true));
    for (int x = -canvas.buffer; x < canvas.getWidth() + canvas.buffer; x++) {
      for (int y = -canvas.buffer; y < canvas.getHeight() + canvas.buffer; y++) {
        if (canvas.getPixelOccupied1(x, y)) {
          // Exactly only point at that location.
          // Calculate the empty buffer around it
          int emptyBufferSize = 0;
          boolean isBufferClear = true;
          while (isBufferClear && emptyBufferSize <= pointsize) {
            emptyBufferSize++;
            for (int dx = -emptyBufferSize; isBufferClear && dx <= emptyBufferSize; dx++) {
              for (int dy = -emptyBufferSize; isBufferClear && dy <= emptyBufferSize; dy++) {
                if (dx != 0 || dy != 0)
                  isBufferClear = !canvas.getPixelOccupied1(x + dx, y + dy);
              }
            }
            if (!isBufferClear)
              emptyBufferSize--;
          }
          emptyBufferSize /= 2;
          if (emptyBufferSize == 0)
            g.fillRect(x, y, 1,  1);
          else
            g.fillOval(x - emptyBufferSize, y - emptyBufferSize, emptyBufferSize * 2 + 1, emptyBufferSize * 2 + 1);
        }
      }
    }
  }

  @Override
  public void writeImage(Canvas layer, OutputStream out, boolean vflip) throws IOException {
    BufferedImage finalImage;
    ImageCanvas imageCanvas = (ImageCanvas) layer;
    if (pointsize > 0) {
      Graphics g = imageCanvas.getOrCreateGraphics(antialiasing);
      plotAllRemainingPoints(imageCanvas, g);
    }
    finalImage = imageCanvas.getImage();
    // Flip image vertically if needed
    if (vflip) {
      AffineTransform tx = AffineTransform.getScaleInstance(1, -1);
      tx.translate(0, -finalImage.getHeight());
      AffineTransformOp op = new AffineTransformOp(tx, AffineTransformOp.TYPE_NEAREST_NEIGHBOR);
      finalImage = op.filter(finalImage, null);
    }
    ImageIO.write(finalImage, "png", out);
  }
}
