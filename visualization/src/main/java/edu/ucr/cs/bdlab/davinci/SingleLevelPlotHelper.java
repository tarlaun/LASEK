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

import edu.ucr.cs.bdlab.beast.cg.Reprojector;
import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.geolite.EmptyGeometry;
import edu.ucr.cs.bdlab.beast.geolite.Feature;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.io.CSVEnvelopeDecoderJTS;
import edu.ucr.cs.bdlab.beast.io.CSVEnvelopeEncoder;
import edu.ucr.cs.bdlab.beast.io.FeatureReader;
import edu.ucr.cs.bdlab.beast.io.SpatialFileRDD;
import edu.ucr.cs.bdlab.beast.util.OperationParam;
import edu.ucr.cs.bdlab.beast.util.Parallel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.beast.CRSServer;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;


/**
 * @author Ahmed Eldawy
 *
 */
public class SingleLevelPlotHelper {

  /**The width of the image in pixels*/
  @OperationParam(
      description = "The width of the image in pixels",
      defaultValue = "1000"
  )
  public static final String ImageWidth = "width";
  /**The height of the image in pixels*/
  @OperationParam(
      description = "The height of the image in pixels",
      defaultValue = "1000"
  )
  public static final String ImageHeight = "height";

  /**Merge the partial images into one for single level plot*/
  @OperationParam(
      description = "Merges the output into a single image",
      defaultValue = "true"
  )
  public static final String MergeImages = "merge";

  private static final Log LOG = LogFactory.getLog(SingleLevelPlotHelper.class);

  /**
   * Creates and configures a plotter from the given Hadoop configuration. The given configuration should contain
   * at least the following configurations.
   * <ul>
   *   <li>{@link CommonVisualizationHelper#PlotterName} the short name of the plotter</li>
   *   <li>{@link SingleLevelPlotHelper#ImageWidth} the width of the image in pixels</li>
   *   <li>{@link SingleLevelPlotHelper#ImageHeight} the height of the image in pixels</li>
   *   <li>{@link CommonVisualizationHelper#InputMBR} the MBR of the input or the region to visualize</li>
   * </ul>
   * @param opts the configuration used to find the plotter
   * @return an instance of type Plotter as configured in the given configuration
   */
  public static Plotter getConfiguredPlotter(BeastOptions opts) {
    Envelope inputMBR = CSVEnvelopeDecoderJTS.instance.apply(opts.getString("mbr"), null);
    int imageWidth = opts.getInt(SingleLevelPlotHelper.ImageWidth, 1000);
    int imageHeight = opts.getInt(SingleLevelPlotHelper.ImageHeight, 1000);
    String plotterName = opts.getString(CommonVisualizationHelper.PlotterName);
    return Plotter.getConfiguredPlotter(plotterName, opts);
  }

  /**
   * Plots a file using a single-machine algorithm
   * @param inPath the path to the input
   * @param output the output stream to write the image to
   * @param plotterClass the class to use as a plotter
   * @param imageWidth the width of the desired image in pixels
   * @param imageHeight the height of the desired image in pixels
   * @param keepRatio whether to keep the aspect ratio of the input or not
   * @param conf environment configuration which is used to get the file system that opens the input file
   * @throws IOException if an error happens while reading the input
   */
  public static void plotLocal(Path inPath, DataOutputStream output,
                               final Class<? extends Plotter> plotterClass,
                               int imageWidth, int imageHeight,
                               boolean keepRatio,
                               final Configuration conf) throws IOException {
    final Envelope mbr = CSVEnvelopeDecoderJTS.instance.apply(conf.get("mbr"), null);

    // Use the given MBR to filter unnecessary data
    try {
      Plotter plotter = plotterClass.newInstance();
      plotter.setup(new BeastOptions(conf));
      Envelope filterMBR = new Envelope(mbr);
      filterMBR.expandBy(plotter.getBufferSize() * filterMBR.getWidth() / imageWidth,
          plotter.getBufferSize() * filterMBR.getHeight() / (double) imageHeight);
      conf.set(SpatialFileRDD.FilterMBR(), CSVEnvelopeEncoder.defaultEncoderJTS.apply(filterMBR));
    } catch (InstantiationException | IllegalAccessException e) {
      e.printStackTrace();
    }

    boolean mercator = conf.getBoolean(CommonVisualizationHelper.UseMercatorProjection, false);
    if (mercator) {
      // Convert the given MBR to the Mercator projection
      Envelope convertedMBR = Reprojector.reprojectEnvelope(mbr,
          FeatureReader.DefaultCRS, CommonVisualizationHelper.MercatorCRS);
      mbr.init(convertedMBR);
    }
    if (keepRatio) {
      // Adjust width and height to maintain aspect ratio and store the adjusted
      // values back in params in case the caller needs to retrieve them
      if (mbr.getWidth() / mbr.getHeight() > (double) imageWidth / imageHeight)
        imageHeight = (int) Math.round(mbr.getHeight() * imageWidth / mbr.getWidth());
      else
        imageWidth = (int) Math.round(mbr.getWidth() * imageHeight / mbr.getHeight());
    }
    // Store width and height in final variable to be able to access them in inner classes
    final int fwidth = imageWidth, fheight = imageHeight;

    // Start reading input file locally
    BeastOptions opts = new BeastOptions(conf);
    Class<? extends FeatureReader> featureReaderClass = SpatialFileRDD.getFeatureReaderClass(inPath.toString(), opts);
    SpatialFileRDD.FilePartition[] partitions = SpatialFileRDD.createPartitions(inPath.toString(), opts, conf);

    try {
      List<Canvas> partialCanvases = Parallel.forEach(partitions.length, (i1, i2) -> {
        try {
          Plotter plotter = plotterClass.newInstance();
          plotter.setup(new BeastOptions(conf));
          // Create the partial layer that will contain the plot of the assigned partitions
          Canvas partialCanvas = plotter.createCanvas(fwidth, fheight, mbr, 0);

          Reprojector.TransformationInfo transformationInfo = null;

          for (int i = i1; i < i2; i++) {
            Iterator<IFeature> features = SpatialFileRDD.readPartitionJ(partitions[i], featureReaderClass, opts);
            while (features.hasNext()) {
              IFeature feature = features.next();
              if (mercator && !feature.getGeometry().isEmpty()) {
                // Reproject the geometry of this feature to mercator space
                if (transformationInfo == null) {
                  // Initialize for the first time
                  transformationInfo = Reprojector.findTransformationInfo(feature.getGeometry().getSRID(),
                      CommonVisualizationHelper.MercatorCRS);
                }
                Geometry geometry = feature.getGeometry();
                if (CommonVisualizationHelper.MercatorMapBoundariesEnvelope.contains(geometry.getEnvelopeInternal())) {
                  // Geometry completely contained, no need to do any clipping
                } else if (CommonVisualizationHelper.MercatorMapBoundariesEnvelope.intersects(geometry.getEnvelopeInternal())) {
                  // Partially intersects, calculate the intersection
                  geometry = geometry.intersection(CommonVisualizationHelper.MercatorMapBoundariesPolygon);
                } else {
                  // Completely disjoint, the intersection is empty
                  geometry = EmptyGeometry.instance;
                }
                if (!geometry.isEmpty()) {
                  geometry = Reprojector.reprojectGeometry(geometry, transformationInfo);
                }
                if (geometry != feature.getGeometry())
                  feature = Feature.create(feature, geometry);
              }
              plotter.plot(partialCanvas, feature);
            }
          }
          return partialCanvas;
        } catch (IllegalAccessException e) {
          throw new RuntimeException(String.format("Cannot access the constructor of '%s'", plotterClass), e);
        } catch (InstantiationException e) {
          throw new RuntimeException(String.format("Error while instantiating '%s'", plotterClass), e);
        }
      });
      Plotter plotter = plotterClass.newInstance();
      plotter.setup(new BeastOptions(conf));

      // Whether we should vertically flip the final image or not
      boolean vflip = conf.getBoolean("vflip", true);
      LOG.info("Merging "+partialCanvases.size()+" partial canvases");
      // Create the final canvas that will contain the final image
      Canvas finalCanvas = plotter.createCanvas(fwidth, fheight, mbr, 0);
      for (Canvas partialCanvas : partialCanvases) {
        plotter.merge(finalCanvas, partialCanvas);
      }

      // Finally, write the resulting image to the given output path
      LOG.info("Writing final image");
      plotter.writeImage(finalCanvas, output, vflip);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(String.format("Cannot access the constructor of '%s'", plotterClass), e);
    } catch (InstantiationException e) {
      throw new RuntimeException(String.format("Error while instantiating '%s'", plotterClass), e);
    } catch (InterruptedException e) {
      throw new RuntimeException("Error while processing the file", e);
    }
  }
}
