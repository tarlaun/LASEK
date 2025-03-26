package edu.ucr.cs.bdlab.raptor;

import java.awt.geom.AffineTransform;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * This exception is thrown when the problem with reading the GeoTiff file has to do with
 * constructing either the raster to model transform, or the coordinate system. A GeoTiffException:
 *
 *
 * <ul>
 *   <li>encapsulates the salient information in the GeoTiff tags, making the values available as
 *       read only properties.</li>
 *   <li>sends the appropriate log message to the log stream</li>
 *   <li>produces a readable message property for later retrieval</li>
 * </ul>
 *
 * <p>This exception is expected to be thrown when there is absolutely nothing wrong with the
 * GeoTiff file which produced it. In this case, the exception is reporting an unsupported
 * coordinate system description or raster to model transform, or some other unrecognized
 * configuration of the GeoTIFFWritingUtilities tags. By doing so, it attempts to record enough
 * information so that the maintainers can support it in the future.</p>
 *
 * @author Bryce Nordgren / USDA Forest Service
 * @author Simone Giannecchini
 */
public final class GeoTiffException extends IOException {

    /** */
    private static final long serialVersionUID = 1008533682021487024L;

    private GeoTiffMetadata metadata = null;

    /**
     * Constructs an instance of <code>GeoTiffException</code> with the specified detail message.
     *
     * @param metadata The metadata from the GeoTIFFWritingUtilities image causing the error.
     * @param msg the detail message.
     * @param t the underlying exception to include
     */
    public GeoTiffException(GeoTiffMetadata metadata, String msg, Throwable t) {
        super(msg);
        this.metadata = metadata;
        if (t != null) this.initCause(t);
    }

    /**
     * Getter for property modelTransformation.
     *
     * @return Value of property modelTransformation.
     */
    public AffineTransform getModelTransformation() {
        if (metadata != null) return metadata.getModelTransformation();
        return null;
    }

    /**
     * Getter for property geoKeys.
     *
     * @return Value of property geoKeys.
     */
    public GeoKeyEntry[] getGeoKeys() {
        return metadata != null
                ? metadata.getGeoKeys().toArray(new GeoKeyEntry[metadata.getGeoKeys().size()])
                : null;
    }

    public String getMessage() {
        final StringWriter text = new StringWriter(1024);
        final PrintWriter message = new PrintWriter(text);

        // Header
        message.println("GEOTIFF Module Error Report");

        // start with the message the user specified
        message.println(super.getMessage());

        // do the model pixel scale tags
        message.print("ModelPixelScaleTag: ");
        if (metadata != null) {
            final PixelScale modelPixelScales = metadata.getModelPixelScales();
            if (modelPixelScales != null) {
                message.println(
                        "["
                                + modelPixelScales.getScaleX()
                                + ","
                                + modelPixelScales.getScaleY()
                                + ","
                                + modelPixelScales.getScaleZ()
                                + "]");
            } else {
                message.println("NOT AVAILABLE");
            }
        } else message.println("NOT AVAILABLE");

        // do the model tie point tags
        message.print("ModelTiePointTag: ");
        if (metadata != null) {
            final TiePoint[] modelTiePoints = metadata.getModelTiePoints();
            if (modelTiePoints != null) {
                final int numTiePoints = modelTiePoints.length;
                message.println("(" + (numTiePoints) + " tie points)");
                for (int i = 0; i < (numTiePoints); i++) {
                    message.print("TP #" + i + ": ");
                    message.print("[" + modelTiePoints[i].getValueAt(0));
                    message.print("," + modelTiePoints[i].getValueAt(1));
                    message.print("," + modelTiePoints[i].getValueAt(2));
                    message.print("] -> [" + modelTiePoints[i].getValueAt(3));
                    message.print("," + modelTiePoints[i].getValueAt(4));
                    message.println("," + modelTiePoints[i].getValueAt(5) + "]");
                }
            } else message.println("NOT AVAILABLE");
        } else message.println("NOT AVAILABLE");

        // do the transformation tag
        message.print("ModelTransformationTag: ");

        AffineTransform modelTransformation = getModelTransformation();

        if (modelTransformation != null) {
            message.println("[");

            message.print(" [" + modelTransformation.getScaleX());
            message.print("," + modelTransformation.getShearX());
            message.print("," + modelTransformation.getScaleY());
            message.print("," + modelTransformation.getShearY());
            message.print("," + modelTransformation.getTranslateX());
            message.print("," + modelTransformation.getTranslateY() + "]");

            message.println("]");
        } else {
            message.println("NOT AVAILABLE");
        }

        // do all the GeoKeys
        if (metadata != null) {
            int i = 1;
            for (GeoKeyEntry geokey : metadata.getGeoKeys()) {
                message.print("GeoKey #" + i + ": ");
                try {
                    message.println(
                            "Key = "
                                    + geokey.getKeyID()
                                    + ", Value = "
                                    + metadata.getGeoKey(geokey.getKeyID()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
                i++;
            }
        }

        // print out the localized message
        Throwable t = getCause();
        if (t != null)
            java.util.logging.Logger.getGlobal().log(java.util.logging.Level.INFO, "", t);

        // close and return
        message.close();
        return text.toString();
    }
}
