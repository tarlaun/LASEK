package edu.ucr.cs.bdlab.raptor;

import edu.ucr.cs.bdlab.beast.io.tiff.AbstractIFDEntry;
import edu.ucr.cs.bdlab.beast.io.tiff.ITiffReader;
import edu.ucr.cs.bdlab.beast.io.tiff.TiffConstants;
import edu.ucr.cs.bdlab.beast.io.tiff.TiffRaster;

import java.awt.geom.AffineTransform;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 *  This class provides required data-structure to store meta-data of GeoTiff files. Specifically it stores the required tags found under GeoKeyDirectoryTag.
 *
 *  This class is designed based on GeoTiffIIOMetadataDecoder from GeoTools library.
 */
public class GeoTiffMetadata {

    /** Get entries to tag */
    private final TiffRaster raster;

    private final ITiffReader reader;

    private final Map<Integer,GeoKeyEntry> geoKeys;


    private int geoKeyDirVersion;

    private int geoKeyRevision;

    private int geoKeyMinorRevision;
    
    private final PixelScale pixelScale;

    private final TiePoint[] tiePoints;

    private final double noData;

    private final AffineTransform modelTransformation;
    public static final int TAG_GEO_KEY_DIRECTORY = 34735;

    public GeoTiffMetadata(ITiffReader reader , TiffRaster raster) throws IOException {
        this.reader = reader;
        this.raster = raster;
        geoKeys = new HashMap<>();

        // 1. Get value associated with GeoKeyDirectoryTag.
        AbstractIFDEntry entry = raster.getEntry((short) TAG_GEO_KEY_DIRECTORY);
        if(entry != null)
        {
            // Get No of keys indexed by GeoKeyDirectory.
            // GeoKeyDirectory header  consists of {KeyDirectoryVersion, KeyRevision, MinorRevision, NumberOfKeys}
            // Value Type SHORT (2-byte unsigned short).
            ByteBuffer buffer = reader.readEntry(entry, null);

            geoKeyDirVersion = buffer.getShort() & 0xffff;
            geoKeyRevision = buffer.getShort() & 0xffff;
            geoKeyMinorRevision = buffer.getShort() & 0xffff;
            short NumberofKeys = buffer.getShort();

            for(short i=0;i<NumberofKeys;i++)
            {
                // next lines of GeoKeyDirectory follows the structure : { KeyID, TIFFTagLocation, Count, Value_Offset }
                int keyID = buffer.getShort()& 0xffff;
                GeoKeyEntry directoryEntry = new GeoKeyEntry(
                        keyID,
                        buffer.getShort()& 0xffff, // Tiff tag location
                        buffer.getShort()& 0xffff, // count
                        buffer.getShort()& 0xffff); // value offset

                if(!geoKeys.containsKey(keyID))
                {
                    geoKeys.put(keyID,directoryEntry);
                }
            }
        }
        pixelScale = calculatePixelScales();
        modelTransformation = calculateModelTransformation();
        tiePoints = calculateTiePoints();
        noData = calculateNoData();
    }
    public static final int TIFFTAG_NODATA = 42113;

    private double calculateNoData() throws IOException {
        AbstractIFDEntry entry = raster.getEntry((short) TIFFTAG_NODATA);
        if (entry != null) {
            ByteBuffer buffer = reader.readEntry(entry,null);
            String noData = getTiffAscii(buffer,-1,-1);

            try {
                return Double.parseDouble(noData);
            }
            catch (NumberFormatException nfe)
            {
                nfe.printStackTrace();
                return Double.NaN;
            }
        }

        return Double.NaN;
    }
    public static final int TAG_MODEL_TRANSFORMATION = 34264;

    /**
     * Gets the model tie points from the appropriate TIFFField
     *
     * <p>Attention, for the moment we support only 2D baseline transformations.
     *
     * @return the transformation, or null if not found
     */
    private AffineTransform calculateModelTransformation() throws IOException {
        AbstractIFDEntry entry = raster.getEntry((short) TAG_MODEL_TRANSFORMATION);
        if (entry != null) {
            double[] modelTransformation = new double[16]; // max possible variable in the tag
            ByteBuffer buffer = reader.readEntry(entry,null);
            if(buffer == null) return null;
            buffer.asDoubleBuffer().get(modelTransformation);
            AffineTransform transform = null;

            // Since library supports only 2d transform, reading only the required values.
            if(entry.getCountAsInt() >= 7) {
                transform = new AffineTransform(
                        modelTransformation[0],
                        modelTransformation[4],
                        modelTransformation[1],
                        modelTransformation[5],
                        modelTransformation[6],
                        modelTransformation[7]);
            }

            return transform;
        }
        return null;
    }
    public static final int TAG_MODEL_TIE_POINT = 33922;

    private TiePoint[] calculateTiePoints() throws IOException {
        AbstractIFDEntry entry = raster.getEntry((short) TAG_MODEL_TIE_POINT);
        if (entry != null) {
            // Translate point
            ByteBuffer buffer = reader.readEntry(entry, null);
            int numTiePoints = entry.getCountAsInt()/(6);
            final TiePoint[] retVal = new TiePoint[numTiePoints];
            for(int i=0;i<numTiePoints;i++)
            {
                retVal[i] = new TiePoint(
                        buffer.getDouble(),
                        buffer.getDouble(),
                        buffer.getDouble(),
                        buffer.getDouble(),
                        buffer.getDouble(),
                        buffer.getDouble()
                        );
            }
            return retVal;
        }
        return null;
    }
    public static final int TAG_MODEL_PIXEL_SCALE = 33550;

    private PixelScale calculatePixelScales() throws IOException {
        AbstractIFDEntry entry = raster.getEntry((short) TAG_MODEL_PIXEL_SCALE);
        if (entry != null) {
            // Scale point
            ByteBuffer buffer = reader.readEntry(entry, null);
            PixelScale retVal = new PixelScale();
            int numValues = entry.getCountAsInt();
            for (int i = 0; i < numValues; i++) {
                switch (i) {
                    case 0:
                        retVal.setScaleX(Double.longBitsToDouble(buffer.getLong(i*8)));
                        break;
                    case 1:
                        retVal.setScaleY(Double.longBitsToDouble(buffer.getLong(i*8)));
                        break;
                    case 2:
                        retVal.setScaleZ(Double.longBitsToDouble(buffer.getLong(i*8)));
                        break;
                }
            }
            return retVal;
        }
        return null;
    }

    /**
     * Gets the version of the GeoKey directory. This is typically a value of 1 and can be used to
     * check that the data is of a valid format.
     * @return the directory version index as an integer
     */
    public int getGeoKeyDirectoryVersion() {
        // now get the value from the correct TIFFShort location
        return geoKeyDirVersion;
    }

    /**
     * Gets the revision number of the GeoKeys in this metadata.
     * @return the revision as an integer
     */
    public int getGeoKeyRevision() {
        // Get the value from the correct TIFFShort
        return geoKeyRevision;
    }

    /**
     * Gets the minor revision number of the GeoKeys in this metadata.
     * @return the minor revision as an integer
     */
    public int getGeoKeyMinorRevision() {
        // Get the value from the correct TIFFShort
        return geoKeyMinorRevision;
    }

    /**
     * Gets a GeoKey value as a String. This implementation should be &quot;quiet&quot; in the sense
     * that it should not throw any exceptions but only return null in the event that the data
     * organization is not as expected.
     *
     * @param keyID The numeric ID of the GeoKey
     * @return A string representing the value, or null if the key was not found.
     * @throws IOException if an error happens while reading the given key from the file
     */
    public String getGeoKey(final int keyID) throws IOException {

        final GeoKeyEntry rec = getGeoKeyRecord(keyID);
        if (rec == null) {
            return null;
        }
        if (rec.getTiffTagLocation() == 0) {
            // value is stored directly in the GeoKey record
            return String.valueOf(rec.getValueOffset());
        }

        // value is stored externally
        // get the TIFF field where the data is actually stored
        AbstractIFDEntry field = raster.getEntry((short) rec.getTiffTagLocation());
        if (field == null) {
            return null;
        }
        ByteBuffer buffer = reader.readEntry(field, null);
        return field.type == TiffConstants.TYPE_ASCII ? getTiffAscii(buffer,rec.getValueOffset(), rec.getCount()) : getValueAttribute(buffer,field.type,rec.getValueOffset());
    }

    /**
     * Gets a record containing the four TIFFShort values for a geokey entry. For more information
     * see the GeoTIFFWritingUtilities specification.
     *
     * @param keyID the ID of the key to read
     * @return the record with the given keyID, or null if none is found
     */
    public GeoKeyEntry getGeoKeyRecord(int keyID) {
        return geoKeys.get(keyID);
    }

    /**
     * Gets a portion of a TIFFAscii string with the specified start character and length;
     *
     * @param buffer An ByteBuffer pointing to a TIFFField element that contains a
     *     TIFFAsciis element. This element should contain a single TiffAscii element.
     * @return A substring of the value contained in the TIFFAscii node, with the final '|'
     *     character removed.
     */
    private String getTiffAscii(final ByteBuffer buffer, int start, int length) {

        // there should be only one, so get the first
        // GeoTIFFWritingUtilities specification places a vertical bar '|' in
        // place of \0
        // null delimiters so drop off the vertical bar for Java Strings
        String valueAttribute = new String(buffer.array(), StandardCharsets.US_ASCII);

        if (start == -1) {
            start = 0;
        }
        if (length == -1) {
            length = valueAttribute.length() + 1;
        }
        return valueAttribute.substring(start, start + length - 1);
    }

    private String getValueAttribute(ByteBuffer buffer,short type, int offset)
    {
        String retVal = null;
        switch (type) {
            case TiffConstants.TYPE_BYTE:
                retVal = String.valueOf(buffer.get(offset) & 0xff);
                break;
            case TiffConstants.TYPE_SBYTE:
                retVal = String.valueOf(buffer.get(offset));
                break;
            case TiffConstants.TYPE_SHORT:
                retVal = String.valueOf(buffer.getShort(offset*2) & 0xffff);
                break;
            case TiffConstants.TYPE_SSHORT:
                retVal = String.valueOf(buffer.getShort(offset*2));
                break;
            case TiffConstants.TYPE_LONG:
                retVal = String.valueOf(buffer.getInt(offset*4) & 0xffffffffL);
                break;
            case TiffConstants.TYPE_SLONG:
                retVal = String.valueOf(buffer.getInt(offset*4));
                break;
            case TiffConstants.TYPE_FLOAT:
                retVal = String.valueOf(Float.intBitsToFloat(buffer.getInt(offset*4)));
                break;
            case TiffConstants.TYPE_DOUBLE:
                retVal = String.valueOf(buffer.getDouble(offset*8));
                break;
        }
        return retVal;
    }

    public AffineTransform getModelTransformation() {
        return modelTransformation;
    }

    /**
     * Return the GeoKeys.
     * @return the collection of all model keys
     */
    public Collection<GeoKeyEntry> getGeoKeys() {
        return geoKeys.values();
    }

    /**
     * Gets the model pixel scales from the correct TIFFField
     * @return the model pixel scales
     */
    public PixelScale getModelPixelScales() {
        return pixelScale;
    }

    /**
     * Gets the model tie points from the appropriate TIFFField
     *
     * @return the tie points, or null if not found
     */
    public TiePoint[] getModelTiePoints() {
        return tiePoints;
    }

    /**
     * Tells me if the underlying metdata contains ModelTiepointTag tag for {@link TiePoint}.
     *
     * @return true if ModelTiepointTag is present, false otherwise.
     */
    public boolean hasTiePoints() {
        return tiePoints != null && tiePoints.length > 0;
    }

    /**
     * Tells me if the underlying metdata contains ModelTransformationTag tag for {@link
     * AffineTransform} that map from Raster Space to World Space.
     *
     * @return true if ModelTransformationTag is present, false otherwise.
     */
    public boolean hasModelTrasformation() {
        return modelTransformation != null;
    }

    /**
     * Tells me if the underlying metadata contains ModelTiepointTag tag for {@link TiePoint}.
     *
     * @return true if ModelTiepointTag is present, false otherwise.
     */
    public boolean hasPixelScales() {
        if (pixelScale == null) {
            return false;
        } else {
            final double[] values = pixelScale.getValues();
            for (double value : values) {
                if (Double.isInfinite(value) || Double.isNaN(value)) {
                    return false;
                }
            }
            return true;
        }
    }
}
