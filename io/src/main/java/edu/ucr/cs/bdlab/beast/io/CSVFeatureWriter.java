package edu.ucr.cs.bdlab.beast.io;

import edu.ucr.cs.bdlab.beast.geolite.GeometryHelper;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.util.DynamicArrays;
import edu.ucr.cs.bdlab.beast.util.OperationParam;
import org.apache.hadoop.conf.Configuration;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.function.BiFunction;

/**
 * Writes records that have value of type {@link edu.ucr.cs.bdlab.beast.geolite.IFeature} while keys are ignored.
 */
@FeatureWriter.Metadata(extension = ".csv", shortName = "csv")
public class CSVFeatureWriter extends MultipartFeatureWriter {

  @OperationParam(
      description = "The field separator for output text files. Defaults to the same as the input"
  )
  public static final String FieldSeparator = "oseparator";
  @OperationParam(
      description = "For CSV files, adds a header line to the output file"
  )
  public static final String WriteHeader = "oheader";

  /**A constant for new line character for code readability*/
  protected static final char NewLine = '\n';

  /**A name to use for each dimension*/
  protected final String[] DimensionNames = {"x", "y", "z", "w", "a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
      "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v"};

  /**The first line of the output will create a CSV header*/
  protected boolean writeHeader;

  /**The output stream that writes the CSV file*/
  protected DataOutputStream out;

  /**Encodes features into text*/
  protected BiFunction<IFeature, StringBuilder, StringBuilder> featureEncoder;

  /**A temporary string builder*/
  protected StringBuilder str;

  /**The indexes of the columns where the geometry will be written*/
  protected int[] columnIndexes;

  /**A single character used as field separator*/
  protected char fieldSeparator;

  /**Type of geometry to be written (point, envelope, wkt)*/
  private String geometryType;

  @Override
  public void initialize(OutputStream out, Configuration conf) throws IOException {
    super.initialize(out, conf);
    this.writeHeader = conf.getBoolean(WriteHeader, conf.getBoolean(CSVFeatureReader.SkipHeader, false));
    this.fieldSeparator = conf.get(FieldSeparator, conf.get(CSVFeatureReader.FieldSeparator, "\t")).charAt(0);
    detectGeometryTypeAndColumnIndexes(conf.get(SpatialWriter.OutputFormat(), conf.get(SpatialFileRDD.InputFormat())));
    switch (geometryType) {
      case "point":
        featureEncoder = new CSVPointEncoder(fieldSeparator, columnIndexes);
        break;
      case "wkt":
        assert columnIndexes.length == 1;
        featureEncoder = new CSVWKTEncoder(fieldSeparator, columnIndexes[0]);
        break;
      case "envelope":
        assert columnIndexes.length % 2 == 0;
        featureEncoder = new CSVEnvelopeEncoder(fieldSeparator, columnIndexes);
        break;
      default:
        throw new RuntimeException(String.format("Unsupported output type '%s'", geometryType));
    }
    str = new StringBuilder();
    this.out = out instanceof DataOutputStream? (DataOutputStream) out : new DataOutputStream(out);
  }

  @Override
  protected boolean isCompressible() {
    return true;
  }

  /**
   * Infers the geometry type and column indexes from the given user-friendly format
   * @param userFriendlyFormat the user-given format to configure the output
   */
  protected void detectGeometryTypeAndColumnIndexes(String userFriendlyFormat) {
    int[] integers; // The integers between the parentheses or an empty array if not parentheses
    int openParenthesis = userFriendlyFormat.indexOf('(');
    int closeParenthesis = userFriendlyFormat.indexOf(')', openParenthesis + 1);

    if (openParenthesis != -1) {
      geometryType = userFriendlyFormat.substring(0, openParenthesis).toLowerCase();
      String[] integersParts = userFriendlyFormat.substring(openParenthesis + 1, closeParenthesis).split(",");
      integers = new int[integersParts.length];
      for (int $i = 0; $i < integersParts.length; $i++) {
        try {
          integers[$i] = Integer.parseInt(integersParts[$i]);
        } catch (NumberFormatException e) {
          // In case the provided format contains a name rather than a geometry ID
          integers[$i] = $i == 0? 0 : integers[$i-1] + 1;
        }
      }
    } else {
      geometryType = userFriendlyFormat.toLowerCase();
      integers = new int[0];
    }
    // The indexes of the columns that contain the geometry
    if (this.geometryType.equals("point")) {
      // A two-dimensional point
      this.geometryType = "point";
      this.columnIndexes = CSVFeatureReader.createColumnIndexes(2, integers);
    } else if (geometryType.equals("envelope")) {
      // A two-dimensional envelope
      this.columnIndexes = CSVFeatureReader.createColumnIndexes(4, integers);
    } else if (geometryType.equals("pointk")) {
      // A k-dimensional point where the first integer indicates the number of dimensions
      this.geometryType = "point";
      int numDimensions = integers[0];
      integers = Arrays.copyOfRange(integers, 1, integers.length);
      this.columnIndexes = CSVFeatureReader.createColumnIndexes(numDimensions, integers);
    } else if (geometryType.equals("envelopek")) {
      // A k-dimensional envelope where the first integer indicates the number of dimensions
      this.geometryType = "envelope";
      int numDimensions = integers[0];
      integers = Arrays.copyOfRange(integers, 1, integers.length);
      this.columnIndexes = CSVFeatureReader.createColumnIndexes(numDimensions * 2, integers);
    } else if (geometryType.equals("wkt")) {
      this.columnIndexes = CSVFeatureReader.createColumnIndexes(1, integers);
    } else if (geometryType.equals("nogeom")) {
      this.columnIndexes = new int[0];
    } else {
      // No valid input format detected
    }
  }

  @Override
  public void writeMiddle(IFeature f) throws IOException, InterruptedException {
    if (writeHeader) {
      writeHeader(f, str);
      writeHeader = false;
    }
    // Write the feature as a CSV file
    featureEncoder.apply(f, str);
    str.append(NewLine);
    if (str.length() > 16*1024) {
      // String builder is long enough to flush to disk
      byte[] bytes = str.toString().getBytes();
      str.setLength(0);
      out.write(bytes, 0, bytes.length);
    }
  }

  private void writeHeader(IFeature feature, StringBuilder str) {
    int numDimensions = GeometryHelper.getCoordinateDimension(feature.getGeometry());
    int[] invertedIndex = DynamicArrays.invertedIndex(columnIndexes);
    int $a = 0; // The index of the attribute to write next
    boolean firstColumn = true;
    for (int i$ : invertedIndex) {
      if (!firstColumn)
        str.append(fieldSeparator);
      if (i$ == -1) {
        if ($a == feature.iGeom())
          $a++;
        str.append(feature.getName($a++));
      } else {
        switch (geometryType) {
          case "wkt": str.append("geometry"); break;
          case "point": str.append(DimensionNames[i$]); break;
          case "envelope": str.append(i$ < numDimensions? (DimensionNames[i$]+"min") : (DimensionNames[i$ - numDimensions]+"max")); break;
          default: throw new RuntimeException(String.format("Unsupported format '%s'", geometryType));
        }
      }
      firstColumn = false;
    }
    while ($a < feature.length()) {
      if ($a == feature.iGeom())
        $a++;
      if ($a < feature.length()) {
        if (!firstColumn)
          str.append(fieldSeparator);
        str.append(feature.getName($a++));
        firstColumn = false;
      }
    }
    str.append(NewLine);
  }

  @Override
  public int estimateSize(IFeature f) {
    StringBuilder str = new StringBuilder();
    featureEncoder.apply(f, str);
    str.append(NewLine);
    return str.length();
  }

  @Override
  public void close() throws IOException, InterruptedException {
    super.close();
    // Write any remaining bytes in the string builder
    byte[] bytes = str.toString().getBytes();
    str.setLength(0);
    out.write(bytes, 0, bytes.length);
    out.close();
  }
}
