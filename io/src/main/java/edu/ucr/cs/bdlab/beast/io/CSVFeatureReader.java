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
package edu.ucr.cs.bdlab.beast.io;

import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.geolite.EmptyGeometry;
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeND;
import edu.ucr.cs.bdlab.beast.geolite.Feature;
import edu.ucr.cs.bdlab.beast.geolite.GeometryHelper;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.geolite.PointND;
import edu.ucr.cs.bdlab.beast.util.OperationParam;
import edu.ucr.cs.bdlab.beast.util.StringUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A record reader that reads CSV file with custom field delimiter
 */
@SpatialReaderMetadata(
    description = "Parses comma- or tab-separated text files which contains either point coordinates," +
        "envelope boundaries or WKT-encoded geometries",
    shortName = "csv",
    extension = ".csv"
)
public class CSVFeatureReader extends FeatureReader {
  private static final Log LOG = LogFactory.getLog(CSVFeatureReader.class);

  /**Default quote characters including single and double quote characters*/
  public static final String DefaultQuoteCharacters = "''\"\"";

  @OperationParam(
      description = "The field separator for text files. Defaults to the tab character",
      defaultValue = "\t"
  )
  public static final String FieldSeparator = "separator";
  @OperationParam(
      description = "When reading a CSV file, skip the first line in the file. {true, false}",
      defaultValue = "false"
  )
  public static final String SkipHeader = "skipheader";

  @OperationParam(
      description = "Characters used to quote fields. Every pair of characters are used as start and end quote characters.",
      defaultValue = "''\"\""
  )
  public static final String QuoteCharacters = "quotes";

  @OperationParam(
      description = "Match geometry column names with case sensitivity (upper and lower cases matter)",
      defaultValue = "false",
      showInUsage = false
  )
  public static final String CaseSensitiveHeader = "csv.casesensitive";

  /**An underlying reader to read the input line-by-line*/
  protected final LineRecordReader lineReader = new LineRecordReader();

  /**Directs the record reader to skip the first line in the file*/
  protected boolean skipHeaderLine;

  /**The CSV feature*/
  protected Feature feature;

  /**A single character for separating fields in the input*/
  protected char fieldSeparator;

  /**A list of quote characters*/
  protected String quotes;

  /**The names or indexes of the columns where geometry WKT or coordinates are encoded*/
  protected String[] coordinateColumns;

  /**The index of columns that contain the coordinates in order*/
  protected int[] coordinateColumnIndexes;

  /**Type of geometry (TODO use an enumerated type instead)*/
  protected String geometryType;

  /**If the input contains wkt-encoded geometries, this is used to parse them*/
  protected WKTReader wktReader;

  /**An optional attributed to filter the geometries in the input file*/
  private EnvelopeND filterMBR;

  /**Path of the input file. Used mainly for reporting errors.*/
  protected Path filePath;

  /**Schema for features including the geometry attribute*/
  protected StructType schema;

  /**Match header columns with case sensitivity*/
  private boolean caseSensitive;

  /**List of attribute names as they appear in the header line*/
  private String[] fieldNames;

  /**
   * Cached schemas for each length of a record to avoid recreating many schemas.
   */
  private final Map<Integer, StructType> cachedSchemas = new HashMap<>();

  public CSVFeatureReader() {
    // TODO take geometry factory configuration from the UserOptions
    // NOTE Using PackedArrayCoordinateSequence breaks the code since it uses 3 dimensions by default even if the
    // code explicitly creates two dimensions only
    this.wktReader = new WKTReader(DefaultGeometryFactory);
    // Leaving this flag as true results in returning CoordinateSequence of dimension 3 even if the input
    // has only two dimensions
    //this.wktReader.setIsOldJtsCoordinateSyntaxAllowed(false);
  }

  @Override
  public void initialize(InputSplit inputSplit, BeastOptions conf) throws IOException {
    Configuration hadoopConf = conf.loadIntoHadoopConf(null);
    lineReader.initialize(inputSplit, new TaskAttemptContextImpl(hadoopConf, new TaskAttemptID()));
    if (inputSplit instanceof FileSplit)
      filePath = ((FileSplit)inputSplit).getPath();
    this.fieldSeparator = conf.getString(FieldSeparator, "\t").charAt(0);
    this.quotes = conf.getString(QuoteCharacters, "''\"\"");
    this.caseSensitive = conf.getBoolean(CaseSensitiveHeader, false);
    setGeometryTypeAndColumnIndexes(conf.getString(SpatialFileRDD.InputFormat()));
    this.skipHeaderLine = conf.getBoolean(SkipHeader, false);
    ArrayList<String> attributeNames = null;
    if (this.skipHeaderLine) {
      Text headerLine;
      attributeNames = new ArrayList<>();
      if (inputSplit instanceof FileSplit && ((FileSplit)inputSplit).getStart() != 0) {
        // Must read the header line separately
        FileSplit fsplit = (FileSplit) inputSplit;
        // TODO read the header line as the file splits are generated and propagate it to the record readers
        try (LineRecordReader otherReader = new LineRecordReader()) {
          FileSplit otherFileSplit = new FileSplit(fsplit.getPath(), 0, 1, null);
          otherReader.initialize(otherFileSplit, new TaskAttemptContextImpl(hadoopConf, new TaskAttemptID()));
          if (!otherReader.nextKeyValue())
            throw new RuntimeException("Could not read header line from split " + otherFileSplit);
          headerLine = otherReader.getCurrentValue();
        }
      } else {
        // Reading first split, the header line is the first line in the input
        lineReader.nextKeyValue();
        headerLine = lineReader.getCurrentValue();
      }
      // Extract attribute names from the header line
      while (headerLine.getLength() > 0)
        attributeNames.add(deleteAttribute(headerLine, fieldSeparator, 0, quotes).trim());
    }
    setCoordinateColumnIndexes(attributeNames);
    String filterMBRStr = conf.getString(SpatialFileRDD.FilterMBR());
    if (filterMBRStr != null) {
      String[] parts = filterMBRStr.split(",");
      double[] dblParts = new double[parts.length];
      for (int i = 0; i < parts.length; i++)
        dblParts[i] = Double.parseDouble(parts[i]);
      this.filterMBR = new EnvelopeND(DefaultGeometryFactory, dblParts.length/2, dblParts);
    }
    cachedSchemas.clear();
  }

  protected void setCoordinateColumnIndexes(ArrayList<String> columnNames) {
    // Initialize coordinate column indexes by either parsing the names as integer or locating them in the header
    coordinateColumnIndexes = new int[coordinateColumns.length];
    for (int $i = 0; $i < coordinateColumnIndexes.length; $i++) {
      if (coordinateColumns[$i] == null) {
        // If nothing is set by the user, assume a default value of 0 for the first one or an increment of one
        coordinateColumnIndexes[$i] = $i == 0 ? 0 : coordinateColumnIndexes[$i - 1] + 1;
      } else {
        try {
          // Try to parse it as integer
          coordinateColumnIndexes[$i] = Integer.parseInt(coordinateColumns[$i]);
        } catch (NumberFormatException e) {
          if (columnNames == null)
            throw new RuntimeException(
                String.format("Attribute '%s' is not an integer and -skipheader is not set in file '%s'",
                    coordinateColumns[$i], filePath));
          // If it's not an integer, it could be a column name

          if (caseSensitive)
            coordinateColumnIndexes[$i] = columnNames.indexOf(coordinateColumns[$i]);
          else {
            // Compare with case insensitivity
            int columnIndex = 0;
            while (columnIndex < columnNames.size() && !columnNames.get(columnIndex).equalsIgnoreCase(coordinateColumns[$i]))
              columnIndex++;
            coordinateColumnIndexes[$i] = columnIndex < columnNames.size() ? columnIndex : -1;
          }
          if (coordinateColumnIndexes[$i] == -1)
            throw new RuntimeException(String.format("Column '%s' not found in the header line of file '%s'",
                coordinateColumns[$i], filePath));
        }
      }
    }
    // Adjust coordinate column indexes to be in the correct order to parse
    adjustCoordinateColumnIndexes(this.coordinateColumnIndexes, columnNames);
    this.fieldNames = columnNames != null? columnNames.toArray(new String[0]) : null;
  }

  /**
   * An initializer that can be used outside the regular MapReduce context.
   * @param inputFile the input file name
   * @param conf the system configuration
   * @throws IOException if an error happens while trying to open the input file
   */
  public void initialize(Path inputFile, BeastOptions conf) throws IOException {
    FileSystem fileSystem = inputFile.getFileSystem(conf.loadIntoHadoopConf(null));
    FileStatus fileStatus = fileSystem.getFileStatus(inputFile);
    FileSplit fileSplit = new FileSplit(inputFile, 0, fileStatus.getLen(), null);
    this.initialize(fileSplit, conf);
  }

  /**
   * Infers the geometry type and column indexes from the given user-friendly format
   * @param userFriendlyFormat the user-given input format
   */
  protected void setGeometryTypeAndColumnIndexes(String userFriendlyFormat) {
    detectGeometryTypeAndColumnIndexes(userFriendlyFormat);
  }

  protected void detectGeometryTypeAndColumnIndexes(String userFriendlyFormat) {
    if (!caseSensitive)
      userFriendlyFormat = userFriendlyFormat.toLowerCase();
    String[] args; // The arguments between the parentheses or an empty array if not parentheses
    int openParenthesis = userFriendlyFormat.indexOf('(');
    int closeParenthesis = userFriendlyFormat.indexOf(')', openParenthesis + 1);

    if (openParenthesis != -1) {
      geometryType = userFriendlyFormat.substring(0, openParenthesis).toLowerCase();
      args = userFriendlyFormat.substring(openParenthesis + 1, closeParenthesis).split(",");
    } else {
      geometryType = userFriendlyFormat.toLowerCase();
      args = new String[0];
    }
    // The indexes of the columns that contain the geometry
    int numCoords;
    int skipFirst = 0;
    switch (this.geometryType) {
      case "point":
        // A two-dimensional point
        numCoords = 2;
        break;
      case "envelope":
        // A two-dimensional envelope
        numCoords = 4;
        break;
      case "pointk":
        // A k-dimensional point where the first integer indicates the number of dimensions
        this.geometryType = "point";
        numCoords = Integer.parseInt(args[0]);
        skipFirst = 1;
        break;
      case "envelopek":
        // A k-dimensional envelope where the first integer indicates the number of dimensions
        this.geometryType = "envelope";
        numCoords = Integer.parseInt(args[0]) * 2;
        skipFirst = 1;
        break;
      case "wkt":
        this.geometryType = "wkt";
        numCoords = 1;
        break;
      case "nogeom":
        this.geometryType = "nogeom";
        numCoords = 0;
        break;
      default:
        throw new RuntimeException("Unrecognized geometry type "+this.geometryType);
    }
    this.coordinateColumns = new String[numCoords];
    System.arraycopy(args, skipFirst, this.coordinateColumns, 0, args.length - skipFirst);
  }

  @Override
  public boolean isRecognized(String iformat) {
    String geometryType;
    int openParenthesis = iformat.indexOf('(');

    if (openParenthesis != -1) {
      geometryType = iformat.substring(0, openParenthesis).toLowerCase();
    } else {
      geometryType = iformat.toLowerCase();
    }
    // TODO Include these in a set to make it easier to test
    switch (geometryType) {
      case "point":
      case "pointk":
      case "envelope":
      case "envelopek":
      case "wkt":
      case "nogeom":
        return true;
      default:
        return false;
    }
  }

  @Override
  public String[] iformatCorrections(String iformat) {
    int indexOfParanthesis = iformat.indexOf('(');
    if (indexOfParanthesis != -1)
      iformat = iformat.substring(0, indexOfParanthesis);
    iformat = iformat.toLowerCase();
    List<String> corrections = new ArrayList<>();
    corrections.addAll(Arrays.asList("point", "pointk", "envelope", "envelopek", "wkt", "nogeom"));
    int i = 0;
    while (i < corrections.size()) {
      if (StringUtil.levenshteinDistance(iformat, corrections.get(i)) <= 2)
        i++;
      else
        corrections.remove(i);
    }
    return corrections.isEmpty()? null : corrections.toArray(new String[0]);
  }

  public static boolean recognize(String iformat) {
    String geometryType;
    int openParenthesis = iformat.indexOf('(');

    if (openParenthesis != -1) {
      geometryType = iformat.substring(0, openParenthesis).toLowerCase();
    } else {
      geometryType = iformat.toLowerCase();
    }
    // TODO Include these in a set to make it easier to test
    switch (geometryType) {
      case "point":
      case "pointk":
      case "envelope":
      case "envelopek":
      case "wkt":
      case "nogeom":
        return true;
      default:
        return false;
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    boolean recordExists = lineReader.nextKeyValue();
    if (!recordExists)
      return false;
    // Parse the next line into CSV and skip if it does not overlap the filter MBR
    // Read the geometry part
    while (recordExists) {
      Geometry geom;
      Text line = lineReader.getCurrentValue();
      switch (this.geometryType) {
        case "nogeom":
          geom = EmptyGeometry.instance;
          break;
        case "wkt":
          String wkt = deleteAttribute(line, this.fieldSeparator, this.coordinateColumnIndexes[0], quotes);
          if (wkt == null) {
            geom = EmptyGeometry.instance;
          } else {
            try {
              geom =  wktReader.read(wkt);
            } catch (ParseException e) {
              throw new RuntimeException(String.format("Error parsing line '%s' wkt '%s' in file '%s'", line, wkt, filePath), e);
            }
          }
          break;
        case "point":
          double[] coords = new double[this.coordinateColumnIndexes.length];
          Arrays.fill(coords, Double.NaN);
          for (int iCoord = 0; iCoord < this.coordinateColumnIndexes.length; iCoord++) {
            String val = deleteAttribute(line, this.fieldSeparator, this.coordinateColumnIndexes[iCoord], quotes);
            try {
              coords[iCoord] = (val == null || val.length() == 0) ? Double.NaN : Double.parseDouble(val);
            } catch (NumberFormatException e) {
              throw new RuntimeException(String.format("Error parsing dimension #%d column #%d, value '%s', text line '%s' in file '%s'",
                  iCoord, this.coordinateColumnIndexes[iCoord], val, line, filePath), e);
            }
          }
          if (coords.length == 2) {
            if (Double.isNaN(coords[0]) || Double.isNaN(coords[1]))
              geom = DefaultGeometryFactory.createPoint();
            else
              geom = DefaultGeometryFactory.createPoint(new CoordinateXY(coords[0], coords[1]));
          } else
            geom = new PointND(DefaultGeometryFactory, coords);
          break;
        case "envelope":
          EnvelopeND env = new EnvelopeND(DefaultGeometryFactory, this.coordinateColumnIndexes.length / 2);
          for (int iCoord = 0; iCoord < this.coordinateColumnIndexes.length; iCoord++) {
            String val = deleteAttribute(line, this.fieldSeparator, this.coordinateColumnIndexes[iCoord], quotes);
            try {
              double coordValue = (val == null || val.length() == 0) ? Double.NaN : Double.parseDouble(val);
              if (iCoord < env.getCoordinateDimension())
                env.setMinCoord(iCoord % env.getCoordinateDimension(), coordValue);
              else
                env.setMaxCoord(iCoord % env.getCoordinateDimension(), coordValue);
            } catch (NumberFormatException e) {
              throw new RuntimeException(String.format("Error parsing dimension #%d column #%d, value '%s', text line '%s' in file '%s'",
                  iCoord, this.coordinateColumnIndexes[iCoord], val, line, filePath), e);
            }
          }
          geom = env.getCoordinateDimension() == 2 ? env.getEnvelope() : env;
          break;
        default:
          throw new RuntimeException("Unrecognized geometry type "+this.geometryType);
      }
      // Create a new value since Spark cannot work with mutable objects
      EnvelopeND geometryMBR = new EnvelopeND(geom.getFactory()).merge(geom);
      if (filterMBR == null || filterMBR.intersectsEnvelope(geometryMBR)) {
        List<Object> values = new ArrayList<>();
        values.add(geom);
        while (line.getLength() > 0) {
          String v = deleteAttribute(line, fieldSeparator, 0, quotes);
          if (v != null)
            v = v.replaceAll("\\\\n", "\n");
          values.add(v);
        }
        // Append null values to match the length of the names
        while (fieldNames != null && values.size() < fieldNames.length)
          values.add(null);
        StructType schema = getOrCreate(values.size());
        this.feature = new Feature(values.toArray(), schema);
        return true;
      }
      // Skip to the next record
      recordExists = lineReader.nextKeyValue();
    }
    return false;
  }

  /**
   * Fetches or creates a schema of the given length. If an existing schema has been created earlier, it will
   * be returned. Otherwise, it will create a new schema. The created schema will always contain the given number
   * of attributes. The names will be taken from the header line if a corresponding entry exists. Otherwise,
   * a place holder $i will be added where i is the index of the attribute. The geometry will always have the name g.
   * @param length the number of attributes in the schema.
   * @return a schema of the given length.
   */
  private StructType getOrCreate(int length) {
    StructType schema = cachedSchemas.get(length);
    if (schema == null) {
      StructField[] attributes = new StructField[length];
      attributes[0] = new StructField("g", GeometryHelper.GeometryType, true, null);
      for (int i = 1; i < length; i++) {
        attributes[i] = new StructField(fieldNames != null && fieldNames.length > i-1? fieldNames[i-1] : "$"+i, DataTypes.StringType, true, null);
      }
      schema = new StructType(attributes);
      cachedSchemas.put(length, schema);
    }
    return schema;
  }

  /**
   * Adjust the coordinate column indexes so that they can be retrieved from a line in order.
   * If a coordinate index is followed by another index that appears after it, then this function
   * reduces the value of the second coordinate. For example, if the input is
   * [1, 2, 3], the output is [1, 1, 1], because removing attributes in this order from an input line
   * will produce the correct result. In other words, after removing attribute #1, then attribute #2 is now #1.
   * Similarly, after removing the second attribute, the third attribute also becomes at position #1.
   * On the other hand, if the input is [3, 2, 1], the output is also [3, 2, 1] because removing the first attribute
   * does not modify the position of the other two attributes because they appear earlier in the line.
   * @param coordinateColumnIndexes array of coordinate column indexes to adjust
   * @param names names in the header lines to adjust accordingly, i.e., remove coordinate columns
   */
  protected static void adjustCoordinateColumnIndexes(int[] coordinateColumnIndexes, List<String> names) {
    for (int $i = 0; $i < coordinateColumnIndexes.length; $i++) {
      if (names != null && names.size() > coordinateColumnIndexes[$i])
        names.remove(coordinateColumnIndexes[$i]);
      for (int $j = $i + 1; $j < coordinateColumnIndexes.length; $j++) {
        if (coordinateColumnIndexes[$j] > coordinateColumnIndexes[$i])
          coordinateColumnIndexes[$j]--;
      }
    }
  }

  @Override
  public IFeature getCurrentValue() {
    return feature;
  }

  @Override
  public float getProgress() throws IOException {
    return lineReader.getProgress();
  }

  @Override
  public void close() throws IOException {
    lineReader.close();
  }

  /**
   * Holds some meta information about columns that can be used to autodetect the format of an input shape
   */
  protected static class ColumnMetadata {
    enum ColumnType {Numeric, WKT, String}
    /**Name of the column or its index if the input file contains no names*/
    String name;
    /**The type of the column*/
    ColumnType type;
    /**Only for numeric columns, the range of values for that column*/
    double minValue, maxValue;

    public ColumnMetadata() {
      // Start with the most restrictive type
      this.type = ColumnType.Numeric;
      // Initialize the range to inverse infinite range
      this.minValue = Double.POSITIVE_INFINITY;
      this.maxValue = Double.NEGATIVE_INFINITY;
    }
  }

  @Override
  public BeastOptions autoDetect(BeastOptions conf, String input) {
    // Detect by reading the first few lines of the input;
    try {
      Path inputPath = new Path(input);
      Configuration hadoopConf = conf.loadIntoHadoopConf(null);
      FileSystem fs = inputPath.getFileSystem(hadoopConf);
      FileStatus fileStatus = fs.getFileStatus(inputPath);
      if (fileStatus.isDirectory()) {
        FileStatus[] contents = fs.listStatus(inputPath, SpatialFileRDD.HiddenFileFilter());
        if (contents.length == 0)
          return null;
        fileStatus = contents[0];
      }
      long length = fileStatus.getLen();
      FileSplit split = new FileSplit(fileStatus.getPath(), 0, Math.min(length, 8192), new String[0]);
      lineReader.initialize(split, new TaskAttemptContextImpl(hadoopConf, new TaskAttemptID()));
      List<String> sampleInput = new ArrayList<>();
      while (lineReader.nextKeyValue())
        sampleInput.add(lineReader.getCurrentValue().toString());
      if (sampleInput.size() <= 1) {
        // Too few lines to detect the input format
        return null;
      }
      // If the sample input contains non-printable characters, fail to avoid misinterpretation of binary files
      for (String line : sampleInput) {
        for (int i = 0; i < line.length(); i++)
          if (line.charAt(i) < 0x20 && "\t\r\n".indexOf(line.charAt(i)) == -1)
            return null;
      }
      // Use the sample lines to detect the geometry, the separator, and whether a header line exists or not

      // 1- Detect the field separator by trying the common ones and finding the one that gives a consistent
      // number of columns for all lines
      List<Character> candidateSeparators;
      if (conf.contains(FieldSeparator)) {
        // If the user already provided a field separator, stick to it
        char fieldSeparator = conf.getString(FieldSeparator).charAt(0);
        candidateSeparators = Collections.singletonList(fieldSeparator);
      } else {
        // Consider all the common field separators
        candidateSeparators = new ArrayList<>(Arrays.asList(' ', ',', '\t', ':', ';'));
      }
      for (char candidateSeparator : candidateSeparators) {
        // Collect some metadata about the columns to use in auto-detection with and without a header line
        boolean useHeaderLine = false;
        do {
          // Collect column metadata
          ColumnMetadata[] columnsMetadata = collectColumnMetadata(sampleInput, candidateSeparator, useHeaderLine);
          // Now let's see if we can infer the input format from the column metadata
          // Try to infer from the combination of column names and types
          int longitudeColumn = -1;
          int latitudeColumn = -1;
          int wktColumn = -1;
          for (int $iAtt = 0; $iAtt < columnsMetadata.length; $iAtt++) {
            ColumnMetadata columnMetadata = columnsMetadata[$iAtt];
            if (columnMetadata.type == ColumnMetadata.ColumnType.WKT)
              wktColumn = $iAtt;
            if (columnMetadata.type == ColumnMetadata.ColumnType.Numeric &&
                Double.isFinite(columnMetadata.minValue) &&
                Double.isFinite(columnMetadata.maxValue)) {
              if (columnMetadata.name.toLowerCase().contains("latitude") ||
                  columnMetadata.name.equalsIgnoreCase("lat") ||
                  columnMetadata.name.equalsIgnoreCase("y"))
                latitudeColumn = $iAtt;
              else if (columnMetadata.name.toLowerCase().contains("longitude") ||
                  columnMetadata.name.equalsIgnoreCase("long") ||
                  columnMetadata.name.equalsIgnoreCase("x"))
                longitudeColumn = $iAtt;
              else if (longitudeColumn == -1)
                longitudeColumn = $iAtt;
              else if (latitudeColumn == -1)
                latitudeColumn = $iAtt;
            }
          }
          String detectedFormat = null;
          if (wktColumn != -1) {
            // Found a WKT column, use it
            detectedFormat = String.format("wkt(%d)", wktColumn);
          } else if (longitudeColumn != -1 && latitudeColumn != -1) {
            // Found two columns for longitude and latitude
            detectedFormat = String.format("point(%d,%d)", longitudeColumn, latitudeColumn);
          }
          if (detectedFormat != null) {
            BeastOptions opts = new BeastOptions(false)
                .set(SpatialFileRDD.InputFormat(), detectedFormat)
                .set(FieldSeparator, Character.toString(candidateSeparator));
            if (useHeaderLine)
              opts.set(SkipHeader, true);
            return opts;
          }

          useHeaderLine = !useHeaderLine;
        } while (useHeaderLine);
      }
      // Could not detect the input format with any of the candidate separators
      return null;
    } catch (IOException e) {
      // Could not open the file for autodetection
      LOG.warn(String.format("Could not open the input file '%s' for autodetection", input));
      return null;
    } finally {
      try {
        lineReader.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Collects some metadata about the columns in the given sample input to help in input format auto detection.
   * @param sampleLines a set of sample lines from the input
   * @param separator the field separator (delimiter) to try to use
   * @param useHeaderLine whether to treat the first line as header o rnot
   * @return an array of column information according to the given input
   */
  private static ColumnMetadata[] collectColumnMetadata(List<String> sampleLines, char separator, boolean useHeaderLine) {
    WKTReader wktReader = new WKTReader();
    Map<Integer, ColumnMetadata> columnsMetadata = new TreeMap<>();
    for (int $iLine = 0; $iLine < sampleLines.size(); $iLine++) {
      Text line = new Text(sampleLines.get($iLine));
      int $iAttr = 0;
      if (useHeaderLine && $iLine == 0) {
        // Parse as header line
        while (line.getLength() > 0) {
          String name = deleteAttribute(line, separator, 0, DefaultQuoteCharacters);
          ColumnMetadata metadata = new ColumnMetadata();
          metadata.name = name;
          columnsMetadata.put($iAttr, metadata);
          $iAttr++;
        }
      } else {
        // Parse as data line
        while (line.getLength() > 0) {
          ColumnMetadata columnMetata  = columnsMetadata.get($iAttr);
          if (columnMetata == null) {
            columnMetata = new ColumnMetadata();
            columnMetata.name = "$attr"+$iAttr;
            columnsMetadata.put($iAttr, columnMetata);
          }

          String value = deleteAttribute(line, separator, 0, DefaultQuoteCharacters);
          if (value != null) {
            if (columnMetata.type == ColumnMetadata.ColumnType.Numeric) {
              // Try to consider this column as numeric
              try {
                double numericValue = Double.parseDouble(value);
                columnMetata.minValue = Double.min(columnMetata.minValue, numericValue);
                columnMetata.maxValue = Double.max(columnMetata.maxValue, numericValue);
              } catch (NumberFormatException e) {
                // Move to the more relaxed format of WKT
                columnMetata.type = ColumnMetadata.ColumnType.WKT;
              }
            }
            if (columnMetata.type == ColumnMetadata.ColumnType.WKT) {
              // Try to parse it as a WKT object
              try {
                StringReader str = new StringReader(value);
                wktReader.read(str);
                // If the string was not fully read, it might indicate that it is not WKT
                if (str.read() != -1)
                  columnMetata.type = ColumnMetadata.ColumnType.String;
              } catch (ParseException e) {
                // Not a valid WKT format. Demote it to a regular string
                columnMetata.type = ColumnMetadata.ColumnType.String;
              } catch (IOException e) {
                throw new RuntimeException("Unexpected error", e);
              }
            }
          }
          $iAttr++;
        }
      }
    }
    return columnsMetadata.values().toArray(new ColumnMetadata[0]);
  }


  /**
   * Drops an attribute from a line that represents a CSV line
   * @param line the text line to delete the attribute from
   * @param fieldSeparator the field separator
   * @param fieldToDrop the index of the field to drop (0-based)
   * @param quotes characters to use as quoting characters. Every pair of characters denote start and end quotes
   * @return the contents of the field that has been deleted without field separators
   */
  public static String deleteAttribute(Text line, char fieldSeparator, int fieldToDrop, String quotes) {
    if (line.getLength() == 0)
      return null;
    // Delete the desired field from the given text line and return it as a string
    int i1 = 0;
    int iField = 0;
    byte[] bytes = line.getBytes();
    String fieldValue = null;
    do {
      // i1 is the offset of the first character in the field.
      // i2 iterates until reaching the first index after then last character in the attribute
      int i2 = i1;
      boolean quoted = false;
      if (quotes != null) {
        int iQuote = quotes.indexOf(bytes[i1]);
        if (iQuote >= 0 && (iQuote & 1) == 0) {
          quoted = true;
          // Quoted field. Skip until the closing quote
          byte endQuote = (byte) quotes.charAt(iQuote + 1);
          do
            i2++;
          while (i2 < line.getLength() && bytes[i2] != endQuote);
        }
      }
      // Non-quoted field, skip until the field separator
      while (i2 < line.getLength() && bytes[i2] != fieldSeparator)
        i2++;
      if (iField == fieldToDrop) {
        // Check an empty range
        if (i1 == i2) {
          // Remove the field separator
          System.arraycopy(bytes, i2 + 1, bytes, i1, line.getLength() - i2 - 1);
          line.set(bytes, 0, line.getLength() - 1);
          return null;
        }
        // Parse and remove the desired field
        if (quoted)
          fieldValue = new String(bytes, i1 + 1, i2 - i1 - 2); // Quoted field
        else
          fieldValue = new String(bytes, i1, i2 - i1);
        if (i2 < line.getLength()) {
          System.arraycopy(bytes, i2 + 1, bytes, i1, line.getLength() - i2 - 1);
          line.set(bytes, 0, line.getLength() - (i2 - i1 + 1));
        } else if (i1 != 0) {
          line.set(bytes, 0, i1-1);
        } else {
          line.clear();
        }
      }
      i1 = i2+1;
      iField++;
    } while (i1 < line.getLength() && iField <= fieldToDrop);
    if (iField == fieldToDrop && i1 == line.getLength())
      fieldValue = "";
    return fieldValue;
  }

  public static int[] createColumnIndexes(int numDimensionColumns, int ... columns) {
    if (columns.length == numDimensionColumns)
      return columns;
    int[] columnIndexes = new int[numDimensionColumns];
    System.arraycopy(columns, 0, columnIndexes, 0, columns.length);
    for (int $i = columns.length; $i < columnIndexes.length; $i++) {
      columnIndexes[$i] = $i == 0 ? 0 : (columnIndexes[$i-1] + 1);
    }
    return columnIndexes;
  }
}
