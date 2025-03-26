package edu.ucr.cs.bdlab.beast.io;

import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.geolite.GeometryReader;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.util.FileUtil;
import edu.ucr.cs.bdlab.beast.util.OperationHelper;
import edu.ucr.cs.bdlab.beast.util.StringUtil;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.GeometryFactory;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * An abstract class for all record readers that read features. It adds a new initialize method that can be initialized
 * from a {@link BeastOptions} rather than {@link org.apache.hadoop.mapreduce.TaskAttemptContext}.
 * This makes it accessible for single-machine algorithms.
 */
public abstract class FeatureReader implements Iterable<IFeature>, Iterator<IFeature>, Closeable {

  /**
   * A default CRS that can be used if the input file does not explicitly specify a CRS.
   * The default is WG84 (EPSG:4326)
   */
  public static final CoordinateReferenceSystem DefaultCRS;

  static {
    try {
      DefaultCRS = CRS.decode("EPSG:4326", true);
    } catch (FactoryException e) {
      throw new RuntimeException("Could not create the default CRS", e);
    }
  }

  /**
   * A default geometry factory that uses the default SRID of 4326
   */
  public static final GeometryFactory DefaultGeometryFactory = GeometryReader.getGeometryFactory(4326);

  /**
   * initialize from an input split and a configuration.
   * @param split the split to initialize this reader to
   * @param conf the configuration to use while initialization including getting the file system
   * @throws IOException if an error happens while opening the file
   * @throws InterruptedException if the task got interrupted.
   */
  public void initialize(InputSplit split, BeastOptions conf) throws IOException, InterruptedException {
    isClosed = false;
  }

  public void initialize(SpatialFileRDD.FilePartition partition, BeastOptions opts) throws IOException, InterruptedException {
    FileSplit fileSplit = new FileSplit(new Path(partition.path()), partition.offset(), partition.length(),
        partition.locations());
    this.initialize(fileSplit, opts);
  }

  public float getProgress() throws IOException {
    return 0.0f;
  }

  /**
   * Move to the next record and returns true if end-of-file is not reached yet
   * @return
   * @throws IOException
   */
  public abstract boolean nextKeyValue() throws IOException;

  /**
   * Return the features currently pointed to
   * @return
   */
  public abstract IFeature getCurrentValue() throws IOException;

  /**Maps the short name of feature readers to their classes*/
  private static Map<String, Class<? extends FeatureReader>> featureReaders;

  /**
   * Returns a map from the short name to the corresponding FeatureReader class.
   * This function lazily loads and caches the feature readers.
   * Note: Eagerly loading feature readers might cause a deadlock when loading the FeatureReader class and one
   * of the FeatureReaders that also depend on this class.
   * @return a map from the short name of each feature reader to its class.
   */
  public static Map<String, Class<? extends FeatureReader>> getFeatureReaders() {
    if (featureReaders != null)
      return featureReaders;
    synchronized (FeatureReader.class) {
      if (featureReaders == null) {
        Map<String, Class<? extends FeatureReader>> frs = new HashMap<>();
        List<String> readers = OperationHelper.readConfigurationXML("beast.xml").get("Readers");
        if (readers != null) {
          for (String readerClassName : readers) {
            try {
              Class<? extends FeatureReader> readerClass = Class.forName(readerClassName).asSubclass(FeatureReader.class);
              SpatialReaderMetadata metadata = readerClass.getAnnotation(SpatialReaderMetadata.class);
              frs.put(metadata.shortName(), readerClass);
            } catch (ClassNotFoundException e) {
              throw new RuntimeException(String.format("Could not find FeatureReader class '%s'", readerClassName), e);
            }
          }
        }
        featureReaders = frs;
      }
      return featureReaders;
    }
  }

  /**
   * Returns the metadata of this FeatureReader or {@code null} if the class is not annotated
   * @return the metadata of this feature reader as defined on the class
   */
  public SpatialReaderMetadata getMetadata() {
    return this.getClass().getAnnotation(SpatialReaderMetadata.class);
  }

  /**
   * Returns the default file extension of the given input format
   * @param iformat the input format in a user-friendly format
   * @return the extension defined on that format in the metadata
   */
  public static String getFileExtension(String iformat) {
    Iterator<Class<? extends FeatureReader>> allReaders = FeatureReader.getFeatureReaders().values().iterator();
    Class<? extends FeatureReader> readerClass = null;
    while (readerClass == null && allReaders.hasNext()) {
      Class<? extends FeatureReader> testClass = allReaders.next();
      try {
        if (testClass.newInstance().isRecognized(iformat))
          readerClass = testClass;
      } catch (InstantiationException | IllegalAccessException e) {
        e.printStackTrace();
      }
    }
    if (readerClass == null)
      return null;
    SpatialReaderMetadata metadata = readerClass.getAnnotation(SpatialReaderMetadata.class);
    if (metadata == null)
      return null;
    return metadata.extension();
  }

  /**
   * Try to autodetect a valid input format from the input path (e.g., extension). If a valid input format is detected,
   * the necessary parameters are returned in the given set of options. Otherwise, if no input format could be detected,
   * a {@code null} is returned.
   * @param conf the system configuration which can be used to open the file or check its status to detect it
   * @param input the input file name
   * @return the options that need to be added to open the input file or {@code null} if the input is not detected
   */
  public BeastOptions autoDetect(BeastOptions conf, String input) {
    SpatialReaderMetadata metadata = this.getMetadata();
    boolean detected = false;
    try {
      Path inputPath = new Path(input);
      FileSystem fs = inputPath.getFileSystem(conf.loadIntoHadoopConf(null));
      FileStatus inputFileStatus = fs.getFileStatus(inputPath);
      if (inputFileStatus.isFile()) {
        detected = FileUtil.extensionMatches(inputPath.getName(), metadata.extension());
      } else if (inputFileStatus.isDirectory()) {
        // Test all files within the directory until one of them matches
        FileStatus[] files = fs.listStatus(inputPath, SpatialFileRDD.HiddenFileFilter());
        for (int $i = 0; $i < files.length && !detected; $i++)
          detected = FileUtil.extensionMatches(files[$i].getPath().getName(), metadata.extension());
      }
    } catch (IOException e) {
      // Just try with the file name
      detected = FileUtil.extensionMatches(input, metadata.extension());
    }
    if (detected)
      return new BeastOptions(false).set(SpatialFileRDD.InputFormat(), metadata.shortName());
    return null;
  }

  /**
   * Suggest a list of corrections for a given input format to report to the user in case the input format
   * could not be detected.
   * @param iformat the given input format that might contain errors
   * @return a list of suggestions or {@code null} if no suggestions can be found
   */
  public String[] iformatCorrections(String iformat) {
    if (StringUtil.levenshteinDistance(iformat, this.getMetadata().shortName()) <= 2)
      return new String[] {this.getMetadata().shortName()};
    return null;
  }

  /**
   * Returns the coordinate reference system of geometries read by this reader.
   * @return the coordinate reference system for this input file or EPSG:4326 by default
   */
  public CoordinateReferenceSystem getCRS() {
    // The default CRS is WGS 84 (EPSG 4326) but child classes can override this function
    return FeatureReader.DefaultCRS;
  }

  /**
   * Returns true if the given user-friendly input format is recognized by this feature reader
   * @param iformat the user-provided input format
   * @return {@code true} if the given format is understandable by this input format, {@code false} otherwise.
   */
  public boolean isRecognized(String iformat) {
    return iformat.equals(this.getMetadata().shortName());
  }

  @Override
  public Iterator<IFeature> iterator() {
    return this;
  }

  /**Used with the iterator interface*/
  private IFeature nextFeature;

  /**Used with the iterator interface*/
  private boolean isClosed;

  @Override
  public boolean hasNext() {
    try {
      if (isClosed)
        return false;
      if (nextFeature != null || nextKeyValue()) {
        nextFeature = getCurrentValue();
        return true;
      }
      if (!isClosed) {
        close();
        isClosed = true;
      }
      return false;
    } catch (IOException e) {
      throw new RuntimeException("Error while reading file", e);
    }
  }

  @Override
  public IFeature next() {
    if (!hasNext())
      throw new RuntimeException("Empty Iterator");
    IFeature retVal = nextFeature;
    nextFeature = null;
    return retVal;
  }
}
