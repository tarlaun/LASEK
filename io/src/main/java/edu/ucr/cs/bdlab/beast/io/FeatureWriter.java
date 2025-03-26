package edu.ucr.cs.bdlab.beast.io;

import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.util.OperationHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A record writer for features as values and no (or ignored) keys
 */
public abstract class FeatureWriter implements AutoCloseable {
  /**If initialized using a path, this stores that path. Otherwise, it will be null.*/
  protected Path outPath;

  /**
   * Returns the extension that should be appended to all created files.
   * @param outPath the output path given by the user. Typically, it is a directory under which files are written.
   * @param opts additional options provided by the user
   * @return the extension (including the dot) that should be appended to created files.
   */
  public String getFileExtension(String outPath, BeastOptions opts) {
    // Default implementation returns the path defined by the metadata
    Metadata metadata = this.getClass().getAnnotation(Metadata.class);
    String extension = metadata.extension();
    if (!isCompressible())
      return extension;
    final CompressionCodec codec = outPath == null? null :
            new CompressionCodecFactory(opts.loadIntoHadoopConf(null)).getCodec(new Path(outPath));
    return codec == null? extension : extension + codec.getDefaultExtension();
  }

  /**
   * Initialize the writer on the given path which points to a file.
   * @param p a path to a file
   * @param conf the configuration that can be used to initialize the file system
   * @throws IOException if an error occurs while creating the file
   */
  public void initialize(Path p, Configuration conf) throws IOException {
    this.outPath = p;
    FileSystem fs = outPath.getFileSystem(conf);
    OutputStream out = fs.create(outPath);
    if (isCompressible()) {
      final CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(outPath);
      if (null != codec) {
        // Compress using the required coded
        out = codec.createOutputStream(out);
      }
    }
    this.initialize(out, conf);
  }

  /**
   * Initializes the writer to an output stream. Not all feature writers might support that.
   * An exception can be thrown if this is not supported.
   * @param out an output stream to write to
   * @param conf environment configuration
   * @throws IOException if an error happens while initializing the output
   */
  public abstract void initialize(OutputStream out, Configuration conf) throws IOException;

  /**
   * Writes a record to the output
   * @param feature the feature to write to the output
   * @throws IOException if an error happens whl
   */
  public abstract void write(IFeature feature) throws IOException, InterruptedException;

  /**
   * Whether this output format can be compressed using an encoder, e.g., gz or bzip2
   * @return {@code true} if it can be compressed
   */
  protected boolean isCompressible() {
    return false;
  }

  /**
   * Annotation for feature writers to provide additional information
   */
  @Target(ElementType.TYPE)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Metadata {
    /**
     * An optional extension to use with files written using a specific writer (including the dot)
     * @return the extnesion including the dot
     */
    String extension() default "";

    /**
     * A short name that can be assigned from command line
     * @return the short name
     */
    String shortName();
  }

  /**Maps the short name of feature writers to their classes*/
  public static Map<String, Class<? extends FeatureWriter>> featureWriters = loadFeatureWriters();

  /**
   * Loads all feature writers listed in the configuration file
   * @return a key-value map that maps the short name of each writer to the FeatureWriter class.
   */
  private static Map<String, Class<? extends FeatureWriter>> loadFeatureWriters() {
    Map<String, Class<? extends FeatureWriter>> featureWriters = new HashMap<>();
    List<String> writers = OperationHelper.readConfigurationXML("beast.xml").get("Writers");
    if (writers != null) {
      for (String writerClassName : writers) {
        try {
          Class<? extends FeatureWriter> writerClass = Class.forName(writerClassName).asSubclass(FeatureWriter.class);
          Metadata metadata = writerClass.getAnnotation(Metadata.class);
          featureWriters.put(metadata.shortName(), writerClass);
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(String.format("Could not find FeatureWriter class '%s'", writerClassName), e);
        }
      }
    }
    return featureWriters;
  }

  /**
   * Returns an estimate of the write size without actually writing it.
   * @param f the feature to estimate the size for
   * @return the estimated number of bytes to write this feature to the output.
   */
  public int estimateSize(IFeature f) {
    return f.getStorageSize();
  }
}
