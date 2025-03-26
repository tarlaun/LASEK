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
package edu.ucr.cs.bdlab.beast.io.shapefile;

import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import edu.ucr.cs.bdlab.beast.io.FeatureWriter;
import edu.ucr.cs.bdlab.beast.util.FileUtil;
import edu.ucr.cs.bdlab.beast.util.OperationParam;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Writes compressed shapefiles as ZIP
 */
@FeatureWriter.Metadata(extension = ".zip", shortName = "zipshapefile")
public class CompressedShapefileWriter extends FeatureWriter {
  /**Whether to print the output using the pretty printer or not*/
  @OperationParam(
      description = "Size of each part in terms of number of bytes in the uncompressed shapfile",
      defaultValue = "128m"
  )
  public static final String PartSize = "shapefile.partsize";

  /**Maximum decompressed size of one shapefile per part*/
  protected long maximumPartSize;

  /**An internal writer that writes the files to a temporary directory before being adding to the ZIP file*/
  protected ShapefileFeatureWriter internalShapefileWriter;

  /**The output ZIP file*/
  protected ZipOutputStream zipOut;

  /**The number of the part currently being written*/
  protected int partNumber;

  /**A temporary directory in which shapefiles are written before being added to the ZIP file*/
  protected File tmpDir;

  /**The environment configuration for creating the file system*/
  private Configuration conf;

  @Override
  public void initialize(OutputStream out, Configuration conf) throws IOException {
    this.zipOut = new ZipOutputStream(out);
    this.tmpDir = Files.createTempDirectory("shapefiles").toFile();
    this.maximumPartSize = conf.getLongBytes(PartSize, 128L * 1024 * 1024);
    this.conf = conf;
    this.internalShapefileWriter = new ShapefileFeatureWriter();
    createNextPart();
  }

  protected void createNextPart() throws IOException {
    String partName = getPartName(partNumber)+".shp";
    File tempShpFile = new File(tmpDir, partName);
    Configuration shpfileConf = new Configuration(this.conf);
    // Set the default file system to the local file system so that it can write to the temp directory
    if (shpfileConf.get("fs.defaultFS") != null)
      shpfileConf.set("fs.defaultFS", "file:///");
    else
      shpfileConf.set("fs.default.name", "file:///");
    internalShapefileWriter.initialize(new Path(tempShpFile.getPath()), shpfileConf);
  }

  /**
   * Get the basename for the given part number
   * @param partNumber the part number starting at zero
   * @return the partition name for the given number
   */
  protected String getPartName(int partNumber) {
    String partName = outPath == null? "data" : FileUtil.replaceExtension(outPath.getName(), "");
    partName += String.format("-%05d", partNumber);
    return partName;
  }

  @Override
  public void write(IFeature feature) throws IOException {
    if (internalShapefileWriter.getShapefileSize() > maximumPartSize) {
      flushData(false);
      partNumber++;
      createNextPart();
    }
    internalShapefileWriter.write(feature);
  }

  /**
   * Flush the current data from the temporary directory to the ZIP file.
   * @param lastPart set to true if this is the last record
   * @throws IOException if an error happens while writing records to disk
   */
  protected void flushData(boolean lastPart) throws IOException {
    // Close the shapefile
    internalShapefileWriter.close();
    // Move the temporary files to the ZIP file
    // Rename the files by appending the part number since there will be additional files
    for (File tmpFile: tmpDir.listFiles()) {
      String zipEntryName = tmpFile.getName();
      if (partNumber == 0 && lastPart) {
        // Remove the part number since this is the first and last (the only) part.
        zipEntryName = zipEntryName.replace("-00000", "");
      }
      ZipEntry zipEntry = new ZipEntry(zipEntryName);
      zipOut.putNextEntry(zipEntry);
      InputStream tempIn = new BufferedInputStream(new FileInputStream(tmpFile));
      IOUtils.copyBytes(tempIn, zipOut, 16 * 1024);
      tempIn.close();
      tmpFile.delete();
    }
    zipOut.flush();
  }

  @Override
  public void close() throws IOException {
    try {
      flushData(true);
      zipOut.close();
    } finally {
      File[] tmpFiles = tmpDir.listFiles();
      if (tmpFiles != null) {
        for (File tmpFile : tmpFiles) {
          tmpFile.delete();
        }
      }
      tmpDir.delete();
    }
  }

}
