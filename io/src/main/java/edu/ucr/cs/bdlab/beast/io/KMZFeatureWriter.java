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
package edu.ucr.cs.bdlab.beast.io;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@FeatureWriter.Metadata(extension = ".kmz", shortName = "kmz")
public class KMZFeatureWriter extends KMLFeatureWriter {

  @Override
  public void initialize(OutputStream out, Configuration conf) throws IOException {
    ZipOutputStream zipOut = new ZipOutputStream(out);
    String kmlFileName = outPath == null? "index.kml" : outPath.getName();
    ZipEntry zipEntry = new ZipEntry(kmlFileName);
    zipOut.putNextEntry(zipEntry);
    super.initialize(zipOut, conf);
  }

  @Override
  public void close() throws IOException, InterruptedException {
    super.close();
  }
}
