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
package edu.ucr.cs.bdlab.beast.indexing;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;

/**
 * Output committer that concatenates all master files into one master file.
 * @author Ahmed Eldawy
 *
 */
public class IndexMasterFileCommitter extends FileOutputCommitter {
  private static final Log LOG = LogFactory.getLog(IndexMasterFileCommitter.class);

  /**Job output path*/
  private Path outPath;

  public IndexMasterFileCommitter(Path outputPath, TaskAttemptContext context)
      throws IOException {
    super(outputPath, context);
    this.outPath = outputPath;
  }

  @Override
  public void commitJob(JobContext context) throws IOException {
    super.commitJob(context);

    Configuration conf = context.getConfiguration();

    FileSystem outFs = outPath.getFileSystem(conf);

    // Concatenate all master files into one file
    FileStatus[] resultFiles = outFs.listStatus(outPath, path -> path.getName().contains("_master"));

    if (resultFiles.length == 0) {
      LOG.warn("No _master files were written by reducers");
    } else {
      // Extract the extension of the first file and use it for the merged file
      String sampleName = resultFiles[0].getPath().getName();
      int lastDot = sampleName.lastIndexOf('.');
      String extension = sampleName.substring(lastDot+1);
      // Create the master file that combines all the files
      Path masterPath = new Path(outPath, "_master." + extension);
      PrintStream masterOut = new PrintStream(outFs.create(masterPath));
      boolean headerWritten = false;
      byte[] buffer = new byte[1024 * 1024];
      for (FileStatus f : resultFiles) {
        InputStream in = outFs.open(f.getPath());
        int size;
        while ((size = in.read(buffer)) > 0) {
          if (!headerWritten) {
            // Count number of attributes in one row to determine number of dimensions
            int i$ = 0;
            int numColumns = 1;
            while (i$ < buffer.length && buffer[i$] != '\n') {
              if (buffer[i$] == '\t')
                numColumns++;
              i$++;
            }
            // Now, write the header
            int numDimensions = (numColumns - 7) / 3;
            IndexHelper.printMasterFileHeader(numDimensions, masterOut);
            masterOut.println();
            headerWritten = true;
          }
          masterOut.write(buffer, 0, size);
        }
        in.close();
        outFs.delete(f.getPath(), false); // Delete the file that has been copied
      }
      masterOut.close();
    }
  }
}
