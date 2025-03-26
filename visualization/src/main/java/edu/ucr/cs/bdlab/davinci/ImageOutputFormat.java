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

import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.io.CSVEnvelopeEncoder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.io.PrintStream;

/**
 * Writes canvases as images to the output file
 * @author Ahmed Eldawy
 *
 */
public class ImageOutputFormat extends FileOutputFormat<Object, Canvas> {

  /**
   * Writes canvases to a file
   * @author Ahmed Eldawy
   *
   */
  class ImageRecordWriter extends RecordWriter<Object, Canvas> {
    /**Plotter used to merge intermediate canvases*/
    private Plotter plotter;
    private Path outPath;
    private FileSystem outFS;
    private int canvasesWritten;
    private boolean vflip;
    /**The associated reduce task. Used to report progress*/
    private TaskAttemptContext task;
    /**PrintStream to write the master file*/
    private PrintStream masterFile;

    public ImageRecordWriter(FileSystem fs, Path taskOutputPath, TaskAttemptContext task) throws IOException {
      Configuration conf = task.getConfiguration();
      this.task = task;
      this.plotter = SingleLevelPlotHelper.getConfiguredPlotter(new BeastOptions(conf));
      this.outPath = taskOutputPath;
      this.outFS = this.outPath.getFileSystem(conf);
      this.canvasesWritten = 0;
      this.vflip = conf.getBoolean(CommonVisualizationHelper.VerticalFlip, true);
      // Create a master file that logs the location of each intermediate image
      String masterFileName = String.format("_master-%05d.heap", task.getTaskAttemptID().getTaskID().getId());
      Path masterFilePath = new Path(outPath.getParent(), masterFileName);
      this.masterFile = new PrintStream(outFS.create(masterFilePath));
    }

    @Override
    public void write(Object dummy, Canvas r) throws IOException {
      String suffix = String.format("-%05d.png", canvasesWritten++);
      Path p = new Path(outPath.getParent(), outPath.getName()+suffix);
      FSDataOutputStream outFile = outFS.create(p);
      // Write the merged canvas
      plotter.writeImage(r, outFile, this.vflip);
      outFile.close();
      task.progress();

      masterFile.printf("%s,%s\n", CSVEnvelopeEncoder.defaultEncoderJTS.apply(r.inputMBR), p.getName());
    }
    
    @Override
    public void close(TaskAttemptContext context) throws IOException,
        InterruptedException {
      masterFile.close();
    }
  }
  
  @Override
  public RecordWriter<Object, Canvas> getRecordWriter( TaskAttemptContext task) throws IOException {
    Path file = getDefaultWorkFile(task, "");
    FileSystem fs = file.getFileSystem(task.getConfiguration());
    return new ImageRecordWriter(fs, file, task);
  }
  
  /**
   * An output committer that merges all master files into one master file.
   * @author Ahmed Eldawy
   *
   */
  public static class MasterMerger extends FileOutputCommitter {
    /**Job output path*/
    private Path outPath;

    public MasterMerger(Path outputPath, TaskAttemptContext context)
        throws IOException {
      super(outputPath, context);
      this.outPath = outputPath;
    }
    
    @Override
    public void commitJob(JobContext context) throws IOException {
      super.commitJob(context);
      
      FileSystem fs = outPath.getFileSystem(context.getConfiguration());
      FileStatus[] masterFiles = fs.listStatus(outPath, new PathFilter() {
        @Override
        public boolean accept(Path p) {
          return p.getName().startsWith("_master");
        }
      });
      
      Path mergedFilePath = new Path(outPath, "_master.heap");
      PrintStream mergedFile = new PrintStream(fs.create(mergedFilePath));
      Path kmlPath = new Path(outPath, "_index.kml");
      PrintStream kmlFile = new PrintStream(fs.create(kmlPath));
      kmlFile.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
      kmlFile.println("<kml xmlns=\"http://www.opengis.net/kml/2.2\">");
      kmlFile.println("<Folder>");
      Text line = new Text();
      for (FileStatus masterFile : masterFiles) {
        LineReader reader = new LineReader(fs.open(masterFile.getPath()));
        while (reader.readLine(line) > 0) {
          mergedFile.println(line);
          String[] parts = line.toString().split(",");
          kmlFile.println("<GroundOverlay>");
          kmlFile.println("<Icon>");
          kmlFile.printf("<href>%s</href>\n", parts[4]);
          kmlFile.println("</Icon>");
          kmlFile.println("<LatLonBox>");
          kmlFile.printf("<north>%s</north>", parts[3]);
          kmlFile.printf("<south>%s</south>", parts[1]);
          kmlFile.printf("<east>%s</east>", parts[2]);
          kmlFile.printf("<west>%s</west>", parts[0]);
          kmlFile.println("</LatLonBox>");
          kmlFile.println("</GroundOverlay>");
        }
        reader.close();
        
        fs.delete(masterFile.getPath(), false);
      }
      kmlFile.println("</Folder>");
      kmlFile.println("</kml>");
      mergedFile.close();
      kmlFile.close();
    }
  }

  @Override
  public synchronized OutputCommitter getOutputCommitter(
      TaskAttemptContext context) throws IOException {
    Path jobOutputPath = getOutputPath(context);
    return new MasterMerger(jobOutputPath, context);
  }

}
