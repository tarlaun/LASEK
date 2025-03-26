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
package edu.ucr.cs.bdlab.beast.util;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Stack;

public class FileUtil {
  private static final Log LOG = LogFactory.getLog(FileUtil.class);

  /**
   * Replaces the existing extension with the given extension. If the file does not have an extension, the given
   * extension is appended. The extension is assumed to have a leading period (.). This way, if the newExtension
   * is an empty string, this method will remove the extension.
   * @param filename An existing filename with or without an extension.
   * @param newExtension The extension to use in the given file including the leading period (.)
   * @return a new filename after replacing the extension
   */
  public static String replaceExtension(String filename, String newExtension) {
    int iLastDot = filename.lastIndexOf('.');
    if (iLastDot == -1)
      return filename + newExtension;
    return filename.substring(0, iLastDot) + newExtension;
  }


  /**
   * Returns the extension of the given file which is the suffix of the filename including the last period (.).
   * If the filename does not contain a period, an empty string is returned.
   * @param filename An existing filename with or without an extension.
   * @return a new filename after replacing the extension
   */
  public static String getExtension(String filename) {
    int iLastDot = filename.lastIndexOf('.');
    if (iLastDot == -1)
      return "";
    return filename.substring(iLastDot);
  }

  /**
   * Flatten the given directory by removing one lever from the directory
   * hierarchy. It finds all subdirectories under the given directory and merges
   * them together while putting their contents into the given directory.
   * It is assumed that the given path does not have any files, only subdirs.
   * @param fs the file system that contains the directory
   * @param path the path of the directory to flatten
   * @throws IOException if an array happens while reading/writing the files
   */
  public static void flattenDirectory(FileSystem fs, Path path) throws IOException {
    // Decide which directory to use as a destination directory based on the
    // number of files in each one
    FileStatus[] subdirs = fs.listStatus(path);
    if (subdirs.length == 0) {
      LOG.warn(String.format("No contents of the directory '%s' to flatten", path.toString()));
      return;
    }
    int maxSize = 0;
    Path destinationPath = null;
    for (FileStatus subdir : subdirs) {
      if (subdir.isDirectory()) {
        int size = fs.listStatus(subdir.getPath()).length;
        if (size > maxSize) {
          maxSize = size;
          destinationPath = subdir.getPath();
        }
      }
    }

    // Scan the paths again and move their contents to the destination path
    for (FileStatus subdir : subdirs) {
      if (subdir.isDirectory() && subdir.getPath() != destinationPath) {
        // Scan all the contents of this path and move it to the destination path
        FileStatus[] files = fs.listStatus(subdir.getPath());
        for (FileStatus file : files) {
          fs.rename(file.getPath(), new Path(destinationPath, file.getPath().getName()));
        }
        // Now, since the path is empty, we can safely delete it
        // We should delete it with non-recursive option for safety
        // However, in HDFS, .crc files are not moved and hence we delete them recursively
        fs.delete(subdir.getPath(), true);
      }
    }

    if (destinationPath != null) {
      // Finally, rename the destination directory to make it similar to its parent
      Path parentPath = path;
      Path renamedParent = new Path(parentPath.getParent(), Math.random() + ".tmp");
      fs.rename(parentPath, renamedParent);
      // Destination path has now changed since we renamed its parent
      destinationPath = new Path(renamedParent, destinationPath.getName());
      fs.rename(destinationPath, parentPath);
      fs.delete(renamedParent, true);
    }
  }

  /**
   * Rewrites the given {@code path} to be relative to {@code refPath}
   * @param path the reference path
   * @param refPath the path that needs to be made relative
   * @return the refPath as a relative path to the reference path
   */
  public static Path relativize(Path path, Path refPath) {
    String str = path.toString();
    String refStr = refPath.toString();
    if (str.equals(refStr))
      return new Path(".");
    // Remove the common prefix
    int prefix = 0;
    int lastSlashInPrefix = 0;
    while (prefix < str.length() && prefix < refStr.length() &&
      str.charAt(prefix) == refStr.charAt(prefix)) {
      if (str.charAt(prefix) == '/')
        lastSlashInPrefix = prefix;
      prefix++;
    }
    // If refPath is empty, it means that the given path is a subdirectory of it
    if (prefix == refStr.length() && str.charAt(prefix) == '/')
      return new Path(str.substring(prefix + 1));
    // Keep the prefix that can be removed from both
    prefix = str.charAt(lastSlashInPrefix) == '/'? lastSlashInPrefix + 1 : lastSlashInPrefix;
    // Paths have a common parent; remove it!
    if (prefix > 0) {
      str = str.substring(prefix);
      refStr = refStr.substring(prefix);
    }

    // Prepend ".."s to the path as equal to the depth of the refPath
    int defDepth = refStr.isEmpty()? 0 : new Path(refStr).depth();
    path = new Path(str);
    for (int $i = 0; $i < defDepth; $i++) {
      path = new Path("..", path);
    }

    return path;
  }

  /**
   * Tests if the extension of the given file name matches the given extension.
   * It assumed that the extension already contains a dot.
   * @param filename the filename to check its extension
   * @param extension the extension including the dot.
   * @return {@code true} if the filename ends in the given extension.
   */
  public static boolean extensionMatches(String filename, String extension) {
    int lastDot = filename.lastIndexOf('.');
    if (lastDot == -1)
      return false;
    // If the extension matches, return the short name of the input format
    return filename.substring(lastDot).equalsIgnoreCase(extension);
  }

  /**
   * Moves the given file or directory to the destination path.
   * If the source is a directory, it is all moved to the destination.
   * If the destination is an existing directory, the source is moved inside that directory with the same name.
   * If the destination does not exist, the source is moved with this name.
   * @param conf the configuration used to create the file systems
   * @param source an existing directory or file
   * @param destination the destination path to move the source file/directory to
   * @throws IOException if thrown while creating a directory or moving one of the files
   */
  public static void move(Configuration conf, Path source, Path destination) throws IOException {
    FileSystem srcFs = source.getFileSystem(conf);
    FileSystem destFs = destination.getFileSystem(conf);
    // If destination is an existing directory, move the source inside that directory with the same name
    if (destFs.exists(destination) && destFs.getFileStatus(destination).isDirectory()) {
      destination = new Path(destination, source.getName());
    }
    if (srcFs.equals(destFs)) {
      // Just rename
      srcFs.rename(source, destination);
    } else {
      // Need to copy all files recursively
      Stack<String> toMove = new Stack<>();
      toMove.push(source+"\n"+destination);
      while (!toMove.isEmpty()) {
        String[] parts = toMove.pop().split("\n");
        Path src = new Path(parts[0]);
        Path dst = new Path(parts[1]);
        if (srcFs.getFileStatus(src).isFile()) {
          // Move a single file from source to destination
          org.apache.hadoop.fs.FileUtil.copy(srcFs, src, destFs, dst, true, conf);
        } else if (srcFs.getFileStatus(src).isDirectory()) {
          // Source is directory. Copy all contents
          destFs.mkdirs(dst);
          for (FileStatus srcFile : srcFs.listStatus(dst)) {
            Path dstFile = new Path(dst, srcFile.getPath().getName());
            toMove.push(srcFile.getPath().toString()+"\n"+dstFile.toString());
          }
        } else {
          throw new RuntimeException("Cannot move "+src);
        }
      }
    }
  }

  /**
   * Concatenates a list of input files into one output file and deletes all input files.
   * @param fs the file system that contains all the input files and the output file
   * @param outFile the path of the resulting (concatenated) files
   * @param inFiles the list of files to concatenate.
   */
  public static void concat(FileSystem fs, Path outFile, Path ... inFiles) throws IOException {
    // First, try the file system concat method. If it fails, concatenate the files manually.
    try {
      // DFS concat requires an empty output file to concatenate to
      if (!fs.exists(outFile))
        fs.create(outFile).close();
      // DFS concat requires all input files to be in the same directory of the output file
      Path outDir = outFile.getParent();
      for (int i = 0; i < inFiles.length; i++) {
        Path inFile = inFiles[i];
        if (!inFile.getParent().equals(outDir)) {
          Path renamedFile = new Path(outDir, inFile.getName());
          while (fs.exists(renamedFile)) {
            // Rename to a random name
            renamedFile = new Path(outDir, "part_"+(long) (Math.random() * 1E9));
          }
          fs.rename(inFile, renamedFile);
          inFiles[i] = renamedFile;
        }
      }
      fs.concat(outFile, inFiles);
    } catch (Exception e) {
      LOG.info("Could not concatenate files efficiently due to "+e.getMessage());
      // Fall back to an inefficient concatenation by reading/writing.
      try (FSDataOutputStream out = fs.create(outFile)) {
        for (Path inFile : inFiles) {
          try (FSDataInputStream in = fs.open(inFile)) {
            IOUtils.copyLarge(in, out);
          }
          if (!fs.delete(inFile, false))
            LOG.warn("Could not delete file '" + inFile + "' after concatenation");
        }
      }
    }
  }
}
