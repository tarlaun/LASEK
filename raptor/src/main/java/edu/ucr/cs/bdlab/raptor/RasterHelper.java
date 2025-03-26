package edu.ucr.cs.bdlab.raptor;

import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.util.IOUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class RasterHelper {

  public static<T> IRasterReader<T> createRasterReader(FileSystem rasterFS, Path rasterFile, BeastOptions opts) {
    String extension = IOUtil.getExtension(rasterFile.getName());
    switch (extension.toLowerCase()) {
      case ".tif":
      case ".geotiff":
        GeoTiffReader greader = new GeoTiffReader();
        int layer = opts.getInt(IRasterReader.RasterLayerID(), 0);
        greader.initialize(rasterFS, rasterFile.toString(), Integer.toString(layer), opts);
        return greader;
      case ".hdf":
        HDF4Reader hreader = new HDF4Reader();
        String layerName = opts.getString(IRasterReader.RasterLayerID());
        hreader.initialize(rasterFS, rasterFile.toString(), layerName, opts);
        return (IRasterReader<T>)hreader;
      default:
        throw new RuntimeException(String.format("Unrecognized extension '%s'", extension));
    }
  }
}
