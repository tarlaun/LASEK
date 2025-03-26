# Raster processing using RDPro
Raster data is a very popular format that stores geospatial data in multidimensional arrays.
The prime example of geospatial raster data is satellite data that come in the form of image-like data.
Beast provides a special component for processing raster data called RDPro, Raster Distributed Processor.
This page describes the basic RDPro support in Beast.

## Setup
- Follow the [setup page](dev-setup.md) to prepare your project for using Beast.
- If you use Scala, add the following line in your code to import all Beast features.
```scala
import edu.ucr.cs.bdlab.beast._
```
- In the following examples, we assume the `sc` points to an initialized SparkContext. 
## Raster data Loading
The first step for processing any dataset in Spark is loading it as an RDD.
RDPro currently supports both GeoTIFF and HDF as input files.
The following is an example of loading raster data in Beast.
Notice that the input can be either a single file or a directory with many files.
In both cases, the input will be loaded in a single RDD.

```scala
// Load GeoTiff file
val raster: RDD[ITile[Int]] = sc.geoTiff("glc2000_v1_1.tif")
// Or using the RasterRDD alias
val raster: RasterRDD[Int] = sc.geoTiff("glc2000_v1_1.tif")
// Load HDF file
val temperatureK: RasterRDD[Float] = sc.hdfFile("MOD11A1.A2022173.h08v05.006.2022174092443.hdf", "LST_Day_1km")
```

*Note*: You can download this file at [this link](https://forobs.jrc.ec.europa.eu/products/glc2000/products.php).

The type parameter `[Int]` indicates that each pixel in the GeoTIFF file contains a single integer value.
This must match the actual contents of the file to work correctly. If you are not sure what type the file contains,
the following code example could help you figure it out.

```scala
val raster = sc.geoTiff("glc2000_v1_1.tif")
println(raster.first.pixelType)
```

In this case, it will print
```
IntegerType
```
The possibilities are:

| Type                        | Loading statement          |
|-----------------------------|----------------------------|
| IntegerType                 | `sc.geoTiff[Int]`          |
| FloatType                   | `sc.geoTiff[Float]`        |
| ArrayType(IntegerType,true) | `sc.geoTiff[Array[Int]]`   |
| ArrayType(FloatType, true)  | `sc.geoTiff[Array[Float]]` |

## Raster data Creation
An alternative way of creating the first raster RDD is by populating it directly from pixel values similar to the example below.
```scala
val metadata = RasterMetadata.create(x1 = -50, y1 = 40, x2 = -60, y2 = 30, srid = 4326,
  rasterWidth = 10, rasterHeight = 10, tileWidth = 10, tileHeight = 10)
val pixels = sc.parallelize(Seq(
  (0, 0, 100),
  (3, 4, 200),
  (8, 9, 300)
))
val raster = sc.rasterizePixels(pixels, metadata)
```

The metadata describes the geographic meaning of the pixel values.
 - The values (x1, y1) represent the north-west corner of the raster dataset.
 - The values (x2, y2) represent the south-east corner of the raster dataset.
 - SRID represents the coordinate reference system. For example, 4326 represents ["EPSG:4325"](https://epsg.io/4326)
 - rasterWidth&times;rasterHeight is the resolution of the entire raster dataset in pixels
 - tileWidth&times;tileHeight is the resolution of each tile in the raster
Notice that in most cases y1 will be greater than y2.
The pixels RDD contains a set of (x, y, m) values where (x, y) is the pixel location and m is the pixel value.

Alternatively, you can use the method `rasterizePoints` which takes the geographic coordinates of each pixel
instead of its raster locations as shown below.
```scala
val metadata = RasterMetadata.create(x1 = -50, y1 = 40, x2 = -60, y2 = 30, srid = 4326,
  rasterWidth = 10, rasterHeight = 10, tileWidth = 10, tileHeight = 10)
val pixels = sc.parallelize(Seq(
  (-51.3, 30.4, 100),
  (-55.2, 34.5, 200),
  (-56.4, 39.2, 300)
))
val raster = sc.rasterizePoints(pixels, metadata)
```

## Pixel-level operations
The following operations manipulates raster at the pixel level.

### MapPixels
This operation applies a mathematical function to each pixel to produce an output raster with the same dimensions
of the input. For example, the following code converts a raster that contains temperature values in Kelvin
to Fahrenheit.
```scala
val temperatureK: RasterRDD[Float] =
  sc.hdfFile("MOD11A1.A2022173.h08v05.006.2022174092443.hdf", "LST_Day_1km")
val temperatureF: RasterRDD[Float] = temperatureK.mapPixels(k => (k-273.15f) * 9 / 5 + 32)
temperatureF.saveAsGeoTiff("temperature_f")
```
Note: The file can be downloaded from the
[LP DAAC archive website](https://e4ftl01.cr.usgs.gov/MOLT/MOD11A1.006/2022.06.22/MOD11A1.A2022173.h08v05.006.2022174092443.hdf)
but you will need to be logged in to download the file.

### FilterPixels
This operation can filter out (remove) some pixels based on a provided condition.
The following example will keep only the pixels with temperature greater than 300&deg;K.
```scala
val temperatureK: RasterRDD[Float] =
  sc.hdfFile("MOD11A1.A2022173.h08v05.006.2022174092443.hdf", "LST_Day_1km")
temperatureK.filterPixels(_>300).saveAsGeoTiff("temperature_high")
```

### Flatten
The flatten method extracts all non-empty pixels from the raster into a non-raster RDD.
This can help with computing global statistics for the entire dataset, e.g., minimum, maximum, average, or histogram.
The following example computes the histogram of the global land cover.
```scala
val raster: RasterRDD[Int] = sc.geoTiff[Int]("glc2000_v1_1.tif")
val histogram: Map[Int, Long] = raster.flatten.map(_._4).countByValue().toMap
println(histogram)
```
The flatten method returns an RDD of tuples where each tuple contains the following values:
 - *x*: The pixel x location on the raster grid, i.e., the index of the column
 - *y*: The pixel y location on the raster grid, i.e., the index of the row
 - *metadata*: The metadata of the raster that can be used to convert the (x, y) coordinates into geographical coordinates
 - *m*: The measure value of that pixel

### Overlay
THe overlay operation stacks multiple rasters on top of each other. This function only works if all the input
rasters have the same metadata, i.e., same resolution, CRS, and tile size. If the two inputs have mixed
metadata, they should be first converted using the reshape operation (see below) to make them compatible.

```scala
val raster1: RasterRDD[Int] = sc.geoTiff[Int]("glc2000_v1_1.tif")
val raster2: RasterRDD[Int] = sc.geoTiff[Int]("vegetation")
val stacked: RasterRDD[Array[Int]] = raster1.overlay(raster2)
```

## Reshape Operations
These operations change the shape of the raster data to prepare it for further processing.

### Retile
The retile function does not change any of the values in the raster but change the way tiles are created.
This can be sometimes helpful to improve the performance by adjusting the size of the tiles.
For example, square and bigger tiles can be more efficient for reprojection and window functions.

```scala
val raster: RasterRDD[Int] = sc.geoTiff[Int]("glc2000_v1_1.tif")
val retiled = raster.retile(64, 64)
retiled.saveAsGeoTiff("glc_retiled")
```

### Explode
The explode function separates each tile from other tiles in the raster.
The values stay the same but this function helps to write each tile in a separate file.
For example, if you prepare your data for a machine learning algorithm that expects
every 256&times;256 tile to be in a separate file, then the explode function will help.
```scala
val raster: RasterRDD[Int] = sc.geoTiff[Int]("glc2000_v1_1.tif")
val retiled = raster.retile(64, 64).explode
retiled.saveAsGeoTiff("glc_retiled", GeoTiffWriter.WriteMode -> "distributed")
```

Hint: It is always better to use the distributed writing mode with exploded rasters
since the number of generated files will usually be very large.

### Reproject
This operation changes the coordinate reference system (CRS) of a raster layer to a different one.
It keeps the resolution and tile size the same as the input.
The following code sample converts an HDF file stored in Sinusoidal projection to EPSG:4326.
```scala
val temperature: RasterRDD[Float] = 
  sc.hdfFile("MOD11A1.A2022173.h08v05.006.2022174092443.hdf", "LST_Day_1km")
temperature.reproject(4326)
```

### Rescale
The rescale operation changes the resolution of the raster, i.e., number of pixels, without changing
the tile size or coordinate reference system. The following example reads an input raster
and generates a thumbnail by reducing its size. Notice that we use the compatibility mode so that
the thumbnail can be loaded on any GIS software.
```scala
val raster: RasterRDD[Int] = sc.geoTiff[Int]("glc2000_v1_1.tif")
val rescaled = raster.rescale(360, 180)
rescaled.saveAsGeoTiff("glc_small", GeoTiffWriter.WriteMode -> "compatibility")
```

### Reshape
The reshape operation is the most general way to change the shape of the raster data.
It takes a new `RasterMetadata` and changes the input raster to match it.
This is an advanced method that should only be used when a specific requirement is needed
and efficiency if crucial.
The following example changes the coordinate reference system, the region of interest, the resolution,
and the tile size all in one call. This is expected to be more efficient and accurate than making
several calls, e.g., reproject, rescale, ... etc.
```scala
val raster: RasterRDD[Int] = sc.geoTiff[Int]("glc2000_v1_1.tif")
val reshaped = RasterOperationsFocal.reshapeNN(raster,
  RasterMetadata.create(-124, 42, -114, 32, 4326, 1000, 1000, 100, 100))
reshaped.saveAsGeoTiff("glc_ca")
```

There are two implementations of the reshape functions, `reshapeNN` and `reshapeAverage`, which are contrasted below.

- `reshapeNN`: This is the default method used with rescale and reproject. In this method, each pixel in the target
get its value from the nearest pixel in the source. In other words, the center of each target pixel is mapped to
the source raster and whatever value is there is used. This method works for any type of data, i.e., categorical or
numerical. It is also generally more efficient. However, if the input raster is being downsized, contains continuous
values, or has a lot of empty pixels, then the result might be of a poor quality.
- `reshapeAverage`: In this method, if multiple source pixels are grouped into a single target pixel, their average
value is computed. This method should only be used if the pixel data is numerical and continuous, e.g., temperature,
vegetation, or visible frequencies. Empty pixels will be ignored while computing the average which makes this method
helpful if the input raster contains a lot of empty patches.

The following code shows how to use the average interpolation method with rescale or reproject functions.
```scala
val temperature: RasterRDD[Float] =
  sc.hdfFile("MOD11A1.A2022173.h08v05.006.2022174092443.hdf", "LST_Day_1km")
val rescaled = temperature.rescale(360, 180, RasterOperationsFocal.InterpolationMethod.Average)
temperature.reproject(CRS.decode("EPSG:4326"), RasterOperationsFocal.InterpolationMethod.Average)
```

## Raptor join operation
Raptor stands for <u>Ra</u>ster-<u>P</u>lus-Vec<u>tor</u>. Raptor operations enable the concurrent processing
of raster and vector data. The following example computes the total area covered by trees for each country in the world.
According to the [land cover legend](https://forobs.jrc.ec.europa.eu/products/glc2000/legend.php),
land cover 1-10 all correspond to trees and we will treat them equally for the sake of this example.
For simplicity, we will use the number of pixels as an approximate for the total area.
Keep in mind that the area of each pixel could be different depending on the CRS in use but we will leave
this calculation out of this example for simplicity.

```scala
val raster: RasterRDD[Int] = sc.geoTiff[Int]("glc2000_v1_1.tif")
val trees = raster.filterPixels(lc => lc >=1 && lc <= 10)
val countries = sc.shapefile("ne_10m_admin_0_countries.zip")
val result = RaptorJoin.raptorJoinFeature(trees, countries, Seq())
  .map(x => x.feature.getAs[String]("NAME")).countByValue().toMap
println(result)
```
Note: The country dataset can be [downloaded from Natural Earth](https://www.naturalearthdata.com/http//www.naturalearthdata.com/download/10m/cultural/ne_10m_admin_0_countries.zip)

Check the [Raptor join page](raptor-join.md) for more details on how to use this operation.

## Raster writing
Any raster RDD can be written as raster files, e.g., GeoTIFF.
The following example reads an HDF file, applies some processing, and write the output as GeoTIFF.
```scala
val temperatureK: RasterRDD[Float] =
  sc.hdfFile("MOD11A1.A2022173.h08v05.006.2022174092443.hdf", "LST_Day_1km")
val temperatureF: RasterRDD[Float] = temperatureK.mapPixels(k => (k-273.15f) * 9 / 5 + 32)
temperatureF.saveAsGeoTiff("temperature_f")
```

Below are some additional options you can use when writing files.

- Compression: You can choose between three types of compression {LZW, Deflate, and None} as follows.
```scala
raster.saveAsGeoTiff("temperature_f", GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_LZW)
raster.saveAsGeoTiff("temperature_f", GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_DEFLATE)
raster.saveAsGeoTiff("temperature_f", GeoTiffWriter.Compression -> TiffConstants.COMPRESSION_NONE)
```
- Write mode: A raster RDD can be written in distributed or compatibility mode.
In distributed mode, each RDD partition is written to a separate file.
These files can be read back and processes by Beast but traditional GIS software might not be able to process it.
If you use compatibility mode, the entire raster RDD is written as a single file that is compatible with
traditional GIS software.
```scala
raster.saveAsGeoTiff("temperature_f", GeoTiffWriter.WriteMode -> "distributed")
raster.saveAsGeoTiff("temperature_f", GeoTiffWriter.WriteMode -> "compatibility")
```
Hint: If you expect a large number of files to be written, e.g., after applying the explode method,
the distributed writing mode is recommended for more efficiency.
- Bit compaction: In GeoTIFF, values can be bit-compacted to use as few bits as possible.
To do that, Beast will need to first scan all the data to determine the maximum value
and use it to calculate the minimal bit representation of values. The following example uses this feature.
```scala
raster.saveAsGeoTiff("temperature_f", GeoTiffWriter.CompactBits -> true)
```
This feature works only with integer pixel values. Float values are always represented in 32-bits.
- BitsPerSample: If you want to use the bit compaction feature without the overhead
of scanning the entire dataset to find the maximum, you can directly set the
number of bits per sample (band) in the raster data. Note that Beast will use these values
without checking their validity so you must be sure that the values are correct.
If unsure, let Beast find out or disable the bit compaction feature.
In the following example, we assume that we write a file with three bands, RGB,
each taking 8 bits.
```scala
raster.saveAsGeoTiff("temperature_f", GeoTiffWriter.BitsPerSample -> "8,8,8")
```