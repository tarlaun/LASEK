# Input-output formats in Beast
Beast supports the following formats for input (I), output (O), or input and output (IO).

* [Text-delimited files (IO)](#markdown-header-text-delimited-files), e.g., comma-separated and tab-separated files
  with either points, rectangles, or WKT-encoded geometries.
* [Esri Shapefile (c) (IO)](#markdown-header-shapefile)
* [GeoJSON (IO)](#markdown-header-geojson-files)
* [JSON+WKT (IO)](#markdown-header-json-files) JSON documents with WKT-encoded geometries.
* [GPX (I)](#markdown-header-gpx-files)
* [KML/KMZ (O)](#markdown-header-kml-output-format)/[KMZ](#markdown-header-kmz-output-format)

## Setup
```Scala
// Scala
val sparkContext = new SparkContext
import edu.ucr.cs.bdlab.beast._
```

```java
// Java
JavaSpatialSparkContext = new JavaSpatialSparkContext();
```

# Beast CLI
curl https://bitbucket.org/bdlabucr/beast/downloads/beast-0.10.1-RC2-bin.tar.gz | tar -xvz
alias beast=$HOME/beast-0.10.1-RC2/bin/beast
```

See more details for [Scala](dev-setup.md), [Java](dev-setup.md), and [Beast CLI](installation.md).

# Quick Reference
Refer to this section as a quick guide for how to read some popular formats.

## Sample Input
```
id,x,y,name
100,-120.2,34.5,some name
...
```

Process from command line
```shell
# Read the input
'iformat:point(x,y)' separator:, -skipheader
# Write as output
'oformat:point(1,2)' oseparator:, -oheader
```

Process in Scala:
```scala
// Read
sparkContext.readCSVPoint("input.csv", "x", "y", ',', true)
// Write
rdd.saveAsCSVPoints("output.csv", 1, 2, ',', true)
```

## Sample Input
```
Geometry;tags
LINESTRING(-120 30, -140 20);key:value
```

Parse from command-line:
```shell
# Read
iformat:wkt 'separator:;' -skipheader
# Write
oformat:wkt 'oseparator:;' -oheader
```

Process in Scala:
```scala
// Read
sparkContext.readCSVPoint("input.csv", 0, ';', true)
// Write
rdd.saveAsWKTFile("output", 0, ';', true)
```

## Shapefile (Either compressed as ZIP or decompressed)
Process from command-line:
```shell
# Read (Works for both compressed and decompressed)
iformat:shapefile
# Write (non-compressed shapefile)
oformat:shapefile
# Write (ZIP-compressed shapefile)
oformat:zipshapefile
```

Prcoess in Scala:
```scala
// Read (Works for both compressed and decompressed)
sparkContext.shapefile("input.zip")
// Write (non-compressed format)
rdd.saveAsShapefile("output")
// Write (ZIP-compressed shapefiles)
rdd.writeSpatialFile("output", "zipshapefile")
```

## GeoJSON files
Beast GeoJSON reader works for both compressed and decompressed GeoJSON files which
are in any of the following formats.

```json
{
  "type" : "FeatureCollection",
  "features" : [ {
    "type" : "Feature",
    "properties" : {
      "attr#0" : "1357773969",
      "attr#1" : "[]"
    },
    "geometry" : {
      "type" : "Point",
      "coordinates" : [ -117.2792905, 34.0065644 ]
    }
  } ]
}
```

```json
{"type" : "Feature", "geometry" : {"type" : "Point", "coordinates" : [ -117.2, 34.0] }}
```

Command line:
```shell
# Read
iformat:geojson
# Write
oformat:geojson
```

Scala:
```scala
// Read
sparkContext.geojson("input.geojson")
// Write
rdd.saveAsGeoJSON("output")
```

### Additional Options
- If you want to create one record per line instead of a `FeatureCollection` object,
add the option `GeoJSONFeatureWriter.OneFeaturePerLine:true`
- If you want to always include a `properties` attribute even if there are no
additional attributes, add the option `GeoJSONFeatureWriter.AlwaysIncludeProperties:true`

## JSON + WKT files

Sample file:
```json
{"g":"POINT (-117.2 34.0)","attr#0":"121239492",
  "attr#1":"[end_date#1998-07,name#Highgrove Landfill,landuse#landfill]"}
```

Command line:
```shell
# Read
iformat:jsonwkt
# Write
oformat:jsonwkt
```

Scala:
```scala
// Read
sparkContext.spatialFile("input.json", "jsonwkt")
// Write
rdd.writeSpatialFile("output", "jsonwkt")
```

## KML/KMZ files (output only)
Command line:
```shell
oformat:kml
oformat:kmz
```

Scala
```scala
rdd.saveAsKML("output.kml")
rdd.writeSpatialFile("output", "kmz")
```

## GPX file (input only)
Command line:
```shell
iformat:gpx
```

Scala:
```scala
sparkContext.spatialFile("input", "gpx")
```

# Details of File Formats
In the following examples, you specify the input as the file you want to read or a directory that contains the files you want to read. Beast can also auto-detect the input format if you are not sure how to use it. The prerequisites for the following examples are.

## Text-delimited files
Text-delimited files are available in several formats. First, they can contain points, rectangles, or other geometries. Also, they can use different delimiters, quote characters, and an optional header line. For the examples below, the input file can be in a raw text format or can be compressed in `.bz2` or `.gz` formats.

### CSV Point Files
Consider the following file that contains points and a header line.
```
ID,Longitude,Latitude,Name
1,-117.5923276516506,34.06019110206596,Ontario Int'l Airport
2,-122.21326125786332,37.71230369516912,Oakland Int'l
3,-118.40246854852198,33.94417425435857,Los Angeles Int'l
```
This file contains points with coordinates in the columns `Longitude` and `Latitude`, it has a header line, and it uses a comma separator.

```scala
// Scala
val airports = sparkContext.readCSVPoint("input.csv", "Longitude", "Latitude", ',', skipHeader = true)
```

```java
// Java
JavaRDD<IFeature> airports = spatialSparkContext.readCSVPoint("input.csv", "Longitude", "Latitude", ',', true)
```

```shell
# Beast CLI
beast <cmd> input.csv 'iformat:point(Longitude,Latitude)' -skipheader separator:, ...
```

### Text files with WKT geometries
Consider the following input file with WKT-encoded geometries.
```
POLYGON ((-120.8513171 35.2067591, -120.851486 35.2064947, -120.8513578 35.20644, -120.8511889 35.2067044, -120.8513171 35.2067591))	201832103	[building#yes]
POLYGON ((-120.8521669 35.2064643, -120.8522542 35.2063547, -120.852032 35.2062365, -120.8519447 35.2063461, -120.8521669 35.2064643))	201832097	[building#yes]
POLYGON ((-120.8490718 35.2077675, -120.8491464 35.2077332, -120.8491216 35.2076971, -120.849047 35.2077315, -120.8490718 35.2077675))	243222047	[building#yes]
POLYGON ((-120.8534714 35.2077175, -120.8536746 35.2077381, -120.85369 35.2076368, -120.8534868 35.2076162, -120.8534714 35.2077175))	201832098	[building#yes]
```
This file is tab-separated with no header line and contains the geometry in the first field in WKT format. To load this file, use the following commands.

```scala
// Scala
val buildings = sparkContext.readWKTFile("input.tsv", 0, '\t', false)
```

```java
// Java
JavaRDD<IFeature> buildings = spatialSparkContext.readWKTFile("input.tsv", 0, '\t', false);
```

```shell
# Beast CLI
beast <cmd> input.tsv 'iformat:wkt(0)' ...
```

### Text file with rectangles
The following sample file contains envelopes (rectangles) identified by four sides, left, bottom, right, and top.
```
913,16,924,51
953,104,1000.0,116
200,728,210,767
557,137,619,166
387,717,468,788
557,668,584,725
277,145,324,246
784,981,830,1000.0
544,571,627,620
617,76,697,101
309,364,368,454
133,905,192,909
954,160,1000.0,239
585,760,655,767
```
Read this file using the following commands.
```scala
// Scala
val rects = sparkContext.spatialFile("rects.csv", "envelope(0,1,2,3)", "separator" -> ",")
```
```java
// Java
JavaRDD<IFeature> rects = spatialSparkContext.spatialFile("rects.csv", "envelope(0,1,2,3)", new BeastOptions("separator:,"));
```
```shell
# Beast CLI
beast <cmd> rects.csv 'iformat:envelope(0,1,2,3)' separator:, ...
```

In all the above examples, "envelope" could be a short-hand for "envelope(0,1,2,3)" which will use the first four attributes by default.

## Shapefile
Esri Shapefiles can be either a set of three files in `.shp`, `.shx`, and `.dbf` format, or a single `.zip` file that contains the three files above. In both cases, you can read the file using the following commands.

```scala
// Scala
val records = sparkContext.shapefile("input.zip")
// Or
val records = sparkContext.shapefile("input.shp")
```
```java
// Java
JavaRDD<IFeature> records = spatialSparkContext.shapefie("input.zip");
```
```shell
# Beast CLI
beast <cmd> input.zip iformat:shapefile ...
```

## GeoJSON files
GeoJSON files can look similar to the following one.
```json
{
   "type": "FeatureCollection",
   "features": [
       {
           "type": "Feature",
           "id": "id0",
           "geometry": {
               "type": "LineString",
               "coordinates": [
                   [102.0, 0.0], [103.0, 1.0], [104.0, 0.0], [105.0, 1.0]
               ]
           },
           "properties": {
               "prop0": "value0",
               "prop1": "value1"
           }
       },
   ]
}
```
To read this file, use the following commands.

```scala
// Scala
val records = sparkContext.geojsonFile("input.json")
```
```java
// Java
JavaRDD<IFeature> records = spatialSparkContext.geojsonFile("input.json");
```
```shell
# Beast CLI
beast <cmd> input.json iformat:geojson ...
```

## GPX files
GPX is an XML-based format for trajectory data. Beast can load these files using the following commands.
```scala
// Scala
val records = sparkContext.spatialFile("input.gpx", "gpx")
```
```java
// Java
JavaRDD<IFeature> records = spatialSparkContext.spatialFile("input.gpx", "gpx")
```
```shell
# Beast CLI
beast <input> input.gpx iformat:gpx ...
```

## Input auto-detect
Beast can auto-detect the input using one of the following methods.

1. Based on the extension. For example, `.shp` files are automatically detected as Shapefiles.
2. Only for CSV files, the content of the file is used to guess the separator, whether there is a header file or not, the geometry type, and the columns that contain the geometries. This is just a best-effort so there is no guarantee it will detect the input correctly.

To instruct Beast to auto-detect the input format, use one of the following methods.


```scala
// Scala
val input = sparkContext.spatialFile("inputpath")
```
```java
// Java
JavaRDD<IFeature> input = sparkContext.spatialFile("inputpath");
```
```shell
# Beast CLI
beast <cmd> inputpath ...
```

# Output formats
In the following examples, we assume that `records` is of type `RDD[IFeature]` in Scala and of type `JavaRDD<IFeature>` in Java.

## Text-delimited files
We will use the same examples above to show how to write the output.

### CSV points
Use the following commands to write an output file similar to this.
```
ID,Longitude,Latitude,Name
1,-117.5923276516506,34.06019110206596,Ontario Int'l Airport
2,-122.21326125786332,37.71230369516912,Oakland Int'l
3,-118.40246854852198,33.94417425435857,Los Angeles Int'l
```

```scala
// Scala
records.saveAsCSVPoints("output.csv", 1, 2, ',', true)
```
```java
// Java
JavaSpatialRDDHelper.saveAsCSVPoints(records, "output.csv", 1, 2, ',', true);
```
```shell
# Beast CLI
beast <cmd> 'oformat:point(1,2)' -oheader oseparator:,
```

### CSV WKT files
Use the following commands to write a WKT-encoded CSV file.
```
POLYGON ((-120.8513171 35.2067591, -120.851486 35.2064947, -120.8513578 35.20644, -120.8511889 35.2067044, -120.8513171 35.2067591))	201832103	[building#yes]
POLYGON ((-120.8521669 35.2064643, -120.8522542 35.2063547, -120.852032 35.2062365, -120.8519447 35.2063461, -120.8521669 35.2064643))	201832097	[building#yes]
POLYGON ((-120.8490718 35.2077675, -120.8491464 35.2077332, -120.8491216 35.2076971, -120.849047 35.2077315, -120.8490718 35.2077675))	243222047	[building#yes]
POLYGON ((-120.8534714 35.2077175, -120.8536746 35.2077381, -120.85369 35.2076368, -120.8534868 35.2076162, -120.8534714 35.2077175))	201832098	[building#yes]
```

```scala
// Scala
records.saveAsWKTFile("output.csv", 0, '\t', false)
```
```java
// Java
JavaSpatialRDDHelper.saveAsWKTFile(records, "output.csv", 0, '\t', false)
```
```shell
# Beast CLI
beast <cmd> oformat:wkt
```

### CSV rectangles
Use the following commands to write a file similar to the following which contains only the MBRs of the records.
```
913,16,924,51
953,104,1000.0,116
200,728,210,767
557,137,619,166
387,717,468,788
```

```scala
// Scala
records.writeSpatialFile("output.csv", "envelope", "oseparator" -> ",")
```
```java
// Java
JavaSpatialRDDHelper.writeSpatialFile(records, "output.csv", "envelope", new BeastOptions("oseparator:,");
```
```shell
# Beast CLI
beast <cmd> output.csv oformat:envelope oseparator:,
```


## Uncompressed Shapefile
To write a Shapefile which consists of three files, `.shp`, `.shx`, and `.dbf`, use the following commands.
```scala
// Scala
records.saveAsShapefile("output.shp")
```
```java
// Java
JavaSpatialRDDHelper.saveAsShapefile(records, "output.shp");
```
```shell
# Beast CLI
beast <cmd> output.shp oformat:shapefile
```

## Compressed Shapefile
If you prefer to directly write a compressed Shapefile in a `.zip` archive, use the following commands.
```scala
// Scala
records.writeSpatialFile("output.zip", "zipshapefile")
```
```java
// Java
JavaSpatialRDDHelper.writeSpatialFile(records, "output.zip", "zipshapefile");
```
```shell
# Beast CLI
beast <cmd> output.zip oformat:zipshapefile
```

## GeoJSON output format
To write the output in GeoJSON format, use the following commands.
```scala
// Scala
records.saveAsGeoJSON("output.geojson")
```
```java
// Java
JavaSpatialRDDHelper.saveAsGeoJSON(records, "output.geojson")
```
```shell
# Beast CLI
beast <cmd> oformat:geojson output.geojson
```

## KML output format
To write the output in KML format, use the following commands.
```scala
// Scala
records.saveAsKML("output.kml")
```
```java
// Java
JavaSpatialRDDHelper.saveAsKML(records, "output.kml")
```
```shell
# Beast CLI
beast <cmd> oformat:kml output.kml
```

## KMZ output format
To write the output in compressed KMZ format, use the following commands.
```scala
// Scala
records.writeSpatialFile("output.kmz", "kmz")
```
```java
// Java
JavaSpatialRDDHelper.writeSpatialFile(records, "output.kmz", "kmz")
```
```shell
# Beast CLI
beast <cmd> oformat:kmz output.kmz
```
