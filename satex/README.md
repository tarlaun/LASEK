# Satellite Data Extractor (SatEx)

The satellite data extractor (SatEx) is used to generate satellite images or files over a set of query geometries.
For example, given a [satellite image](https://www.naturalearthdata.com/downloads/10m-raster-data/10m-cross-blend-hypso/) like the one below:

![Elevation map](doc/images/elevation.png)

And a set of query polygons, e.g., [world countries](https://star.cs.ucr.edu/?NE/countries&d):

![Countries](doc/images/countries.png)

It would produce an individual clipped image for each polygon.
![India](doc/images/India_256_elevation.png)

## SatEx CLI
Compile SatEx into a JAR file by running `mvn clean package`.
Below is an example of how to use SatEx from command line:

```shell
beast --jar target/satex-*.jar satex HYP_HR_SR_W.tif iformat:shapefile ne_10m_admin_0_countries.zip country_images filenameattr:NAME
```

This command generates an individual image for each country over the TIFF file.
It will use the "NAME" attribute of each country as a name for the generated image.
Below are some additional parameters you can use with this command:

| Parameter (default) | Description                                                                                                      |
|---------------------|------------------------------------------------------------------------------------------------------------------|
| resolution (256)    | The resolution of the output image in pixels                                                                     |
| keepratio (true)    | Whether to maintain the aspect ratio of the geometry when producing an image or not                              |
| zipimages (false)   | Set this flag to true to combine output images into ZIP archives. Useful to reduce the number of generated files |
| buffersize (10)     | The buffer size in pixels to expand around non-polygon geometries, i.e., points and lines                        |
| rounds (1)          | If set to a value greater than one, the geometries will be processed in that number of rounds                    |
| raptorresult (tile) | The version of Raptor join to use, either "pixel", "tile", or "tileItr"                                          |
| imageformat (png)   | The format of the output images, either "png" or "npy"                                                           |

Note that both raster and vector input data can be a directory of files rather than a single file.

## Implementation Details
You can find more implementation details on image extraction on the following pages:
 - [Simple straight-forward implementation](doc/image-extractor.md)
 - [More flexible version that resizes the output images](doc/image-extractor-resize.md)
 - [A scalable version that runs more steps in parallel](doc/image-extractor-scalable.md)

Notice that the main implementation is more scalable and more sophisticated than the previous versions.