# Install Beast command-line interface (CLI) and shell
This page describes the recommended way for installing the command-line tools for Beast.
These tools allow you to run the prepackaged operations that ships with Beast
and your own programs that your write for Beast.

## Install the binaries

The recommended way to install the Beast CLI and shell is to use the following command.
```shell
curl -o beast-0.10.1-RC2-bin.tar.gz https://bitbucket.org/bdlabucr/beast/downloads/beast-0.10.1-RC2-bin.tar.gz
tar -xvzf beast-0.10.1-RC2-bin.tar.gz
echo 'export PATH=$PATH:'`pwd`/beast-0.10.1-RC2/bin >> .bashrc
```
If you do not install beast in your home directory, adjust the paths accordingly.

## Test with simple CLI commands

1. Download a sample test file from UCR-Star. Follow [this link](https://doi.org/10.6086/N1C24TGK#mbr=9qh961gutd,9qh96kh7h0) and click on the Shapefile download link.
2. To summarize the dataset, use the following command:
```shell
beast summary MSBuildings_data_index.zip iformat:shapefile
```
The output will look similar to the following.
```text
{
  "extent" : [ -117.328976, 33.979798, -117.316777, 33.991514 ],
  "size" : 189552,
  "num_features" : 840,
  "num_non_empty_features" : 840,
  "num_points" : 5757,
  "avg_sidelength" : [ 1.9788214285716845E-4, 1.673523809523083E-4 ],
  "srid" : 4326
}
INFO Main: The operation summary finished in 2.525107 seconds
```
3. Convert the file from Shapefile to GeoJSON.
```shell
beast cat MSBuildings_data_index.zip iformat:shapefile MSBuildings.geojson oformat:geojson
```
The output should include a line similar to the following to indicate success.
```
INFO Main: The operation cat finished in 2.961719 seconds
```
The output file `MSBuildings.geojson/part-0000.geojson` will look similar to the following.
```json
{
  "type" : "FeatureCollection",
  "features" : [ {
    "type" : "Feature",
    "geometry" : {
      "type" : "Polygon",
      "coordinates" : [ [ [ -117.323635, 33.983144 ], [ -117.323633, 33.983269 ], [ -117.323783, 33.98327 ], [ -117.323785, 33.983146 ], [ -117.323635, 33.983144 ] ] ]
    }
  },
...
```

4. Visualize the dataset using the following command.
```shell
beast splot MSBuildings_data_index.zip iformat:shapefile plotter:gplot MSBuildings.png
```
You will again see a success message similar to the following:
```text
INFO Main: The operation splot finished in 3.592639 seconds
```
The output MSBuildings.png will look similar to the following.
![MSBuildings visualization](images/MSBuildings.png)

5. You can also access the shell by running the command `beast-shell`.
Once the shell is available, you can try the following commands which are similar to the ones
   listed above.
```scala
val buildings = sc.shapefile("MSBuildings_data_index.zip")
println(buildings.summary)
buildings.saveAsGeoJSON("MSBuildings.geojson")
buildings.plotImage(1000, 1000, "MSBuildings.png")
```
