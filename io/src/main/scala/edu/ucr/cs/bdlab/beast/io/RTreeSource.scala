package edu.ucr.cs.bdlab.beast.io

import edu.ucr.cs.bdlab.beast.indexing.{RTreeFeatureReader, RTreeFeatureWriter}

class RTreeSource extends SpatialFileSourceSink[RTreeFeatureReader, RTreeFeatureWriter] {

  /**A short name to use by users to access this source */
  override def shortName(): String = "rtree"

}
