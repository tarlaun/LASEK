package edu.ucr.cs.bdlab.beast.io

import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.{EnvelopeND, IFeature}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{InputSplit, TaskAttemptContext}

@SpatialReaderMetadata(shortName = "fakescalareader", noSplit = true, filter = "*.xyz")
class FakeScalaReader extends FeatureReader {
  override def initialize(inputSplit: InputSplit, conf: BeastOptions): Unit = ???

  override def nextKeyValue(): Boolean = ???

  override def getCurrentValue: IFeature = ???

  override def getProgress: Float = ???

  override def close(): Unit = ???
}