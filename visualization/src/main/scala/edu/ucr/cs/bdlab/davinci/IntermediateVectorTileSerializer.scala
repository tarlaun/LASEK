package edu.ucr.cs.bdlab.davinci

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import edu.ucr.cs.bdlab.beast.util.BitArray
import org.apache.spark.sql.Row

import java.awt.geom.AffineTransform

class IntermediateVectorTileSerializer extends Serializer[IntermediateVectorTile] {
  override def write(kryo: Kryo, output: Output, t: IntermediateVectorTile): Unit = {
    output.writeInt(t.resolution)
    output.writeInt(t.buffer)
    output.writeBoolean(t.isRasterized)
    if (t.isRasterized) {
      // Write a rasterized tile
      kryo.writeObject(output, t.interiorPixels)
      kryo.writeObject(output, t.boundaryPixels)
    } else {
      output.writeInt(t.numPoints)
      output.writeInt(t.features.size)
      for (f <- t.features)
        kryo.writeClassAndObject(output, f)
      for (g <- t.geometries)
        kryo.writeClassAndObject(output, g)
    }
  }

  override def read(kryo: Kryo, input: Input, aClass: Class[IntermediateVectorTile]): IntermediateVectorTile = {
    val resolution = input.readInt()
    val buffer = input.readInt()
    val tile = new IntermediateVectorTile(resolution, buffer)
    val rasterized = input.readBoolean()
    if (rasterized) {
      tile.interiorPixels = kryo.readObject(input, classOf[BitArray])
      tile.boundaryPixels = kryo.readObject(input, classOf[BitArray])
    } else {
      // Read a non-rasterized tile
      tile.numPoints = input.readInt()
      val numFeatures = input.readInt()
      for (_ <- 0 until numFeatures)
        tile.features.append(kryo.readClassAndObject(input).asInstanceOf[Row])
      for (_ <- 0 until numFeatures)
        tile.geometries.append(kryo.readClassAndObject(input).asInstanceOf[LiteGeometry])
    }
    tile
  }
}
