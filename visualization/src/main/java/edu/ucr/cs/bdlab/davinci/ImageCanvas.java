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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import edu.ucr.cs.bdlab.beast.geolite.GeometryHelper;
import edu.ucr.cs.bdlab.beast.util.BitArray;
import edu.ucr.cs.bdlab.beast.util.KryoInputToObjectInput;
import edu.ucr.cs.bdlab.beast.util.KryoOutputToObjectOutput;
import org.locationtech.jts.geom.Envelope;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * A canvas that contains an in-memory image backed by a regular Java-provided BufferedImage
 * @author Ahmed Eldawy
 *
 */
public class ImageCanvas extends Canvas implements Externalizable, KryoSerializable {

  static {
    System.setProperty("java.awt.headless", "true");
  }

  /**
   * The underlying image
   */
  protected BufferedImage image;

  /**
   * The graphics associated with the image if the image is in draw mode.
   * If the image is not in draw mode, graphics will be null.
   */
  protected transient Graphics2D graphics;

  /**Buffer size in pixels*/
  protected int buffer;

  /**
   * A bit array that tells which pixels are occupied including the buffer around it.
   */
  protected BitArray occupiedPixels1;

  /**
   * A flag to use a pure Java graphics library to work-around a JVM crash that happens on some systems.
   */
  private boolean usePureJavaGraphics = false;

  /**Default constructor is necessary to be able to deserialize it*/
  public ImageCanvas() {
  }

  /**
   * Creates a canvas of the given size for a given (portion of) input
   * data.
   * @param inputMBR - the MBR of the input area to plot
   * @param width - width the of the image to generate in pixels
   * @param height - height of the image to generate in pixels
   * @param tileID - the ID of the tile associated with this canvas or zero if no tile is associated with it
   */
  public ImageCanvas(Envelope inputMBR, int width, int height, long tileID) {
    this(inputMBR, width, height, tileID, 0);
  }

  public ImageCanvas(Envelope inputMBR, int width, int height, long tileID, int buffer) {
    super(inputMBR, width, height, tileID);
    this.image = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
    this.buffer = buffer;
    if (this.buffer > 0) {
      int numPixelsWithBuffer = (width + 2 * buffer) * (height + 2 * buffer);
      this.occupiedPixels1 = new BitArray(numPixelsWithBuffer);
    }
  }

  public void setUsePureJavaGraphics(boolean u) {
    this.usePureJavaGraphics = u;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    GeometryHelper.writeEnvelope(inputMBR, out);
    out.writeInt(width);
    out.writeInt(height);
    out.writeLong(tileID);
    out.writeBoolean(usePureJavaGraphics);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ImageIO.write(getImage(), "png", baos);
    baos.close();
    byte[] bytes = baos.toByteArray();
    out.writeInt(bytes.length);
    out.write(bytes);
    out.writeInt(buffer);
    if (this.buffer > 0)
      occupiedPixels1.write(out);
  }

  @Override
  public void write(Kryo kryo, Output output) {
    try {
      writeExternal(new KryoOutputToObjectOutput(kryo, output));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void read(Kryo kryo, Input input) {
    try {
      readExternal(new KryoInputToObjectInput(kryo, input));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException {
    GeometryHelper.readEnvelope(inputMBR, in);
    this.width = in.readInt();
    this.height = in.readInt();
    this.tileID = in.readLong();
    calculateScale();
    usePureJavaGraphics = in.readBoolean();
    int length = in.readInt();
    byte[] bytes = new byte[length];
    in.readFully(bytes);
    this.image = ImageIO.read(new ByteArrayInputStream(bytes));
    this.buffer = in.readInt();
    if (this.buffer > 0) {
      // Read non-plotted points and occupied pixels
      if (occupiedPixels1 == null)
        occupiedPixels1 = new BitArray();
      occupiedPixels1.readFields(in);
    }
  }

  public void mergeWith(ImageCanvas another) {
    int x = (int) this.transformX(another.getInputMBR().getMinX());
    int y = (int) this.transformY(another.getInputMBR().getMinY());
    getOrCreateGraphics(false).drawImage(another.getImage(), x, y, null);
    // If occupiedPixels1 is set in both, then set occupiedPixels2 in the result
    if (buffer != 0)
      this.occupiedPixels1.inplaceOr(another.occupiedPixels1);
  }

  public BufferedImage getImage() {
    if (graphics != null) {
      graphics.dispose();
      graphics = null;
    }
    return image;
  }
  
  protected Graphics2D getOrCreateGraphics(boolean antialiasing) {
    if (graphics == null) {
      if (usePureJavaGraphics)
        graphics = new SimpleGraphics(image);
      else {
        //Create graphics for the first time
        try {
          graphics = image.createGraphics();
          if (antialiasing)
            graphics.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        } catch (Throwable e) {
          graphics = new SimpleGraphics(image);
        }
      }
    }
    return graphics;
  }

  private long getPixelOffset(int x, int y) {
    if (x < -buffer || x >= width + buffer || y < -buffer || y >= height + buffer)
      return -1;
    return (y + buffer) * (width + 2 * buffer) + x;
  }

  public void setPixelOccupied1(int x, int y) {
    long pixelOffset = getPixelOffset(x, y);
    if (pixelOffset != -1)
      occupiedPixels1.set(pixelOffset, true);
  }

  public boolean getPixelOccupied1(int x, int y) {
    long pixelOffset = getPixelOffset(x, y);
    return pixelOffset == -1 ? false : occupiedPixels1.get(pixelOffset);
  }

}
