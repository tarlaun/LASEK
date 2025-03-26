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

import junit.framework.TestCase;
import org.locationtech.jts.geom.Envelope;

import java.awt.*;
import java.awt.image.BufferedImage;

public class ImageCanvasTest extends TestCase {

  public void testMerge() {
    Envelope mbr = new Envelope( 0.0, 100.0, 100.0, 200.0);
    int imageSize = 100;
    ImageCanvas canvas = new ImageCanvas(mbr, imageSize, imageSize, 0);
    Graphics2D g = canvas.getOrCreateGraphics(false);
    g.setColor(Color.BLACK);
    g.fillRect(0, 0, 20, 20);

    ImageCanvas canvas2 = new ImageCanvas(mbr, imageSize, imageSize, 0);
    canvas2.mergeWith(canvas);

    BufferedImage img = canvas2.getImage();
    int count = 0;
    for (int x = 0; x < imageSize; x++) {
      for (int y = 0; y < imageSize; y++) {
        int alpha = new Color(img.getRGB(x, y), true).getAlpha();
        if (alpha != 0)
          count++;
      }
    }
    assertEquals(20 * 20, count);
  }
}