/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.ucr.cs.bdlab.davinci;

import org.locationtech.jts.geom.Envelope;

import java.io.Serializable;

/**An abstract interface for any canvas*/
public abstract class Canvas implements Serializable {
  /**The MBR of this layer in input coordinates*/
  protected final Envelope inputMBR = new Envelope();
  
  /**Width of this layer in pixels*/
  protected int width;
  
  /**Height of this layer in pixels*/
  protected int height;

  /**If this canvas belongs to tile, this attribute stores its tile ID. Otherwise, it is zero*/
  protected long tileID;

  /**The scale of the image in terms of pixels per input unit*/
  protected transient double scaleX, scaleY;

  public Canvas() {}
  
  public Canvas(Envelope inputMBR, int width, int height, long tileID) {
    super();
    if (inputMBR != null)
      this.inputMBR.init(inputMBR);
    this.width = width;
    this.height = height;
    this.tileID = tileID;
    calculateScale();
  }

  protected void calculateScale() {
    this.scaleX = width / inputMBR.getWidth();
    this.scaleY = height / inputMBR.getHeight();
  }

  /**
   * Returns the width of the canvas in pixels
   * @return the width of the canvas in pixels
   */
  public int getWidth() {
    return width;
  }

  /**
   * Returns the height of the canvas in pixels
   * @return the height of the canvas in pixels
   */
  public int getHeight() {
    return height;
  }

  public Envelope getInputMBR() {
    return inputMBR;
  }

  /**
   * Transforms the given x-coordinate from input domain to image domain.
   * @param x the x-coordinate to transform to pixel space
   * @return the transformed x coordinate
   */
  public double transformX(double x) {
    return (x - inputMBR.getMinX()) * scaleX;
  }

  /**
   * Transforms the given y-coordinate from input domain to image domain.
   * @param y the y-coordinate to transform to pixel space
   * @return the transformed y coordinate
   */
  public double transformY(double y) {
    return (y - inputMBR.getMinY()) * scaleY;
  }

  public long getTileID() {
    return this.tileID;
  }
}