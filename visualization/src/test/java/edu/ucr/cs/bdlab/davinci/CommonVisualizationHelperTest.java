/*
 * Copyright 2020 University of California, Riverside
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

import java.awt.*;

public class CommonVisualizationHelperTest extends TestCase {

  public void testParseNamedColors() {
    assertEquals(Color.BLACK, CommonVisualizationHelper.getColor("Black"));
    assertEquals(Color.BLACK, CommonVisualizationHelper.getColor("black"));
  }

  public void testHTMLColors() {
    assertEquals(Color.BLACK, CommonVisualizationHelper.getColor("#000"));
    assertEquals(Color.RED, CommonVisualizationHelper.getColor("#ff0000"));
    assertEquals(Color.RED, CommonVisualizationHelper.getColor("#f00"));
    assertEquals(new Color(0, 0xcc, 0xdd), CommonVisualizationHelper.getColor("#0CD"));
    assertEquals(new Color(0xab, 0xcd, 0xef), CommonVisualizationHelper.getColor("#abcdef"));
    // Test with alpha
    assertEquals(new Color(0xaa, 0xbb, 0xcc, 0xdd), CommonVisualizationHelper.getColor("#abcd"));
    assertEquals(new Color(0xab, 0xbc, 0xcd, 0xef), CommonVisualizationHelper.getColor("#abbccdef"));
  }

}