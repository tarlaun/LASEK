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
package edu.ucr.cs.bdlab.beast.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class StringUtil {
  /**
   * Computed the edit distance between two strings. This simple implementation adapted from publicly available works
   * by Preston Lee. This is fairly naïve implementation and is not time efficient, so it'll fall over for long strings.
   * @param p1 the first string
   * @param p2 the second string
   * @return the edit distance (Levenshtien distance) as an integer. Zero means identical strings.
   * @see <a href="https://github.com/preston/emphasays/blob/master/src/main/java/com/prestonlee/emphasays/calculator/distance/LevenshteinDistanceCalculator.java">
   *   Lavenshtein Distance</a>
   */
  public static int levenshteinDistance(final String p1, final String p2) {
    final int[] costs = new int[p2.length() + 1];
    for (int i = 0; i <= p1.length(); i++) {
      int last = i;
      for (int j = 0; j <= p2.length(); j++) {
        if (i == 0) {
          costs[j] = j;
        } else {
          if (j > 0) {
            int newValue = costs[j - 1];
            if (p1.charAt(i - 1) != p2.charAt(j - 1)) {
              newValue = Math.min(Math.min(newValue, last), costs[j]) + 1;
            }
            costs[j - 1] = last;
            last = newValue;
          }
        }
      }
      if (i > 0) {
        costs[p2.length()] = last;
      }
    }
    return costs[p2.length()];
  }

  /**
   * Finds the strings in the {@code candidates} list that are within the given distance from the string {@code s}.
   * If no strings are found within the given distance, an empty collection is returned.
   * @param s the string to search for
   * @param candidates the list of strings to find the nearest neighbor from
   * @param d the maximum edit distance to consider. If the nearest neighbor is farther than d, no results are returned.
   * @return a collection of all strings that have less than {@code d} distance to the string {@code s}
   */
  public static Collection<String> nearestLevenshteinDistance(final String s, Collection<String> candidates, int d) {
    List<String> suggestions = new ArrayList<>();
    for (String candidate : candidates) {
      if (levenshteinDistance(s, candidate) <= d)
        suggestions.add(candidate);
    }
    return suggestions;
  }

  /**
   * Displays a list of strings in a human readable format. E.g., for one string, that string is displayed.
   * For two strings, the return value is 'str1' or 'str2', for three or more, the return value is 'str1', 'str2', ...,
   * or 'str3'
   * @param strings a list of strings to write
   * @param link how to link the last item in a list, either "and" or "or"
   * @return one string that contains all the given strings in a human readable way.
   */
  public static String humandReable(Collection<String> strings, String link) {
    if (strings == null || strings.isEmpty())
      return "";
    String[] strArray = strings.toArray(new String[strings.size()]);
    if (strArray.length == 1)
      return String.format("'%s'", strArray[0]);
    if (strArray.length == 2)
      return String.format("'%s' %s '%s'", strArray[0], link, strArray[1]);
    // Three or more
    StringBuilder str = new StringBuilder();
    for (int $i = 0; $i < strArray.length; $i++) {
      if ($i > 0) {
        str.append(',');
        str.append(' ');
      }
      if ($i == strArray.length - 1) {
        // Last element
        str.append(' ');
        str.append(link);
        str.append(' ');
      }
      str.append('\'');
      str.append(strArray[$i]);
      str.append('\'');
    }
    return str.toString();
  }
}
