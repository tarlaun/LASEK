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
package edu.ucr.cs.bdlab.beast.util;


import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.common.CLIOperation;
import edu.ucr.cs.bdlab.beast.common.JCLIOperation;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.Stack;

public class OperationUtilTestOperations {


  @OperationMetadata(shortName = "baseclass", description = "...")
  public static class TestBaseClass implements JCLIOperation {
    @OperationParam(
        description = "description",
        defaultValue = "false"
    )
    public static final String Param1 = "param1";

    @Override
    public Object run(BeastOptions opts, String[] inputs, String[] outputs, JavaSparkContext sc) {
      return null;
    }
  }

  @OperationMetadata(shortName = "subclass", description = "...", inheritParams = {TestBaseClass.class})
  public static class TestSubClass implements CLIOperation {
    @OperationParam(
        description = "description",
        defaultValue = "false"
    )
    public static final String Param2 = "param2";

    @Override
    public Object run(BeastOptions opts, String[] inputs, String[] outputs, SparkContext sc) throws IOException {
      return null;
    }
  }

  @OperationMetadata(shortName = "subclass", description = "...")
  public static class TestSubClass3 implements JCLIOperation {
    @OperationParam(
        description = "description",
        defaultValue = "false"
    )
    public static final String Param3 = "param3";

    @Override
    public void addDependentClasses(BeastOptions opts, Stack<Class<?>> classes) {
      classes.add(TestBaseClass.class);
    }

    @Override
    public Object run(BeastOptions opts, String[] inputs, String[] outputs, JavaSparkContext sc) {
      return null;
    }
  }
}