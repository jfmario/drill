/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.exec.udfs.ml;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class H2OPredictFunctions {

  private static final Logger logger = LoggerFactory.getLogger(H2OPredictFunctions.class);

  @FunctionTemplate(name = "predict",
    isVarArg = true,
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL )

  public static class PredictFunction implements DrillSimpleFunc {

    /**
     * The fileURI is the URI to the JAR file for the POJO or MOJO of the trained model.
     */
    @Param VarCharHolder fileURI;

    /**
     * The inputs is an array of doubles (FLOAT8) columns. H2O models only accept doubles as input so all
     * ints, strings etc. must be cast to doubles. The length of the input array must match the expected
     * number of columns in the model.
     */
    @Param Float8Holder[] inputs;

    @Workspace
    hex.genmodel.MojoModel model;

    @Output
    Float8Holder score;

    @Override
    public void setup() {
      hex.genmodel.MojoReaderBackend mojoBackendReader;
      String fileName = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(fileURI);
      File inFile = new File(fileName);

      // If the user sends an empty filename, throw an exception
      if (fileName.isEmpty()) {
        throw UserException
          .validationError()
          .message("You must specify a valid file containing an H2O MOJO or POJO to use the predict function.")
          .build(logger);
      }

      if (!inFile.exists()) {
        throw UserException
          .validationError()
          .message("Can't find file " + fileName)
          .build(logger);
      }

      try {
        mojoBackendReader = hex.genmodel.MojoReaderBackendFactory.createReaderBackend(inFile);
        model = hex.genmodel.ModelMojoReader.readFrom(mojoBackendReader, true);
      } catch (IOException e) {
        throw UserException
          .dataReadError()
          .message("Could not open MOJO at " + fileName)
          .addContext(e.getMessage())
          .build(logger);
      }
    }

    @Override
    public void eval() {
      // User must supply at least one feature to the model, if not, throw an exception
      if (inputs.length == 0) {
        throw UserException
          .validationError()
          .message("No arguments were passed to the predict function.")
          .build(logger);
      } else if (model.nfeatures() != inputs.length) {
        // Validates that the user supplied the correct number of features to the model
        throw UserException
          .validationError()
          .message("Features supplied {} do not match the number of features the model requires: {}", inputs.length, model.nfeatures())
          .build(logger);
      }

      // Create an array of double primitives from the double row holder
      double data[] = new double[inputs.length];

      // Populate the data array
      for (int i = 0; i < inputs.length; i++) {
        data[i] = inputs[i].value;
      }

      // Get the predictions
      try {
        // The predictions function returns an array of predictions
        double[] predictions = new double[model.getPredsSize()];

        // Score the data
        model.score0(data, predictions);

        // Write the score to the column writer
        logger.debug("Prediction: " + predictions[0]);
        score.value = predictions[0];
      } catch (Exception e) {
        throw UserException
          .dataReadError()
          .message(e.getMessage())
          .build(logger);
      }
    }
  }
}
