/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hama.ml.util;

import org.apache.hama.commons.math.DoubleVector;

/**
 * FeatureTransformer defines the interface to transform the original features
 * to new space.
 * 
 * NOTE: the user defined feature transformer must have a constructor with no parameters.
 * 
 */
public abstract class FeatureTransformer {
  
  public FeatureTransformer() {
  }
  
  /**
   * Transform the original features to transformed space.
   * @param originalFeatures
   * @return a new vector with the result of the operation.
   */
  public abstract DoubleVector transform(DoubleVector originalFeatures);
  
}
