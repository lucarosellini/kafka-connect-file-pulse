/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamthoughts.kafka.connect.filepulse.expression;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;

import java.util.Objects;

public class ValueExpression implements Expression {

    private final String value;

    /**
     * Creates a new {@link ValueExpression} instance.
     * @param value the static value.
     */
    public ValueExpression(final String value) {
        Objects.requireNonNull(value, "value cannot be null");
        this.value = value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SchemaAndValue evaluate(final EvaluationContext context) {
        return new SchemaAndValue(Schema.STRING_SCHEMA, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String originalExpression() {
        return value;
    }
}