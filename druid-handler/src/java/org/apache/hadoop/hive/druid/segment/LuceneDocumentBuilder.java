/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.druid.segment;

import com.google.common.collect.Maps;
import com.metamx.common.Pair;
import com.metamx.common.parsers.ParseException;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.DimensionSchema.ValueType;
import io.druid.segment.column.Column;
import org.apache.lucene.document.*;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;

import java.util.*;

public class LuceneDocumentBuilder
{
  private final FieldMappings fieldMappings;
  private final Map<String, DimensionSchema> fieldTypes;

  private Pair<Field, Field> fields;
  private HashMap<String, Pair<Field, Field>> fieldMap = Maps.newHashMap();
  private HashMap<String, BytesRef> stringFieldDocValues = Maps.newHashMap();

  private final boolean isDynamic;

  public LuceneDocumentBuilder(FieldMappings fieldMappings)
  {
    this.fieldMappings = fieldMappings;
    this.isDynamic = fieldMappings.getDynamic();
    this.fieldTypes = fieldMappings.getFieldTypes();
  }

  private void setTimestampField(long timestamp, Document doc)
  {
    fields = fieldMap.get(Column.TIME_COLUMN_NAME);
    if (fields == null) {
        fields = new Pair<Field, Field>(
                new LongField(Column.TIME_COLUMN_NAME, timestamp, Store.NO),
                new NumericDocValuesField(Column.TIME_COLUMN_NAME, timestamp)
        );

        fieldMap.put(Column.TIME_COLUMN_NAME, fields);
    } else {
        fields.lhs.setLongValue(timestamp);
        fields.rhs.setLongValue(timestamp);
    }
    doc.add(fields.lhs);
    doc.add(fields.rhs);
  }

  public Document buildLuceneDocument(InputRow row) throws ParseException
  {
    Document doc = new Document();
    try {
      setTimestampField(row.getTimestampFromEpoch(), doc);

      Iterator<String> dimensionIt = row.getDimensions().iterator();
      
      DimensionSchema dimensionSchema;
      String dimension;
      while (dimensionIt.hasNext()) {
        dimension = dimensionIt.next();
        if (isDynamic) {
          dimensionSchema = fieldMappings.addDimensionSchema(dimension);
        } else {
          dimensionSchema = fieldTypes.get(dimension);
        }
        if (dimensionSchema == null) {
          continue;
        }
        Object value = row.getRaw(dimension);
        if (value == null) {
          continue;
        }

        if (value instanceof List) {
          addMultiFields(dimensionSchema, dimensionSchema.getName(), (List<Object>)value, doc);
        } else {
          if (value instanceof List) {
            List<Object> listVal = (List<Object>)value;
            if (listVal.isEmpty()) {
              continue;
            } else if (listVal.size() == 1) {
              addSingleFields(dimensionSchema, dimensionSchema.getName(), listVal.get(0), doc);
              continue;
            }
            throw new ParseException("multi values passed to a single-value dimension, dimension: " + dimension);
          } else {
            addSingleFields(dimensionSchema, dimensionSchema.getName(), value, doc);
          }
        }
      }
    } catch (Exception ex) {
      throw new ParseException(ex, "buildLuceneDocument error ");
    }
    return doc;
  }

  private void addSingleFields(DimensionSchema dimensionSchema, String key, Object value, Document doc) {
      fields = fieldMap.get(key);

      if (fields == null) {
          fields = initFields(key, dimensionSchema, value, null, null);
          if (fields == null) {
              return;
          }
          fieldMap.put(key, fields);
      } else {
          initFields(key, dimensionSchema, value, fields, stringFieldDocValues.get(key));
      }
      doc.add(fields.lhs);
      doc.add(fields.rhs);
  }

  private Pair<Field, Field> initFields(String key, DimensionSchema dimensionSchema, Object value, Pair<Field, Field> fields, BytesRef bytesRef)
  {
      switch (dimensionSchema.getValueType()) {
        case FLOAT:
              float fVal;
              if (value instanceof Number) {
                  fVal = ((Number)value).floatValue();
              } else {
                  fVal = Float.parseFloat(value.toString());
              }
              if (fields == null) {
                  return new Pair<Field, Field>(
                          new FloatField(key, fVal, Store.NO),
                          new FloatDocValuesField(key, fVal)
                  );
              } else {
                  fields.lhs.setFloatValue(fVal);
                  fields.rhs.setFloatValue(fVal);
                  return fields;
              }
        case LONG:
              long lVal;
              if (value instanceof Number) {
                  lVal = ((Number)value).longValue();
              } else {
                  lVal = Long.parseLong(value.toString());
              }
              if (fields == null) {
                  return new Pair<Field, Field>(
                          new LongField(key, lVal, Store.NO),
                          new NumericDocValuesField(key, lVal)
                  );
              } else {
                  fields.lhs.setLongValue(lVal);
                  fields.rhs.setLongValue(lVal);
                  return fields;
              }
        case STRING:
              String v = value.toString();
              if (fields == null) {
                  BytesRef ref = new BytesRef(new byte[1024]);
                  ref.length = UnicodeUtil.UTF16toUTF8(v, 0, v.length(), ref.bytes);
                  stringFieldDocValues.put(key, ref);
                  return new Pair<Field, Field>(
                          new StringField(key, v, Store.NO),
                          new SortedDocValuesField(key, ref)
                  );
              } else {
                  bytesRef.length = UnicodeUtil.UTF16toUTF8(v, 0, v.length(), bytesRef.bytes);
                  fields.lhs.setStringValue(v);
                  fields.rhs.setBytesValue(bytesRef);
                  return fields;
              }
          default:
              break;
      }
      return null;
  }

  private void addMultiFields(DimensionSchema dimensionSchema, String key, List<Object> values, Document doc) {
    if (values.isEmpty()) {
      return;
    }

    switch (dimensionSchema.getValueType()) {
      case FLOAT:
        for (Object value : values) {
          float fVal = ((Number) value).floatValue();
          doc.add(new FloatField(key, fVal, Store.NO));
          doc.add(new SortedNumericDocValuesField(key, Float.floatToIntBits(fVal)));
        }
        break;
      case LONG:
        for (Object value : values) {
          long lVal = ((Number) value).longValue();
          doc.add(new LongField(key, lVal, Store.NO));
          doc.add(new SortedNumericDocValuesField(key, lVal));
        }
        break;
      case STRING:
        for (Object value : values) {
          doc.add(new StringField(key, String.valueOf(value), Store.NO));
          doc.add(new SortedSetDocValuesField(key, new BytesRef(String.valueOf(value))));
        }
        break;
      default:
        break;
    }
  }

  public Map<String, DimensionSchema> getFieldTypes()
  {
      return fieldTypes;
  }
}
