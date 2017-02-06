package org.apache.hadoop.hive.druid.segment;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.data.input.impl.*;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.segment.indexing.DataSchema;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class FieldMappings
{
    public static final String FILE_NAME = ".mapping";
    private static final ObjectMapper MAPPER = new DefaultObjectMapper();
    private Boolean isDynamic;

    public Boolean getDynamic()
    {
      return isDynamic;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    private FieldMappings() {}


    private Map<String, DimensionSchema> fieldTypes;

    public DimensionSchema addDimensionSchema(String dimensionName)
    {
        int index = dimensionName.indexOf('|');
        String name = dimensionName.substring(index + 1);
        DimensionSchema dimensionSchema = fieldTypes.get(name);
        if (dimensionSchema != null) {
          return dimensionSchema;
        }
        if (index != 1) {
            dimensionSchema = new StringDimensionSchema(name);
            this.fieldTypes.put(name, dimensionSchema);
            return dimensionSchema;
        }

        switch (dimensionName.charAt(0)) {
          case 'l':
            dimensionSchema = new LongDimensionSchema(name);
            this.fieldTypes.put(name, dimensionSchema);
            return dimensionSchema;
          case  'f':
            dimensionSchema = new FloatDimensionSchema(name);
            this.fieldTypes.put(name, dimensionSchema);
            return dimensionSchema;
          default:
            dimensionSchema = new StringDimensionSchema(name);
            this.fieldTypes.put(name, dimensionSchema);
            return dimensionSchema;
        }
    }

    public Map<String, DimensionSchema> getFieldTypes()
    {
        return  fieldTypes;
    }

    public void delete(File segmentDir) throws IOException
    {
        FileUtils.forceDelete(new File(segmentDir, FILE_NAME));
    }

    public void writeFieldTypesTo(File segmentDir) throws IOException
    {
        if (!segmentDir.exists()) {
            throw new IOException("WTF! Segment directory is not exist, file path: " + segmentDir.toString());
        }
        List<Field> fields = Lists.newArrayList();
        for (DimensionSchema dimensionSchema : fieldTypes.values()) {
            fields.add(Field.instance().withName(dimensionSchema.getName())
                                       .withType(dimensionSchema.getTypeName())
                                       .withHasMultipleValues(false)
            );
        }
        MAPPER.writeValue(new File(segmentDir, FILE_NAME), fields);
    }


    public static class Builder
    {

        private Map<String, DimensionSchema> fieldTypes = Maps.newHashMap();
        private boolean isDynamic = false;

        public Builder buildFieldTypesFrom(DataSchema schema)
        {
            DimensionsSpec dimensionsSpec = schema.getParser().getParseSpec().getDimensionsSpec();

            this.isDynamic = false;
            this.fieldTypes = getFieldTypes(dimensionsSpec);
            return this;
        }

        public Builder buildFieldTypesFrom(File segmentDir) throws IOException
        {
            if (segmentDir == null || !segmentDir.exists()) {
                throw new IOException("WTF! Segment directory is not exist"
                        + segmentDir == null? ".":", segmentDir: "+ segmentDir.getAbsolutePath());
            }
            List<Field> fields = MAPPER.readValue(new File(segmentDir, FILE_NAME), new TypeReference<List<Field>>() {});
            this.fieldTypes = getFieldTypes(fields);
            return this;
        }

        public Builder buildFieldTypesFrom(ZipInputStream stream) throws IOException
        {
            ZipEntry tmpEntry;
            while(null != (tmpEntry = (stream.getNextEntry()))) {
                if (tmpEntry.getName().equals(FILE_NAME)) {
                    break;
                }
            }
            List<Field> fields = MAPPER.readValue(stream, new TypeReference<List<Field>>() {});
            this.fieldTypes = getFieldTypes(fields);
            return this;
        }

        private Map<String, DimensionSchema> getFieldTypes(DimensionsSpec dimensionsSpec)
        {
            if(isDynamic) {
                fieldTypes = Maps.newConcurrentMap();
            } else {
                fieldTypes = Maps.newHashMap();
            }
            Set<String> excludedDimensions = dimensionsSpec.getDimensionExclusions();
            for (String dimensionName : dimensionsSpec.getDimensionNames()) {
                if (excludedDimensions != null && !excludedDimensions.isEmpty() && excludedDimensions.contains(dimensionName)) {
                    continue;
                }
                DimensionSchema dimensionSchema = dimensionsSpec.getSchema(dimensionName);
                fieldTypes.put(dimensionSchema.getName(), dimensionSchema);
            }
            return fieldTypes;
        }

        private Map<String, DimensionSchema> getFieldTypes(List<Field> fields) throws IOException {
            Map<String, DimensionSchema> fieldTypes = new ConcurrentHashMap<>();
            for (Field field : fields) {
                DimensionSchema dimensionSchema = MAPPER.readValue(
                        MAPPER.writeValueAsString(field),
                        DimensionSchema.class
                );
                fieldTypes.put(dimensionSchema.getName(), dimensionSchema);
            }
            return fieldTypes;
        }

        public FieldMappings build()
        {
            FieldMappings mappings = new FieldMappings();
            mappings.isDynamic = this.isDynamic;
            if (this.isDynamic) {
              mappings.fieldTypes = this.fieldTypes;
            } else {
              mappings.fieldTypes = Collections.unmodifiableMap(this.fieldTypes);
            }
            return mappings;
        }

    }

    public static class Field
    {
        public static Field instance()
        {
            return new Field();
        }

        private String name;
        private String type = DimensionSchema.STRING_TYPE_NAME;

        private boolean hasMultipleValues = false;

        @JsonProperty
        public String getName()
        {
            return name;
        }

        @JsonProperty
        public String getType()
        {
            return type;
        }

        @JsonProperty
        public boolean isHasMultipleValues()
        {
            return hasMultipleValues;
        }

        public Field withName(String name)
        {
            this.name = name;
            return this;
        }

        public Field withType(String type)
        {
            this.type = type;
            return this;
        }

        public Field withHasMultipleValues(boolean hasMultipleValues)
        {
            this.hasMultipleValues = hasMultipleValues;
            return this;
        }
    }
}
