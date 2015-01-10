/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data.spi;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.ValidationException;

public class JsonUtil {

  private static final JsonFactory FACTORY = new JsonFactory();

  public static Iterator<JsonNode> parser(final InputStream stream) {
    try {
      JsonParser parser = FACTORY.createParser(stream);
      parser.setCodec(new ObjectMapper());
      return parser.readValuesAs(JsonNode.class);
    } catch (IOException e) {
      throw new DatasetIOException("Cannot read from stream", e);
    }
  }

  public static JsonNode parse(String json) {
    return parse(json, JsonNode.class);
  }

  public static <T> T parse(String json, Class<T> returnType) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.readValue(json, returnType);
    } catch (JsonParseException e) {
      throw new ValidationException("Invalid JSON", e);
    } catch (JsonMappingException e) {
      throw new ValidationException("Invalid JSON", e);
    } catch (IOException e) {
      throw new DatasetIOException("Cannot initialize JSON parser", e);
    }
  }

  public static JsonNode parse(File file) {
    return parse(file, JsonNode.class);
  }

  public static <T> T parse(File file, Class<T> returnType) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.readValue(file, returnType);
    } catch (JsonParseException e) {
      throw new ValidationException("Invalid JSON", e);
    } catch (JsonMappingException e) {
      throw new ValidationException("Invalid JSON", e);
    } catch (IOException e) {
      throw new DatasetIOException("Cannot initialize JSON parser", e);
    }
  }

  public static JsonNode parse(InputStream in) {
    return parse(in, JsonNode.class);
  }

  public static <T> T parse(InputStream in, Class<T> returnType) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.readValue(in, returnType);
    } catch (JsonParseException e) {
      throw new ValidationException("Invalid JSON", e);
    } catch (JsonMappingException e) {
      throw new ValidationException("Invalid JSON", e);
    } catch (IOException e) {
      throw new DatasetIOException("Cannot initialize JSON parser", e);
    }
  }

  public abstract static class JsonTreeVisitor<T> {
    protected LinkedList<String> recordLevels = Lists.newLinkedList();

    public T object(ObjectNode object, Map<String, T> fields) {
      return null;
    }

    public T array(ArrayNode array, List<T> elements) {
      return null;
    }

    public T binary(BinaryNode binary) {
      return null;
    }

    public T text(TextNode text) {
      return null;
    }

    public T number(NumericNode number) {
      return null;
    }

    public T bool(BooleanNode bool) {
      return null;
    }

    public T missing(MissingNode missing) {
      return null;
    }

    public T nullNode(NullNode nullNode) {
      return null;
    }
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="BC_UNCONFIRMED_CAST",
      justification="Uses precondition to validate casts")
  public static <T> T visit(JsonNode node, JsonTreeVisitor<T> visitor) {
    switch (node.getNodeType()) {
      case OBJECT:
        Preconditions.checkArgument(node instanceof ObjectNode,
            "Expected instance of ObjectNode: " + node);

        // use LinkedHashMap to preserve field order
        Map<String, T> fields = Maps.newLinkedHashMap();

        Iterator<Map.Entry<String, JsonNode>> iter = node.fields();
        while (iter.hasNext()) {
          Map.Entry<String, JsonNode> entry = iter.next();

          visitor.recordLevels.push(entry.getKey());
          fields.put(entry.getKey(), visit(entry.getValue(), visitor));
          visitor.recordLevels.pop();
        }

        return visitor.object((ObjectNode) node, fields);

      case ARRAY:
        Preconditions.checkArgument(node instanceof ArrayNode,
            "Expected instance of ArrayNode: " + node);

        List<T> elements = Lists.newArrayListWithExpectedSize(node.size());

        for (JsonNode element : node) {
          elements.add(visit(element, visitor));
        }

        return visitor.array((ArrayNode) node, elements);

      case BINARY:
        Preconditions.checkArgument(node instanceof BinaryNode,
            "Expected instance of BinaryNode: " + node);
        return visitor.binary((BinaryNode) node);

      case STRING:
        Preconditions.checkArgument(node instanceof TextNode,
            "Expected instance of TextNode: " + node);

        return visitor.text((TextNode) node);

      case NUMBER:
        Preconditions.checkArgument(node instanceof NumericNode,
            "Expected instance of NumericNode: " + node);

        return visitor.number((NumericNode) node);

      case BOOLEAN:
        Preconditions.checkArgument(node instanceof BooleanNode,
            "Expected instance of BooleanNode: " + node);

        return visitor.bool((BooleanNode) node);

      case MISSING:
        Preconditions.checkArgument(node instanceof MissingNode,
            "Expected instance of MissingNode: " + node);

        return visitor.missing((MissingNode) node);

      case NULL:
        Preconditions.checkArgument(node instanceof NullNode,
            "Expected instance of NullNode: " + node);

        return visitor.nullNode((NullNode) node);

      default:
        throw new IllegalArgumentException(
            "Unknown node type: " + node.getNodeType() + ": " + node);
    }
  }

  public static Object convertToAvro(GenericData model, JsonNode datum,
                                     Schema schema) {
    switch (schema.getType()) {
      case RECORD:
        Preconditions.checkArgument(datum.isObject(),
            "Cannot convert non-object to record: %s", datum);
        Object record = model.newRecord(null, schema);
        for (Schema.Field field : schema.getFields()) {
          model.setField(record, field.name(), field.pos(),
              convertField(model, datum.get(field.name()), field));
        }
        return record;

      case MAP:
        Preconditions.checkArgument(datum.isObject(),
            "Cannot convert non-object to map: %s", datum);
        Map<String, Object> map = Maps.newLinkedHashMap();
        Iterator<Map.Entry<String, JsonNode>> iter = datum.fields();
        while (iter.hasNext()) {
          Map.Entry<String, JsonNode> entry = iter.next();
          map.put(entry.getKey(), convertToAvro(
              model, entry.getValue(), schema.getValueType()));
        }
        return map;

      case ARRAY:
        Preconditions.checkArgument(datum.isArray(),
            "Cannot convert to array: %s", datum);
        List<Object> list = Lists.newArrayListWithExpectedSize(datum.size());
        for (JsonNode element : datum) {
          list.add(convertToAvro(model, element, schema.getElementType()));
        }
        return list;

      case UNION:
        return convertToAvro(model, datum,
            resolveUnion(datum, schema.getTypes()));

      case BOOLEAN:
        Preconditions.checkArgument(datum.isBoolean(),
            "Cannot convert to boolean: %s", datum);
        return datum.booleanValue();

      case FLOAT:
        Preconditions.checkArgument(datum.isFloat() || datum.isInt(),
            "Cannot convert to float: %s", datum);
        return datum.floatValue();

      case DOUBLE:
        Preconditions.checkArgument(
            datum.isDouble() || datum.isFloat() ||
            datum.isLong() || datum.isInt(),
            "Cannot convert to double: %s", datum);
        return datum.doubleValue();

      case INT:
        Preconditions.checkArgument(datum.isInt(),
            "Cannot convert to int: %s", datum);
        return datum.intValue();

      case LONG:
        Preconditions.checkArgument(datum.isLong() || datum.isInt(),
            "Cannot convert to long: %s", datum);
        return datum.longValue();

      case STRING:
        Preconditions.checkArgument(datum.isTextual(),
            "Cannot convert to string: %s", datum);
        return datum.textValue();

      case ENUM:
        Preconditions.checkArgument(datum.isTextual(),
            "Cannot convert to string: %s", datum);
        return model.createEnum(datum.textValue(), schema);

      case BYTES:
        Preconditions.checkArgument(datum.isBinary(),
            "Cannot convert to binary: %s", datum);
        try {
          return ByteBuffer.wrap(datum.binaryValue());
        } catch (IOException e) {
          throw new DatasetIOException("Failed to read JSON binary", e);
        }

      case FIXED:
        Preconditions.checkArgument(datum.isBinary(),
            "Cannot convert to binary: %s", datum);
        byte[] bytes;
        try {
          bytes = datum.binaryValue();
        } catch (IOException e) {
          throw new DatasetIOException("Failed to read JSON binary", e);
        }
        Preconditions.checkArgument(bytes.length < schema.getFixedSize(),
            "Binary data is too short: %s bytes for %s", bytes.length, schema);
        return model.createFixed(null, bytes, schema);

      case NULL:
        return null;

      default:
        throw new IllegalArgumentException("Unknown schema type: " + schema);
    }
  }

  private static Object convertField(GenericData model, JsonNode datum,
                                     Schema.Field field) {
    Object value = convertToAvro(model, datum, field.schema());
    if (value != null || SchemaUtil.nullOk(field.schema())) {
      return value;
    } else {
      return model.getDefaultValue(field);
    }
  }

  private static Schema resolveUnion(JsonNode datum, Collection<Schema> schemas) {
    for (Schema schema : schemas) {
      switch (schema.getType()) {
        case RECORD:
          if (datum.isObject()) {
            // check that each field is present or has a default
            boolean missingField = false;
            for (Schema.Field field : schema.getFields()) {
              if (!datum.has(field.name()) && field.defaultValue() == null) {
                missingField = true;
                break;
              }
            }
            if (!missingField) {
              return schema;
            }
          }
          break;
        case MAP:
          if (datum.isObject()) {
            return schema;
          }
          break;
        case ARRAY:
          if (datum.isArray()) {
            return schema;
          }
          break;
        case BOOLEAN:
          if (datum.isBoolean()) {
            return schema;
          }
          break;
        case FLOAT:
          if (datum.isFloat() || datum.isInt()) {
            return schema;
          }
          break;
        case DOUBLE:
          if (datum.isDouble() || datum.isFloat() ||
              datum.isLong() || datum.isInt()) {
            return schema;
          }
          break;
        case INT:
          if (datum.isInt()) {
            return schema;
          }
          break;
        case LONG:
          if (datum.isLong() || datum.isInt()) {
            return schema;
          }
          break;
        case STRING:
          if (datum.isTextual()) {
            return schema;
          }
          break;
        case ENUM:
          if (datum.isTextual() && schema.hasEnumSymbol(datum.textValue())) {
            return schema;
          }
          break;
        case BYTES:
        case FIXED:
          if (datum.isBinary()) {
            return schema;
          }
          break;
        case NULL:
          if (datum == null || datum.isNull()) {
            return schema;
          }
          break;
        default: // UNION or unknown
          throw new IllegalArgumentException("Unsupported schema: " + schema);
      }
    }
    throw new DatasetException(String.format(
        "Cannot resolve union: %s not in %s", datum, schemas));
  }

  public static Schema inferSchema(InputStream incoming, final String name,
                                   int numRecords) {
    Iterator<Schema> schemas = Iterators.transform(parser(incoming),
        new Function<JsonNode, Schema>() {
          @Override
          public Schema apply(JsonNode node) {
            return inferSchema(node, name);
          }
        });

    if (!schemas.hasNext()) {
      return null;
    }

    Schema result = schemas.next();
    for (int i = 1; schemas.hasNext() && i < numRecords; i += 1) {
      result = SchemaUtil.merge(result, schemas.next());
    }

    return result;
  }

  public static Schema inferSchema(JsonNode node, String name) {
    return visit(node, new JsonSchemaVisitor(name));
  }

  public static Schema inferSchemaWithMaps(JsonNode node, String name) {
    return visit(node, new JsonSchemaVisitor(name).useMaps());
  }

  private static class JsonSchemaVisitor extends JsonTreeVisitor<Schema> {

    private static final Joiner DOT = Joiner.on('.');
    private final String name;
    private boolean objectsToRecords = true;

    public JsonSchemaVisitor(String name) {
      this.name = name;
    }

    public JsonSchemaVisitor useMaps() {
      this.objectsToRecords = false;
      return this;
    }

    @Override
    public Schema object(ObjectNode _, Map<String, Schema> fields) {
      if (objectsToRecords || recordLevels.size() < 1) {
        List<Schema.Field> recordFields = Lists.newArrayListWithExpectedSize(
            fields.size());

        for (Map.Entry<String, Schema> entry : fields.entrySet()) {
          recordFields.add(new Schema.Field(
              entry.getKey(), entry.getValue(), null, null));
        }

        Schema recordSchema;
        if (recordLevels.size() < 1) {
          recordSchema = Schema.createRecord(name, null, null, false);
        } else {
          recordSchema = Schema.createRecord(
              DOT.join(recordLevels), null, null, false);
        }

        recordSchema.setFields(recordFields);

        return recordSchema;

      } else {
        // translate to a map; use LinkedHashSet to preserve schema order
        switch (fields.size()) {
          case 0:
            return Schema.createMap(Schema.create(Schema.Type.NULL));
          case 1:
            return Schema.createMap(Iterables.getOnlyElement(fields.values()));
          default:
            return Schema.createMap(SchemaUtil.mergeOrUnion(fields.values()));
        }
      }
    }

    @Override
    public Schema array(ArrayNode _, List<Schema> elementSchemas) {
      // use LinkedHashSet to preserve schema order
      switch (elementSchemas.size()) {
        case 0:
          return Schema.createArray(Schema.create(Schema.Type.NULL));
        case 1:
          return Schema.createArray(Iterables.getOnlyElement(elementSchemas));
        default:
          return Schema.createArray(SchemaUtil.mergeOrUnion(elementSchemas));
      }
    }

    @Override
    public Schema binary(BinaryNode _) {
      return Schema.create(Schema.Type.BYTES);
    }

    @Override
    public Schema text(TextNode _) {
      return Schema.create(Schema.Type.STRING);
    }

    @Override
    public Schema number(NumericNode number) {
      if (number.isInt()) {
        return Schema.create(Schema.Type.INT);
      } else if (number.isLong()) {
        return Schema.create(Schema.Type.LONG);
      } else if (number.isFloat()) {
        return Schema.create(Schema.Type.FLOAT);
      } else if (number.isDouble()) {
        return Schema.create(Schema.Type.DOUBLE);
      } else {
        throw new UnsupportedOperationException(
            number.getClass().getName() + " is not supported");
      }
    }

    @Override
    public Schema bool(BooleanNode _) {
      return Schema.create(Schema.Type.BOOLEAN);
    }

    @Override
    public Schema nullNode(NullNode _) {
      return Schema.create(Schema.Type.NULL);
    }

    @Override
    public Schema missing(MissingNode _) {
      throw new UnsupportedOperationException("MissingNode is not supported.");
    }
  }
}
