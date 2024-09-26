/*
 * Copyright 2023-2024 benchANT GmbH. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package site.ycsb.db;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.htrace.shaded.fasterxml.jackson.databind.JsonNode;
import org.apache.htrace.shaded.fasterxml.jackson.databind.ObjectMapper;
import org.apache.htrace.shaded.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.htrace.shaded.fasterxml.jackson.databind.node.BooleanNode;
import org.apache.htrace.shaded.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.htrace.shaded.fasterxml.jackson.databind.node.TextNode;

import site.ycsb.workloads.schema.SchemaHolder;
import site.ycsb.workloads.schema.SchemaHolder.SchemaColumn;
import site.ycsb.workloads.schema.SchemaHolder.SchemaColumnKind;
import site.ycsb.workloads.schema.SchemaHolder.SchemaColumnType;

public final class JdbcDBInitHelper {

    private static final SchemaHolder schema = SchemaHolder.INSTANCE;

    public static void createTable(String tableName, List<Connection> conns, String textKeyType) throws SQLException {
        final StringBuilder b = new StringBuilder("CREATE TABLE ");
        b.append(tableName).append(" ( ");
        // this is YCSB nonetheless, so we need an ID
        b.append( JdbcDBConstants.PRIMARY_KEY).append(" " + textKeyType + " PRIMARY KEY");
        for(SchemaColumn column : schema.getOrderedListOfColumns()) {
            if(column.getColumnKind() == SchemaColumnKind.SCALAR) {
                b.append(", ").append(column.getColumnName()).append(" ");
                if(column.getColumnType() == SchemaColumnType.TEXT) b.append(" " + textKeyType + " ");
                else if( column.getColumnType() == SchemaColumnType.INT) b.append(" INTEGER ");
                else if( column.getColumnType() == SchemaColumnType.LONG) b.append(" BIGINT ");
            } else if(column.getColumnKind() == SchemaColumnKind.ARRAY) {
                throw new SQLException("array column types currently not supported");
            } else if(column.getColumnKind() == SchemaColumnKind.NESTED) {
                throw new SQLException("nested column types currently not supported");
            } else {
                throw new SQLException("other column types currently not supported");
            }
        }
        b.append(")");
        String sql = b.toString();
        for(Connection c : conns) {
            Statement stmt = c.createStatement();
            stmt.executeUpdate(sql);
        }
    }
  
  public static void dropTable(String tableName, List<Connection> conns) throws SQLException {
    System.err.println("droppting table");
    String dropper = "DROP TABLE " + tableName;
    for(Connection c : conns) {
      Statement stmt = c.createStatement();
      stmt.executeUpdate(dropper);
    }
  }

  static List<IndexDescriptor> getIndexList(Properties props) {
    String indexeslist = props.getProperty(JdbcDBConstants.INDEX_LIST_PROPERTY);
    if(indexeslist == null) {
      return Collections.emptyList();
    }
    ObjectMapper mapper = new ObjectMapper();
    JsonNode root = null;
    try {
      root = mapper.readTree(indexeslist);
    } catch(IOException ex) {
      throw new RuntimeException(ex);
    }
    if(!root.isArray()) {
      throw new IllegalArgumentException("index specification must be a JSON array");
    }
    ArrayNode array = (ArrayNode) root;
    if(array.size() == 0) {
      return Collections.emptyList();
    }
    List<IndexDescriptor> retVal = new ArrayList<>();
    for(int i = 0; i < array.size(); i++) {
      JsonNode el = array.get(i);
      if(!el.isObject()) {
        throw new IllegalArgumentException("index elements must be a JSON object");
      }

      IndexDescriptor desc = new IndexDescriptor();
      ObjectNode object = (ObjectNode) el;
      JsonNode name = object.get("name");
      if(name == null || !name.isTextual()) {
        throw new IllegalArgumentException("index elements must be a JSON object with 'name' of type string");
      }
      desc.name = ((TextNode) name).asText();

      JsonNode conc = object.get("concurrent");
      if(conc == null || !conc.isBoolean()) {
        desc.concurrent = false;
      } else {
        desc.concurrent = ((BooleanNode) conc).asBoolean();
      }

      JsonNode type = object.get("method");
      if(type == null || !type.isTextual()) {
        desc.method = "btree";
      } else {
        desc.method = ((TextNode) type).asText();
      }

      JsonNode order = object.get("order");
      if(order == null || !order.isTextual()) {
        desc.order = "ASC";
      } else {
        desc.order = ((TextNode) order).asText();
      }
      JsonNode sorts = object.get("columns");
      if(sorts == null || !sorts.isArray()) {
        throw new IllegalArgumentException("index elements must be a JSON object with 'columns' set as an array of strings");
      }
      ArrayNode columnsArray = (ArrayNode) sorts;
      for(int j = 0; j < columnsArray.size(); j++) {
        JsonNode sortEl = columnsArray.get(j);
        if(sortEl == null || !sortEl.isTextual()) {
          throw new IllegalArgumentException("index elements must be a JSON object with 'sortAttributes' set as an array of strings");
        }
        desc.columnNames.add(((TextNode) sortEl).asText());
      }
      retVal.add(desc);
    }
    return retVal;
  }

  private JdbcDBInitHelper() {
      // private constructor
  }
}
