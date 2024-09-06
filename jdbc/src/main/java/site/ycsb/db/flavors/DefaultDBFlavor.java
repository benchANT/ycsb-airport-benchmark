/**
 * Copyright (c) 2016, 2019 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package site.ycsb.db.flavors;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import site.ycsb.db.JdbcDBConstants;
import site.ycsb.db.JdbcDBInitHelper;
import site.ycsb.db.StatementType;
import site.ycsb.wrappers.Comparison;
import site.ycsb.wrappers.DatabaseField;
import site.ycsb.db.FilterBuilder;
import site.ycsb.db.IndexDescriptor;

/**
 * A default flavor for relational databases.
 */
public class DefaultDBFlavor extends DBFlavor {
  public DefaultDBFlavor() {
    super(DBName.DEFAULT);
  }
  public DefaultDBFlavor(DBName dbName) {
    super(dbName);
  }

  @Override 
  public void createDbAndSchema(String tableName, List<Connection> conns) throws SQLException {
    JdbcDBInitHelper.createTable(tableName, conns, "TEXT");
  }

  @Override
  public String createInsertStatement(StatementType insertType, String key) {
    StringBuilder insert = new StringBuilder("INSERT INTO ");
    insert.append(insertType.getTableName());
    insert.append(" (" + JdbcDBConstants.PRIMARY_KEY + "," + insertType.getFieldString() + ")");
    insert.append(" VALUES(?");
    for (int i = 0; i < insertType.getNumFields(); i++) {
      insert.append(",?");
    }
    insert.append(")");
    return insert.toString();
  }

  @Override
  public String createReadStatement(StatementType readType, String key) {
    StringBuilder read = new StringBuilder("SELECT * FROM ");
    read.append(readType.getTableName());
    read.append(" WHERE ");
    read.append(JdbcDBConstants.PRIMARY_KEY);
    read.append(" = ");
    read.append("?");
    return read.toString();
  }

  @Override
  public String createDeleteStatement(StatementType deleteType, String key) {
    StringBuilder delete = new StringBuilder("DELETE FROM ");
    delete.append(deleteType.getTableName());
    delete.append(" WHERE ");
    delete.append(JdbcDBConstants.PRIMARY_KEY);
    delete.append(" = ?");
    return delete.toString();
  }

  @Override
  public String createUpdateStatement(StatementType updateType, String key) {
    String[] fieldKeys = updateType.getFieldString().split(",");
    StringBuilder update = new StringBuilder("UPDATE ");
    update.append(updateType.getTableName());
    update.append(" SET ");
    for (int i = 0; i < fieldKeys.length; i++) {
      update.append(fieldKeys[i]);
      update.append("=?");
      if (i < fieldKeys.length - 1) {
        update.append(", ");
      }
    }
    update.append(" WHERE ");
    update.append(JdbcDBConstants.PRIMARY_KEY);
    update.append(" = ?");
    return update.toString();
  }

  @Override
  public String createScanStatement(StatementType scanType, String key, boolean sqlserverScans, boolean sqlansiScans) {
    StringBuilder select;
    if (sqlserverScans) {
      select = new StringBuilder("SELECT TOP (?) * FROM ");
    } else {
      select = new StringBuilder("SELECT * FROM ");
    }
    select.append(scanType.getTableName());
    select.append(" WHERE ");
    select.append(JdbcDBConstants.PRIMARY_KEY);
    select.append(" >= ?");
    select.append(" ORDER BY ");
    select.append(JdbcDBConstants.PRIMARY_KEY);
    if (!sqlserverScans) {
      if (sqlansiScans) {
        select.append(" FETCH FIRST ? ROWS ONLY");
      } else {
        select.append(" LIMIT ?");
      }
    }
    return select.toString();
  }

  @Override
  public String createUpdateOneStatement(String tablename, List<Comparison> filters, List<DatabaseField> fields) {
      final String template = "UPDATE %1$s SET %2$s WHERE %3$s = (%4$s)";
      // https://dba.stackexchange.com/questions/69471/postgres-update-limit-1
      final String innerTemplate = "SELECT %1$s from %2$s WHERE %3$s LIMIT 1 FOR UPDATE SKIP LOCKED";
      
      String setString = FilterBuilder.buildConcatenatedPlaceholderSet(fields);
      String filterString = FilterBuilder.buildConcatenatedPlaceholderFilter(filters);
      final String inner = String.format(innerTemplate, JdbcDBConstants.PRIMARY_KEY, tablename, filterString);
      return String.format(template, tablename, setString, JdbcDBConstants.PRIMARY_KEY, inner);
    }

  public List<String> buildIndexCommands(String table, List<IndexDescriptor> indexes) {
    if(indexes.size() == 0) {
      return Collections.emptyList();
    }
    System.err.println("indexes: " + indexes.get(0).columnNames);
    /* 
     * CREATE [ UNIQUE ] INDEX [ CONCURRENTLY ] [ [ IF NOT EXISTS ] name ] ON [ ONLY ] table_name [ USING method ]
     * ( { column_name | ( expression ) } [ COLLATE collation ] [ opclass [ ( opclass_parameter = value [, ... ] ) ] ] [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [, ...] )
     * [ INCLUDE ( column_name [, ...] ) ]
     * [ NULLS [ NOT ] DISTINCT ]
     * [ WITH ( storage_parameter [= value] [, ... ] ) ]
     * [ TABLESPACE tablespace_name ]
     * [ WHERE predicate ]
     */
    List<String> indexCommands = new ArrayList<>();
    for(IndexDescriptor idx : indexes) {
      // Our index is not unique, this is not supported
      StringBuilder b = new StringBuilder("CREATE INDEX ");
      if(idx.concurrent) {
        b.append(" CONCURRENTLY ");
      }
      if(idx.name != null) {
        b.append(idx.name);
      }
      b.append(" ON ").append(table);
      addAllIndexColumns(idx, table, b);
      // USING method is not used; it is always the default
      // INCLUDE is not used for now
      b.append(" NULLS DISTINCT ");
      // WITH is not used for now
      // TABLESPACE is not used for now
      // WHERE is not used for now
      indexCommands.add(b.toString());
    }
    System.err.println("Default DB Flavor: collected index commands");
    return indexCommands;
  }

  protected static void addAllIndexColumns(IndexDescriptor idx, String column, StringBuilder b) {
    // first colum, this is mandatory, we provoke an exception in case it is missing
    b.append(" ( ");
    addIndexColumn(idx, idx.columnNames.get(0), b);
    for(int i = 1; i < idx.columnNames.size(); i++) {
        b.append(", ");
        addIndexColumn(idx, idx.columnNames.get(i), b);
    }
    b.append(" )");
  }

  protected static void addIndexColumn(IndexDescriptor idx, String column, StringBuilder b) {
    b.append(column);
    if(idx.order != null) {
      b.append(" ").append(idx.order);
    }
    b.append(" ");
  }
}
