package site.ycsb.db.flavors;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import site.ycsb.db.FilterBuilder;
import site.ycsb.db.IndexDescriptor;
import site.ycsb.db.JdbcDBInitHelper;
import site.ycsb.wrappers.Comparison;
import site.ycsb.wrappers.DatabaseField;

public final class MySqlFlavor extends DefaultDBFlavor {
    @Override 
    public void createDbAndSchema(String tableName, List<Connection> conns) throws SQLException {
        JdbcDBInitHelper.createTable(tableName, conns, "VARCHAR(255)");
    }

    @Override
    public List<String> buildIndexCommands(String table, List<IndexDescriptor> indexes) {
        if(indexes.size() == 0) {
            return Collections.emptyList();
        }
        System.err.println("indexes: " + indexes.get(0).columnNames);
        /*
         * CREATE [UNIQUE | FULLTEXT | SPATIAL] INDEX index_name
            USING {BTREE | HASH}
            ON tbl_name (key_part,...)
            [index_option]
            [algorithm_option | lock_option] ...

        key_part: {col_name [(length)] | (expr)} [ASC | DESC]
        */
        List<String> indexCommands = new ArrayList<>();
        for(IndexDescriptor idx : indexes) {
            // Our index is not unique, this is not supported
            StringBuilder b = new StringBuilder("CREATE INDEX ");
            if(idx.concurrent) {
                System.err.println("'CONCURRENT' mode not supported by MySQL, ignoring.");
            }
            if(idx.name != null) {
                b.append(idx.name);
            }
            b.append(" ON ").append(table);
            addAllIndexColumns(idx, table, b);
            if(idx.method != null) {
                b.append(" USING ").append(idx.method);
            }
            indexCommands.add(b.toString());
        }
        System.err.println("MySQL Flavor: collected index commands");
        return indexCommands;
    }

    @Override
    public String createUpdateOneStatement(String tablename, List<Comparison> filters, List<DatabaseField> fields) {
        final String template = "UPDATE %1$s SET %2$s WHERE %3$s LIMIT 1";
        String setString = FilterBuilder.buildConcatenatedPlaceholderSet(fields);
        String filterString = FilterBuilder.buildConcatenatedPlaceholderFilter(filters);
        return String.format(template, tablename, setString, filterString);
    }
}
