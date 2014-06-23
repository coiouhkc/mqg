package org.abratuhi.mqg;

import lombok.Getter;
import org.javatuples.Pair;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by abratuhi on 23.06.14.
 */
public class Database {

    @Getter Map<Table, List<Pair<Column, Column>>> imp = new HashMap<>();
    @Getter Map<Table, List<Pair<Column, Column>>> exp = new HashMap<>();

    public void init(Connection connection) throws SQLException {
        DatabaseMetaData meta = connection.getMetaData();
        try (ResultSet rs = meta.getTables(null, null, null, null)) {
            while (rs.next()) {
                String tableCat = rs.getString("TABLE_CAT");
                String tableSchem = rs.getString("TABLE_SCHEM");
                String tableName = rs.getString("TABLE_NAME");

                Table table = new Table(tableSchem, tableName);

                ResultSet rsImp = meta.getImportedKeys(connection.getCatalog(), table.getSchema(), table.getName());
                List<Pair<Column, Column>> related = readRelatedResultSet(table, rsImp);
                rsImp.close();

                imp.put(table, related);

                for (Pair<Column, Column> pair : related) {
                    Table to = pair.getValue1().getTable();
                    if (null == exp.get(to)) {
                        exp.put(to, new ArrayList<Pair<Column, Column>>());
                    }
                    exp.get(to).add(new Pair<>(pair.getValue1(), pair.getValue0()));
                }
            }

        }
    }

    public Pair<Column, Column> getJoinColumns(Table from, Table to) {
        List<Pair<Column, Column>> impPairs = imp.get(from);
        if (null != impPairs ) {
            for (Pair<Column, Column> impPair : impPairs) {
                if (impPair.getValue1().getTable().equals(to)) {
                    return impPair;
                }
            }
        }

        List<Pair<Column, Column>> expPairs = imp.get(to);
        if (null != expPairs ) {
            for (Pair<Column, Column> expPair : expPairs) {
                if (expPair.getValue1().getTable().equals(from)) {
                    return expPair;
                }
            }
        }

        return null;
    }

    private List<Pair<Column, Column>> readRelatedResultSet(Table from, ResultSet rs) throws SQLException {
        List<Pair<Column, Column>> result = new ArrayList<>();
        while(rs.next()) {
            String pkTableCat = rs.getString("PKTABLE_CAT");
            String pkTableSchema = rs.getString("PKTABLE_SCHEM");
            String pkTableName = rs.getString("PKTABLE_NAME");
            String pkColumnName = rs.getString("PKCOLUMN_NAME");
            String fkTableCat = rs.getString("FKTABLE_CAT");
            String fkTableSchema = rs.getString("FKTABLE_SCHEM");
            String fkTableName = rs.getString("FKTABLE_NAME");
            String fkColumnName = rs.getString("FKCOLUMN_NAME");

            Table to = new Table(pkTableSchema, pkTableName);
            Column cto = new Column(to, pkColumnName);
            Column cfrom = new Column(from, fkColumnName);

            result.add(new Pair<>(cfrom, cto));
        }
        return result;
    }
}
