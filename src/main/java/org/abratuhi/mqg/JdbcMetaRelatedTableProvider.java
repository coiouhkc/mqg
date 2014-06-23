package org.abratuhi.mqg;

import lombok.Getter;
import lombok.Setter;
import lombok.Delegate;
import org.javatuples.Pair;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by abratuhi on 13.06.14.
 */
public class JdbcMetaRelatedTableProvider implements IRelatedTableProvider {
    @Getter @Setter private String url;
    @Getter @Setter private String driver;
    @Getter @Setter private String username;
    @Getter @Setter private String password;

    @Delegate private Database database;

    public void init() throws SQLException, ClassNotFoundException {
        Class.forName(driver);
        Connection connection = DriverManager.getConnection(url, username, password);

        database = new Database();
        database.init(connection);

        connection.close();
    }

    private List<Pair<Column, Column>> getRelatedInternal(Table from) throws SqlGeneratorException {
        try {

            List<Pair<Column, Column>> result = new ArrayList<>();

            if (null != database.getImp().get(from)) {
                result.addAll(database.getImp().get(from));
            }

            if (null != database.getExp().get(from)) {
                result.addAll(database.getExp().get(from));
            }


            return result;
        } catch (Exception e) {
            throw new SqlGeneratorException(e);
        }
    }

    @Override
    public List<Pair<Column, Column>> getRelated(Table from, Table to) throws SqlGeneratorException {
        List<Pair<Column, Column>> intermediate = getRelatedInternal(from);
        List<Pair<Column, Column>> result = new ArrayList<>();
        for (Pair<Column, Column> pair: intermediate) {if(pair.getValue1().getTable().equals(to)) {result.add(pair);}}
        return result;
    }

    @Override
    public Set<Table> getRelated(Table from) throws SqlGeneratorException {
        List<Pair<Column, Column>> intermediate = getRelatedInternal(from);
        Set<Table> result = new HashSet<>();
        for (Pair<Column, Column> pair: intermediate) {result.add(pair.getValue1().getTable());}
        return result;
    }
}
