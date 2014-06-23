package org.abratuhi.mqg;

import lombok.Getter;
import lombok.Setter;

/**
 * Created by abratuhi on 13.06.14.
 */
public class Column {

    @Getter
    @Setter
    private Table table;

    @Getter
    @Setter
    private String name;

    public Column(){

    }

    public Column(Table table, String columnName) {
        setTable(table);
        setName(columnName);
    }
}
