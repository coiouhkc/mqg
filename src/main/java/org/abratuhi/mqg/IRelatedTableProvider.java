package org.abratuhi.mqg;

import org.javatuples.Pair;

import java.util.List;
import java.util.Set;

/**
 * Created by abratuhi on 13.06.14.
 */
public interface IRelatedTableProvider {


    List<Pair<Column,Column>> getRelated(Table from, Table to) throws SqlGeneratorException;

    Set<Table> getRelated(Table from) throws SqlGeneratorException;

    Pair<Column, Column> getJoinColumns(Table from, Table to);


}
