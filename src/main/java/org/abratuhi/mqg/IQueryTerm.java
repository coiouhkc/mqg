package org.abratuhi.mqg;

import java.util.Set;

public interface IQueryTerm {
	
	Set<Table> getUniqueTables();

    String toSql();

}
