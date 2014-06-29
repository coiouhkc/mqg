package org.abratuhi.mqg;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import lombok.Getter;
import lombok.Setter;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.Transformer;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.javatuples.Pair;

public class QueryGenerator {
	
	private static final Logger LOG = Logger.getLogger(QueryGenerator.class);
	
	public class Path {
		@Getter @Setter List<Table> tables = new ArrayList<>();
		
		public Path() {}
		
		public Path(Table table) {
			tables.add(table);
		}
		
		public Path(Path path, Table table) {
			tables.addAll(path.getTables());
			tables.add(table);
		}
		
		public Table last() {
			return tables.get(tables.size()-1);
		}
		
		public boolean isEmpty() {
			return tables.isEmpty();
		}
		
		public String toString() {
			StringBuffer sb = new StringBuffer();
			for(int i=0; i<tables.size()-1; i++) {sb.append(tables.get(i).getName() + " -> "); }
			sb.append(last().getName());
			return sb.toString();
		}
	}

    @Getter @Setter private IRelatedTableProvider provider;
	
	public List<Table> computeIntermediateJoinTables(Table start, Table end) {
		List<Table> result = new ArrayList<>();
		return result;
	}

	public List<Path> computeJoinPaths(List<Table> tables) throws SqlGeneratorException {
		List<Path> result = new ArrayList<>();
		if(!tables.isEmpty()) {
			Table master = tables.get(0);
			for (int i = 1; i<tables.size(); i++) {
				Table slave = tables.get(i);
				Path path = visit(master, slave);
				if (path == null) {
					path = visit(slave, master);
					if (path == null) {
						throw new SqlGeneratorException("No join path found for " + master.getName() + " and " + slave.getName());
					}
				}
				result.add(path);
			}
		}
		return result;
	}
	
	public Path visit(Table start, Table end) throws SqlGeneratorException {
		return visit(start, end, new ArrayList<Path>(), new HashSet<Table>());
	}
	
	public Path visit(Table start, Table end, List<Path> paths, Set<Table> visited) throws SqlGeneratorException {
		LOG.debug("visit ");
        for (Table visit : visited) {
            LOG.debug("visit - visited = " + visit.getName());
        }
		List<Path> npaths = new ArrayList<>();
		
		if (start.equals(end)) {
			LOG.debug("visit - start=end, empty path");
			return new Path();
		} else if (paths.isEmpty()) {
			LOG.debug("visit - path list empty, add start path to list");
			npaths.add(new Path(start));
			visited.add(start);
			return visit(start, end, npaths, new HashSet<Table>(visited));
		} else {
			LOG.debug("visit - examine neighbors");
			for (Path path : paths) {
                Table last = path.last();

				Set<Table> neighbors = new HashSet<>();
				neighbors.addAll(provider.getRelated(last));
				neighbors.removeAll(visited);
				
				for (Table neighbor : neighbors) {
                    LOG.debug("visit - neighbour (" + last.getName() + ") = " + neighbor.getName());
                }
				
				for (Table table: neighbors) {
					Path p = new Path(path, table);
					visited.add(table);
					
					if (table.equals(end)) {
						return p;
					} else {
						npaths.add(p);
					}
				}
			}
			
			if (npaths.isEmpty()) {
				return null;
			} else {
				return visit(start, end, npaths, visited);
			}
		}
	}

    public Query path2join(Path path) {
        Query query = new Query(Junctor.AND);
        for(int i=0; i<path.getTables().size()-1; i++) {
            Table from = path.getTables().get(i);
            Table to = path.getTables().get(i+1);
            Pair<Column, Column> joinColumns = provider.getJoinColumns(from, to);
            if (null != joinColumns) {
                query.addQueryTerm(new Predicate<Column>(Operator.EQUALS, joinColumns.getValue0(), joinColumns.getValue1()));
            }
        }
        return query;
    }

    public String toSql(Query query) throws SqlGeneratorException {
        List<Table> queryTables = new ArrayList<>(query.getUniqueTables());
        List<Path> joinPaths = computeJoinPaths(queryTables);
        List<Query> joinQueries = new ArrayList<Query>(CollectionUtils.collect(joinPaths, new Transformer<Path, Query>() {
            @Override
            public Query transform(Path o) {
                return QueryGenerator.this.path2join(o);
            }
        }));

        Query queryWithJoins = new Query(Junctor.AND);
        queryWithJoins.addQueryTerm(query);
        queryWithJoins.addQueryTerms(joinQueries);

        StringBuffer sb = new StringBuffer();
        sb.append("SELECT ");
        if (query.getProjectiles().isEmpty()) {
            sb.append("* ");
        } else {
            String selectSql = StringUtils.join(CollectionUtils.collect(query.getProjectiles(), new Transformer<Column,String>() {
                @Override
                public String transform(Column projectile) {
                    return projectile.getTable().getAlias() + "." + projectile.getName();
                }
            }), ", ");
            sb.append(selectSql);
        }
        queryTables = new ArrayList<>(queryWithJoins.getUniqueTables());
        String fromSql = StringUtils.join(CollectionUtils.collect(queryTables, new Transformer<Table,String>() {
            @Override
            public String transform(Table table) {
                return table.getFqdn() + " " + table.getAlias();
            }
        }), ", ");
        sb.append(" FROM ");
        sb.append(fromSql);
        sb.append(" WHERE ");
        sb.append(StringUtils.join(CollectionUtils.collect(queryWithJoins.getTerms(), new Transformer<IQueryTerm, String>() {
            @Override
            public String transform(IQueryTerm o) {
                return o.toSql();
            }
        }), " " + queryWithJoins.getJunctor() + " "));
        return sb.toString();
    }

}
