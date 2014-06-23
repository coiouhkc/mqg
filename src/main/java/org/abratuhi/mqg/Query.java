package org.abratuhi.mqg;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Transformer;
import org.apache.commons.lang.StringUtils;

public class Query implements IQueryTerm {
	@Getter @Setter private Junctor junctor;
	@Getter @Setter private List<IQueryTerm> terms = new ArrayList<>();

    @Getter private List<Column> projectiles = new ArrayList<>();
	
	public Query(Junctor junctor) {
		this.junctor = junctor;
	}
	
	public Query(Junctor junctor, List<IQueryTerm> terms) {
		this.junctor = junctor;
		this.terms = terms;
	}
	
	public void addQueryTerm(IQueryTerm term) {
		terms.add(term);
	}

    public void addProjectile(Column column) {
        projectiles.add(column);
    }

    public void addProjectile(Table table, String name) {
        projectiles.add(new Column(table, name));
    }

    public void addQueryTerms(List<? extends IQueryTerm> terms) {
        for (IQueryTerm term: terms) {
            addQueryTerm(term);
        }
    }

	@Override
	public Set<Table> getUniqueTables() {
		Set<Table> result = new HashSet<>();
		for(IQueryTerm qt: terms) { result.addAll(qt.getUniqueTables()); }
		return result;
	}

    @Override
    public String toSql() {
        StringBuffer sb = new StringBuffer();
        sb.append("(");
        sb.append(StringUtils.join(CollectionUtils.collect(getTerms(), new Transformer() {
            @Override
            public Object transform(Object o) {
                return ((IQueryTerm) o).toSql();
            }
        }), " " + getJunctor() + " "));
        sb.append(")");
        return sb.toString();
    }
}
