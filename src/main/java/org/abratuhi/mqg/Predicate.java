package org.abratuhi.mqg;

import java.util.*;

import lombok.Getter;
import lombok.Setter;

public class Predicate<T> implements IQueryTerm{
	@Getter  @Setter private Operator operator;
	@Getter @Setter private Column column;
	@Getter @Setter private List<T> values;
	
	public Predicate(Operator operator, Column column, List<T> values) {
		this.operator = operator;
		this.column = column;
		this.values = values;
	}

    public Predicate(Operator operator, Column column, T value) {
        this.operator = operator;
        this.column = column;
       setValue(value);
    }
	
	public Predicate(Operator operator, Table table, String column, List<T> values) {
		this.operator = operator;
		this.column = new Column(table, column);
		this.values = values;
	}

    public Predicate(Operator operator, Table table, String column, T value) {
        this.operator = operator;
        this.column = new Column(table, column);
        setValue(value);
    }

	@Override
	public Set<Table> getUniqueTables() {
		Set<Table> result = new HashSet<>();
		result.add(column.getTable());
		return result;
	}

    @Override
    public String toSql() {
        StringBuffer sb = new StringBuffer();
        sb.append("(");
        sb.append(column.getTable().getAlias() + "." + column.getName());
        sb.append(" " + operator + " ");
        // TODO: fix for different data types and multivalue operators
        if(values.get(0) instanceof  Column) {
            sb.append(((Column) values.get(0)).getTable().getAlias() + "." + ((Column) values.get(0)).getName());
        } else if (values.get(0) instanceof  String) {
            sb.append("'" + values.get(0) + "'");
        } else {
            sb.append(values.get(0));
        }
        sb.append(")");
        return sb.toString();
    }

    public void setValue(T value) {
        values = new ArrayList<>();
        values.add(value);
    }

    public void setValues(T[] values) {
        this.values = new ArrayList<>();
        for (T t: values) {this.values.add(t);}
    }

}
