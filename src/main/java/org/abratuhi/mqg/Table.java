package org.abratuhi.mqg;

import lombok.Getter;
import lombok.Setter;

/**
 * Created by abratuhi on 13.06.14.
 */
public class Table {
    @Getter @Setter private String schema;

    @Getter @Setter private String name;

    public Table() {}
    public Table(String schema, String name) {setSchema(schema); setName(name);}

    public String getFqdn() {return schema != null ? (schema+"."+name) : name; }

    public String getAlias() {
        return (schema + "_dot_" + name).toLowerCase();
    }

    @Override
    public boolean equals(Object obj) {
        Table t = (Table) obj;
        boolean result = true;
        result &= ((schema != null)? schema.equals(t.getSchema()) : true);
        result &= ((name != null)? name.equals(t.getName()) : true);

        return result;
    }

    public int hashCode() {
        return schema != null ? schema.hashCode() << 4 + name.hashCode() : name.hashCode();
    }
}
