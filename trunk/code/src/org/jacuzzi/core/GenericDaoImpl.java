package org.jacuzzi.core;

import javax.sql.DataSource;
import java.lang.reflect.ParameterizedType;
import java.util.List;
import java.util.ArrayList;
import java.sql.SQLException;

/**
 * @author: Mike Mirzayanov
 */
public class GenericDaoImpl<T,K> implements GenericDao<T,K> {
    private Jacuzzi jacuzzi;
    private TypeOracle<T> typeOracle;
    private Class<T> typeClass;
    private Class<K> keyClass;

    protected Jacuzzi getJacuzzi() {
        return jacuzzi;
    }

    public GenericDaoImpl(DataSource source) {
        jacuzzi = Jacuzzi.getJacuzzi(source);
        typeOracle = TypeOracle.getTypeOracle(getTypeClass());
    }

    public T find(K id) {
        String idColumn = typeOracle.getIdColumn();

        List<T> list = findBy(Query.format("?f = ?", idColumn), id);

        if (list.size() == 0) {
            return null;
        }

        if (list.size() == 1) {
            return list.get(0);
        }

        throw new IllegalStateException("There are more than one row this the same id " +
                id + " for " + typeClass + ".");
    }

    public List<T> findAll() {
        return findBy("1");
    }

    public List<T> findBy(String query, Object ... args) {
        if (!query.toUpperCase().startsWith("SELECT ")) {
            if (!query.toUpperCase().startsWith("WHERE ")) {
                query = "WHERE " + query;
            }

            String table = typeOracle.getTableName();
            query = Query.format("SELECT ?t.* FROM ?t ", table, table) + query;
        }

        try {
            List<Row> rows = jacuzzi.findRows(query, args);

            List<T> result = new ArrayList<T>(rows.size());
            for (Row row: rows) {
                result.add(typeOracle.convertFromRow(row));
            }

            return result;
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    public void save(T object) {
        try {
            Long count = (Long) jacuzzi.findOne(Query.format("SELECT COUNT(*) FROM ?t WHERE ?f = ?",
                    typeOracle.getTableName(), typeOracle.getIdColumn()), typeOracle.getIdValue(object));

            if (count == 0) {
                insert(object);
                return;
            }

            if (count == 1) {
                update(object);
                return;
            }

            throw new MappingException("There are more than one instance of " +
                    typeClass + " with id = " + typeOracle.getIdValue(object) + ".");

        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    public boolean insert(T object) {
        StringBuffer query = new StringBuffer(Query.format("INSERT INTO ?t ", typeOracle.getTableName()));
        query.append(typeOracle.getQuerySetSql());

        try {
            int count = jacuzzi.execute(query.toString(), typeOracle.getQuerySetArguments(object));
            return count == 1;
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    public boolean update(T object) {
        StringBuffer query = new StringBuffer(Query.format("UPDATE ?t ", typeOracle.getTableName()));
        query.append(typeOracle.getQuerySetSql());
        query.append(Query.format(" WHERE ?f = ?", typeOracle.getIdColumn()));

        try {
            Object[] setArguments = typeOracle.getQuerySetArguments(object);
            Object[] arguments = new Object[setArguments.length + 1];
            System.arraycopy(setArguments, 0, arguments, 0, setArguments.length);
            arguments[arguments.length - 1] = typeOracle.getIdValue(object);

            int count = jacuzzi.execute(query.toString(), arguments);
            return count == 1;
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    public T newInstance() {
        return typeOracle.newInstance();
    }

    protected synchronized Class<T> getTypeClass() {
        if (typeClass == null) {
            ParameterizedType type = (ParameterizedType)this.getClass().getGenericSuperclass();
            typeClass = (Class<T>) type.getActualTypeArguments()[0];
        }

        return typeClass;
    }

    protected synchronized Class<K> getKeyClass() {
        if (keyClass == null) {
            ParameterizedType type = (ParameterizedType)this.getClass().getGenericSuperclass();
            keyClass = (Class<K>) type.getActualTypeArguments()[1];
        }

        return keyClass;
    }
}
