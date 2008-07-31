package org.jacuzzi.core;

import javax.sql.DataSource;
import java.lang.reflect.ParameterizedType;
import java.util.List;
import java.util.ArrayList;

/** @author: Mike Mirzayanov */
public class GenericDaoImpl<T, K> implements GenericDao<T, K> {
    private Jacuzzi jacuzzi;
    private TypeOracle<T> typeOracle;
    private Class<T> typeClass;
    private Class<K> keyClass;

    protected Jacuzzi getJacuzzi() {
        return jacuzzi;
    }

    protected GenericDaoImpl(DataSource source) {
        jacuzzi = Jacuzzi.getJacuzzi(source);
        typeOracle = TypeOracle.getTypeOracle(getTypeClass());
    }

    /**
     * Call it to begin transaction around
     * current connection in current thread.
     * <p/>
     * You should call commit() or rollback() at
     * the end of transaction.
     * <p/>
     * Usually you should call commit or rollback
     * in the current DAO method.
     */
    protected void beginTransaction() {
        getJacuzzi().beginTransaction();
    }

    /** Commits current transaction. */
    protected void commit() {
        getJacuzzi().commit();
    }

    /** Rollbacks current transaction. */
    protected void rollback() {
        getJacuzzi().rollback();
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

    public List<T> findBy(String query, Object... args) {
        if (!query.toUpperCase().startsWith("SELECT ")) {
            if (!query.toUpperCase().startsWith("WHERE ")) {
                query = "WHERE " + query;
            }

            String table = typeOracle.getTableName();
            query = Query.format("SELECT ?t.* FROM ?t ", table, table) + query;
        }

        List<Row> rows = jacuzzi.findRows(query, args);

        return convertFromRows(rows);
    }

    protected List<T> convertFromRows(List<Row> rows) {
        List<T> result = new ArrayList<T>(rows.size());
        for (Row row : rows) {
            result.add(convertFromRow(row));
        }
        return result;
    }

    protected T convertFromRow(Row row) {
        return typeOracle.convertFromRow(row);
    }

    public void save(T object) {
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
    }

    public boolean insert(T object) {
        StringBuffer query = new StringBuffer(Query.format("INSERT INTO ?t ", typeOracle.getTableName()));
        query.append(typeOracle.getQuerySetSql());
        return 1 == jacuzzi.execute(query.toString(), typeOracle.getQuerySetArguments(object));
    }

    public boolean update(T object) {
        StringBuffer query = new StringBuffer(Query.format("UPDATE ?t ", typeOracle.getTableName()));
        query.append(typeOracle.getQuerySetSql());
        query.append(Query.format(" WHERE ?f = ?", typeOracle.getIdColumn()));

        Object[] setArguments = typeOracle.getQuerySetArguments(object);
        Object[] arguments = new Object[setArguments.length + 1];
        System.arraycopy(setArguments, 0, arguments, 0, setArguments.length);
        arguments[arguments.length - 1] = typeOracle.getIdValue(object);

        return 1 == jacuzzi.execute(query.toString(), arguments);
    }

    public T newInstance() {
        return typeOracle.newInstance();
    }

    protected synchronized Class<T> getTypeClass() {
        if (typeClass == null) {
            ParameterizedType type = (ParameterizedType) this.getClass().getGenericSuperclass();
            typeClass = (Class<T>) type.getActualTypeArguments()[0];
        }

        return typeClass;
    }

    protected synchronized Class<K> getKeyClass() {
        if (keyClass == null) {
            ParameterizedType type = (ParameterizedType) this.getClass().getGenericSuperclass();
            keyClass = (Class<K>) type.getActualTypeArguments()[1];
        }

        return keyClass;
    }
}
