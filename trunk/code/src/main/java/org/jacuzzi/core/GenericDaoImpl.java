package org.jacuzzi.core;

import javax.sql.DataSource;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

/** @author Mike Mirzayanov */
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

    /** @return Current time as Date. */
    protected Date findNow() {
        return getJacuzzi().findDate("SELECT CURRENT_TIMESTAMP");
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
        return findBy("TRUE");
    }

    public List<T> findBy(String query, Object... args) {
        if (!query.toUpperCase().startsWith("SELECT ")) {
            if (!query.toUpperCase().startsWith("WHERE ")) {
                query = "WHERE " + query;
            }

            String table = typeOracle.getTableName();
            query = "SELECT " + typeOracle.getFieldList(true, true) + Query.format(" FROM ?t ", table) + query;
        }

        List<Row> rows = jacuzzi.findRows(query, args);

        return convertFromRows(rows);
    }

    @Override
    public T findOnlyBy(boolean throwIfNotUnique, String query, Object... args) {
        List<T> instances = findBy(query, args);

        if (instances.size() == 0) {
            return null;
        }

        if (instances.size() > 1 && throwIfNotUnique) {
            throw new DatabaseException("Too many instances of " + getKeyClass().getSimpleName() + " returned by \"" + query + "\".");
        }

        return instances.get(0);
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
        boolean includeId = typeOracle.hasReasonableId(object);

        StringBuffer query = new StringBuffer(Query.format("INSERT INTO ?t ", typeOracle.getTableName()));
        query.append("(").append(typeOracle.getFieldList(includeId, false)).append(") ");
        query.append("VALUES (").append(typeOracle.getValuesPatternListForInsert(includeId, object)).append(")");

        Jacuzzi.InsertResult result = jacuzzi.insert(query.toString(), typeOracle.getValueListForInsert(includeId, object));

        if (result.getCount() != 1) {
            return false;
        } else {
            Collection<Object> keys = result.getGeneratedKeys().values();

            if (keys.size() == 1) {
                Object key = keys.iterator().next();
                typeOracle.setIdValue(object, key);
            }

            return true;
        }
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

    public boolean delete(T object) {
        String idColumn = typeOracle.getIdColumn();
        StringBuffer query = new StringBuffer(Query.format("DELETE FROM ?t WHERE ?f = ?", typeOracle.getTableName(), idColumn));
        return 1 == jacuzzi.execute(query.toString(), typeOracle.getIdValue(object));
    }

    public T newInstance() {
        return typeOracle.newInstance();
    }

    public String getTableName() {
        return typeOracle.getTableName();
    }

    @SuppressWarnings({"unchecked"})
    protected synchronized Class<T> getTypeClass() {
        Class<?> clazz = this.getClass();

        while (typeClass == null) {
            Type superClazz = clazz.getGenericSuperclass();

            if (superClazz instanceof ParameterizedType) {
                ParameterizedType type = (ParameterizedType) superClazz;
                typeClass = (Class<T>) type.getActualTypeArguments()[0];
            }
            clazz = clazz.getSuperclass();
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
