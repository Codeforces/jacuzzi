package org.jacuzzi.core;

import javax.sql.DataSource;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;

/**
 * @author Mike Mirzayanov
 */
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

    /**
     * Commits current transaction.
     */
    protected void commit() {
        getJacuzzi().commit();
    }

    /**
     * Rollbacks current transaction.
     */
    protected void rollback() {
        getJacuzzi().rollback();
    }

    /**
     * @return Current time as Date.
     */
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

    public void insert(T object) {
        boolean includeId = typeOracle.hasReasonableId(object);

        StringBuffer query = new StringBuffer(Query.format("INSERT INTO ?t ", typeOracle.getTableName()));
        query.append("(").append(typeOracle.getFieldList(includeId, false)).append(") ");
        query.append("VALUES (").append(typeOracle.getValuesPatternListForInsert(includeId, object)).append(")");

        Jacuzzi.InsertResult result = jacuzzi.insert(query.toString(), typeOracle.getValueListForInsert(includeId, object));

        if (result.getCount() != 1) {
            throw new DatabaseException("Can't insert row into " + getTableName() + " for class " + getKeyClass().getName() + ".");
        } else {
            Collection<Object> keys = result.getGeneratedKeysForOneRow().values();

            if (keys.size() == 1) {
                Object key = keys.iterator().next();
                typeOracle.setIdValue(object, key);
            }
        }
    }

    public void insert(T... objects) {
        insert(Arrays.asList(objects));
    }

    public void insert(List<T> objects) {
        boolean includeId = false;

        for (T object : objects) {
            if (typeOracle.hasReasonableId(object)) {
                includeId = true;
                break;
            }
        }

        StringBuilder query = new StringBuilder(Query.format("INSERT INTO ?t ", typeOracle.getTableName()));
        query.append("(").append(typeOracle.getFieldList(includeId, false)).append(") ");
        query.append("VALUES ");

        boolean first = true;
        for (T object : objects) {
            if (first) {
                first = false;
            } else {
                query.append(",");
            }

            query.append("(");

            boolean hasReasonableId = typeOracle.hasReasonableId(object);

            if (includeId && !hasReasonableId) {
                query.append("NULL,");
            }

            Object[] valueListForInsert = typeOracle.getValueListForInsert(hasReasonableId, object);

            for (int i = 0; i < valueListForInsert.length; i++) {
                if (i > 0) {
                    query.append(",");
                }

                if (valueListForInsert[i] == null) {
                    query.append(valueListForInsert[i]);
                } else {
                    query.append("'").append(valueListForInsert[i]).append("'");
                }
            }

            query.append(")");
        }

        Jacuzzi.InsertResult result = jacuzzi.multipleInsert(query.toString());
        List<Row> generatedKeys = result.getGeneratedKeys();

        if (result.getCount() != objects.size() || generatedKeys.size() != objects.size()) {
            throw new DatabaseException(
                    "Can't insert multiple rows into " + getTableName() + " for class " + getKeyClass().getName() + "."
            );
        }

        for (int i = 0; i < generatedKeys.size(); i++) {
            Collection<Object> keys = result.getGeneratedKeysForRow(i).values();

            if (keys.size() == 1) {
                Object key = keys.iterator().next();
                typeOracle.setIdValue(objects.get(i), key);
            }
        }
    }

    public void update(T object) {
        StringBuffer query = new StringBuffer(Query.format("UPDATE ?t ", typeOracle.getTableName()));
        query.append(typeOracle.getQuerySetSql());
        query.append(Query.format(" WHERE ?f = ?", typeOracle.getIdColumn()));

        Object[] setArguments = typeOracle.getQuerySetArguments(object);
        Object[] arguments = new Object[setArguments.length + 1];
        System.arraycopy(setArguments, 0, arguments, 0, setArguments.length);
        arguments[arguments.length - 1] = typeOracle.getIdValue(object);

        if (1 != jacuzzi.execute(query.toString(), arguments)) {
            throw new DatabaseException("Can't update instance of class " + getKeyClass().getName()
                    + " with id " + typeOracle.getIdValue(object) + ".");
        }
    }

    public void delete(T object) {
        String idColumn = typeOracle.getIdColumn();
        StringBuffer query = new StringBuffer(Query.format("DELETE FROM ?t WHERE ?f = ?", typeOracle.getTableName(), idColumn));
        if (1 != jacuzzi.execute(query.toString(), typeOracle.getIdValue(object))) {
            throw new DatabaseException("Can't delete instance of class " + getKeyClass().getName()
                    + " with id " + typeOracle.getIdValue(object) + ".");
        }
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

    @SuppressWarnings({"unchecked"})
    protected synchronized Class<K> getKeyClass() {
        if (keyClass == null) {
            ParameterizedType type = (ParameterizedType) this.getClass().getGenericSuperclass();
            keyClass = (Class<K>) type.getActualTypeArguments()[1];
        }

        return keyClass;
    }
}
