package org.jacuzzi.core;

import javax.sql.DataSource;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;
import java.util.regex.Pattern;

/**
 * @author Mike Mirzayanov
 */
public class GenericDaoImpl<T, K> implements GenericDao<T, K> {
    private static final Pattern STARTS_WITH_SELECT_PATTERN =
            Pattern.compile("[\\s]*SELECT[\\s]+.*", Pattern.DOTALL | Pattern.CASE_INSENSITIVE);

    private static final Pattern STARTS_WITH_WHERE_PATTERN =
            Pattern.compile("[\\s]*WHERE[\\s]+.*", Pattern.DOTALL | Pattern.CASE_INSENSITIVE);

    private final Jacuzzi jacuzzi;
    private final TypeOracle<T> typeOracle;
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
        jacuzzi.beginTransaction();
    }

    /**
     * Commits current transaction.
     */
    protected void commit() {
        jacuzzi.commit();
    }

    /**
     * Rollbacks current transaction.
     */
    protected void rollback() {
        jacuzzi.rollback();
    }

    /**
     * @return Current time as Date.
     */
    protected Date findNow() {
        return jacuzzi.findDate("SELECT CURRENT_TIMESTAMP");
    }

    @Override
    public T find(K id) {
        String idColumn = typeOracle.getIdColumn();

        List<T> instances = findBy(Query.format("?f = ?", idColumn), id);
        int instanceCount = instances.size();

        if (instanceCount == 0) {
            return null;
        }

        if (instanceCount == 1) {
            return instances.get(0);
        }

        throw new IllegalStateException("There are more than one row this the same id " +
                id + " for " + typeClass + '.');
    }

    @Override
    public List<T> findAll() {
        return findBy("TRUE");
    }

    @Override
    public List<T> findBy(String query, Object... args) {
        if (!STARTS_WITH_SELECT_PATTERN.matcher(query).matches()) {
            if (!STARTS_WITH_WHERE_PATTERN.matcher(query).matches()) {
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
        int instanceCount = instances.size();

        if (instanceCount == 0) {
            return null;
        }

        if (instanceCount > 1 && throwIfNotUnique) {
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

    @Override
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
                typeClass + " with id = " + typeOracle.getIdValue(object) + '.');
    }

    @Override
    public void insert(T object) {
        boolean includeId = typeOracle.hasReasonableId(object);

        StringBuilder query = new StringBuilder(Query.format("INSERT INTO ?t ", typeOracle.getTableName()));
        query.append('(').append(typeOracle.getFieldList(includeId, false)).append(") ");
        query.append("VALUES (").append(typeOracle.getValuesPatternListForInsert(includeId, object)).append(')');

        Jacuzzi.InsertResult result =
                jacuzzi.insert(query.toString(), typeOracle.getValueListForInsert(includeId, object));

        if (result.getCount() == 1) {
            Collection<Object> keys = result.getGeneratedKeysForOneRow().values();

            if (keys.size() == 1) {
                Object key = keys.iterator().next();
                typeOracle.setIdValue(object, key);
            }
        } else {
            throw new DatabaseException(
                    "Can't insert row into " + getTableName() + " for class " + getKeyClass().getName() + '.'
            );
        }
    }

    @Override
    public void insert(T... objects) {
        if (objects == null || objects.length == 0) {
            return;
        }

        insert(Arrays.asList(objects));
    }

    @Override
    public void insert(List<T> objects) {
        if (objects == null || objects.isEmpty()) {
            return;
        }

        boolean includeId = false;

        for (T object : objects) {
            if (typeOracle.hasReasonableId(object)) {
                includeId = true;
                break;
            }
        }

        StringBuilder query = new StringBuilder(Query.format("INSERT INTO ?t ", typeOracle.getTableName()));
        query.append('(').append(typeOracle.getFieldList(includeId, false)).append(") ");
        query.append("VALUES ");

        List<Object> values = new ArrayList<Object>();

        boolean first = true;
        for (T object : objects) {
            if (first) {
                first = false;
            } else {
                query.append(',');
            }

            query.append('(');

            boolean hasReasonableId = typeOracle.hasReasonableId(object);

            if (includeId && !hasReasonableId) {
                query.append("NULL,");
            }

            query.append(typeOracle.getValuesPatternListForInsert(hasReasonableId, object));

            Object[] valueListForInsert = typeOracle.getValueListForInsert(hasReasonableId, object);
            values.addAll(Arrays.asList(valueListForInsert));

            query.append(')');
        }

        Jacuzzi.InsertResult result = jacuzzi.multipleInsert(query.toString(), values.toArray());
        List<Row> generatedKeys = result.getGeneratedKeys();

        if (result.getCount() != objects.size() || generatedKeys.size() != objects.size()) {
            throw new DatabaseException(
                    "Can't insert multiple rows into " + getTableName() + " for class " + getKeyClass().getName() + '.'
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

    @Override
    public void update(T object) {
        StringBuilder query = new StringBuilder(Query.format("UPDATE ?t ", typeOracle.getTableName()));
        query.append(typeOracle.getQuerySetSql());
        query.append(Query.format(" WHERE ?f = ?", typeOracle.getIdColumn()));

        Object[] setArguments = typeOracle.getQuerySetArguments(object);
        Object[] arguments = new Object[setArguments.length + 1];
        System.arraycopy(setArguments, 0, arguments, 0, setArguments.length);
        arguments[arguments.length - 1] = typeOracle.getIdValue(object);

        if (jacuzzi.execute(query.toString(), arguments) != 1) {
            throw new DatabaseException("Can't update instance of class " + getKeyClass().getName()
                    + " with id " + typeOracle.getIdValue(object) + '.');
        }
    }

    @Override
    public void delete(T object) {
        String idColumn = typeOracle.getIdColumn();
        StringBuilder query = new StringBuilder(Query.format("DELETE FROM ?t WHERE ?f = ?", typeOracle.getTableName(), idColumn));
        if (jacuzzi.execute(query.toString(), typeOracle.getIdValue(object)) != 1) {
            throw new DatabaseException("Can't delete instance of class " + getKeyClass().getName()
                    + " with id " + typeOracle.getIdValue(object) + '.');
        }
    }

    @Override
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
