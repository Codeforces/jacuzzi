package org.jacuzzi.core;

import javax.sql.DataSource;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

/**
 * @author Mike Mirzayanov
 */
@SuppressWarnings("AbstractClassWithoutAbstractMethods")
public abstract class GenericDaoImpl<T, K> implements GenericDao<T, K> {
    private static final Pattern STARTS_WITH_SELECT_PATTERN
            = Pattern.compile("[\\s]*SELECT[\\s]+.*", Pattern.DOTALL | Pattern.CASE_INSENSITIVE);

    private static final Pattern STARTS_WITH_WHERE_PATTERN
            = Pattern.compile("[\\s]*WHERE[\\s]+.*", Pattern.DOTALL | Pattern.CASE_INSENSITIVE);

    private final Jacuzzi jacuzzi;
    private final TypeOracle<T> typeOracle;

    private Class<T> typeClass;
    private final Lock typeClassLock = new ReentrantLock();

    private Class<K> keyClass;
    private final Lock keyClassLock = new ReentrantLock();

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
                id + " for " + getTypeClass() + '.');
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

    @SuppressWarnings("OverloadedVarargsMethod")
    @Override
    public T findOnlyBy(boolean throwIfNotUnique, String query, Object... args) {
        List<T> instances = findBy(query, args);
        int instanceCount = instances.size();

        if (instanceCount == 0) {
            return null;
        }

        if (instanceCount > 1 && throwIfNotUnique) {
            throw new DatabaseException("Too many instances of " + getTypeClass().getSimpleName() + " returned by \"" + query + "\".");
        }

        return instances.get(0);
    }

    @SuppressWarnings("OverloadedVarargsMethod")
    @Override
    public T findOnlyBy(String query, Object... args) {
        return findOnlyBy(true, query, args);
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
        if (object == null) {
            return;
        }

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
                getTypeClass() + " with id = " + typeOracle.getIdValue(object) + '.');
    }

    @Override
    public void insert(T object) {
        if (object == null) {
            return;
        }

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
                    "Can't insert row into " + getTableName() + " for class " + getTypeClass().getName() + '.'
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

            query.append(typeOracle.getValuesPatternListForInsert(includeId, object));

            Object[] valueListForInsert = typeOracle.getValueListForInsert(
                    includeId && typeOracle.hasReasonableId(object), object
            );
            values.addAll(Arrays.asList(valueListForInsert));

            query.append(')');
        }

        Jacuzzi.InsertResult result = jacuzzi.multipleInsert(query.toString(), values.toArray());

        if (result.getCount() != objects.size()) {
            throw new DatabaseException(
                    "Can't insert multiple rows into " + getTableName() + " for class " + getTypeClass().getName() + '.'
            );
        }

        List<Row> generatedKeys = result.getGeneratedKeys();
        if (!generatedKeys.isEmpty()) {
            if (generatedKeys.size() == objects.size()) {
                for (int i = 0; i < generatedKeys.size(); i++) {
                    Collection<Object> keys = result.getGeneratedKeysForRow(i).values();

                    if (keys.size() == 1) {
                        Object key = keys.iterator().next();
                        typeOracle.setIdValue(objects.get(i), key);
                    }
                }
            } else {
                throw new DatabaseException(
                        "Unexpected number of rows with generated keys: " + generatedKeys.size() + " instead of " +
                                objects.size() + '.'
                );
            }
        }
    }

    @Override
    public void update(T object) {
        if (object == null) {
            return;
        }

        StringBuilder query = new StringBuilder(Query.format("UPDATE ?t ", typeOracle.getTableName()));
        query.append(typeOracle.getQuerySetSql());
        query.append(Query.format(" WHERE ?f = ?", typeOracle.getIdColumn()));

        Object[] setArguments = typeOracle.getQuerySetArguments(object);
        Object[] arguments = new Object[setArguments.length + 1];
        System.arraycopy(setArguments, 0, arguments, 0, setArguments.length);
        arguments[arguments.length - 1] = typeOracle.getIdValue(object);

        if (jacuzzi.execute(query.toString(), arguments) != 1) {
            throw new DatabaseException("Can't update instance of class " + getTypeClass().getName()
                    + " with id " + typeOracle.getIdValue(object) + '.');
        }
    }

    @SuppressWarnings({"unchecked"})
    public void delete(T object) {
        if (object == null) {
            return;
        }

        deleteById((K) typeOracle.getIdValue(object));
    }

    @Override
    public void delete(T... objects) {
        if (objects == null || objects.length == 0) {
            return;
        }

        delete(Arrays.asList(objects));
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public void delete(Collection<T> objects) {
        if (objects == null || objects.isEmpty()) {
            return;
        }

        Collection<K> objectIds = new ArrayList<K>(objects.size());
        for (T object : objects) {
            objectIds.add((K) typeOracle.getIdValue(object));
        }

        deleteById(objectIds);
    }

    @Override
    public void deleteById(K id) {
        if (id == null) {
            return;
        }

        String idColumn = typeOracle.getIdColumn();
        String query = Query.format("DELETE FROM ?t WHERE ?f = ?", typeOracle.getTableName(), idColumn);
        if (jacuzzi.execute(query, id) != 1) {
            throw new DatabaseException("Can't delete instance of class " + getTypeClass().getName()
                    + " with id " + id + '.');
        }
    }

    @Override
    public void deleteById(K... ids) {
        if (ids == null || ids.length == 0) {
            return;
        }

        deleteById(Arrays.asList(ids));
    }

    @Override
    public void deleteById(Collection<K> ids) {
        if (ids == null || ids.isEmpty()) {
            return;
        }

        String idColumn = typeOracle.getIdColumn();
        StringBuilder query = new StringBuilder(
                Query.format("DELETE FROM ?t WHERE ?f IN (", typeOracle.getTableName(), idColumn)
        );

        Object[] idValues = new Object[ids.size()];
        int index = 0;

        for (K id : ids) {
            if (index > 0) {
                query.append(',');
            }
            query.append('?');

            idValues[index] = id;

            ++index;
        }

        query.append(')');

        if (ids.size() != jacuzzi.execute(query.toString(), idValues)) {
            throw new DatabaseException(
                    "Can't delete multiple instances of class " + getTypeClass().getName() + '.'
            );
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
    protected final Class<T> getTypeClass() {
        typeClassLock.lock();
        try {
            if (typeClass == null) {
                Class clazz = this.getClass();

                while (clazz != null) {
                    Type genericSuperclass = clazz.getGenericSuperclass();

                    if (genericSuperclass instanceof ParameterizedType) {
                        Type type = ((ParameterizedType) genericSuperclass).getActualTypeArguments()[0];
                        return typeClass = (Class<T>) type;
                    }

                    clazz = clazz.getSuperclass();
                }

                throw new DatabaseException("DAO implementation should have an ancestor with generic parameter T.");
            }

            return typeClass;
        } finally {
            typeClassLock.unlock();
        }
    }

    @SuppressWarnings({"unchecked"})
    protected final Class<K> getKeyClass() {
        keyClassLock.lock();
        try {
            if (keyClass == null) {
                Class clazz = this.getClass();

                while (clazz != null) {
                    Type genericSuperclass = clazz.getGenericSuperclass();

                    if (genericSuperclass instanceof ParameterizedType
                            && ((ParameterizedType) genericSuperclass).getActualTypeArguments().length == 2) {
                        Type type = ((ParameterizedType) genericSuperclass).getActualTypeArguments()[1];
                        if (type instanceof Class) {
                            return keyClass = (Class<K>) type;
                        } else {
                            throw new DatabaseException("DAO implementation should have an ancestor"
                                    + " with exactly two generic parameters: T, K.");
                        }
                    }

                    clazz = clazz.getSuperclass();
                }

                throw new DatabaseException("DAO implementation should have an ancestor"
                        + " with exactly two generic parameters: T, K.");
            }

            return keyClass;
        } finally {
            keyClassLock.unlock();
        }
    }
}
