package org.jacuzzi.core;

import net.sf.cglib.reflect.FastClass;
import net.sf.cglib.reflect.FastMethod;
import org.apache.log4j.Logger;
import org.jacuzzi.mapping.Id;
import org.jacuzzi.mapping.MappedTo;
import org.jacuzzi.mapping.OperationControl;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Mike Mirzayanov
 */
class TypeOracleImpl<T> extends TypeOracle<T> {
    private static final Logger logger = Logger.getLogger(TypeOracleImpl.class);

    private static final Object[] EMPTY_OBJECT_ARRAY = {};
    private static final Map<Long, String> LOWER_CASED_COLUMNS_CACHE = new ConcurrentHashMap<>();

    private final Class<T> clazz;
    private final FastClass fastClazz;
    private final List<Field> fields;
    private final Map<String, Field> fieldByColumn;
    private final Field idField;
    private final String tableName;

    @SuppressWarnings("OverlyLongMethod")
    TypeOracleImpl(Class<T> clazz) {
        this.clazz = clazz;

        fastClazz = FastClass.create(clazz);
        List<Field> internalFields = new ArrayList<Field>();
        Map<String, Field> internalFieldByColumn = new HashMap<String, Field>();

        if (clazz.getAnnotation(MappedTo.class) == null) {
            tableName = clazz.getSimpleName();
        } else {
            tableName = clazz.getAnnotation(MappedTo.class).value();
        }

        String[] fieldNames = ReflectionUtil.findFields(clazz);
        Map<String, java.lang.reflect.Field> fieldByName = ReflectionUtil.findFieldsMap(clazz);
        Field internalIdField = null;

        for (String fieldName : fieldNames) {
            Field field = new Field(fieldName);
            java.lang.reflect.Field javaField = fieldByName.get(fieldName);

            field.setSetter(ReflectionUtil.findSetter(clazz, fieldName));
            field.setGetter(ReflectionUtil.findGetter(clazz, fieldName));

            boolean isId = false;
            String column = fieldName;

            if (javaField != null) {
                MappedTo mappedTo = javaField.getAnnotation(MappedTo.class);
                if (mappedTo != null) {
                    column = mappedTo.value();
                }

                OperationControl operationControl = javaField.getAnnotation(OperationControl.class);
                if (operationControl != null) {
                    field.setIgnoreSelect(operationControl.ignoreSelect());
                    field.setIgnoreInsert(operationControl.ignoreInsert());
                    field.setIgnoreUpdate(operationControl.ignoreUpdate());
                }

                isId = javaField.getAnnotation(Id.class) != null;
            }

            field.setColumn(column);

            if (field.isValid()) {
                isId = isId || field.getSetter().getJavaMethod().getAnnotation(Id.class) != null;
                isId = isId || field.getGetter().getJavaMethod().getAnnotation(Id.class) != null;

                field.setId(isId);

                if (isId) {
                    if (field.isIgnoreSelect() || field.isIgnoreInsert() || field.isIgnoreUpdate()) {
                        throw new MappingException("Can't apply OperationControl to ID field.");
                    }

                    internalIdField = field;
                }

                internalFields.add(field);
                internalFieldByColumn.put(field.getColumn(), field);
            }
        }

        idField = internalIdField;
        fields = Collections.unmodifiableList(internalFields);
        fieldByColumn = Collections.unmodifiableMap(internalFieldByColumn);
    }

    @Override
    public String getIdColumn() {
        if (idField == null) {
            throw new MappingException("Can't find ID for class " + clazz + '.');
        }

        return idField.getColumn();
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public String getQuerySetSql() {
        if (fields.isEmpty()) {
            throw new MappingException("Nothing to set for class " + clazz + '.');
        }

        StringBuilder result = new StringBuilder();
        result.append("SET");

        for (Field field : fields) {
            if (field.isIgnoreUpdate()) {
                continue;
            }

            result.append(Query.format(" ?f = ?,", field.getColumn()));
        }

        return result.substring(0, result.length() - 1);
    }

    @Override
    public String getQueryFindSql(String[] fields) {
        if (fields.length == 0) {
            return "TRUE";
        }

        StringBuilder result = new StringBuilder();

        for (String ignored : fields) {
            if (result.length() > 0) {
                result.append(" AND ");
            }
            result.append("?f=?");
        }

        return Query.format(result.toString(), (Object[]) fields);
    }

    @Override
    public String getFieldList(boolean includeId, boolean useTablePrefix, OperationType operationType) {
        if (fields.isEmpty()) {
            throw new MappingException("Nothing to set for class " + clazz + '.');
        }

        StringBuilder result = new StringBuilder();

        for (Field field : fields) {
            if (field.isId() && !includeId
                    || operationType == OperationType.SELECT && field.isIgnoreSelect()
                    || operationType == OperationType.INSERT && field.isIgnoreInsert()
                    || operationType == OperationType.UPDATE && field.isIgnoreUpdate()) {
                continue;
            }

            if (result.length() > 0) {
                result.append(", ");
            }

            if (useTablePrefix) {
                result.append(Query.format("?t.?f", tableName, field.getColumn()));
            } else {
                result.append(Query.format("?f", field.getColumn()));
            }
        }

        return result.toString();
    }

    @Override
    public String getValuesPatternListForInsert(boolean includeId, T instance) {
        if (fields.isEmpty()) {
            throw new MappingException("Nothing to set for class " + clazz + '.');
        }

        StringBuilder result = new StringBuilder();

        for (Field field : fields) {
            if (field.isId() && !includeId || field.isIgnoreInsert()) {
                continue;
            }

            if (result.length() > 0) {
                result.append(", ");
            }

            if (!field.isId() || hasReasonableId(instance)) {
                result.append('?');
            } else {
                result.append("NULL");
            }
        }

        return result.toString();
    }

    @Override
    List<Object> getValueListForInsert(boolean includeId, T instance) {
        List<Object> result = new ArrayList<Object>(fields.size());

        try {
            for (Field field : fields) {
                if (field.isId() && !includeId || field.isIgnoreInsert()) {
                    continue;
                }

                result.add(field.getGetter().invoke(instance, EMPTY_OBJECT_ARRAY));
            }
        } catch (InvocationTargetException e) {
            throw new MappingException("Can't invoke getter for class " + clazz.getName() + '.', e);
        }

        return result;
    }

    @Override
    @SuppressWarnings({"RedundantIfStatement"})
    public boolean hasReasonableId(T instance) {
        Object id = getIdValue(instance);
        if (id == null) {
            return false;
        }

        if (id instanceof Long && (Long) id == 0L) {
            return false;
        }

        if (id instanceof Integer && (Integer) id == 0) {
            return false;
        }

        if (id instanceof Short && (Short) id == 0) {
            return false;
        }

        if (id instanceof String && ((String) id).isEmpty()) {
            return false;
        }

        return true;
    }

    @Override
    public List<Object> getQuerySetArguments(T instance) {
        if (fields.isEmpty()) {
            throw new MappingException("Nothing to set for class " + clazz + '.');
        }

        List<Object> result = new ArrayList<Object>(fields.size() + 1);

        for (Field field : fields) {
            if (field.isIgnoreUpdate()) {
                continue;
            }

            try {
                result.add(field.getGetter().invoke(instance, EMPTY_OBJECT_ARRAY));
            } catch (InvocationTargetException e) {
                throw new MappingException("Can't invoke getter " + field.getGetter() + '.', e);
            }
        }

        return result;
    }

    @Override
    public Object getIdValue(T instance) {
        if (idField == null) {
            throw new MappingException("Can't find id field for class " + clazz + '.');
        }

        try {
            return idField.getGetter().invoke(instance, EMPTY_OBJECT_ARRAY);
        } catch (InvocationTargetException e) {
            throw new MappingException("Can't invoke getter " + idField.getGetter() + " to get id value.", e);
        }
    }

    @Override
    @SuppressWarnings({"unchecked"})
    public T newInstance() {
        T instance;

        try {
            instance = (T) fastClazz.newInstance();
        } catch (InvocationTargetException e) {
            throw new MappingException("Can't instantiate class " + clazz + '.', e);
        }

        return instance;
    }

    @Override
    public void setIdValue(T instance, Object value) {
        try {
            idField.getSetter().invoke(instance, new Object[]{value});
        } catch (InvocationTargetException e) {
            throw new MappingException("Can't set value of type " + value.getClass().getName() +
                    " to id of " + instance.getClass().getName() + '.', e);
        }
    }

    private static String toString(Row row) {
        StringBuilder result = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            if (first) {
                first = false;
            } else {
                result.append(',');
            }
            if (entry.getValue() == null) {
                result.append(entry.getKey()).append(':').append("null");
            } else {
                result.append(entry.getKey()).append(':').append(entry.getValue().getClass().getName())
                        .append('=').append(entry.getValue());
            }
        }
        result.append('}');
        return result.toString();
    }

    private static final String toLowerCase(String s) {
        if (s == null) {
            return null;
        }

        long hash = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            hash = hash * 103079 + 983 + c;
        }

        String result = LOWER_CASED_COLUMNS_CACHE.get(hash);
        if (result != null) {
            return result;
        }

        result = s.toLowerCase();
        LOWER_CASED_COLUMNS_CACHE.putIfAbsent(hash, result);
        return result;
    }

    @SuppressWarnings({"OverlyComplexMethod", "OverlyLongMethod"})
    @Override
    public T convertFromRow(Row row) {
        Set<String> columns = row.keySet();
        T instance = newInstance();

        Map<String, String> columnNameByLowercaseColumnName = new HashMap<String, String>();
        for (String column : fieldByColumn.keySet()) {
            columnNameByLowercaseColumnName.put(toLowerCase(column), column);
        }

        for (String column : columns) {
            String lowerCasedColumn = toLowerCase(column);
            if (columnNameByLowercaseColumnName.containsKey(lowerCasedColumn)) {
                Field field = fieldByColumn.get(columnNameByLowercaseColumnName.remove(lowerCasedColumn));
                try {
                    Object parameter = row.get(column);
                    Class<?> expectedParameterType = field.getSetter().getParameterTypes()[0];
                    Object castedParameter = convertTo(parameter, expectedParameterType);
                    field.getSetter().invoke(instance, new Object[]{castedParameter});
                } catch (InvocationTargetException e) {
                    if (row.get(column) != null) {
                        throw new MappingException("Can't invoke setter " + field.getSetter() + "[clazz="
                                + clazz.getName() + "] for parameter " + row.get(column).getClass() + " [row=" + toString(row) + "].", e);
                    } else {
                        throw new MappingException("Can't invoke setter " + field.getSetter() + " for class "
                                + clazz.getName() + " [row=" + toString(row) + "].", e);
                    }
                } catch (RuntimeException e) {
                    if (row.get(column) != null) {
                        throw new MappingException("Can't invoke setter " + field.getSetter() + "[clazz="
                                + clazz.getName() + "] for parameter " + row.get(column).getClass() + " [row=" + toString(row) + "].", e);
                    } else {
                        throw new MappingException("Can't invoke setter " + field.getSetter() + " for class "
                                + clazz.getName() + " [row=" + toString(row) + "].", e);
                    }
                }
            }
        }

        if (!columnNameByLowercaseColumnName.isEmpty()) {
            StringBuilder message = new StringBuilder("There is uninitialized field(s) remained in the entity ")
                    .append(instance);

            boolean first = true;

            for (String columnName : columnNameByLowercaseColumnName.values()) {
                message.append(first ? ": '" : ", '");
                first = false;
                message.append(columnName).append('\'');
            }

            message.append(".\n\tStack trace:");

            StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();

            // Element 0 is always java.lang.Thread.getStackTrace().
            // Element stackTraceElements.length - 1 is always java.lang.Thread.run().
            for (int index = 1, lastIndex = stackTraceElements.length - 2; index <= lastIndex; ++index) {
                StackTraceElement stackTraceElement = stackTraceElements[index];
                message.append(index == 1 ? "\n\t\t" : ",\n\t\t");
                message.append(stackTraceElement);
            }

            message.append('.');

            logger.error(message.toString());
        }

        return instance;
    }

    @Override
    public List<T> convertFromRows(List<Row> rows) {
        List<T> instances = new ArrayList<T>(rows.size());
        for (Row row : rows) {
            instances.add(convertFromRow(row));
        }
        return instances;
    }

    private static class Field {
        private FastMethod setter;
        private FastMethod getter;
        private String name;
        private String column;
        private boolean id;
        private boolean ignoreSelect;
        private boolean ignoreInsert;
        private boolean ignoreUpdate;

        private Field(String name) {
            this.name = name;
        }

        public FastMethod getSetter() {
            return setter;
        }

        public void setSetter(FastMethod setter) {
            this.setter = setter;
        }

        public FastMethod getGetter() {
            return getter;
        }

        public void setGetter(FastMethod getter) {
            this.getter = getter;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getColumn() {
            return column;
        }

        public void setColumn(String column) {
            this.column = column;
        }

        public void setId(boolean id) {
            this.id = id;
        }

        public boolean isId() {
            return id;
        }

        public boolean isIgnoreSelect() {
            return ignoreSelect;
        }

        public void setIgnoreSelect(boolean ignoreSelect) {
            this.ignoreSelect = ignoreSelect;
        }

        public boolean isIgnoreInsert() {
            return ignoreInsert;
        }

        public void setIgnoreInsert(boolean ignoreInsert) {
            this.ignoreInsert = ignoreInsert;
        }

        public boolean isIgnoreUpdate() {
            return ignoreUpdate;
        }

        public void setIgnoreUpdate(boolean ignoreUpdate) {
            this.ignoreUpdate = ignoreUpdate;
        }

        private boolean isValid() {
            return name != null && !name.isEmpty()
                    && column != null && !column.isEmpty()
                    && setter != null && getter != null;
        }
    }
}
