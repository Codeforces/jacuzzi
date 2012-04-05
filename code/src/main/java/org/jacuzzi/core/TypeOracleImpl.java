package org.jacuzzi.core;

import net.sf.cglib.reflect.FastClass;
import net.sf.cglib.reflect.FastMethod;
import org.apache.log4j.Logger;
import org.jacuzzi.mapping.Id;
import org.jacuzzi.mapping.MappedTo;

import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * @author Mike Mirzayanov
 */
class TypeOracleImpl<T> extends TypeOracle<T> {
    private static final Logger logger = Logger.getLogger(TypeOracleImpl.class);

    private static final Object[] EMPTY_OBJECT_ARRAY = {};

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
        Map<String, java.lang.reflect.Field> fieldsMap = ReflectionUtil.findFieldsMap(clazz);
        Field internalIdField = null;

        for (String fieldName : fieldNames) {
            Field field = new Field(fieldName);

            field.setSetter(ReflectionUtil.findSetter(clazz, fieldName));
            field.setGetter(ReflectionUtil.findGetter(clazz, fieldName));

            boolean isId = false;
            String column = fieldName;
            if (fieldsMap.containsKey(fieldName)) {
                MappedTo mappedTo = fieldsMap.get(fieldName).getAnnotation(MappedTo.class);
                if (mappedTo != null) {
                    column = mappedTo.value();
                }
                isId = fieldsMap.get(fieldName).getAnnotation(Id.class) != null;
            }
            field.setColumn(column);

            if (field.isValid()) {
                if (!isId) {
                    isId = field.getSetter().getJavaMethod().getAnnotation(Id.class) != null;
                    isId = isId || field.getSetter().getJavaMethod().getAnnotation(Id.class) != null;
                }

                field.setId(isId);

                if (isId) {
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
            result.append(Query.format(" ?f = ?,", field.getColumn()));
        }

        return result.substring(0, result.length() - 1);
    }

    @Override
    public String getFieldList(boolean includeId, boolean useTablePrefix) {
        if (fields.isEmpty()) {
            throw new MappingException("Nothing to set for class " + clazz + '.');
        }

        StringBuilder result = new StringBuilder();

        for (Field field : fields) {
            if (!field.isId() || includeId) {
                if (result.length() > 0) {
                    result.append(", ");
                }

                if (useTablePrefix) {
                    result.append(Query.format("?t.?f", tableName, field.getColumn()));
                } else {
                    result.append(Query.format("?f", field.getColumn()));
                }
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
            if (field.isId() && !includeId) {
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
    Object[] getValueListForInsert(boolean includeId, T instance) {
        Object[] result = new Object[fields.size() - (includeId ? 0 : 1)];

        try {
            int index = 0;
            for (Field field : fields) {
                if (!field.isId() || includeId) {
                    result[index++] = field.getGetter().invoke(instance, EMPTY_OBJECT_ARRAY);
                }
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

        if (id instanceof String && "".equals(id)) {
            return false;
        }

        return true;
    }

    @Override
    public Object[] getQuerySetArguments(T instance) {
        if (fields.isEmpty()) {
            throw new MappingException("Nothing to set for class " + clazz + '.');
        }

        Object[] result = new Object[fields.size()];

        int index = 0;

        for (Field field : fields) {
            try {
                result[index++] = field.getGetter().invoke(instance, EMPTY_OBJECT_ARRAY);
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

    @Override
    public T convertFromRow(Row row) {
        Set<String> columns = row.keySet();
        T instance = newInstance();

        Map<String, String> columnNameByLowercaseColumnName = new HashMap<String, String>();
        for (String column : fieldByColumn.keySet()) {
            columnNameByLowercaseColumnName.put(column.toLowerCase(), column);
        }

        for (String column : columns) {
            if (columnNameByLowercaseColumnName.containsKey(column.toLowerCase())) {
                Field field = fieldByColumn.get(columnNameByLowercaseColumnName.remove(column.toLowerCase()));
                try {
                    Object parameter = row.get(column);
                    Class<?> expectedParameterType = field.getSetter().getParameterTypes()[0];
                    Object castedParameter = convertTo(parameter, expectedParameterType);
                    field.getSetter().invoke(instance, new Object[]{castedParameter});
                } catch (InvocationTargetException e) {
                    if (row.get(column) != null) {
                        throw new MappingException("Can't invoke setter " + field.getSetter() + "[clazz="
                                + clazz.getName() + "] for parameter " + row.get(column).getClass() + '.', e);
                    } else {
                        throw new MappingException("Can't invoke setter " + field.getSetter() + " for class "
                                + clazz.getName() + '.', e);
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

        private Field(String name) {
            this.name = name;
        }

        private boolean isValid() {
            return name != null && setter != null && getter != null && column != null;
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
    }
}
