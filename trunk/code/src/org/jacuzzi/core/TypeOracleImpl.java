package org.jacuzzi.core;

import net.sf.cglib.reflect.FastClass;
import net.sf.cglib.reflect.FastMethod;
import org.jacuzzi.mapping.Id;
import org.jacuzzi.mapping.MappedTo;

import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * @author: Mike Mirzayanov
 */
class TypeOracleImpl<T> extends TypeOracle<T> {
    private FastClass fastClazz;
    private Class<T> clazz;
    private List<Field> fields;
    private Map<String, Field> fieldsByColumns;
    private Field idField;
    private String tableName;

    private synchronized void ensureMapping() {
        if (fields == null) {
            System.out.println("ensureMapping");
            fields = new ArrayList<Field>();
            fieldsByColumns = new HashMap<String, Field>();
            fastClazz = FastClass.create(clazz);

            if (clazz.getAnnotation(MappedTo.class) != null) {
                tableName = clazz.getAnnotation(MappedTo.class).value();
            } else {
                tableName = clazz.getSimpleName();
            }

            String[] fieldNames = ReflectionUtil.findFields(clazz);
            Map<String, java.lang.reflect.Field> fieldsMap = ReflectionUtil.findFieldsMap(clazz);

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
                        idField = field;
                    }

                    fields.add(field);
                    fieldsByColumns.put(field.getColumn(), field);
                }
            }
        }
    }

    TypeOracleImpl(Class<T> clazz) {
        System.out.println("TypeOracleImpl created!");
        this.clazz = clazz;
    }

    public String getIdColumn() {
        ensureMapping();

        if (idField == null) {
            throw new MappingException("Can't find Id for class " + clazz + ".");
        }

        return idField.getColumn();
    }

    public String getTableName() {
        ensureMapping();

        return tableName;
    }

    public String getQuerySetSql() {
        ensureMapping();

        if (fields.size() == 0) {
            throw new MappingException("Nothing to set for class " + clazz + ".");
        }

        StringBuffer result = new StringBuffer();
        result.append("SET");

        for (Field field : fields) {
            result.append(Query.format(" ?f = ?,", field.getColumn()));
        }

        return result.substring(0, result.length() - 1);
    }

    public Object[] getQuerySetArguments(T instance) {
        ensureMapping();

        if (fields.size() == 0) {
            throw new MappingException("Nothing to set for class " + clazz + ".");
        }

        Object[] result = new Object[fields.size()];

        int index = 0;

        for (Field field : fields) {
            try {
                result[index++] = field.getGetter().invoke(instance, new Object[0]);
            } catch (InvocationTargetException e) {
                throw new MappingException("Can't invoke getter " + field.getGetter() + ".");
            }
        }

        return result;
    }

    public Object getIdValue(T instance) {
        ensureMapping();

        if (idField == null) {
            throw new MappingException("Can't find id field for class " + clazz + ".");
        }

        try {
            return idField.getGetter().invoke(instance, new Object[]{});
        } catch (InvocationTargetException e) {
            throw new MappingException("Can't invoke getter " + idField.getGetter() + " to get id value.");
        }
    }

    public T newInstance() {
        T instance;

        ensureFastClazz();

        try {
            instance = (T) fastClazz.newInstance();
        } catch (InvocationTargetException e) {
            throw new MappingException("Can't instantiate class " + clazz + ".", e);
        }

        return instance;
    }

    private void ensureFastClazz() {
        if (fastClazz == null) {
            fastClazz = FastClass.create(clazz);
        }
    }

    public T convertFromRow(Row row) {
        ensureMapping();

        Set<String> columns = row.keySet();

        T instance = newInstance();

        for (String column: columns) {
            if (fieldsByColumns.containsKey(column)) {
                Field field = fieldsByColumns.get(column);
                try {
                    field.getSetter().invoke(instance, new Object[]{row.get(column)});
                } catch (InvocationTargetException e) {
                    throw new MappingException("Can't invoke setter " + field.getSetter() + " for parameter " + row.get(column).getClass() + ".");
                }
            }
        }

        return instance;
    }

    private class Field {

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
