package org.jacuzzi.core;

import net.sf.cglib.reflect.FastClass;
import net.sf.cglib.reflect.FastMethod;
import org.jacuzzi.mapping.Transient;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * @author Mike Mirzayanov
 */
class ReflectionUtil {
    static FastMethod findGetter(Class<?> clazz, String field) {
        String getterName = getGetterName(field);
        FastMethod result = findGetterByName(clazz, field, getterName);
        if (result != null) {
            return result;
        }
        getterName = getBooleanGetterName(field);
        return findGetterByName(clazz, field, getterName);
    }

    static FastMethod findSetter(Class<?> clazz, String field) {
        return findSetterByName(clazz, field, getSetterName(field));
    }

    static String getSetterName(String name) {
        if (name.length() < 1) {
            throw new IllegalArgumentException("Field name too short '" + name + "'.");
        }

        return "set" + Character.toUpperCase(name.charAt(0)) + name.substring(1);
    }

    static String getGetterName(String name) {
        if (name.length() < 1) {
            throw new IllegalArgumentException("Field name too short '" + name + "'.");
        }

        return "get" + Character.toUpperCase(name.charAt(0)) + name.substring(1);
    }

    static String getBooleanGetterName(String name) {
        if (name.length() < 1) {
            throw new IllegalArgumentException("Field name too short '" + name + "'.");
        }

        return "is" + Character.toUpperCase(name.charAt(0)) + name.substring(1);
    }

    public static FastMethod findSetterByName(Class<?> clazz, String field, String setterName) {
        FastMethod setter = null;

        while (setter == null && clazz != null) {
            if (clazz.getAnnotation(Transient.class) != null) {
                break;
            }

            ReflectionFindResult<Field> clazzField =
                    findMappedClassField(clazz, field);

            ReflectionFindResult<FastMethod> clazzMethod =
                    findMappedClassMethod(clazz, setterName);

            if (!clazzField.isTransientAnnotated()) {
                if (clazzMethod.getResult() != null) {
                    setter = clazzMethod.getResult();
                }
            }

            clazz = clazz.getSuperclass();
        }

        return setter;
    }

    public static FastMethod findGetterByName(Class<?> clazz, String field, String getterName) {
        FastMethod getter = null;

        while (getter == null && clazz != null) {
            if (clazz.getAnnotation(Transient.class) != null) {
                break;
            }

            ReflectionFindResult<Field> clazzField =
                    findMappedClassField(clazz, field);

            ReflectionFindResult<FastMethod> clazzMethod =
                    findMappedClassMethodWithNoArguments(clazz, getterName);

            if (!clazzField.isTransientAnnotated()) {
                if (clazzMethod.getResult() != null) {
                    getter = clazzMethod.getResult();
                }
            }

            clazz = clazz.getSuperclass();
        }

        if (getterName.startsWith("is") && getter != null) {
            if (!getter.getReturnType().equals(Boolean.class) && !getter.getReturnType().equals(boolean.class)) {
                return null;
            }
        }

        return getter;
    }

    private static ReflectionFindResult<FastMethod> findMappedClassMethodWithNoArguments(Class<?> clazz, String method) {
        FastClass fastClazz = getFastClass(clazz);
        try {
            FastMethod clazzMethod = fastClazz.getMethod(clazz.getDeclaredMethod(method));

            if (clazzMethod != null) {
                return new ReflectionFindResult<FastMethod>(clazzMethod, false);
            }

            return new ReflectionFindResult<FastMethod>();
        } catch (NoSuchMethodError e) {
            return new ReflectionFindResult<FastMethod>();
        } catch (NoSuchMethodException e) {
            return new ReflectionFindResult<FastMethod>();
        }
    }

    public static String[] findFields(Class<?> clazz) {
        Set<String> result = new TreeSet<String>();

        while (clazz != null) {
            if (clazz.getAnnotation(Transient.class) != null) {
                break;
            }

            for (Field field : clazz.getDeclaredFields()) {
                if (field.getAnnotation(Transient.class) == null) {
                    result.add(field.getName());
                }
            }

            for (Method method : clazz.getDeclaredMethods()) {
                if (method.getAnnotation(Transient.class) == null) {
                    String name = method.getName();

                    if (name.length() > 3 && (name.startsWith("get") || name.startsWith("set"))
                            && Character.isUpperCase(name.charAt(3))) {
                        String fieldName = Character.toLowerCase(name.charAt(3))
                                + (name.length() > 4 ? name.substring(4) : "");
                        result.add(fieldName);
                    }

                    if (name.length() > 2 && name.startsWith("is")
                            && Character.isUpperCase(name.charAt(2))) {
                        String fieldName = Character.toLowerCase(name.charAt(2))
                                + (name.length() > 3 ? name.substring(3) : "");
                        result.add(fieldName);
                    }
                }
            }

            clazz = clazz.getSuperclass();
        }

        return result.toArray(new String[result.size()]);
    }

    public static Map<String, Field> findFieldsMap(Class<?> clazz) {
        Map<String, Field> result = new HashMap<String, Field>();

        while (clazz != null) {
            if (clazz.getAnnotation(Transient.class) != null) {
                break;
            }

            for (Field field : clazz.getDeclaredFields()) {
                if (!result.containsKey(field.getName())
                        && field.getAnnotation(Transient.class) == null) {
                    result.put(field.getName(), field);
                }
            }

            clazz = clazz.getSuperclass();
        }

        return result;
    }

    private static ReflectionFindResult<FastMethod> findMappedClassMethod(Class<?> clazz, String method) {
        FastClass fastClazz = getFastClass(clazz);

        for (Method clazzMethod : clazz.getDeclaredMethods()) {
            if (clazzMethod.getName().equals(method)) {
                return new ReflectionFindResult<FastMethod>(fastClazz.getMethod(clazzMethod),
                        false);
            }
        }

        return new ReflectionFindResult<FastMethod>();
    }

    private static FastClass getFastClass(Class<?> clazz) {
        return FastClass.create(clazz);
    }

    private static ReflectionFindResult<Field> findMappedClassField(Class<?> clazz,
                                                                    String field) {
        try {
            Field clazzField = clazz.getDeclaredField(field);
            return new ReflectionFindResult<Field>(clazzField,
                    clazzField.getAnnotation(Transient.class) != null);
        } catch (NoSuchFieldException e) {
            return new ReflectionFindResult<Field>();
        }
    }

    private static class ReflectionFindResult<T> {
        private T result;
        private boolean transientAnnotated;

        private ReflectionFindResult(T result, boolean transientAnnotated) {
            this.result = result;
            this.transientAnnotated = transientAnnotated;
        }

        public ReflectionFindResult() {
        }

        public T getResult() {
            return result;
        }

        public boolean isTransientAnnotated() {
            return transientAnnotated;
        }
    }
}
