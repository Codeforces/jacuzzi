package org.jacuzzi.core;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

@SuppressWarnings("unused")
public class ArrayMap<K, V> implements Map<K, V>, Serializable {
    private static final int MAX_CAPACITY = 64;

    private int size;

    private final K[] keys;
    private final int[] hashCodes;
    private final V[] values;

    private transient volatile Set<Entry<K, V>> entrySet;
    private transient volatile Set<K> keySet;
    private transient volatile Collection<V> valuesCollection;

    @SuppressWarnings("unchecked")
    public ArrayMap(int capacity) {
        keys = (K[]) new Object[capacity];
        hashCodes = new int[capacity];
        values = (V[]) new Object[capacity];
    }

    @SuppressWarnings({"unchecked", "UnusedDeclaration"})
    public ArrayMap(Map<? extends K, ? extends V> map) {
        int mapSize = map.size();
        if (mapSize > MAX_CAPACITY) {
            throw new IllegalArgumentException("ArrayMap can have more than " + MAX_CAPACITY + " elements, but tried to have " + mapSize + '.');
        }

        keys = (K[]) new Object[mapSize];
        hashCodes = new int[mapSize];
        values = (V[]) new Object[mapSize];

        putAll(map);
    }

    private ArrayMap(K[] keys, V[] values) {
        this.keys = keys;
        this.hashCodes = new int[keys.length];
        for (int i = 0; i < keys.length; i++) {
            this.hashCodes[i] = keys[i].hashCode();
        }
        this.values = values;
        this.size = keys.length;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        int keyHash = key == null ? 0 : key.hashCode();
        for (int i = 0; i < size; ++i) {
            if (same(keyHash, key, hashCodes[i], keys[i])) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean containsValue(Object value) {
        for (int i = 0; i < size; ++i) {
            if (same(value, values[i])) {
                return true;
            }
        }
        return false;
    }

    @Override
    public V get(Object key) {
        int keyHash = key == null ? 0 : key.hashCode();
        for (int i = 0; i < size; ++i) {
            if (same(keyHash, key, hashCodes[i], keys[i])) {
                return values[i];
            }
        }
        return null;
    }

    @Override
    public V put(K key, V value) {
        int keyHash = key == null ? 0 : key.hashCode();
        for (int i = 0; i < size; ++i) {
            if (same(keyHash, key, hashCodes[i], keys[i])) {
                V result = values[i];
                values[i] = value;
                return result;
            }
        }

        keys[size] = key;
        hashCodes[size] = keyHash;
        values[size] = value;
        size++;

        return null;
    }

    @Override
    public V remove(Object key) {
        int keyHash = key == null ? 0 : key.hashCode();
        for (int i = 0; i < size; ++i) {
            if (same(keyHash, key, hashCodes[i], keys[i])) {
                V result = values[i];
                System.arraycopy(keys, i + 1, keys, i, size - (i + 1));
                System.arraycopy(hashCodes, i + 1, hashCodes, i, size - (i + 1));
                System.arraycopy(values, i + 1, values, i, size - (i + 1));
                size--;
                return result;
            }
        }

        return null;
    }

    @Override
    public final void putAll(@Nonnull Map<? extends K, ? extends V> m) {
        for (Map.Entry<? extends K, ? extends V> e : m.entrySet())
            put(e.getKey(), e.getValue());
    }

    @Override
    public void clear() {
        size = 0;
    }

    @Override
    @Nonnull
    public Set<K> keySet() {
        if (keySet == null) {
            keySet = new AbstractSet<K>() {
                @Nonnull
                public Iterator<K> iterator() {
                    return new Iterator<K>() {
                        private Iterator<Entry<K, V>> i = entrySet().iterator();

                        public boolean hasNext() {
                            return i.hasNext();
                        }

                        public K next() {
                            return i.next().getKey();
                        }

                        public void remove() {
                            i.remove();
                        }
                    };
                }

                public int size() {
                    return ArrayMap.this.size();
                }

                public boolean isEmpty() {
                    return ArrayMap.this.isEmpty();
                }

                public void clear() {
                    ArrayMap.this.clear();
                }

                public boolean contains(Object k) {
                    return ArrayMap.this.containsKey(k);
                }
            };
        }
        return keySet;
    }

    @Override
    @Nonnull
    public Collection<V> values() {
        if (valuesCollection == null) {
            valuesCollection = new AbstractCollection<V>() {
                @Nonnull
                public Iterator<V> iterator() {
                    return new Iterator<V>() {
                        private Iterator<Entry<K, V>> i = entrySet().iterator();

                        public boolean hasNext() {
                            return i.hasNext();
                        }

                        public V next() {
                            return i.next().getValue();
                        }

                        public void remove() {
                            i.remove();
                        }
                    };
                }

                public int size() {
                    return ArrayMap.this.size();
                }

                public boolean isEmpty() {
                    return ArrayMap.this.isEmpty();
                }

                public void clear() {
                    ArrayMap.this.clear();
                }

                public boolean contains(Object v) {
                    return ArrayMap.this.containsValue(v);
                }
            };
        }
        return valuesCollection;
    }

    @Override
    @Nonnull
    public Set<Entry<K, V>> entrySet() {
        if (entrySet == null) {
            entrySet = new ArrayMapEntrySet();
        }
        return entrySet;
    }

    private boolean same(@Nullable Object a, @Nullable Object b) {
        return a == b || (a != null && a.equals(b));
    }

    private boolean same(int aHashCode, @Nullable Object a, int bHashCode, @Nullable Object b) {
        return a == b || (a != null && aHashCode == bHashCode && a.equals(b));
    }

    private class ArrayMapEntrySet extends AbstractSet<Entry<K, V>> implements Set<Entry<K, V>> {
        @Override
        @Nonnull
        public Iterator<Entry<K, V>> iterator() {
            return new ArrayMapEntryIterator(0);
        }

        @Override
        public int size() {
            return size;
        }
    }

    private class ArrayMapEntryIterator implements Iterator<Entry<K, V>> {
        private int position;

        private ArrayMapEntryIterator(int position) {
            this.position = position;
        }

        @Override
        public boolean hasNext() {
            return position < size;
        }

        @Override
        public Entry<K, V> next() {
            position++;
            return new AbstractMap.SimpleEntry<>(keys[position - 1], values[position - 1]);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    public static int toBinaryArray(byte[] bytes, int offset, @Nonnull List<Row> rows) {
        offset = ByteArrayUtil.writeString(bytes, offset, "ROWS");
        offset = ByteArrayUtil.writeInt(bytes, offset, rows.size());
        if (!rows.isEmpty()) {
            Row zeroRow = rows.get(0);
            if (zeroRow.delegateMap instanceof HashMap) {
                offset = ByteArrayUtil.writeByte(bytes, offset, (byte) 'H');
                offset = convertHashMapRowsToBinaryArray(bytes, offset, rows);
            } else if (zeroRow.delegateMap instanceof ArrayMap) {
                offset = ByteArrayUtil.writeByte(bytes, offset, (byte) 'A');
                offset = convertArrayMapRowsToBinaryArray(bytes, offset, rows);
            } else {
                throw new RuntimeException("Unexpected Row.delegateMap type '" + zeroRow.delegateMap.getClass() + ".");
            }
        }
        return offset;
    }

    private static int convertHashMapRowsToBinaryArray(byte[] bytes, int offset, List<Row> rows) {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(rows);
            objectOutputStream.close();
            byteArrayOutputStream.close();

            byte[] resultBytes = byteArrayOutputStream.toByteArray();
            System.arraycopy(resultBytes, 0, bytes, offset, resultBytes.length);
            offset += resultBytes.length;

            return offset;
        } catch (IOException e) {
            throw new RuntimeException("Can't convert Row nested HashMap to binary array.", e);
        }
    }

    private static int convertArrayMapRowsToBinaryArray(byte[] bytes, int offset, @Nonnull List<Row> rows) {
        ArrayMap map = (ArrayMap) rows.get(0).delegateMap;
        int n = map.keys.length;

        int[] keyTypes = new int[n];
        Arrays.fill(keyTypes, -1);
        offset = writeObjectArray(bytes, offset, keyTypes, map.keys);

        int[] valueTypes = new int[n];
        Arrays.fill(valueTypes, -1);
        int valueTypeOffset = offset;
        offset += n;

        for (Row row : rows) {
            ArrayMap item = (ArrayMap) row.delegateMap;
            if (item.values.length != n) {
                throw new RuntimeException("item.values.length != n.");
            }

            offset = writeObjectArray(bytes, offset, valueTypes, item.values);
        }

        for (int valueType : valueTypes) {
            valueTypeOffset = ByteArrayUtil.writeByte(bytes, valueTypeOffset, (byte) valueType);
        }

        return offset;
    }

    public static List<Row> fromBinaryArray(byte[] bytes) {
        int[] offset = new int[]{0};
        String header = ByteArrayUtil.readString(bytes, offset);
        if (!"ROWS".equals(header)) {
            throw new RuntimeException("Expected 'ROWS'.");
        }

        int size = ByteArrayUtil.readInt(bytes, offset);
        if (size == 0) {
            return Collections.emptyList();
        }

        byte type = ByteArrayUtil.readByte(bytes, offset);
        if (type == 'H') {
            return convertBinaryArrayToHashMapRows(bytes, offset);
        } else if (type == 'A') {
            return convertBinaryArrayToArrayMapRows(bytes, offset, size);
        } else {
            throw new RuntimeException("Unexpected type to convert byte array to List<Row> [type=" + type + "].");
        }
    }

    private static List<Row> convertBinaryArrayToArrayMapRows(byte[] bytes, int[] offset, int size) {
        List<Row> rows = new ArrayList<>(size);

        Object[] objectKeys = readObjectArray(bytes, offset, null);
        String[] keys = new String[objectKeys.length];
        for (int i = 0; i < objectKeys.length; i++) {
            keys[i] = (String) objectKeys[i];
        }

        int[] valueTypes = new int[size];
        for (int i = 0; i < keys.length; i++) {
            valueTypes[i] = ByteArrayUtil.readByte(bytes, offset);
        }

        for (int i = 0; i < size; i++) {
            rows.add(new Row(new ArrayMap<>(keys, readObjectArray(bytes, offset, valueTypes))));
        }

        return rows;
    }

    private static List<Row> convertBinaryArrayToHashMapRows(byte[] bytes, int[] offset) {
        try {
            ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(
                    bytes, offset[0], bytes.length - offset[0]));
            //noinspection unchecked
            List<Row> rows = (List<Row>) objectInputStream.readObject();
            offset[0] = bytes.length;
            return rows;
        } catch (Exception e) {
            throw new RuntimeException("Can't run convertBinaryArrayToHashMapRows.", e);
        }
    }

    private static Object[] readObjectArray(byte[] bytes, int[] offset, int[] types) {
        int n = ByteArrayUtil.readInt(bytes, offset);
        Object[] result = new Object[n];

        for (int i = 0; i < n; i++) {
            byte nil = ByteArrayUtil.readByte(bytes, offset);
            if (nil == 0) {
                continue;
            }
            if (nil != 1) {
                throw new RuntimeException("Expected 1.");
            }

            int type = types == null ? 20 : types[i];
            if (type == 0) {
                result[i] = ByteArrayUtil.readByte(bytes, offset);
            } else if (type == 2) {
                result[i] = ByteArrayUtil.readInt(bytes, offset);
            } else if (type == 4) {
                result[i] = ByteArrayUtil.readLong(bytes, offset);
            } else if (type == 6) {
                result[i] = ByteArrayUtil.readDouble(bytes, offset);
            } else if (type == 8) {
                result[i] = ByteArrayUtil.readBoolean(bytes, offset);
            } else if (type == 20) {
                result[i] = ByteArrayUtil.readString(bytes, offset);
            } else if (type == 22) {
                result[i] = ByteArrayUtil.readDate(bytes, offset);
            } else {
                throw new RuntimeException("Unexpected type=" + type + ".");
            }
        }

        return result;
    }

    private static int writeObjectArray(byte[] bytes, int offset, int[] types, Object[] array) {
        offset = ByteArrayUtil.writeInt(bytes, offset, array.length);
        for (int i = 0; i < array.length; i++) {
            Object item = array[i];
            if (item == null) {
                offset = ByteArrayUtil.writeByte(bytes, offset, (byte) 0);
            } else {
                offset = ByteArrayUtil.writeByte(bytes, offset, (byte) 1);

                int type = types[i];
                // 0 - byte
                // 1 - Byte
                // 2 - int
                // 3 - Integer
                // 4 - long
                // 5 - Long
                // 6 - double
                // 7 - Double
                // 8 - boolean
                // 9 - Boolean
                // 20 - String
                // 22 - Date

                if (type == -1) {
                    if (item instanceof Byte) {
                        type = 0;
                    } else if (item instanceof Integer) {
                        type = 2;
                    } else if (item instanceof Long) {
                        type = 4;
                    } else if (item instanceof Double) {
                        type = 6;
                    } else if (item instanceof Boolean) {
                        type = 8;
                    } else if (item instanceof String) {
                        type = 20;
                    } else if (item instanceof Date) {
                        type = 22;
                    } else {
                        throw new RuntimeException("ArrayMap type '" + item.getClass() + "' is not for custom serialization.");
                    }
                    types[i] = type;
                }

                if (type == 0) {
                    offset = ByteArrayUtil.writeByte(bytes, offset, (byte) item);
                } else if (type == 2) {
                    offset = ByteArrayUtil.writeInt(bytes, offset, (int) item);
                } else if (type == 4) {
                    offset = ByteArrayUtil.writeLong(bytes, offset, (long) item);
                } else if (type == 6) {
                    offset = ByteArrayUtil.writeDouble(bytes, offset, (double) item);
                } else if (type == 8) {
                    offset = ByteArrayUtil.writeBoolean(bytes, offset, (boolean) item);
                } else if (type == 20) {
                    offset = ByteArrayUtil.writeString(bytes, offset, (String) item);
                } else if (type == 22) {
                    offset = ByteArrayUtil.writeDate(bytes, offset, (Date) item);
                } else {
                    throw new RuntimeException("Unexpected type=" + type + ".");
                }
            }
        }
        return offset;
    }

    private static final class ByteArrayUtil {
        private static int writeByte(byte[] bytes, int offset, byte n) {
            bytes[offset++] = n;
            return offset;
        }

        private static int writeInt(byte[] bytes, int offset, int n) {
            for (int i = 0; i < 4; i++) {
                bytes[offset++] = (byte) (n & 255);
                n >>>= 8;
            }
            return offset;
        }

        private static int writeLong(byte[] bytes, int offset, long n) {
            for (int i = 0; i < 8; i++) {
                bytes[offset++] = (byte) (n & 255);
                n >>>= 8;
            }
            return offset;
        }

        private static int writeDouble(byte[] bytes, int offset, double n) {
            return writeLong(bytes, offset, Double.doubleToLongBits(n));
        }

        private static int writeBoolean(byte[] bytes, int offset, boolean n) {
            bytes[offset++] = (byte) (n ? 1 : 0);
            return offset;
        }

        private static int writeString(byte[] bytes, int offset, String s) {
            byte[] buffer = s.getBytes(StandardCharsets.UTF_8);
            offset = writeInt(bytes, offset, buffer.length);
            System.arraycopy(buffer, 0, bytes, offset, buffer.length);
            offset += buffer.length;
            return offset;
        }

        private static int writeDate(byte[] bytes, int offset, Date date) {
            return writeLong(bytes, offset, date.getTime());
        }

        private static byte readByte(byte[] bytes, int[] offset) {
            return bytes[offset[0]++];
        }

        private static int readInt(byte[] bytes, int[] offset) {
            int n = 0;
            for (int i = 0; i < 4; i++) {
                int value = bytes[offset[0]++];
                if (value < 0) {
                    value += 256;
                }
                n ^= (value << (8 * i));
            }
            return n;
        }

        private static long readLong(byte[] bytes, int[] offset) {
            long n = 0;
            for (int i = 0; i < 8; i++) {
                long value = bytes[offset[0]++];
                if (value < 0) {
                    value += 256;
                }
                n ^= (value << (8 * i));
            }
            return n;
        }

        private static double readDouble(byte[] bytes, int[] offset) {
            return Double.longBitsToDouble(readLong(bytes, offset));
        }

        private static boolean readBoolean(byte[] bytes, int[] offset) {
            return readByte(bytes, offset) != 0;
        }

        private static String readString(byte[] bytes, int[] offset) {
            int length = readInt(bytes, offset);
            if (length == Integer.MIN_VALUE) {
                return null;
            } else {
                byte[] buffer = new byte[length];
                System.arraycopy(bytes, offset[0], buffer, 0, length);
                String s = new String(buffer, 0, length, StandardCharsets.UTF_8);
                offset[0] += length;
                return s;
            }
        }

        private static Date readDate(byte[] bytes, int[] offset) {
            long n = readLong(bytes, offset);
            if (n == Long.MIN_VALUE) {
                return null;
            } else {
                return new Date(n);
            }
        }
    }
}
