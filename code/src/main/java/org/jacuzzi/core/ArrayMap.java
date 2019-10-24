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

    @SuppressWarnings({"unchecked", "WeakerAccess"})
    public ArrayMap(int capacity) {
        keys = (K[]) new Object[capacity];
        hashCodes = new int[capacity];
        values = (V[]) new Object[capacity];
    }

    @SuppressWarnings({"unchecked", "UnusedDeclaration"})
    public ArrayMap(Map<? extends K, ? extends V> map) {
        int mapSize = map.size();
        if (mapSize > MAX_CAPACITY) {
            throw new IllegalArgumentException("ArrayMap can have no more than " + MAX_CAPACITY + " elements, but tried to have " + mapSize + '.');
        }

        keys = (K[]) new Object[mapSize];
        hashCodes = new int[mapSize];
        values = (V[]) new Object[mapSize];

        putAll(map);
    }

    @SuppressWarnings("WeakerAccess")
    ArrayMap(K[] keys, V[] values) {
        this.keys = keys;
        this.hashCodes = new int[keys.length];
        for (int i = 0; i < keys.length; i++) {
            this.hashCodes[i] = keys[i].hashCode();
        }
        this.values = values;
        this.size = keys.length;
    }

    ArrayMap(K[] keys, int[] hashCodes, V[] values) {
        this.keys = keys;
        this.hashCodes = hashCodes;
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
        if (key == null) {
            for (int i = size; --i >= 0; ) {
                if (keys[i] == null) {
                    return true;
                }
            }
        } else {
            int hashCode = key.hashCode();

            for (int i = size; --i >= 0; ) {
                if (same(hashCode, key, hashCodes[i], keys[i])) {
                    return true;
                }
            }
        }

        return false;
    }

    @Override
    public boolean containsValue(Object value) {
        if (value == null) {
            for (int i = size; --i >= 0; ) {
                if (values[i] == null) {
                    return true;
                }
            }
        } else {
            for (int i = size; --i >= 0; ) {
                if (same(value, values[i])) {
                    return true;
                }
            }
        }

        return false;
    }

    @Override
    public V get(Object key) {
        if (key == null) {
            for (int i = size; --i >= 0; ) {
                if (keys[i] == null) {
                    return values[i];
                }
            }
        } else {
            int hashCode = key.hashCode();

            for (int i = size; --i >= 0; ) {
                if (same(hashCode, key, hashCodes[i], keys[i])) {
                    return values[i];
                }
            }
        }

        return null;
    }

    @Override
    public V put(K key, V value) {
        if (key == null) {
            for (int i = size; --i >= 0; ) {
                if (keys[i] == null) {
                    V result = values[i];
                    values[i] = value;
                    return result;
                }
            }

            hashCodes[size] = 0;
        } else {
            int hashCode = key.hashCode();

            for (int i = size; --i >= 0; ) {
                if (same(hashCode, key, hashCodes[i], keys[i])) {
                    V result = values[i];
                    values[i] = value;
                    return result;
                }
            }

            hashCodes[size] = hashCode;
        }

        keys[size] = key;
        values[size] = value;
        ++size;

        return null;
    }

    @Override
    public V remove(Object key) {
        if (key == null) {
            for (int i = size; --i >= 0; ) {
                if (keys[i] == null) {
                    return remove(i);
                }
            }
        } else {
            int hashCode = key.hashCode();

            for (int i = size; --i >= 0; ) {
                if (same(hashCode, key, hashCodes[i], keys[i])) {
                    return remove(i);
                }
            }
        }

        return null;
    }

    private V remove(int i) {
        V result = values[i];
        int length = size - i - 1;

        if (length > 0) {
            System.arraycopy(keys, i + 1, keys, i, length);
            System.arraycopy(hashCodes, i + 1, hashCodes, i, length);
            System.arraycopy(values, i + 1, values, i, length);
        }

        --size;
        return result;
    }

    @Override
    public final void putAll(@Nonnull Map<? extends K, ? extends V> map) {
        for (Map.Entry<? extends K, ? extends V> e : map.entrySet()) {
            put(e.getKey(), e.getValue());
        }
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

    @SuppressWarnings("ObjectEquality")
    private static boolean same(@Nonnull Object objectA, @Nullable Object objectB) {
        return objectA == objectB || objectA.equals(objectB);
    }

    @SuppressWarnings("ObjectEquality")
    private static boolean same(int hashCodeA, @Nonnull Object objectA, int hashCodeB, @Nullable Object objectB) {
        return objectA == objectB || hashCodeA == hashCodeB && objectA.equals(objectB);
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
            ++position;
            return new AbstractMap.SimpleEntry<>(keys[position - 1], values[position - 1]);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    @Deprecated
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

    @Deprecated
    public static int toBinaryArray(byte[] bytes, int offset, @Nonnull RowRoll rowRoll) {
        offset = ByteArrayUtil.writeString(bytes, offset, "ROWS");
        offset = ByteArrayUtil.writeInt(bytes, offset, rowRoll.size());
        if (!rowRoll.isEmpty()) {
            offset = ByteArrayUtil.writeByte(bytes, offset, (byte) 'A');
            offset = convertRowRollToBinaryArray(bytes, offset, rowRoll);
        }
        return offset;
    }

    public static void writeRowRoll(OutputStream outputStream, @Nonnull RowRoll rowRoll) throws IOException {
        OutputStreamUtil.writeString(outputStream, "ROWS");
        OutputStreamUtil.writeInt(outputStream, rowRoll.size());
        if (!rowRoll.isEmpty()) {
            OutputStreamUtil.writeByte(outputStream, (byte) 'A');
            writeRowRollBody(outputStream, rowRoll);
        }
    }

    private static void writeRowRollBody(OutputStream outputStream, RowRoll rowRoll) throws IOException {
        int n = rowRoll.getColumnCount();

        int[] keyTypes = new int[n];
        calculateTypes(keyTypes, rowRoll.getKeys());
        writeObjectArray(outputStream, keyTypes, rowRoll.getKeys());

        int[] valueTypes = new int[n];
        Arrays.fill(valueTypes, -1);
        for (Object[] values : rowRoll.getValueList()) {
            calculateTypes(valueTypes, values);
            boolean containsMinusOne = false;
            for (int valueType : valueTypes) {
                if (valueType == -1) {
                    containsMinusOne = true;
                    break;
                }
            }
            if (!containsMinusOne) {
                break;
            }
        }

        for (int valueType : valueTypes) {
            OutputStreamUtil.writeByte(outputStream, (byte) valueType);
        }

        for (Object[] values : rowRoll.getValueList()) {
            writeObjectArray(outputStream, valueTypes, values);
        }
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

    private static int convertRowRollToBinaryArray(byte[] bytes, int offset, RowRoll rowRoll) {
        int n = rowRoll.getColumnCount();

        int[] keyTypes = new int[n];
        Arrays.fill(keyTypes, -1);
        offset = writeObjectArray(bytes, offset, keyTypes, rowRoll.getKeys());

        int[] valueTypes = new int[n];
        Arrays.fill(valueTypes, -1);
        int valueTypeOffset = offset;
        offset += n;

        for (Object[] values : rowRoll.getValueList()) {
            offset = writeObjectArray(bytes, offset, valueTypes, values);
        }

        for (int valueType : valueTypes) {
            valueTypeOffset = ByteArrayUtil.writeByte(bytes, valueTypeOffset, (byte) valueType);
        }

        return offset;
    }

    @Deprecated
    public static RowRoll toRowRoll(byte[] bytes) {
        int[] offset = new int[]{0};
        String header = ByteArrayUtil.readString(bytes, offset);
        if (!"ROWS".equals(header)) {
            throw new RuntimeException("Expected 'ROWS'.");
        }

        int size = ByteArrayUtil.readInt(bytes, offset);
        if (size == 0) {
            return new RowRoll();
        }

        byte type = ByteArrayUtil.readByte(bytes, offset);
        if (type != 'A') {
            throw new IllegalStateException("Expected 'A', but '" + (char) type + "' found.");
        }

        return convertBinaryArrayToRowRoll(bytes, offset, size);
    }

    public static RowRoll readRowRoll(InputStream inputStream) throws IOException {
        String header = InputStreamUtil.readString(inputStream);
        if (!"ROWS".equals(header)) {
            throw new RuntimeException("Expected 'ROWS'.");
        }

        int size = InputStreamUtil.readInt(inputStream);
        if (size == 0) {
            return new RowRoll();
        }

        byte type = InputStreamUtil.readByte(inputStream);
        if (type != 'A') {
            throw new IllegalStateException("Expected 'A', but '" + (char) type + "' found.");
        }

        return readRowRollBody(inputStream, size);
    }

    private static RowRoll readRowRollBody(InputStream inputStream, int size) throws IOException {
        RowRoll rowRoll = new RowRoll();

        Object[] objectKeys = readObjectArray(inputStream, null);
        String[] keys = new String[objectKeys.length];
        for (int i = 0; i < objectKeys.length; i++) {
            keys[i] = (String) objectKeys[i];
        }
        rowRoll.setKeys(keys);

        int[] valueTypes = new int[keys.length];
        for (int i = 0; i < keys.length; i++) {
            valueTypes[i] = InputStreamUtil.readByte(inputStream);
        }

        for (int i = 0; i < size; i++) {
            rowRoll.addValues(readObjectArray(inputStream, valueTypes));
        }

        return rowRoll;
    }
    
    private static RowRoll convertBinaryArrayToRowRoll(byte[] bytes, int[] offset, int size) {
        RowRoll rowRoll = new RowRoll();

        Object[] objectKeys = readObjectArray(bytes, offset, null);
        String[] keys = new String[objectKeys.length];
        for (int i = 0; i < objectKeys.length; i++) {
            keys[i] = (String) objectKeys[i];
        }
        rowRoll.setKeys(keys);

        int[] valueTypes = new int[keys.length];
        for (int i = 0; i < keys.length; i++) {
            valueTypes[i] = ByteArrayUtil.readByte(bytes, offset);
        }

        for (int i = 0; i < size; i++) {
            rowRoll.addValues(readObjectArray(bytes, offset, valueTypes));
        }

        return rowRoll;
    }

    @Deprecated
    public static List<Row> toRows(byte[] bytes) {
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

        int[] valueTypes = new int[keys.length];
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
    
    private static Object[] readObjectArray(InputStream inputStream, int[] types) throws IOException {
        int n = InputStreamUtil.readInt(inputStream);
        Object[] result = new Object[n];

        for (int i = 0; i < n; i++) {
            byte nil = InputStreamUtil.readByte(inputStream);
            if (nil == 0) {
                continue;
            }
            if (nil != 1) {
                throw new RuntimeException("Expected 1.");
            }

            int type = types == null ? 20 : types[i];
            if (type == 0) {
                result[i] = InputStreamUtil.readByte(inputStream);
            } else if (type == 2) {
                result[i] = InputStreamUtil.readInt(inputStream);
            } else if (type == 4) {
                result[i] = InputStreamUtil.readLong(inputStream);
            } else if (type == 6) {
                result[i] = InputStreamUtil.readDouble(inputStream);
            } else if (type == 8) {
                result[i] = InputStreamUtil.readBoolean(inputStream);
            } else if (type == 20) {
                result[i] = InputStreamUtil.readString(inputStream);
            } else if (type == 22) {
                result[i] = InputStreamUtil.readDate(inputStream);
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

                if (type == -1) {
                    type = getType(item);
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

    private static int getType(Object item) {
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

        int type;
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
        return type;
    }

    private static void calculateTypes(int[] types, Object[] array) {
        for (int i = 0; i < array.length; i++) {
            Object item = array[i];
            if (item != null) {
                types[i] = getType(item);
            }
        }
    }

    private static void writeObjectArray(OutputStream outputStream, int[] types, Object[] array) throws IOException {
        OutputStreamUtil.writeInt(outputStream, array.length);
        for (int i = 0; i < array.length; i++) {
            Object item = array[i];
            int type = types[i];
            if (item == null) {
                OutputStreamUtil.writeByte(outputStream, (byte) 0);
            } else {
                OutputStreamUtil.writeByte(outputStream, (byte) 1);

                if (type == 0) {
                    OutputStreamUtil.writeByte(outputStream, (byte) item);
                } else if (type == 2) {
                    OutputStreamUtil.writeInt(outputStream, (int) item);
                } else if (type == 4) {
                    OutputStreamUtil.writeLong(outputStream, (long) item);
                } else if (type == 6) {
                    OutputStreamUtil.writeDouble(outputStream, (double) item);
                } else if (type == 8) {
                    OutputStreamUtil.writeBoolean(outputStream, (boolean) item);
                } else if (type == 20) {
                    OutputStreamUtil.writeString(outputStream, (String) item);
                } else if (type == 22) {
                    OutputStreamUtil.writeDate(outputStream, (Date) item);
                } else {
                    throw new RuntimeException("Unexpected type=" + type + ".");
                }
            }
        }
    }

    private static final class InputStreamUtil {
        private static byte readByte(InputStream inputStream) throws IOException {
            int result = inputStream.read();
            if (result == -1) {
                throw new IOException("Unexpected end of the inputStream.");
            }
            return (byte) result;
        }

        private static int readInt(InputStream inputStream) throws IOException {
            int n = 0;
            for (int i = 0; i < 4; i++) {
                int value = readByte(inputStream);
                if (value < 0) {
                    value += 256;
                }
                n ^= (value << (8 * i));
            }
            return n;
        }

        private static long readLong(InputStream inputStream) throws IOException {
            long n = 0;
            for (int i = 0; i < 8; i++) {
                long value = readByte(inputStream);
                if (value < 0) {
                    value += 256;
                }
                n ^= (value << (8 * i));
            }
            return n;
        }

        private static double readDouble(InputStream inputStream) throws IOException {
            return Double.longBitsToDouble(readLong(inputStream));
        }

        private static boolean readBoolean(InputStream inputStream) throws IOException {
            return readByte(inputStream) != 0;
        }

        private static String readString(InputStream inputStream) throws IOException {
            int length = readInt(inputStream);
            if (length == Integer.MIN_VALUE) {
                return null;
            } else {
                byte[] buffer = new byte[length];
                int offset = 0;
                while (offset < length) {
                    int readBytes = inputStream.read(buffer, offset, length - offset);
                    if (readBytes == -1) {
                        throw new IOException("Unexpected end of the inputStream.");
                    }
                    offset += readBytes;
                }
                return new String(buffer, 0, length, StandardCharsets.UTF_8);
            }
        }

        private static Date readDate(InputStream inputStream) throws IOException {
            long n = readLong(inputStream);
            if (n == Long.MIN_VALUE) {
                return null;
            } else {
                return new Date(n);
            }
        }
    }
    
    private static final class OutputStreamUtil {
        private static void writeByte(OutputStream outputStream, byte n) throws IOException {
            outputStream.write(n);
        }

        private static void writeInt(OutputStream outputStream, int n) throws IOException {
            for (int i = 0; i < 4; i++) {
                writeByte(outputStream, (byte) (n & 255));
                n >>>= 8;
            }
        }

        private static void writeLong(OutputStream outputStream, long n) throws IOException {
            for (int i = 0; i < 8; i++) {
                writeByte(outputStream, (byte) (n & 255));
                n >>>= 8;
            }
        }

        private static void writeDouble(OutputStream outputStream, double n) throws IOException {
            writeLong(outputStream, Double.doubleToLongBits(n));
        }

        private static void writeBoolean(OutputStream outputStream, boolean n) throws IOException {
            writeByte(outputStream, (byte) (n ? 1 : 0));
        }

        private static void writeString(OutputStream outputStream, String s) throws IOException {
            byte[] buffer = s.getBytes(StandardCharsets.UTF_8);
            writeInt(outputStream, buffer.length);
            outputStream.write(buffer);
        }

        private static void writeDate(OutputStream outputStream, Date date) throws IOException {
            writeLong(outputStream, date.getTime());
        }
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
