package org.jacuzzi.core;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

public class ArrayMap<K, V> implements Map<K, V> {
    private static final int MAX_CAPACITY = 64;

    private int size = 0;

    private final K[] keys;
    private final int[] hashCodes;
    private final V[] values;

    private transient volatile Set<Entry<K, V>> entrySet = null;
    private transient volatile Set<K> keySet = null;
    private transient volatile Collection<V> valuesCollection = null;

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
            throw new IllegalArgumentException("ArrayMap can have more than " + MAX_CAPACITY + " elements, but tried to have " + mapSize + ".");
        }

        keys = (K[]) new Object[mapSize];
        hashCodes = new int[mapSize];
        values = (V[]) new Object[mapSize];

        putAll(map);
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
        for (int i = 0; i < size; i++) {
            if (same(keyHash, key, hashCodes[i], keys[i])) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean containsValue(Object value) {
        for (int i = 0; i < size; i++) {
            if (same(value, values[i])) {
                return true;
            }
        }
        return false;
    }

    @Override
    public V get(Object key) {
        int keyHash = key == null ? 0 : key.hashCode();
        for (int i = 0; i < size; i++) {
            if (same(keyHash, key, hashCodes[i], keys[i])) {
                return values[i];
            }
        }
        return null;
    }

    @Override
    public V put(K key, V value) {
        int keyHash = key == null ? 0 : key.hashCode();
        for (int i = 0; i < size; i++) {
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
        for (int i = 0; i < size; i++) {
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
    public void putAll(@Nonnull Map<? extends K, ? extends V> m) {
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

        public ArrayMapEntryIterator(int position) {
            this.position = position;
        }

        @Override
        public boolean hasNext() {
            return position < size;
        }

        @Override
        public Entry<K, V> next() {
            position++;
            return new AbstractMap.SimpleEntry<K, V>(keys[position - 1], values[position - 1]);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    public static void test() {
        int result = 0;

        for (int i = 0; i < 10000000; i++) {
            Map<Integer, Integer> z = new HashMap<Integer, Integer>();
            z.put(i, i * i);
            z.put(i + 1, i * i + 1);
            result += z.size();
        }

        for (int i = 0; i < 100000; i++) {
            Map<Integer, Integer> z = new HashMap<Integer, Integer>();

            for (int j = 0; j < 4; j++) {
                z.put(j * j, j);
            }

            for (int j = 0; j < 1000; j++) {
                if (z.containsKey(j)) {
                    result++;
                }
            }

            result += z.size();
        }

        System.out.println(result);

    }

    public static void main(String[] args) {
        long start = System.currentTimeMillis();

        test();

        System.out.println(System.currentTimeMillis() - start);
    }
}
