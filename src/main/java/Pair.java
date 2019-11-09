import java.util.AbstractMap;

public class Pair<E> extends AbstractMap.SimpleImmutableEntry<E, E> {
    public Pair(E key, E value) {
        super(key, value);
    }

    public E first() {
        return getKey();
    }

    public E second() {
        return getValue();
    }

    @Override
    public String toString() {
        return this.getKey() + "," + this.getValue();
    }
}
