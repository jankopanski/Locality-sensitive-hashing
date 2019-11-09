import java.util.Random;

import static org.apache.commons.math3.util.ArithmeticUtils.gcd;

public class RandomIterator {
    private int range;
    private int prime;
    private int value;
    private int index = 0;

    public RandomIterator(int range, int seed) {
        this.range = range;
        Random random = new Random(seed);
        value = random.nextInt(range);
        do {
            prime = random.nextInt(range);
        } while (prime <= 1 || !coprime(prime, range));
    }

    public boolean hasNext() {
        return index < range;
    }

    public int next() {
        value += prime;
        if (value >= range) value -= range;
        index++;
        return value;
    }

    private static boolean coprime(int p, int q) {
        return gcd(p, q) == 1;
    }
}
