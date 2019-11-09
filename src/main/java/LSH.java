import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import static java.lang.Math.min;

public class LSH {
    private static final int SHINGLE_LENGTH = 10;
    private static final int SIGNATURE_LENGTH = 100;
    private static final int BUCKET_NUM = 1000000;
    private static final int BAND_NUM = 5;
    private static final int BAND_SIZE = 20;

    private static List<String> readCSV(Path path) throws IOException, URISyntaxException {
        try (
                final DistributedFileSystem dFS = new DistributedFileSystem() {
                    {
                        initialize(new URI("hdfs://localhost:9000"), new Configuration());
                    }
                };
                final FSDataInputStream inputStream = dFS.open(path);
                final InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                final CSVReader reader = new CSVReaderBuilder(inputStreamReader).withSkipLines(1).build();
        ) {
            List<String[]> csv = reader.readAll();
            List<String> lines = new ArrayList<>(csv.size());
            for (String[] entry : csv) {
                lines.add(entry[1]);
            }
            return lines;
        }
    }

    private static Set<String> getShingles(String line) {
        line = line.replaceAll("[^\\x00-\\x7F]", "");
        Set<String> shingles = new HashSet<>();
        for (int i = 0; i < line.length() - SHINGLE_LENGTH; i++) {
            shingles.add(line.substring(i, i + SHINGLE_LENGTH));
        }
        return shingles;
    }

    private static Set<Integer> getTokens(String line) {
        Set<Integer> tokens = new HashSet<>();
        for (int i = 0; i < line.length() - SHINGLE_LENGTH; i++) {
            tokens.add(line.substring(i, i + SHINGLE_LENGTH).hashCode());
        }
        return tokens;
    }

    private static List<Set<Integer>> shingling(List<String> lines) throws IOException, URISyntaxException {
        List<Set<Integer>> tokensList = new ArrayList<>(lines.size());
        for (String line : lines) {
            if (line.length() >= SHINGLE_LENGTH) {
                tokensList.add(getTokens(line));
            }
        }
        return tokensList;
    }

    private static Set<Integer> getTokenUniverse(List<Set<Integer>> tokensList) {
        Set<Integer> universe = new HashSet<>();
        for (Set<Integer> tokens : tokensList) {
            universe.addAll(tokens);
        }
        return universe;
    }

    private static int[][] minhashing(List<Set<Integer>> tokenColumns, Set<Integer> universe) {
        final List<RandomIterator> permutations = new ArrayList<>(SIGNATURE_LENGTH);
        for (int i = 0; i < SIGNATURE_LENGTH; i++) {
            permutations.add(new RandomIterator(universe.size(), i));
        }

        final int[][] signatures = new int[permutations.size()][tokenColumns.size()];
        for (int[] row : signatures) {
            Arrays.fill(row, universe.size());
        }

        int counter = 0;
        for (Integer token : universe) {
            if (++counter % 10000 == 0) {
                System.out.println("Minhashing processed rows: " + counter + "/" + universe.size());
            }

            Integer[] h = new Integer[permutations.size()];
            for (int j = 0; j < permutations.size(); j++) {
                h[j] = permutations.get(j).next();
            }

            for (int i = 0; i < tokenColumns.size(); i++) {
                if (tokenColumns.get(i).contains(token)) {
                    for (int j = 0; j < permutations.size(); j++) {
                        signatures[j][i] = min(signatures[j][i], h[j]);
                    }
                }
            }
        }
        return signatures;
    }

    private static Set<Pair<Integer>> localitySensitiveHashing(int[][] signatures, int columnNum) {
        Set<Pair<Integer>> pairs = new HashSet<>();
        for (int startRow = 0; startRow < SIGNATURE_LENGTH; startRow += BAND_SIZE) {
            if (startRow > 0) {
                System.out.println("Locality Sensitive Hashing processed rows: " + startRow + "/" + SIGNATURE_LENGTH);
            }
            List<Set<Integer>> buckets = new ArrayList<>(BUCKET_NUM);
            for (int bucketIndex = 0; bucketIndex < BUCKET_NUM; bucketIndex++) {
                buckets.add(new HashSet<Integer>());
            }

            for (int columnIndex = 0; columnIndex < columnNum; columnIndex++) {
                int[] band = new int[BAND_SIZE];
                for (int rowIndex = startRow; rowIndex < startRow + BAND_SIZE; rowIndex++) {
                    band[rowIndex - startRow] = signatures[rowIndex][columnIndex];
                }
                int bandHash = Arrays.hashCode(band);
                int bucketIndex = (bandHash & 0x7FFFFFFF) % BUCKET_NUM;
                Set<Integer> bucket = buckets.get(bucketIndex);
                for (Integer bucketColumnIndex : bucket) {
                    pairs.add(new Pair<>(bucketColumnIndex, columnIndex));
                }
                bucket.add(columnIndex);
            }
        }
        return pairs;
    }

    public static void main(String[] args) throws IOException, URISyntaxException {
        assert BAND_NUM * BAND_SIZE == SIGNATURE_LENGTH;
        final Path path = new Path("/user/hadoop/tweets1-large.csv");

        List<String> lines = readCSV(path);
        System.out.println("CSV read");

        final List<Set<Integer>> tokenColumns = shingling(lines);
        final Set<Integer> universe = getTokenUniverse(tokenColumns);
        System.out.println("Shingling done, columns: " + tokenColumns.size() +", universe size: " + universe.size());

        final int[][] signatures = minhashing(tokenColumns, universe);
        System.out.println("Minhashing done");

        int columnNum = tokenColumns.size();
        final Set<Pair<Integer>> similarPairs = localitySensitiveHashing(signatures, columnNum);

        System.out.println("Locality Sensitive Hashing done, found pairs: "
                + similarPairs.size() + "/" + (columnNum * (columnNum - 1) / 2));

        for (Pair p : similarPairs) {
            System.out.println(p);
            int index1 = (int) p.first();
            int index2 = (int) p.second();
            System.out.println(lines.get(index1));
            System.out.println(lines.get(index2));
            Set<Integer> intersection = new HashSet<>(tokenColumns.get(index1));
            Set<Integer> column = tokenColumns.get(index2);
            intersection.retainAll(column);
            System.out.println(intersection.size());
        }
    }
}
