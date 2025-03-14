package nl.tue.bdm;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Random;

public class CountMinSketch implements Serializable {
  private int[][] data;
  private static int rows;
  private static int width;

  // hash function constants
  private static int[] a;
  private static int[] b;
  private static int[] p;

  public CountMinSketch() {
    data = new int[rows][width];
  }

  public static void init(double eps, double conf) {
    rows = (int) Math.ceil(2 / eps);
    width = (int) Math.ceil(-Math.log(1 - conf) / Math.log(2)) * 1000;
    initHashFunctions(rows);
  }

  public static void initHashFunctions(int rows) {
    a = new int[rows];
    b = new int[rows];
    p = new int[rows];

    Random rand = new Random();
    for (int i = 0; i < rows; i++) {
      a[i] = rand.nextInt(Integer.MAX_VALUE);
      b[i] = rand.nextInt(Integer.MAX_VALUE);
      p[i] = BigInteger.probablePrime(31, rand).intValue();
    }
  }

  public static CountMinSketch merge(CountMinSketch sketch1, CountMinSketch sketch2) {
    CountMinSketch result = new CountMinSketch();

    for (int i = 0; i < rows; i++) {
      for (int j = 0; j < width; j++) {
        result.data[i][j] = sketch1.data[i][j] + sketch2.data[i][j];
      }
    }
    return result;
  }

  public void add(int value) {
    for (int i = 0; i < rows; i++) {
      int hash = hash(value, i);
      data[i][hash]++;
    }
  }

  public int getFreq(int value) {
    int min = Integer.MAX_VALUE;
    for (int i = 0; i < rows; i++) {
      int hash = hash(value, i);
      if (data[i][hash] < min)
        min = data[i][hash];
    }
    return min;
  }

  private int hash(int value, int row) {
    // h(x) = ((a * x + b) % p) % width

    long intermediate = (long) a[row] * value + b[row]; // prevent overflow with long
    return (int) ((intermediate % p[row] + p[row]) % p[row] % width);

  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("CountMinSketch(rows=").append(rows).append(", width=").append(width).append(")\n");

    // Print data array
    sb.append("Data:\n");
    for (int i = 0; i < rows; i++) {
      sb.append("Row ").append(i).append(": ");
      for (int j = 0; j < width; j++) {
        sb.append(data[i][j]).append(" ");
      }
      sb.append("\n");
    }

    // Print hash function constants
    sb.append("Hash Function Constants:\n");
    for (int i = 0; i < rows; i++) {
      sb.append("Row ").append(i).append(": a=").append(a[i])
          .append(", b=").append(b[i])
          .append(", p=").append(p[i]).append("\n");
    }

    return sb.toString();
  }
}
