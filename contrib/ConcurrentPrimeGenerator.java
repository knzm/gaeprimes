import java.math.BigInteger;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

class ConcurrentPrimeGenerator implements Runnable {
    private static BlockingQueue<BigInteger> sieve;
    private static BlockingQueue<BigInteger> primes;
    private static BigInteger sieveMax;
    private static final BigInteger sentinel = BigInteger.valueOf(0);

    private static final long sieveSize = 256;
    private static final long limitTime = 2000;
    private static final int threadSize = 100;
    private static final int waitTime = 200;

    private static volatile long primeNumbers = 0;

    public BigInteger findPrime() {
        BigInteger newPrime = null;

        try {
            BigInteger p;
            synchronized (primes) {
                p = primes.take();
                primes.put(p);
                if (p.equals(sentinel)) {
                    newPrime = sieve.take();
                    primes.put(newPrime);
                    p = newPrime;
                    primeNumbers++;
                }
            }

            BigInteger max;
            synchronized (sieve) {
                max = sieveMax;
            }

            BigInteger n = p.add(p);
            if (n.compareTo(max) > 0) {
                long supp = n.subtract(max).longValue();
                if (supp > sieveSize)
                    supp = sieveSize;
                while (supp > 0) {
                    synchronized (sieve) {
                        sieve.put(sieveMax);
                        sieveMax = sieveMax.add(BigInteger.valueOf(1));
                    }
                    supp--;
                }
            } else {
                while (n.compareTo(max) < 0) {
                    sieve.remove(n);
                    n = n.add(p);
                }
            }

        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }

        return newPrime;
    }

    public void run() {
        // System.out.println(Thread.currentThread() + " Thread start.");

        long startTime = System.currentTimeMillis();
        BigInteger newPrime;
        int n = 0;
        while (true) {
            // System.out.println(Thread.currentThread() + " Loop in " + n);
            newPrime = findPrime();
            if (newPrime != null) break;
            long elapsedTime = System.currentTimeMillis() - startTime;
            n += 1;
            if ((double)elapsedTime / n * (n + 1) > limitTime)
                break;
        }

        if (newPrime != null) {
            System.out.println(Thread.currentThread() + " NEW PRIME FOUND: " + newPrime);
        } else {
            System.out.println(Thread.currentThread() + " New prime not found.");
        }
    }

    public static void main(String[] args) {
        sieve = new LinkedBlockingQueue<BigInteger>();
        primes = new LinkedBlockingQueue<BigInteger>();

        try {
            for (long i = 2; i < sieveSize; i++) {
                sieve.put(BigInteger.valueOf(i));
            }
            sieveMax = BigInteger.valueOf(sieveSize);

            primes.put(sentinel);

            ExecutorService pool = Executors.newFixedThreadPool(threadSize);
            Random random = new Random();

            long count = 0;
            while (true) {
                pool.execute(new ConcurrentPrimeGenerator());
                Thread.sleep((long)(random.nextInt(waitTime)));

                if (count % 100 == 0) {
                    // Check integrity
                    synchronized (primes) {
                        if (primes.size() - 1 != primeNumbers) {
                            throw new RuntimeException("Race condition detected");
                        }
                    }
                }
                count++;
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }
}
