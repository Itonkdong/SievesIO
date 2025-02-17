import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class Result
{
    private final boolean[] isPrime;
    private final List<Integer> primes;
    private final long totalExecutionTime;
    private final long algorithmExecutionTime;

    public Result(boolean[] isPrime, long startTotalTime, long statAlgTime, long endAlgTime, boolean makePrimeList)
    {
        this.isPrime = isPrime;
        if (makePrimeList)
        {
            this.primes = toPrimesList(isPrime, 2, isPrime.length);
        }
        else
        {
            this.primes = new ArrayList<>();
        }
        this.algorithmExecutionTime = endAlgTime - statAlgTime;
        this.totalExecutionTime = System.currentTimeMillis() - startTotalTime;
    }

    public Result(List<Integer> primes, int n, long startTotalTime, long statAlgTime, long endAlgTime)
    {
        boolean[] isPrime = new boolean[n + 1];
        Arrays.fill(isPrime, false);
        for (Integer prime : primes)
        {
            isPrime[prime] = true;
        }
        this.isPrime = isPrime;
        this.primes = primes;
        this.algorithmExecutionTime = endAlgTime - statAlgTime;
        this.totalExecutionTime = System.currentTimeMillis() - startTotalTime;
    }

    public Result(boolean[] isPrime, long startTotalTime, long totalAlgTime, boolean makePrimeList)
    {
        this.isPrime = isPrime;
        if (makePrimeList)
        {
            this.primes = toPrimesList(isPrime, 2, isPrime.length);
        }
        else
        {
            this.primes = new ArrayList<>();
        }
        this.algorithmExecutionTime = totalAlgTime;
        this.totalExecutionTime = System.currentTimeMillis() - startTotalTime;
    }

    public List<Integer> getPrimes()
    {
        return primes;
    }

    public long getTotalExecutionTime()
    {
        return totalExecutionTime;
    }

    public long getAlgorithmExecutionTime()
    {
        return algorithmExecutionTime;
    }

    public int getPrimeCount()
    {
        if (this.primes.isEmpty())
        {
            return this.countPrimes();
        }
        return this.primes.size();
    }

    public static List<Integer> toPrimesList(boolean[] isPrime, int start, int end)
    {
        List<Integer> primes = new ArrayList<>();
        for (int i = start; i < end; i++)
        {
            if (isPrime[i])
            {
                primes.add(i);
            }
        }
        return primes;
    }

    private int countPrimes()
    {
        int primes = 0;
        for (int i = 2; i < this.isPrime.length; i++)
        {
            if (this.isPrime[i])
            {
                primes++;
            }
        }
        return primes;
    }

    public void print(boolean parallel, int totalPrimes, boolean includeTotalTime, int numThreads)
    {
        System.out.println(parallel ? "Parallel" : "Sequential");

        if (parallel)
        {
            System.out.println("Threads: " + numThreads);
        }

        if (includeTotalTime)
        {
            System.out.println("Total time taken: " + this.totalExecutionTime + " ms.");

        }
        System.out.println("Algorithm execution time: " + this.algorithmExecutionTime  + " ms.");
        System.out.println("Found: " + this.getPrimeCount() + " out of: " + totalPrimes);
        System.out.println();
    }
}

enum SieveAlgorithm
{
    Eratosthenes,
    Euler
}


public class SievesIO
{

    public static Result sieveOfEratosthenes(int n)
    {
        long startTotalTime = System.currentTimeMillis();
        boolean[] isPrime = new boolean[n + 1];
        for (int i = 2; i <= n; i++)
        {
            isPrime[i] = true;
        }

        long startAlgTime = System.currentTimeMillis();
        for (int p = 2; p <= n; p++)
        {
            if (isPrime[p])
            {
                for (int i = p + p; i <= n; i += p)
                {
                    isPrime[i] = false;
                }
            }
        }
        long endAlgTime = System.currentTimeMillis();

        return new Result(isPrime, startTotalTime, startAlgTime, endAlgTime, false);
    }

    public static Result sieveOfEratosthenesParallel(int n, int numThreads) throws InterruptedException
    {
        long startTotalTime = System.currentTimeMillis();

        int markerLimit = (int) Math.ceil(Math.sqrt(n));
        boolean[] isPrime = new boolean[n + 1];
        Arrays.fill(isPrime, true);

        //================ Find the markers up to sqrt(n) sequentially==========
        long totalAlgTime = 0;
        long startAlgTime = System.currentTimeMillis();

        for (int p = 2; p <= markerLimit; p++)
        {
            if (isPrime[p])
            {
                for (int i = p + p; i <= markerLimit; i += p)
                {
                    isPrime[i] = false;
                }
            }
        }
        totalAlgTime += System.currentTimeMillis() - startAlgTime;
        //======================================================================

        //==================Preparing for the parallel part=====================
        List<Integer> markers = Result.toPrimesList(isPrime, 2, markerLimit);
        int segmentSize = (n - markerLimit) / numThreads + 1;
        //======================================================================

        //Process the segments in parallel
        startAlgTime = System.currentTimeMillis();
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        for (int i = 0; i < numThreads; i++)
        {
            final int start = (markerLimit + i * segmentSize) + 1;
            final int end = Math.min(markerLimit + (i + 1) * segmentSize, n);

            executor.submit(() -> processSegment(markers, start, end, isPrime));
        }
        executor.shutdown();
        executor.awaitTermination(100, TimeUnit.SECONDS);

        totalAlgTime += System.currentTimeMillis() - startAlgTime;

        return new Result(isPrime, startTotalTime, totalAlgTime, false);
    }

    public static Result sieveOfEuler(int n)
    {
        long startTotalTime = System.currentTimeMillis();
        long startAlgTime = System.currentTimeMillis();

        // Create a boolean array to mark composites
        boolean[] isComposite = new boolean[n + 1];
        List<Integer> primes = new ArrayList<>((int) (n / Math.log(n)) + 100);

        for (int i = 2; i <= n; i++)
        {
            if (!isComposite[i])
            {
                primes.add(i); // Add the prime to the list
            }

            // Mark multiples of the current number
            for (int prime : primes)
            {
                int composite = i * prime;
                if (composite > n) break; // Stop if the product exceeds n
                isComposite[composite] = true;
                if (i % prime == 0) break; // Stop further marking to ensure smallest prime factor
            }
        }

        long endAlgTime = System.currentTimeMillis();

        return new Result(primes, n, startTotalTime, startAlgTime, endAlgTime);
    }

    public static Result sieveOfEulerParallel(int n, int numThreads) throws InterruptedException
    {
        long startTotalTime = System.currentTimeMillis();

        int markerLimit = (int) Math.ceil(Math.sqrt(n));
        boolean[] isPrime = new boolean[n + 1];
        boolean[] isComposite = new boolean[n + 1];
        Arrays.fill(isPrime, true);
        List<Integer> primes = new ArrayList<>((int) (n / Math.log(n)) + 100);

        //================ Find the markers up to sqrt(n) sequentially==========
        long totalAlgTime = 0;
        long startAlgTime = System.currentTimeMillis();

        for (int i = 2; i <= markerLimit; i++)
        {
            if (!isComposite[i])
            {
                primes.add(i);
            }

            for (int prime : primes)
            {
                int composite = i * prime;
                if (composite > markerLimit) break; // Stop if the product exceeds n
                isComposite[composite] = true;
                isPrime[composite] = false;
                if (i % prime == 0) break; // Stop further marking to ensure smallest prime factor
            }
        }
        totalAlgTime += System.currentTimeMillis() - startAlgTime;
        //======================================================================

        //==================Preparing for the parallel part=====================
        List<Integer> markers = primes;
        int segmentSize = (n - markerLimit) / numThreads + 1;
        //======================================================================

        //Process the segments in parallel
        startAlgTime = System.currentTimeMillis();
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        for (int i = 0; i < numThreads; i++)
        {
            final int start = (markerLimit + i * segmentSize) + 1;
            final int end = Math.min(markerLimit + (i + 1) * segmentSize, n);
            executor.submit(() -> processSegment(markers, start, end, isPrime));
        }

        executor.shutdown();
        executor.awaitTermination(100, TimeUnit.SECONDS);

        totalAlgTime += System.currentTimeMillis() - startAlgTime;

        return new Result(isPrime, startTotalTime, totalAlgTime, false);
    }

    private static void processSegment(List<Integer> markers, int start, int end, boolean[] isPrime)
    {
        for (int marker : markers)
        {
            int firstMultiple = start + (marker - (start % marker)) % marker;
            for (int j = firstMultiple; j <= end; j += marker)
            {
                isPrime[j] = false;
            }
        }
    }

    public static void benchmark(String fileName) throws IOException, InterruptedException
    {
        FileWriter fileWriter = new FileWriter(fileName, true);
        fileWriter.write(String.format("Limit,Found,Total,Parallel,Threads,ExecTime\n"));

        int[] increaseByFactors = new int[]{10};
        for (int i = 0; i < increaseByFactors.length; ++i)
        {
            int increaseByFactor = increaseByFactors[i];
            for (int j = 100; j <= 1_000_000_000; j *= increaseByFactor)
            {
                System.out.printf("====== N  = %d ======%n", j);

                Result sequentalResult = sieveOfEuler(j);
                fileWriter.write(String.format("%d,%d,%d,False,nan,%d\n", j, sequentalResult.getPrimeCount(), sequentalResult.getPrimeCount(), sequentalResult.getAlgorithmExecutionTime()));
                sequentalResult.print(false, sequentalResult.getPrimeCount(), false, 1);
            }
        }

        int[] threads = new int[]{2, 3, 4, 8};
        for (int k = 0; k < threads.length; ++k)
        {
            int numThreads = threads[k];
            for (int i = 0; i < increaseByFactors.length; ++i)
            {
                int increaseByFactor = increaseByFactors[i];
                for (int j = 100; j <= 1_000_000_000; j *= increaseByFactor)
                {
                    System.out.printf("====== N  = %d ======%n", j);

                    Result parallelResult = sieveOfEulerParallel(j, numThreads);
                    parallelResult.print(true, parallelResult.getPrimeCount(), false, numThreads);
                    fileWriter.write(String.format("%d,%d,%d,True,%d,%d\n", j, parallelResult.getPrimeCount(), parallelResult.getPrimeCount(), numThreads, parallelResult.getAlgorithmExecutionTime()));
                }
            }

        }
        fileWriter.flush();
        fileWriter.close();
    }

    public static int getScientificNotation(int limit)
    {
        return (int) Arrays.stream(Integer.valueOf(limit)
                .toString()
                .split(""))
                .skip(1)
                .count();
    }

    public static void test(int start, int limit, int increaseFactor, int numThreads, SieveAlgorithm alg) throws InterruptedException, IOException
    {
        Runtime runtime = Runtime.getRuntime();

        long totalMemory = runtime.totalMemory();
        long maxMemory = runtime.maxMemory();
        long freeMemory = runtime.freeMemory();

        System.out.println("Total Memory (Heap Size): " + totalMemory / (1024 * 1024) + " MB");
        System.out.println("Max Memory (Max Heap Size): " + maxMemory / (1024 * 1024) + " MB");
        System.out.println("Free Memory: " + freeMemory / (1024 * 1024) + " MB");

        System.out.println("Alg = " + alg.toString());


        for (int i = start; i <= limit; i *= increaseFactor)
        {
            System.out.printf("====== Limit (N) = 10^%d ======%n", getScientificNotation(i));

            if (alg == SieveAlgorithm.Euler)
            {
                Result sequentalResult = sieveOfEuler(i);
                sequentalResult.print(false, sequentalResult.getPrimeCount(), false, 1);


                Result parallelResult = sieveOfEulerParallel(i, numThreads);
                parallelResult.print(true, sequentalResult.getPrimeCount(), false, numThreads);
            }
            else
            {
                Result sequentalResult = sieveOfEratosthenes(i);
                sequentalResult.print(false, sequentalResult.getPrimeCount(), false, 1);


                Result parallelResult = sieveOfEratosthenesParallel(i, numThreads);
                parallelResult.print(true, sequentalResult.getPrimeCount(), false, numThreads);

            }
        }
    }


    public static void main(String[] args) throws InterruptedException, IOException
    {
        test(100, 1_000_000_000, 10, 2, SieveAlgorithm.Eratosthenes);
    }
}
