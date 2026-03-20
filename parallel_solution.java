import java.util.BitSet;
import java.util.Collections;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.Semaphore;
import java.util.concurrent.CyclicBarrier;

class parallel_solution {

    static class parallel_sieve {

        long root;
        BitSet bs;
        long n;
        int thread_number;

        //Use cyclicBarrier for synchronization, + 1 is added to include main thread
        //so it waits as well
        CyclicBarrier barrier;

        //When finding chunk_sizes - subract amount of numbers that are < root
        //(since we know the primes in this range and don't have to look through
        //them)? Should include this range - ex. to cross out 9 for n = 100
        int chunk_size;
    
        Thread[] thread_list;

        public parallel_sieve(int n, int k){
            root = (long) Math.sqrt(n);
            //CHANGE TO BE: bs = new BitSet(n);
            bs = new BitSet(((int) n / 2) + 1); //false = prime. Only need half of n due to only needing odd numbers. The + 1 accounts for 2
            this.n = n;
            thread_number = k;
            barrier = new CyclicBarrier(thread_number + 1);
            chunk_size = (int) ((n + thread_number - 1) / thread_number);
            thread_list = new Thread[thread_number];
        }

        //LOOP THROUGH bs (CHANGE bs to include even numbers, must still increment by 2 for each iteration when this is done)
        //HAVE ANOTHER IF-CHECK BEFORE (isPrime(i)){ THAT CHECKS IF NEXT bs-element is false (aka. has not been crossed out yet)
            //EVEN FASTER SOLUTION - PERHAPS THIS CHECK IS NOT NECESSARY, MAYBE YOU CAN SET i to be nextClearBit() for each iteration in for-loop?
        //ONCE WE HAVE CONFIRMATION THAT i IS A PRIME BY USING isPrime(i), RUN THREAD SETUP TO CROSS OUT p*p, p*p+2, etc. for this prime
        public int nextPrime(int prev) {
            for (int i = prev + 2; i <= root; i += 2){
                if (isPrime(i)){
                    return i;
                }
            }
            return -1;
        }

        public boolean isPrime(long num) {
            long index = num / 2;
            return !bs.get((int) index);
        }

        //Delete if not used
        // public void traverse(long prime) {
        //     for (int i = prime * prime; i <= root; i += prime * 2){
        //         mark(i);
        //     }
        // }

        // //Delete if not used
        // public void mark(long num) {
        //     long index = num / 2;
        //     bs.set((int) index);
        // }

        public long[] find_primes(){
            
            bs.set(0); //Sets 1 as composite, we implicity set 2 as prime number

            int prime = nextPrime(1);

            //Find all prime numbers < root before parallelization?
            while (prime != -1) { //Find all primes that are less than root
                //traverse(prime);
                prime = nextPrime(prime);
            }

            int prime_index = bs.nextClearBit(1);

            BitSet cloned_bs;

            //Loop through all primes in bs that are < root and that have not been
            //crossed out by preceeding threads (aka. set to be true):
            //NOT NEEDED WITH NEW SETUP?
            while (prime_index < bs.size() && (prime_index * 2 + 1) <= root) {

                //Get acutal number instead of index
                int current_prime = (prime_index * 2 + 1);

                //Loop through thread list
                for (int i = 0; i < thread_list.length; i++){

                    int start = i * chunk_size;
                    int end = (int) Math.min(start + chunk_size, n);

                    // Find first prime that is actually inside the range of the chunk
                    int squared = (int) current_prime * current_prime;
                    int actual_start;
                    
                    if (squared >= start) {
                        actual_start = squared;
                    } else {
                        int k = (start + current_prime - 1) / current_prime;
                        actual_start = k * current_prime;
                        if (actual_start % 2 == 0) {
                            actual_start += current_prime;
                        }
                    }

                    //Give each thread a bitset, merge each copy of bs bitsets together 
                    //original bs with or() - does not have to be synchronized, since the
                    //same numbers won't be crossed by more than one thread. 
                    cloned_bs = (BitSet) bs.clone();

                    thread_list[i] = new Thread(new multiplications(
                        bs,
                        cloned_bs,
                        barrier,
                        actual_start,
                        end,
                        current_prime
                    ));

                    thread_list[i].start();
                }

                try {
                    barrier.await(); //so main thread waits as well
                }
                catch (InterruptedException | BrokenBarrierException a) {

                }

                //Don't have to check if prime_index has been crossed out by previous
                //threads, as it naturally would not be fetched by bs.nextClearBit if
                //it was set to false
                prime_index = bs.nextClearBit(prime_index + 1);
            }

            //Make new list based on how many non-prime numbers we found
            long[] all_primes = new long[(int) n - bs.cardinality() + 1];
    
            // First prime is always 2
            all_primes[0] = 2;
            int counter = 1;
            
            // Finds all odd primes (bits that are NOT set)
            int index = bs.nextClearBit(1); // Start from index 1 (number 3)
    
            while (index < bs.size() && (index * 2 + 1) <= n) {
                all_primes[counter] = (index * 2 + 1);
                index = bs.nextClearBit(index + 1);
                counter++;
            }
            
            long[] result = new long[counter];
            System.arraycopy(all_primes, 0, result, 0, counter);

            return result;
        }

    }

    public static class multiplications implements Runnable {
        BitSet bs;
        BitSet cloned_bs;
        CyclicBarrier barrier;
        int start;
        int end;
        int prime;
        public multiplications(BitSet bs, BitSet cloned_bs, CyclicBarrier barrier, int start, int end, int prime){
            this.bs = bs;
            this.cloned_bs = cloned_bs;
            this.barrier = barrier;
            this.start = start;
            this.end = end;
            this.prime = prime;
        }

        @Override
        public void run(){

            try {

                //Follows the traverse method from precode
                for (int i = start; i < end; i += prime * 2){
                    cloned_bs.set(i / 2);
                }

                bs.or(cloned_bs); //Updates bs with numbers crossed out by one thread

                barrier.await(); //barrier waits here

            }
            catch (InterruptedException | BrokenBarrierException e) {
                
            }

        }

    }

    public static void write_to_file(String name, int n, double runtime){

        String line = n +  " " + runtime;

        String file_name = String.format("./%d_medians.txt", name);

        try {
            FileWriter fileWriter = new FileWriter(file_name, true); 
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
            bufferedWriter.write(line);
            bufferedWriter.newLine();
            bufferedWriter.close();
        } 
        catch (IOException e) {

        }

    }

    public static void main (String[] args){

        int n;

        try {
            n = Integer.parseInt(args[0]);
        if (n <= 16)
            throw new Exception();
        }
        catch (Exception e) {
            System.out.println("Correct use of program is: " +
            "java SieveOfEratosthenes <n> where <n> is a positive integer and larger than or equals to 16.");
            return;
        }

        int k = Integer.parseInt(args[1]);

        if (k == 0){
            k = Runtime.getRuntime().availableProcessors(); //use all available threads
        }

        double[] sieve_runs = new double[7];
        double[] factorization_runs = new double[7];

        //WRAP EVERYTHING UP UNTILL Oblig3Precode IN FOR LOOP THAT RUNS 7 TIMES
        //for (int index = 0; index < 7; index++){

        //Parallel sieve of Eratosthenes:
        //-------------------------------------------------

        long start_time_1 = System.nanoTime(); //TIME START
        ////////////////////////////////////////////////////

        parallel_sieve seq_s = new parallel_sieve(n, k);

        long[] all_primes = seq_s.find_primes();

        ////////////////////////////////////////////////////
        long end_time_1 = System.nanoTime(); //TIME END
        double sieve_time = (end_time_1 - start_time_1) / 1e6;

        System.out.println("Runtime of parallel sieve: " + sieve_time);

        //PRINT ALL PRIMES FOUND:
        // for (int i = 0; i < all_primes.length; i++){
        //     System.out.println(all_primes[i]);
        // }

        //Parallel factorization:
        //-------------------------------------------------

        long start_time_2 = System.nanoTime(); //TIME START
        ///////////////////////////////////////////////////

        long prevent_overflow = (long) n * n;

        long M = prevent_overflow - 1;

        //k as argument
        parallel_factorization sf = new parallel_factorization(k);

        HashMap<Long, List<Long>> factorizations = new HashMap<Long, List<Long>>();

        for (int i = 100; i > 0; i--){

            //call factorization method and add it to factorizations-list
            factorizations.put(Long.valueOf(M), sf.factorize(M, all_primes));

            M--;
        }

        ////////////////////////////////////////////////////
        long end_time_2 = System.nanoTime(); //TIME END
        double factorization_time = (end_time_2 - start_time_2) / 1e6;

        System.out.println("Runtime of parallel prime factorization: " + factorization_time);

        // sieve_runs[i] = sieve_time;
        // factorization_runs[i] = factorization_time;
        //} //End of 7 runs

        //Find median values and print to respective files
        // Arrays.sort(sieve_runs);
        // Arrays.sort(factorization_runs);

        // median_sieve = sieve_runs[(sieve_runs.length - 1) / 2];
        // median_factorization = factorization_runs[(factorization_runs.length - 1) / 2];

        // write_to_file("parallel_sieves", n, median_sieve);
        // write_to_file("parallel_factorizations", n, median_factorization);

        //Checking results of factorizations against precode:
        Oblig3Precode precode = new Oblig3Precode(n);
        
        M++;
        for (int i = 100; i > 0; i--){
            System.out.print(M + " ");
            System.out.println(factorizations.get(M));
            // for (int factor : factorizations.get(M)){
            //     precode.addFactor(M, factor);
            // }
            M++;
        }

        // precode.writeFactors();

    }

    static class parallel_factorization {

        int thread_number;

        public parallel_factorization(int k){
            thread_number = k;
        }

        public List<Long> factorize(long M, long[] all_primes){

            long limit = (long) Math.sqrt(M); //use all numbers from all_primes that are less than limit

            int max_index = 0;

            //CAN max_index BE REPLACED WITH JUST all_primes.length ???
            //So we only go thorugh primes that are less than or equals to the square root of M
            while (max_index < all_primes.length && all_primes[max_index] < limit) {
                max_index++;
            }

            AtomicLong M_rest = new AtomicLong(M);

            //CAN synchronizedList BE USED??? THIS IS WAY FASTER THAN USING BLOCKING TO CREATE A CRITICAL SECTION
            //FOR ACCESSING LIST
            List<Long> factorizations = Collections.synchronizedList(new ArrayList<>());
            //Create two semaphores?
            //Semaphore access_factorizations = new Semaphore(1);
            //Semaphore access_M_rest = new Semaphore(1);

            long chunk_size = (max_index + thread_number - 1) / thread_number;

            CountDownLatch latch = new CountDownLatch(thread_number);

            Thread[] thread_list = new Thread[thread_number];

            try {
                for (int i = 0; i < thread_number; i++) {
                    
                    long start = i * chunk_size;
                    long end = Math.min(start + chunk_size, max_index);

                    //If a thread's entire chunk goes beyond boundary of prime indexes we need to check
                    if (start >= max_index) {
                        latch.countDown();
                        continue;
                    }

                    //call run in thread class and decrement latch once finished
                    thread_list[i] = new Thread(new factorizer(
                        factorizations,
                        all_primes,
                        start,
                        end,
                        M_rest,
                        latch
                    ));

                    thread_list[i].start();

                }
                latch.await();
            }
            catch (InterruptedException ie){

            }

            long any_remaining = M_rest.get();
            if (any_remaining > 1) {
                factorizations.add(any_remaining);
            }

            return factorizations;
        }

    }

    static class factorizer implements Runnable {
        List<Long> factorizations;
        long[] all_primes;
        long start;
        long end;
        AtomicLong M_rest;
        CountDownLatch latch;
        public factorizer(List<Long> factorizations, long[] all_primes, long start, long end, AtomicLong M_rest, CountDownLatch latch){
            this.factorizations = factorizations;
            this.all_primes = all_primes;
            this.start = start;
            this.end = end;
            this.M_rest = M_rest;
            this.latch = latch;
        }

        @Override
        public void run(){
            
            for (int i = (int) start; i < end; i++) { //Check primes within chunk

                long prime = all_primes[i];
                
                //Continue trying to divide this prime
                while (true) {

                    long current = M_rest.get();

                    if (current == 1) break;
                    
                    if (current % prime == 0) {
                        //Atomically check if M_rest has changed
                        if (M_rest.compareAndSet(current, current / prime)) {


                            factorizations.add(prime);
                        }
                    } 
                    else {
                    
                        break; //This prime doesn't divide anymore

                    }
                }
            }
            latch.countDown();
        }

    }

}