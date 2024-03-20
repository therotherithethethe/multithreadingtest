import com.github.javafaker.Faker;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class Main {

    private static final int NUM_THREADS = Runtime.getRuntime().availableProcessors();
    private static final String URL = "jdbc:h2:./data/db";
    //private static final String URL1 = "jdbc:h2:./data/db";
    private static final int NUM_RECORDS = 999984;

    public static void main(String[] args) throws SQLException {
        long last;
        long first = System.currentTimeMillis();
        countAvgSalarySThread();
        last = System.currentTimeMillis();
        long delta = last - first;
        System.out.println(delta);

    }

    static void multiThreadCreateWorkers() {
        String insertWorkersSql = "INSERT INTO workers (name, age, sex, EDUCATION, JOB_NAME, WORK_EXPERIENCE, SALARY) VALUES (?, ?, ?, ?, ?, ?, ?)";

        final int recordsPerThread = NUM_RECORDS / NUM_THREADS;

        try (Connection conn = DriverManager.getConnection(URL);
            PreparedStatement insertRecipeStmt = conn.prepareStatement(insertWorkersSql);
            ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);) {

            for (int i = 0; i < NUM_THREADS; i++) {
                int finalI = i;

                executor.submit(() -> {
                    try (Connection threadConn = DriverManager.getConnection(URL);
                        PreparedStatement threadStmt = threadConn.prepareStatement(insertWorkersSql)) {

                        int start = finalI * recordsPerThread + 1;
                        int end = (finalI + 1) * recordsPerThread;

                        for (int j = start; j <= end; j++) {
                            Faker threadFaker = new Faker(); //
                            threadStmt.setString(1, threadFaker.name().fullName());
                            threadStmt.setInt(2, threadFaker.number().numberBetween(18, 65));
                            threadStmt.setString(3, threadFaker.options().option("man", "woman"));
                            threadStmt.setString(4, threadFaker.educator().course());
                            threadStmt.setString(5, threadFaker.job().title());
                            threadStmt.setInt(6, threadFaker.number().numberBetween(0, 10));
                            threadStmt.setInt(7, threadFaker.number().numberBetween(300, 6000));
                            threadStmt.addBatch();

                            if (j % 1000 == 0) {
                                threadStmt.executeBatch();
                            }
                        }
                        threadStmt.executeBatch();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                });
            }
            executor.shutdown();
            while (!executor.isTerminated()) {
                // Wait for all threads to finish
            }
        }
        catch (SQLException ex) {

        }
    }
   /* static void countAvgSalary() {
        String avgSalarySql = "SELECT WORKERS.SALARY FROM WORKERS";
        final int recordsPerThread = NUM_RECORDS / NUM_THREADS;

        try(ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS)) {
            for (int i = 0; i < NUM_THREADS; i++) {
                int finalI = i;
                executor.submit(() -> {
                    try (Connection threadConn = DriverManager.getConnection(URL);
                        PreparedStatement threadStmt = threadConn.prepareStatement(avgSalarySql)) {
                        int start = finalI * recordsPerThread + 1;
                        int end = (finalI + 1) * recordsPerThread;

                        ResultSet rs = threadStmt.executeQuery();




                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                });
            }

        }
        catch (Exception ex) {

        }
    }*/
   static void countAvgSalaryMThread() {
       String avgSalarySql = "SELECT SUM(SALARY) AS PARTIAL_SUM FROM WORKERS LIMIT ? OFFSET ?";
       final int recordsPerThread = NUM_RECORDS / NUM_THREADS;
       AtomicLong totalSum = new AtomicLong(0);
       CountDownLatch latch = new CountDownLatch(NUM_THREADS);

       try (ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS)) {
           for (int i = 0; i < NUM_THREADS; i++) {
               int finalI = i;
               executor.submit(() -> {
                   try (Connection threadConn = DriverManager.getConnection(URL);
                       PreparedStatement threadStmt = threadConn.prepareStatement(avgSalarySql)) {
                       int offset = finalI * recordsPerThread;
                       threadStmt.setInt(1, recordsPerThread);
                       threadStmt.setInt(2, offset);

                       ResultSet rs = threadStmt.executeQuery();
                       if (rs.next()) {
                           long partialSum = rs.getLong("PARTIAL_SUM");
                           totalSum.addAndGet(partialSum);
                       }
                   } catch (SQLException e) {
                       e.printStackTrace();
                   } finally {
                       latch.countDown();
                   }
               });
           }

           latch.await();  // Wait for all threads to finish
           double averageSalary = (double) totalSum.get() / NUM_RECORDS;
           System.out.println("Average Salary: " + averageSalary);

       } catch (Exception ex) {
           ex.printStackTrace();
       }
   }
    static void countWorkersThatHaveMoreExperienceThenMThread() {
        String avgSalarySql = "SELECT * FROM WORKERS LIMIT ? OFFSET ?";
        final int recordsPerThread = NUM_RECORDS / NUM_THREADS;
        AtomicLong totalSum = new AtomicLong(0);
        CountDownLatch latch = new CountDownLatch(NUM_THREADS);

        try (ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS)) {
            for (int i = 0; i < NUM_THREADS; i++) {
                int finalI = i;
                executor.submit(() -> {
                    try (Connection threadConn = DriverManager.getConnection(URL);
                        PreparedStatement threadStmt = threadConn.prepareStatement(avgSalarySql)) {
                        int offset = finalI * recordsPerThread;
                        threadStmt.setInt(1, recordsPerThread);
                        threadStmt.setInt(2, offset);

                        ResultSet rs = threadStmt.executeQuery();
                        if (rs.next()) {
                            long partialSum = rs.getLong("PARTIAL_SUM");
                            totalSum.addAndGet(partialSum);
                        }
                    } catch (SQLException e) {
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                });
            }

            latch.await();  // Wait for all threads to finish
            double averageSalary = (double) totalSum.get() / NUM_RECORDS;
            System.out.println("Average Salary: " + averageSalary);

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    static void countAvgSalarySThread() {
        String avgSalarySql = "SELECT AVG(SALARY) AS AVERAGE_SALARY FROM WORKERS";

        try (Connection conn = DriverManager.getConnection(URL);
            PreparedStatement stmt = conn.prepareStatement(avgSalarySql)) {

            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                double averageSalary = rs.getDouble("AVERAGE_SALARY");
                System.out.println("Average Salary: " + averageSalary);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}