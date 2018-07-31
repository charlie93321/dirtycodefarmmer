package com.hbgj;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

public class SemaphoreTest {

    public static void main(String[] args){

        final Semaphore semaphore=new Semaphore(5);

        ExecutorService service=Executors.newFixedThreadPool(20);

        for (int i = 0; i <20; i++) {
            service.execute(new Runnable() {
                @Override
                public void run() {

                    try {
                        semaphore.acquire();


                          Thread.sleep(1000);
                        System.out.println(Thread.currentThread().getId()+" is working  ............. ");


                        semaphore.release();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        service.shutdown();
    }
}
