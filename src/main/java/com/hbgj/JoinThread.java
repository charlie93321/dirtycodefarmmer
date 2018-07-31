package com.hbgj;

public class JoinThread {

    public static void main(String[] args) throws InterruptedException {

        Thread t1=new Thread(new Runnable() {
            @Override
            public void run() {
                  int count=1;
                  while(count++<10){
                      try {
                          Thread.sleep(1000);
                      } catch (InterruptedException e) {
                          e.printStackTrace();
                      }

                      System.out.println("I am Working -------- ");
                  }
            }
        });

        Thread t2=new Thread(new Runnable() {
            @Override
            public void run() {
                int count=1;
                while(count++<5){
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    System.out.println("I am  t2  -------- ");
                }
            }
        });


        t1.start();
        t1.join(5000l);
        t2.start();
        t2.join();

        System.out.println("program is end-----------");

    }
}
