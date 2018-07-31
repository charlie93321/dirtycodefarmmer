package com.hbgj

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong

/**
 * 原子性
 *
 *
 *
 */
class Test6{
    static AtomicLong count=new AtomicLong(0l)
    static  void   main(String[] args){

        int total=50
        def executorService= Executors.newFixedThreadPool(total)
        for (int i = 0; i <total ; i++) {
            executorService.execute(new Runnable() {
                @Override
                void run() {
                      count.getAndAdd(1l)
                      Thread.sleep(100)
                }
            })
        }

        executorService.shutdown()

        while(true) {

            if(count.get()>=total){
                println("count is final :  $count")
                break
            }else{
                println("count is $count")
            }
        }
    }

}


class Test7{
    static def count=0l
    static  void   main(String[] args){

        int total=50
        def executorService= Executors.newFixedThreadPool(total)
        for (int i = 0; i <total ; i++) {
            executorService.execute(new Runnable() {
                @Override
                void  run() {
                    synchronized (this) {

                        count++
                    }

                    Thread.sleep(100)
                }
            })
        }

        executorService.shutdown()

        while(true) {

            if(count>=total){
                println("count is final :  $count")
                break
            }else{
                println("count is $count")
            }
        }
    }

}


class Test8{
    def static   volatile  count=0l
    static  void   main(String[] args){

        int total=1
        def executorService= Executors.newFixedThreadPool(total)
        for (int i = 0; i <total ; i++) {
            executorService.execute(new Runnable() {
                @Override
                void  run() {
                         count++
                }
            })

        }

        executorService.shutdown()

       // while(true) {
            if (count >= 1)
                println("volatile is success !")
            else
                println("volatile is failure !")
      //  }


    }

}



