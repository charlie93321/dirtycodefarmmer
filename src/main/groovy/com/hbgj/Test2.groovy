package com.hbgj

println("groovy script running ")

/**
 *  Groovy提供多种内置数据类型。以下是在Groovy中定义的数据类型的列表 -
 *
 *     byte -这是用来表示字节值。例如2。
 *
 *     short -这是用来表示一个短整型。例如10。
 *
 *     int -这是用来表示整数。例如1234。
 *
 *     long -这是用来表示一个长整型。例如10000090。
 *
 *     float -这是用来表示32位浮点数。例如12.34。
 *
 *     double -这是用来表示64位浮点数，这些数字是有时可能需要的更长的十进制数表示。例如12.3456565。
 *
 *     char -这定义了单个字符文字。例如“A”。
 *
 *     Boolean -这表示一个布尔值，可以是true或false。
 *
 *     String -这些是以字符串的形式表示的文本。例如，“Hello World”的。
 */
int a = 2147483648
def b = 2147483648
if (b instanceof Integer){
    println("b is int")
}else{
    println("b is not  int")
}


def main(){
    println("call me ")
    return 10;
}

boolean flag=false

// Groovy定义正则表达式
def regex=/.*hello world.*/



//创建包含不同类型列表
def myList=["aaa",123,true]

List list=[1,2,3]
LinkedList list01=[1,2,3]
def list02=[1,2,3] as LinkedList








