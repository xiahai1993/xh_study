package cn.spark.study.test;

import java.util.Scanner;
import java.util.Stack;

/**
 * 括号的全排序
 */
public class AllParenthesesSort {
    public static void main(String[] args) {
        Scanner sc =new Scanner(System.in);
        while(sc.hasNext()){
            int m =sc.nextInt();
            //Stack<String> s  =new Stack<String>();
            String n ="";
            generate(m , m, n);
        }
    }

    /**
     * 分析：
     *
     * 　　很经典的需要迭代来进行实现。
     *
     * 　　迭代的关键在于找到跳出迭代的条件，以及每次迭代的策略。
     *
     * 　　出口：
     *
     * 　　　　此题，迭代，每次只能画括号的一半，故而出口为，左边括号剩余和右边括号剩余均为0；
     * @param leftNum
     * @param rightNum
     * @param s
     */
    public static void generate(int leftNum,int rightNum,String s)
    {
        if (leftNum==0&&rightNum==0)
        {
            System.out.println(s);
        }

        if (leftNum>0)
        {
            generate(leftNum-1,rightNum,s+"(");
        }

        if (rightNum>0&&leftNum<rightNum)
        {
            generate(leftNum,rightNum-1,s+")");
        }
    }
}
