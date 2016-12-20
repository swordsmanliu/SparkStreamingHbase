package com.jacker.Kafka;

public class test {
 public static void main(String[] args) {
	 int x1 =1;
	 int n = 3;
	 int sum =0;
	 for (int i = 1; i <= n; i++) {

	      x1 = fun(i);

	      sum = sum + x1;

	    }

	 System.out.println(sum);
}
	public static int fun(int x){
		if(x > 2){
			return fun(x-1)+fun(x-2);
		}
		else{
			return 1;
		}
		
		
	}
}
