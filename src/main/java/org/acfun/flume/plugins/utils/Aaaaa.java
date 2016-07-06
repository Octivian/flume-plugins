package org.acfun.flume.plugins.utils;

public class Aaaaa {

	public static void main(String[] args) {
		
		Node node5 = new Node(5, null);
		Node node4 = new Node(4, node5);
		Node node3 = new Node(3, node4);
		Node node2 = new Node(2, node3);
		Node node1 = new Node(1, node2);
		Node current = node1;
		Node pre = null;
		Node next ;
		while(current!=null){
			next = current.next;
			current.next = pre;
			pre = current;
			current = next;
		}
		
	}
	
	public static class Node{
		int content;
		Node next;
		public Node(int content,Node next) {
			this.content=content;
			this.next=next;
		}
		
		
	}
}
