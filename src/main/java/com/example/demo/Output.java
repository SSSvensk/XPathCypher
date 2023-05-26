package com.example.demo;

public class Output {
	private String warningText = "WARN";
	private String separator = ": ";
	private String foundText = "found: ";
	private String spacing = "      ";
	
	public Output() {
		
	}
	
	public void printWarning(String msg, String details) {
		System.out.println(this.warningText + this.separator + msg);
		System.out.println(this.spacing + this.foundText + details);
		System.out.println();
	}
	
	public void printWarning(String msg) {
		System.out.println(this.warningText + this.separator + msg);
		System.out.println();
	}
}