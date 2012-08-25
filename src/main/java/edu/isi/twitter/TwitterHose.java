package edu.isi.twitter;

public class TwitterHose extends TwitterBase {
	@Override
	public String getName() {
		return "TwitterHose";
	}

	public static void main(String... args) {
		System.out.println("Testing TwitterHose()");
		TwitterBase t = new TwitterHose();
		test(t);
	}
}
