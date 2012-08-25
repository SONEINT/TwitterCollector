package edu.isi.twitter;

@SuppressWarnings("serial")
public class FollowTwitsServlet extends TwitterServletBase {
	@Override
	protected TwitterBase getTwitterReader() {
		return new FollowTwits();
	}

	@Override
	String getH1() {
		return "FOLLOWING TWITTERERS</h1><h1>Processed";
	}
}
