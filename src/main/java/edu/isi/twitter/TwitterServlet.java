package edu.isi.twitter;

@SuppressWarnings("serial")
public class TwitterServlet extends TwitterServletBase {
	@Override
	protected TwitterBase getTwitterReader() {
		return new TwitterHose();
	}
}
