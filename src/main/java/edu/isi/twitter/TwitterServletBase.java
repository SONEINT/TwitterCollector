package edu.isi.twitter;

import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@SuppressWarnings("serial")
public abstract class TwitterServletBase extends HttpServlet {
	protected TwitterBase twitterReader = null;

	protected abstract TwitterBase getTwitterReader();

	@Override
	public void destroy() {
		if (twitterReader == null)
			return;
		twitterReader.shutdown();
	}

	@Override
	public void init() {
		twitterReader = getTwitterReader();
		twitterReader.setTmpRoot(getServletContext().getRealPath("/"));
		twitterReader.init();
		twitterReader.start();
	}

	String getH1() {
		return "Processed ";
	}

	@Override
	protected void doGet(HttpServletRequest a_req, HttpServletResponse a_resp)
			throws IOException {
		a_resp.setContentType("text/html");
		final PrintWriter out = a_resp.getWriter();
		final int lr = (twitterReader == null ? -1 : twitterReader.linesRead());
		final int fw = (twitterReader == null ? -1 : twitterReader
				.filesWritten());
		out.println("<h1>" + getH1() + " " + lr + " messages, created " + fw
				+ " files</h1>");
	}
}
