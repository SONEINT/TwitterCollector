package edu.isi.twitter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import sun.misc.BASE64Encoder;
import edu.isi.twitter.io.IO;
import edu.isi.util.Log;

public abstract class TwitterBase {
	protected TwitterConfig config = null;
	protected String rootTmpDir = ".";
	protected Log log = null;

	int s_lines = 0;
	int s_numFiles = 0;
	int respCode = -1;
	BufferedWriter tweetBuffer = null;
	HttpURLConnection tConn = null;

	Thread listener = null;
	boolean keepRunning = true;
	int lastWait = 0;

	IO io = null;

	public int fileLimit() {
		return config.getInt(TwitterConfig.LIMIT, 10000);
	}

	public int linesRead() {
		return s_lines;
	}

	public int filesWritten() {
		return s_numFiles;
	}

	public void shutdown() {
		keepRunning = false;
		if (listener != null) {
			final int THREAD_JOIN = config
					.getInt(TwitterConfig.THREAD_JOIN, 15);
			final int THREAD_DIE = config.getInt(TwitterConfig.THREAD_DIE, 10);

			// wait up to THREAD_DIE seconds in 100 millisecond intervals
			// for thread to die by itself
			final int wt = THREAD_DIE * 10;
			for (int i = 0; listener.isAlive() && i < wt; i++) {
				try {
					Thread.sleep(100); // sleep a few seconds to wait for the
										// thread to stop by itself and copy
										// over any files
				} catch (Exception ex) {
				}
			}

			try {
				if (listener.isAlive())
					listener.interrupt();
				// wait up to THREAD_JOIN seconds for thread to die by itself
				listener.join(THREAD_JOIN * 1000);
			} catch (Exception e) {
				log.plog("destroy() ERROR: " + e.getMessage());
				e.printStackTrace(System.err);
			}
			// let's try one last time to save any opened tweet file
			io.closeTweetFile();
			s_numFiles++;
		}
	}

	public void init() {
		Log.setDefaultName(getName());
		log = log.getDefault();

		loadConfig();

		io = new IO(this);
	}

	// Where to store temporary files
	public void setTmpRoot(String root) {
		rootTmpDir = root;
	}

	// Where to store temporary files
	public String getTmpRoot() {
		return rootTmpDir;
	}

	// Where to store final files
	public String getRoot() {
		return config.getString(TwitterConfig.ROOT, "/");
	}

	/**
	 * override if you want to have custom default values or non-standard
	 * values. Remember to first call super.loadConfig()
	 **/
	void loadConfig() {
		config = TwitterConfig.getConfig(getName());
	}

	public abstract String getName();

	URL getTwitterURL() throws MalformedURLException {
		return new URL(config.get(TwitterConfig.URL));
	}

	String getAuthentication() {
		return config.get(TwitterConfig.USER) + ":"
				+ config.get(TwitterConfig.PSWD);
	}

	void loadListsToFollow() {
	}

	String getURLParams() throws UnsupportedEncodingException {
		return null;
	}

	BufferedReader connect() {
		log.plog("Connecting to twitter...");
		try {
			final int TIMEOUT = config.getInt(TwitterConfig.TIMEOUT, 60000);
			final URL tUrl = getTwitterURL();
			tConn = (HttpURLConnection) tUrl.openConnection();
			final String authorizationString = "Basic "
					+ new BASE64Encoder()
							.encode(getAuthentication().getBytes());
			tConn.setRequestProperty("Authorization", authorizationString);
			tConn.setReadTimeout(TIMEOUT);

			String urlParams = getURLParams();
			if (urlParams != null && urlParams.length() > 0) {
				log.plog("Got URL parameters and doing a 'POST'");
				log.plog("URL parameterstring: " + urlParams);
				tConn.setRequestMethod("POST");
				tConn.setRequestProperty("Content-Type",
						"application/x-www-form-urlencoded");
				tConn.setUseCaches(false);
				tConn.setDoInput(true);
				tConn.setDoOutput(true);

				DataOutputStream wr = new DataOutputStream(
						tConn.getOutputStream());
				wr.writeBytes(urlParams);
				wr.flush();
				wr.close();
			}
		} catch (Exception ioe) {
			log.error("Could not connect to Twitter: " + ioe.getMessage(), ioe);
			tConn = null;
		}

		respCode = -1;
		BufferedReader in = null;
		if (tConn != null) {
			try {
				in = new BufferedReader(new InputStreamReader(
						tConn.getInputStream()));
				respCode = tConn.getResponseCode();
			} catch (IOException ioe) {
				log.error("Error opening input stream: " + ioe.getMessage(),
						ioe);
				in = null;
			}
			log.info("Got input stream.  Response code=" + respCode);
		}
		return in;
	}

	void waitForReconnect() {
		if (!keepRunning)
			return;

		if (lastWait == 0)
			lastWait = 1000;
		final long time = lastWait;
		lastWait *= 2;
		if (lastWait > 240000)
			lastWait = 240000;
		log.plog("Reconnecting to twitter in " + (int) (time / 1000)
				+ " seconds.");

		// if it has been less than WAIT time since last connect, wait
		// WAIT time. Otherwise wait 5 seconds before trying to reconnect.
		try {
			Thread.sleep(time);
		} catch (Exception ex) {
		}
	}

	void processStream(final BufferedReader br) throws IOException {
		final int limit = fileLimit();
		String inputLine;
		log.plog("Reading input...");
		tweetBuffer = io.openTweetFile();
		s_lines = 0;
		while (keepRunning && (inputLine = br.readLine()) != null) {
			tweetBuffer.write(inputLine);
			tweetBuffer.newLine();
			tweetBuffer.flush();
			s_lines++;

			if (s_lines % limit == 0) {
				io.closeTweetFile();
				tweetBuffer = io.openTweetFile();
				s_lines = 0;
			}
		}
		br.close();
	}

	void start() {
		log.plog("Starting query thread...");
		listener = new Thread() {
			public void run() {
				lastWait = 0;
				while (keepRunning) {
					final BufferedReader br = connect();
					boolean twitterError = (br != null && respCode >= 400);
					if (!twitterError) {
						lastWait = 0;
						try {
							processStream(br);
						} catch (Exception e) {
							if (keepRunning) {
								System.err.println("Error: " + e.getMessage());
								log.error(
										"processStream() ERROR: "
												+ e.getMessage(), e);
								twitterError = true;
							}
						}
					}

					if (tConn != null) {
						try {
							tConn.disconnect();
						} catch (Exception ex) {
							twitterError = true;
							log.error(
									"start(): Error disconnecting: "
											+ ex.getMessage(), ex);
						}
						tConn = null;
					}

					io.closeTweetFile();
					if (twitterError)
						waitForReconnect();
				}
			}
		};
		listener.start();
	}

	protected static void test(final TwitterBase t) {
		t.init();
		System.out.println("---------------------");
		System.out.println("Name:" + t.getName());
		System.out.println("---------------------");
		System.out.println("Config:");
		for (String k : t.config) {
			System.out.println(k + ":" + t.config.get(k));
		}
		System.out.println("---------------------");
		try {
			System.out.println("url=" + t.getTwitterURL());
		} catch (Exception ex) {
			System.out.println("getTwitterURL() threw an exception: "
					+ ex.getMessage());
			ex.printStackTrace(System.out);
		}
		System.out.println("filelimit=" + t.fileLimit());
		System.out.println("linesRead=" + t.linesRead());
		System.out.println("filesWritten=" + t.filesWritten());
		System.out.println("tmpRoot=" + t.getTmpRoot());
		System.out.println("root=" + t.getRoot());
		System.out.println("authentication=" + t.getAuthentication());
		try {
			System.out.println("urlparams=" + t.getURLParams());
		} catch (Exception ex) {
			System.out.println("urlparams() threw an exception: "
					+ ex.getMessage());
			ex.printStackTrace(System.out);
		}
	}
}
