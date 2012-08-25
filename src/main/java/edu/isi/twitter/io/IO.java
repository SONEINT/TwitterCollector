package edu.isi.twitter.io;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.zip.GZIPOutputStream;

import edu.isi.twitter.TwitterBase;
import edu.isi.util.Log;

public class IO {
	public static final String pathSeparator = System
			.getProperty("file.separator");

	private TwitterBase twitter = null;
	private File tweetFile = null;
	private String finalFile = null;
	private String finalFolder = null;
	private BufferedWriter tweetBuffer = null;

	public IO(final TwitterBase twitterBase) {
		this.twitter = twitterBase;
	}

	/*
	 * Opens a local gzipstream to save the incoming tweets
	 */
	public BufferedWriter openTweetFile() {
		// for safety, let's close up any currently open stream
		closeTweetFile();

		final String parentDirectory = twitter.getTmpRoot();
		final String name = twitter.getName();

		Date now = new Date();
		SimpleDateFormat fileNameDate = new SimpleDateFormat(
				"yyyyMMdd'T'HHmmssSSS'Z'");
		SimpleDateFormat folderNameDate = new SimpleDateFormat("yyyy-MM-dd");

		finalFolder = folderNameDate.format(now);
		finalFile = fileNameDate.format(now) + ".txt.gz";

		String localfile = parentDirectory + pathSeparator + name + "."
				+ finalFile;

		try {
			// create the file, to write
			tweetFile = new File(localfile);
			FileOutputStream fos = new FileOutputStream(tweetFile);
			GZIPOutputStream gzfos = new GZIPOutputStream(fos);
			OutputStreamWriter osw = new OutputStreamWriter(gzfos, "UTF-8");
			tweetBuffer = new BufferedWriter(osw);
		} catch (Exception e) {
			e.printStackTrace(System.err);
		}
		return tweetBuffer;
	}

	/*
	 * closes the local gzipstream file and copies it over to the final
	 * destination
	 */
	public boolean closeTweetFile() {
		if (tweetBuffer == null || finalFile == null || finalFolder == null)
			return true;

		boolean closed = false;
		try {
			tweetBuffer.close();
			closed = true;
			tweetBuffer = null;

			final File tFile = tweetFile;
			final String fFold = finalFolder;
			final String fFile = finalFile;

			new Thread() {
				public void run() {
					copyTweetFile(tFile, fFold, fFile);
				}
			}.start();

			tweetFile = null;
			finalFolder = null;
			finalFile = null;

		} catch (Exception e) {
			Log.getDefault().plog(
					"ERROR: Failed to close " + tweetFile.getName() + ": "
							+ e.getMessage());
			e.printStackTrace(System.err);
		}

		return closed;
	}

	public void copyTweetFile(final File tweetFile, final String finalFolder,
			final String finalFile) {
		// create the final file, to write and copy over from the local
		// tweetFile
		final String rootDir = twitter.getRoot();
		final String fullPathName = rootDir + pathSeparator + finalFolder
				+ pathSeparator + finalFile;

		try {
			// first, if we don't have this folder, make it!
			File writeDir = new File(rootDir + pathSeparator + finalFolder);
			if (!writeDir.exists()) {
				boolean makeSuccess = writeDir.mkdir();
				if (!makeSuccess) {
					throw new Exception(
							"Could not create writing dir...FAILING...");
				}
			}

			File destFile = new File(fullPathName);
			if (!destFile.exists()) {
				destFile.createNewFile();
			}

			FileChannel source = null;
			FileChannel destination = null;
			try {
				source = new FileInputStream(tweetFile).getChannel();
				destination = new FileOutputStream(destFile).getChannel();
				destination.transferFrom(source, 0, source.size());
			} finally {
				if (source != null) {
					source.close();
				}
				if (destination != null) {
					destination.close();
				}
			}
			Log.getDefault().plog(
					"Copied " + tweetFile.getName() + " to " + fullPathName);
			tweetFile.delete();
		} catch (Exception e) {
			Log.getDefault().plog(
					"ERROR: Failed to copy " + tweetFile.getName() + " to "
							+ fullPathName + ": " + e.getMessage());
			e.printStackTrace(System.err);
		}
	}
}
