package edu.isi.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Log {
	public static final String pathSeparator = System
			.getProperty("file.separator");

	private static final DateFormat df = new SimpleDateFormat(
			"yyyy.MM.dd HH:mm:ss");

	private PrintWriter log = null;

	private static String name = "noname";
	private static Log def = null;

	private Log(final String name) {
		this.name = name;
	}

	public static void setDefaultName(final String name) {
		if (def == null)
			getDefault();
		def.name = name;
	}

	public static Log getDefault() {
		if (def == null)
			def = new Log(name);
		return def;
	}

	public static void setDefault(final Log log) {
		def = log;
	}

	public String getName() {
		return name;
	}

	public static Log getLog(final String name) {
		return new Log(name);
	}

	public void closeLog() {
		if (log != null)
			log.close();
		log = null;
	}

	public void openLog(final File f) {
		closeLog();
		try {
			log = new PrintWriter(new FileWriter(f));
		} catch (IOException ioe) {
			System.err.println("Could not open log file[" + f.getAbsolutePath()
					+ "]: " + ioe.getMessage());
			ioe.printStackTrace(System.err);
		}
	}

	public void error(String msg, Throwable t) {
		plog("[ERROR] " + msg, t, true);
	}

	public void error(String msg) {
		error(msg, null);
	}

	public void warn(String msg, Throwable t) {
		plog("[WARN] " + msg, t, true);
	}

	public void warn(String msg) {
		warn(msg, null);
	}

	public void info(String msg, Throwable t) {
		plog("[INFO] " + msg, t, true);
	}

	public void info(String msg) {
		info(msg, null);
	}

	public void plog(String msg) {
		plog(msg, null, true);
	}

	public void plog(String msg, Throwable t) {
		plog(msg, t, true);
	}

	public void plog(String msg, Throwable t, boolean newline) {
		String dt = df.format(new Date());
		System.out.print("[" + dt + "] [" + getName() + "] " + msg);
		if (newline)
			System.out.println();
		if (t != null) {
			System.err.print("[" + dt + "] [" + getName() + "] " + msg);
			if (newline)
				System.err.println();
			t.printStackTrace(System.err);
			System.out.print("[" + dt + "] [" + getName() + "] " + msg);
			if (newline)
				System.out.println();
			t.printStackTrace(System.out);
		}
		if (log != null) {
			try {
				log.write("[" + dt + "] [" + getName() + "] " + msg);
				if (newline)
					log.println();
				if (t != null) {
					t.printStackTrace(log);
				}
				log.flush();
			} catch (Exception ex) {
				System.err.println("[" + dt + "] [" + getName()
						+ "] ERROR writing [" + msg + "] to logfile: "
						+ ex.getMessage());
				ex.printStackTrace(System.err);
			}
		}
	}
}
