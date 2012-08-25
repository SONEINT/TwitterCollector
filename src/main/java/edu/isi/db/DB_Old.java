package edu.isi.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;

import edu.isi.util.Log;

public final class DB_Old {
	private static Connection con = null;

	/**
	 * Convert standard formats into a Database Timespamp.<br>
	 * <LI>An integer (e.g., "12391231") is converted as though it is a standard
	 * milli-seconds<BR> <LI>Any "/" is converted into "-" (e.g., 12/5/2010
	 * becomes "12-5-2010")<BR> <LI>If no time is given, then 12:00:00 is
	 * appended (e.g., noon on a given date)<BR>
	 * <BR>
	 * The final string is then assumed to by in the format
	 * "yyyy-mm-dd hh:mm:ss"
	 * 
	 * @param dtStr
	 * @return
	 */
	public static Timestamp getTimestamp(String dtStr) {
		if (dtStr == null)
			return null;
		if (dtStr.matches("^\\d+$"))
			return new Timestamp(Long.parseLong(dtStr));

		if (dtStr.matches("\\d+/\\d+/\\d+")) {
			dtStr = dtStr.replaceAll("/", "-");
		}
		if (!dtStr.matches("\\d+:\\d+:\\d+")) {
			dtStr += " 12:00:00";
		}
		try {
			return Timestamp.valueOf(dtStr);
		} catch (Exception ex) {
			Log.getDefault().warn("Could not parse date '" + dtStr + "'", ex);
		}
		return null;
	}

	public static boolean hasConnection() {
		try {
			return (con != null && con.isValid(0));
		} catch (Exception ex) {
		}
		return false;
	}

	public static Connection getConnection() {
		if (con != null) {
			try {
				if (!con.isValid(0))
					con.close();
				if (con.isClosed())
					con = null;
			} catch (SQLException sqlex) {
				Log.getDefault().warn("Failed to check connection", sqlex);
			}
		}
		if (con == null) {
			DBConfig config = DBConfig.getConfig();
			String s_driver = config.get(DBConfig.DB_DRIVER,
					"com.mysql.jdbc.Driver");
			String s_dburl = config.get(DBConfig.DB_URL,
					"jdbc:mysql://localhost:3306/twitter");
			String s_dbpass = config.get(DBConfig.DB_PASSWORD, "root");
			String s_dbuser = config.get(DBConfig.DB_USERNAME, "root");
			try {
				Class.forName(s_driver);
				con = DriverManager.getConnection(s_dburl, s_dbuser, s_dbpass);
				Log.getDefault().info(
						"Created database connection [" + con + "]");
			} catch (SQLException sqlex) {
				con = null;
				Log.getDefault().warn("Failed to open connection", sqlex);
			} catch (ClassNotFoundException cfe) {
				Log.getDefault().warn(
						"Failed to open connection - could not find driver class "
								+ s_driver, cfe);
			}
		}
		return con;
	}

	public static void openConnection() {
		getConnection();
	}

	public static void closeConnection() {
		try {
			if (con != null)
				con.close();
			con = null;
		} catch (SQLException sqlex) {
			Log.getDefault().warn("Failed to close connection", sqlex);
		}
	}

	public static PreparedStatement getPreparedStatement(String sql) {
		try {
			return getConnection().prepareStatement(sql);
		} catch (SQLException sqlex) {
			Log.getDefault().warn("Failed to create prepared statement", sqlex);
		}
		return null;
	}

	public static Statement getStatement() {
		try {
			return getConnection().createStatement();
		} catch (SQLException sqlex) {
			Log.getDefault().warn("Failed to create statement", sqlex);
		}
		return null;
	}

	public static long getNewPK(final Statement stmt) throws SQLException {
		final ResultSet rs = stmt.getGeneratedKeys();
		long id = -1;
		if (rs.next())
			id = rs.getLong(1);
		rs.close();
		return id;
	}

	public static String getString(final String query, final String column) {
		final String[] result = DB_Old.getStringResult(query, column);
		return (result == null || result.length == 0) ? null : result[0];
	}

	public static String getString(final PreparedStatement query,
			final int column) {
		final String[] result = DB_Old.getStringResult(query, column);
		return (result == null || result.length == 0) ? null : result[0];
	}

	public static long getLong(final PreparedStatement query,
			final String column) {
		final long[] result = DB_Old.getLongResult(query, column);
		return (result == null || result.length == 0) ? -1 : result[0];
	}

	public static long getLong(final String query, final String column) {
		final long[] result = DB_Old.getLongResult(query, column);
		return (result == null || result.length == 0) ? -1 : result[0];
	}

	/**
	 * Query the Database with the given query, looking for the named columns in
	 * the first returned row as it assumes only one row will be returned.
	 * <P>
	 * It will return a string array of the same size as the number of columns
	 * provided, where each element is null if there was an error, otherwise
	 * they will contain the result from the database query.
	 */
	public static String[] getStringResult(final PreparedStatement query,
			final int... column) {
		String[] result = new String[column.length];
		Arrays.fill(result, null);
		try {
			ResultSet rs = query.executeQuery();
			if (rs.next()) {
				for (int i = 0; i < column.length; i++)
					result[i] = rs.getString(column[i]);
			}
			rs.close();
		} catch (SQLException sqlex) {
			Log.getDefault().warn(
					"DB::getStringResult() --- Failed to get query result set (query=["
							+ query + "]", sqlex);
		}
		return result;
	}

	/**
	 * Query the Database with the given query, looking for the named columns in
	 * the first returned row as it assumes only one row will be returned.
	 * <P>
	 * It will return a string array of the same size as the number of columns
	 * provided, where each element is null if there was an error, otherwise
	 * they will contain the result from the database query.
	 */
	public static String[] getStringResult(final String sql,
			final String... column) {
		String[] result = new String[column.length];
		Arrays.fill(result, null);
		try {
			Statement stmt = getStatement();
			ResultSet rs = stmt.executeQuery(sql);
			if (rs.next()) {
				for (int i = 0; i < column.length; i++)
					result[i] = rs.getString(column[i]);
			}
			rs.close();
			stmt.close();
		} catch (SQLException sqlex) {
			Log.getDefault().warn(
					"DB::getStringResult() --- Failed to get query result set (query=["
							+ sql + "]", sqlex);
		}
		return result;
	}

	/**
	 * Query the Database with the given query, looking for the named columns in
	 * the first returned row as it assumes only one row will be returned.
	 * <P>
	 * It will return a string array of the same size as the number of columns
	 * provided, where each element is null if there was an error, otherwise
	 * they will contain the result from the database query.
	 */
	public static long[] getLongResult(final PreparedStatement stmt,
			final String... column) {
		Long[] list = new Long[column.length];
		Arrays.fill(list, null);
		try {
			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				for (int i = 0; i < column.length; i++)
					list[i] = rs.getLong(column[i]);
			}
			rs.close();
		} catch (SQLException sqlex) {
			Log.getDefault().warn(
					"DB::getStringResult() --- Failed to get query result set (query=["
							+ stmt + "]", sqlex);
		}
		long[] result = new long[list.length];
		for (int i = 0; i < list.length; i++)
			result[i] = (list[i] == null ? -1 : list[i]);
		return result;
	}

	/**
	 * Query the Database with the given query, looking for the named columns in
	 * the first returned row as it assumes only one row will be returned.
	 * <P>
	 * It will return a string array of the same size as the number of columns
	 * provided, where each element is null if there was an error, otherwise
	 * they will contain the result from the database query.
	 */
	public static long[] getLongResult(final String sql, final String... column) {
		Long[] list = new Long[column.length];
		Arrays.fill(list, null);
		try {
			Statement stmt = getStatement();
			ResultSet rs = stmt.executeQuery(sql);
			if (rs.next()) {
				for (int i = 0; i < column.length; i++)
					list[i] = rs.getLong(column[i]);
			}
			rs.close();
			stmt.close();
		} catch (SQLException sqlex) {
			Log.getDefault().warn(
					"DB::getStringResult() --- Failed to get query result set (query=["
							+ sql + "]", sqlex);
		}
		long[] result = new long[list.length];
		for (int i = 0; i < list.length; i++)
			result[i] = (list[i] == null ? -1 : list[i]);
		return result;
	}
}
