package edu.isi.twitter;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import edu.isi.db.DB_Old;
import edu.isi.db.DBConfig;

public class FollowTwits extends TwitterBase {
	Set<Long> users = null;
	String s_userListToFollow = null;

	@Override
	public String getName() {
		return "FollowTwits";
	}

	@Override
	public void init() {
		super.init();
		loadListsToFollow();
	}

	@Override
	void loadListsToFollow() {
		final String db = config.get(TwitterConfig.DB, "db");
		final DBConfig dbCfg = DBConfig.getConfig(db);
		final String table = dbCfg.get(DBConfig.DB_USER_TABLE);

		final String query = "SELECT uid from `" + table + "` WHERE follow=1";

		log.plog("FollowTwitsServlet reading user list from DB(config=" + db
				+ ";url=" + dbCfg.get(DBConfig.DB_URL) + ") table=" + table);
		users = new HashSet<Long>();
		try {
			Statement stmt = DB_Old.getStatement();
			ResultSet rs = stmt.executeQuery(query);
			while (rs.next()) {
				long uid = rs.getLong(1);
				users.add(uid);
			}
			rs.close();
			log.plog("Read in " + users.size() + " users.");
		} catch (SQLException sqlex) {
			log.warn(
					"DB::getStringResult() --- Failed to get query result set (query=["
							+ query + "]", sqlex);
		}
		StringBuffer sb = new StringBuffer();
		boolean first = true;
		for (long uid : users) {
			if (!first)
				sb.append(",");
			sb.append(uid);
			first = false;
		}
		s_userListToFollow = sb.toString();
	}

	@Override
	public String getURLParams() throws UnsupportedEncodingException {
		return "follow=" + URLEncoder.encode(s_userListToFollow, "UTF-8");
	}

	public static void main(String... args) {
		System.out.println("Testing FollowTwits");
		FollowTwits t = new FollowTwits();
		test(t);
	}
}
