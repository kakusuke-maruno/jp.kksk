package jp.kksk.datareplicate.oracle.logmnr;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.concurrent.ArrayBlockingQueue;

public class Trail {
	public static ArrayBlockingQueue<Trail> LOG = new ArrayBlockingQueue<>(15000);
	public String operation;
	public String sqlRedo;
	public String sqlUndo;
	public Timestamp timestamp;
	public long commitScn;

	public Trail(ResultSet resultSet) {
		try {
			this.operation = resultSet.getString("OPERATION");
			this.sqlRedo = resultSet.getString("SQL_REDO");
			this.sqlUndo = resultSet.getString("SQL_UNDO");
			this.timestamp = resultSet.getTimestamp("TIMESTAMP");
			this.commitScn = resultSet.getLong("COMMIT_SCN");
		} catch (SQLException e) {
		}
	}
}
