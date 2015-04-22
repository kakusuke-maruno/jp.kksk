package jp.kksk.datareplicate.oracle.logmnr;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.sql.DataSource;

public class Capture {
	private DataSource dataSource;
	private Connection connection;
	private PreparedStatement preparedStatement;
	private ResultSet resultSet;
	private String SQL_START_TIME;
	private String SQL_START_SCN;
	private String SQL_QUERY;
	private String SQL_END;
	private ExecutorService executorService = Executors.newSingleThreadExecutor();
	private Extract extract;

	public Capture(DataSource dataSource, Extract extract, String schemaName, String... tableNames) {
		this.dataSource = dataSource;
		this.extract = extract;
		SQL_START_TIME = "execute DBMS_LOGMNR.START_LOGMNR(STARTTIME => ?, ENDTIME => SYSTIMESTAMP, OPTIONS => DBMS_LOGMNR.COMMITTED_DATA_ONLY + DBMS_LOGMNR.CONTINUOUS_MINE);";
		SQL_START_SCN = "execute DBMS_LOGMNR.START_LOGMNR(STARTSCN => ?, ENDTIME => SYSTIMESTAMP, OPTIONS => DBMS_LOGMNR.COMMITTED_DATA_ONLY + DBMS_LOGMNR.CONTINUOUS_MINE);";
		String s = "";
		for (String t : tableNames) {
			if (!s.equals("")) {
				s = s + ",";
			}
			s = s + "'" + t + "'";
		}
		SQL_QUERY = String.format("SELECT OPERATION, SQL_REDO, SQL_UNDO, COMMIT_SCN, CSF, TIMESTAMP FROM V$LOGMNR_CONTENTS WHERE OPERATION IN ('INSERT', 'UPDATE', 'DELETE') AND SEG_OWNER = '%s' AND TABLE_NAME IN (%s)", schemaName, s);
		SQL_END = "execute DBMS_LOGMNR.END_LOGMNR();";
	}

	private Connection connect() {
		try {
			if (connection == null || connection.isClosed()) {
				connection = dataSource.getConnection();
			}
			return connection;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public long captureByScn(long scn) {
		try {
			CallableStatement statement = connect().prepareCall(SQL_START_SCN);
			statement.setLong(1, scn);
			return captureByStatement(statement);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public long captureByTime(java.util.Date date) {
		try {
			CallableStatement statement = connect().prepareCall(SQL_START_TIME);
			statement.setDate(1, new Date(date.getTime()));
			return captureByStatement(statement);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private long captureByStatement(CallableStatement statement) {
		long result = -1L;
		try {
			statement.executeUpdate();
			preparedStatement = connect().prepareStatement(SQL_QUERY);
			preparedStatement.setFetchSize(15000);
			resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				Trail loadResult = new Trail(resultSet);
				result = loadResult.commitScn;
				while (resultSet.getInt("CSF") == 1 && resultSet.next()) {
					loadResult.sqlRedo += resultSet.getString("SQL_REDO");
					loadResult.sqlUndo += resultSet.getString("SQL_UNDO");
				}
				Trail.LOG.put(loadResult);
				executorService.execute(extract);
			}
			statement = connect().prepareCall(SQL_END);
			statement.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
		return result;
	}
}
