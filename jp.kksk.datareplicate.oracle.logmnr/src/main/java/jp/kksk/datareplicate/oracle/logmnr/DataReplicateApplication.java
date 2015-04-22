package jp.kksk.datareplicate.oracle.logmnr;

import java.io.FileInputStream;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import oracle.jdbc.pool.OracleDataSource;

public class DataReplicateApplication {
	private static OracleDataSource getDataSource(Properties dbProperties) throws SQLException {
		OracleDataSource ds = new OracleDataSource();
		ds.setURL(dbProperties.getProperty("ORACLE_DB_URL"));
		ds.setUser(dbProperties.getProperty("ORACLE_DB_USERNAME"));
		ds.setPassword(dbProperties.getProperty("ORACLE_DB_PASSWORD"));
		return ds;
	}

	private static class ReplicationConfig {
		String schema;
		SimpleDateFormat dateFormat;
		long interval;
		String[] tables;

		public ReplicationConfig(Properties replicationProperties) {
			List<String> tableList = new ArrayList<>();
			for (int i = 0; i < 999; i++) {
				String key = String.format("TABLE_%03d", i);
				if (replicationProperties.contains(key)) {
					tableList.add(replicationProperties.getProperty(key));
				}
			}
			this.schema = replicationProperties.getProperty("SCHEMA");
			this.dateFormat = new SimpleDateFormat(replicationProperties.getProperty("DATE_FORMAT"));
			this.interval = Integer.parseInt(replicationProperties.getProperty("INTERVAL", "1000"));
			this.tables = tableList.toArray(new String[] {});
		}
	}

	public static void main(String[] args) {
		try (FileInputStream dbPropertiesFile = new FileInputStream("db.properties"); FileInputStream replicationPropertiesFile = new FileInputStream("replication.properties");) {
			Properties dbProperties = new Properties();
			dbProperties.load(dbPropertiesFile);
			OracleDataSource ds = getDataSource(dbProperties);

			Properties replicationProperties = new Properties();
			replicationProperties.load(replicationPropertiesFile);
			ReplicationConfig replicationConfig = new ReplicationConfig(replicationProperties);

			Capture capture = new Capture(ds, new Extract() {
				@Override
				protected void execute(Trail trail) {
					// TODO
				}
			}, replicationConfig.schema, replicationConfig.tables);

			long scn = capture.captureByTime(replicationConfig.dateFormat.parse(args[0]));
			while (true) {
				long start = System.currentTimeMillis();
				long scn2 = capture.captureByScn(scn + 1);
				if (scn2 > 0)
					scn = scn2;
				long gap = replicationConfig.interval - (System.currentTimeMillis() - start);
				if (gap > 0)
					Thread.sleep(gap);
			}
		} catch (InterruptedException e) {
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(9);
		}
	}
}
