```java
@Public
public class BatchIntervalJDBCSink extends BatchIntervalSinkOperator<NewArgusEtl.JRecord>{
    private static final Logger logger = LoggerFactory.getLogger(BatchIntervalJDBCSink.class);
    private static final long serialVersionUID = 1L;

    private String username;
    private String password;
    private String driverName;
    private String dbURL;
    private String query;
    private String database;

    private Connection dbConn;
    private PreparedStatement upload;

    public BatchIntervalJDBCSink(int batchSize, long batchInteval) {
        super(batchSize,batchInteval);
    }

    public BatchIntervalJDBCSink setUsername(String username) {
        this.username = username;
        return this;
    }

    public BatchIntervalJDBCSink setPassword(String password) {
        this.password = password;
        return this;
    }

    public BatchIntervalJDBCSink setDriverName(String driverName) {
        this.driverName = driverName;
        return this;
    }

    public BatchIntervalJDBCSink setDbURL(String dbURL) {
        this.dbURL = dbURL;
        return this;
    }

    public BatchIntervalJDBCSink setDatabase(String database) {
        this.database = database;
        return this;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getDriverName() {
        return driverName;
    }

    public String getDbURL() {
        return dbURL;
    }

    public String getQuery() {
        return query;
    }

    public String getDatabase() {
        return database;
    }

    @Override
    public void open() throws Exception {
        super.open();
        try {
            establishConnection();
            if(null != query){
                upload = dbConn.prepareStatement(query);
            }
        } catch (SQLException sqe) {
            throw new IllegalArgumentException("open() failed.", sqe);
        } catch (ClassNotFoundException cnfe) {
            throw new IllegalArgumentException("JDBC driver class not found.", cnfe);
        }
    }

    private void establishConnection() throws Exception {
        //Class.forName(drivername);
        //if (username == null) {
        //    dbConn = DriverManager.getConnection(dbURL + database);
        //} else {
        //    dbConn = DriverManager.getConnection(dbURL + database, username, password);
        //}

        BalancedClickhouseDataSource executorBalanced =
                new BalancedClickhouseDataSource(
                        dbURL + database,
                        ClickhouseUtils.getCHConf(true, database));
        dbConn = executorBalanced.getConnection();
    }

    private void flush() {
        try {
            upload.executeBatch();
        } catch (SQLException e) {
            logger.error("Execution of JDBC statement failed.",e);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        closeUpload();
        if (dbConn != null) {
            try {
                dbConn.close();
            } catch (SQLException se) {
                LOG.info("JDBC connection could not be closed: " + se.getMessage());
            } finally {
                dbConn = null;
            }
        }
    }

    private void closeUpload(){
        if (upload != null) {
            flush();
            try {
                upload.close();
            } catch (SQLException e) {
                LOG.info("JDBC statement could not be closed: " + e.getMessage());
            } finally {
                upload = null;
            }
        }
    }

    private void resetUpload(String newQuery){
        closeUpload();
        try {
            upload = dbConn
                    .prepareStatement(newQuery);
            query = newQuery;
        } catch (Exception e) {
            throw new IllegalArgumentException("resetUpload() failed.", e);
        }
    }

    @Override
    public void saveRecords(List<NewArgusEtl.JRecord> batch) {
        long begin = System.currentTimeMillis();
        batch.forEach(meta -> {
            if(!meta.query().equals(query)){
                resetUpload(meta.query());
                ClickhouseUtils.newInsert(meta.json(),upload,meta.keys());
            } else {
                ClickhouseUtils.newInsert(meta.json(),upload,meta.keys());
            }
        });
        flush();
        long end = System.currentTimeMillis();
        logger.info("write to ch: {}ms, {}records", end - begin, batch.size());
    }
}
```
引用
```java
...
datastream.transform(
      "sink-Clickhouse",
      new BatchIntervalJDBCSink(bulkSize,1000 * 10)
        .setDriverName("ru.yandex.clickhouse.ClickhouseDriver")
        .setDbURL("jdbc:clickhouse://127.0.0.1:8123/")
        .setDatabase(database)
        .setUsername("username")
        .setPassword("password")
    )
...
```