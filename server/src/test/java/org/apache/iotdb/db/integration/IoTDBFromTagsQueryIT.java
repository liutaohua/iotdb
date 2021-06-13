package org.apache.iotdb.db.integration;

import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.fail;

public class IoTDBFromTagsQueryIT {

    public static String[] sqls = new String[]{
            "create timeseries root.ln.wf01.wt01.status with datatype=INT32,encoding=PLAIN",
            "ALTER timeseries root.ln.wf01.wt01.status ADD TAGS tag1=1",

            "create timeseries root.ln.wf01.wt02.status with datatype=INT32,encoding=PLAIN",
            "ALTER timeseries root.ln.wf01.wt02.status ADD TAGS tag1=1",
            "ALTER timeseries root.ln.wf01.wt02.status ADD TAGS tag2=2",

            "create timeseries root.ln.wf01.wt03.status with datatype=INT32,encoding=PLAIN",
            "ALTER timeseries root.ln.wf01.wt03.status ADD TAGS tag3=3",
            "ALTER timeseries root.ln.wf01.wt03.status ADD TAGS tag2=2",

            "create timeseries root.ln.wf01.wt04.status with datatype=INT32,encoding=PLAIN",
            "ALTER timeseries root.ln.wf01.wt04.status ADD TAGS tag4=4",
            "ALTER timeseries root.ln.wf01.wt04.status ADD TAGS tag2=2",

            "create timeseries root.ln.wf01.wt05.status with datatype=INT32,encoding=PLAIN",
            "ALTER timeseries root.ln.wf01.wt05.status ADD TAGS tag5=5",

            "create timeseries root.ln.wf01.wt06.status with datatype=INT32,encoding=PLAIN",
            "ALTER timeseries root.ln.wf01.wt06.status ADD TAGS tag6=6",
            "ALTER timeseries root.ln.wf01.wt06.status ADD TAGS tag2=2",
            "ALTER timeseries root.ln.wf01.wt06.status ADD TAGS tag3=3",
            "ALTER timeseries root.ln.wf01.wt06.status ADD TAGS tag1=1",


            "create timeseries root.ln.wf01.wt07.plus with datatype=INT32,encoding=PLAIN",
            "ALTER timeseries root.ln.wf01.wt07.plus ADD TAGS tag7=7",

            "create timeseries root.ln.wf01.wt08.plus2 with datatype=INT32,encoding=PLAIN",
            "ALTER timeseries root.ln.wf01.wt08.plus2 ADD TAGS tag8=8",



            "insert into root.ln.wf01.wt01(timestamp,status) values(1509465600000,1)",
            "insert into root.ln.wf01.wt02(timestamp,status) values(1509465600000,1)",
            "insert into root.ln.wf01.wt03(timestamp,status) values(1509465600000,1)",
            "insert into root.ln.wf01.wt04(timestamp,status) values(1509465600000,1)",
            "insert into root.ln.wf01.wt05(timestamp,status) values(1509465600000,1)",
            "insert into root.ln.wf01.wt06(timestamp,status) values(1509465600000,2)",
            "insert into root.ln.wf01.wt07(timestamp,plus) values(1509465600000,100)",
            "insert into root.ln.wf01.wt08(timestamp,plus2) values(1509465600000,99)",

    };


    private static void importData() throws ClassNotFoundException {
        Class.forName(Config.JDBC_DRIVER_NAME);
        try (Connection connection =
                     DriverManager.getConnection(
                             Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
             Statement statement = connection.createStatement()) {

            for (String sql : sqls) {
                statement.execute(sql);
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @BeforeClass
    public static void setUp() throws Exception {
        EnvironmentUtils.closeStatMonitor();
        EnvironmentUtils.envSetUp();

        importData();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        EnvironmentUtils.cleanEnv();
    }

    private List<Integer> checkHeader(
            ResultSetMetaData resultSetMetaData, String expectedHeaderStrings, int[] expectedTypes)
            throws SQLException {
        String[] expectedHeaders = expectedHeaderStrings.split(",");
        Map<String, Integer> expectedHeaderToTypeIndexMap = new HashMap<>();
        for (int i = 0; i < expectedHeaders.length; ++i) {
            expectedHeaderToTypeIndexMap.put(expectedHeaders[i], i);
        }


        List<Integer> actualIndexToExpectedIndexList = new ArrayList<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            Integer typeIndex = expectedHeaderToTypeIndexMap.get(resultSetMetaData.getColumnName(i));
            Assert.assertNotNull(typeIndex);
            Assert.assertEquals(expectedTypes[typeIndex], resultSetMetaData.getColumnType(i));
            actualIndexToExpectedIndexList.add(typeIndex);
        }
        return actualIndexToExpectedIndexList;
    }


    @Test
    public void TestTagsAndQuery() throws ClassNotFoundException {
        String[] retArray = new String[]{"1509465600000,2,",};

        Class.forName(Config.JDBC_DRIVER_NAME);
        try (Connection connection =
                     DriverManager.getConnection(
                             Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
             Statement statement = connection.createStatement()) {
            boolean hasResultSet = statement.execute("select * from Tags(tag1='1' and tag2='2' and tag3='3' and tag6='6') where time>10");
            Assert.assertTrue(hasResultSet);

            try (ResultSet resultSet = statement.getResultSet()) {
                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                List<Integer> actualIndexToExpectedIndexList =
                        checkHeader(
                                resultSetMetaData, "Time,root.ln.wf01.wt06.status,",
                                new int[]{
                                        Types.TIMESTAMP,
                                        Types.INTEGER,
                                });

                int cnt = 0;
                while (resultSet.next()) {
                    String[] expectedStrings = retArray[cnt].split(",");
                    StringBuilder expectedBuilder = new StringBuilder();
                    StringBuilder actualBuilder = new StringBuilder();
                    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                        actualBuilder.append(resultSet.getString(i)).append(",");
                        expectedBuilder
                                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                                .append(",");
                    }
                    Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
                    cnt++;
                }
                Assert.assertEquals(1, cnt);
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }


    @Test
    public void TestOrTagsQuery() throws ClassNotFoundException {
        String[] retArray = new String[]{"1509465600000,1,1,1,1,2",};

        Class.forName(Config.JDBC_DRIVER_NAME);
        try (Connection connection =
                     DriverManager.getConnection(
                             Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
             Statement statement = connection.createStatement()) {
            boolean hasResultSet = statement.execute("select * from Tags(tag1='1' or tag2='2' or tag3='3' or tag6='6') where time > 10");
            Assert.assertTrue(hasResultSet);

            try (ResultSet resultSet = statement.getResultSet()) {
                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                List<Integer> actualIndexToExpectedIndexList =
                        checkHeader(
                                resultSetMetaData, "Time,root.ln.wf01.wt01.status," +
                                        "root.ln.wf01.wt02.status," +
                                        "root.ln.wf01.wt03.status," +
                                        "root.ln.wf01.wt04.status," +
                                        "root.ln.wf01.wt06.status",
                                new int[]{
                                        Types.TIMESTAMP,
                                        Types.INTEGER,
                                        Types.INTEGER,
                                        Types.INTEGER,
                                        Types.INTEGER,
                                        Types.INTEGER,
                                });

                int cnt = 0;
                while (resultSet.next()) {
                    String[] expectedStrings = retArray[cnt].split(",");
                    StringBuilder expectedBuilder = new StringBuilder();
                    StringBuilder actualBuilder = new StringBuilder();
                    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                        actualBuilder.append(resultSet.getString(i)).append(",");
                        expectedBuilder
                                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                                .append(",");
                    }
                    Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
                    cnt++;
                }
                Assert.assertEquals(1, cnt);
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void TestMixTagsQuery() throws ClassNotFoundException {
        String[] retArray = new String[]{"1509465600000,1,2",};

        Class.forName(Config.JDBC_DRIVER_NAME);
        try (Connection connection =
                     DriverManager.getConnection(
                             Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
             Statement statement = connection.createStatement()) {
            boolean hasResultSet = statement.execute("select * from Tags(tag1='1' and tag2='2' and tag6='6' or tag5='5') where time > 10");
            Assert.assertTrue(hasResultSet);

            try (ResultSet resultSet = statement.getResultSet()) {
                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                List<Integer> actualIndexToExpectedIndexList =
                        checkHeader(
                                resultSetMetaData, "Time,root.ln.wf01.wt05.status," +
                                        "root.ln.wf01.wt06.status,",
                                new int[]{
                                        Types.TIMESTAMP,
                                        Types.INTEGER,
                                        Types.INTEGER,
                                });

                int cnt = 0;
                while (resultSet.next()) {
                    String[] expectedStrings = retArray[cnt].split(",");
                    StringBuilder expectedBuilder = new StringBuilder();
                    StringBuilder actualBuilder = new StringBuilder();
                    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                        actualBuilder.append(resultSet.getString(i)).append(",");
                        expectedBuilder
                                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                                .append(",");
                    }
                    Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
                    cnt++;
                }
                Assert.assertEquals(1, cnt);
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void TestMix2TagsQuery() throws ClassNotFoundException {
        String[] retArray = new String[]{"1509465600000,1,1,2",};

        Class.forName(Config.JDBC_DRIVER_NAME);
        try (Connection connection =
                     DriverManager.getConnection(
                             Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
             Statement statement = connection.createStatement()) {
            boolean hasResultSet = statement.execute("select * from Tags(tag5='5' or (tag3='3' and (tag2='2' or tag1='1')) ) where time > 10 ");
            Assert.assertTrue(hasResultSet);

            try (ResultSet resultSet = statement.getResultSet()) {
                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                List<Integer> actualIndexToExpectedIndexList =
                        checkHeader(
                                resultSetMetaData, "Time,root.ln.wf01.wt03.status," +
                                        "root.ln.wf01.wt05.status," +
                                        "root.ln.wf01.wt06.status,",
                                new int[]{
                                        Types.TIMESTAMP,
                                        Types.INTEGER,
                                        Types.INTEGER,
                                        Types.INTEGER,
                                });

                int cnt = 0;
                while (resultSet.next()) {
                    String[] expectedStrings = retArray[cnt].split(",");
                    StringBuilder expectedBuilder = new StringBuilder();
                    StringBuilder actualBuilder = new StringBuilder();
                    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                        actualBuilder.append(resultSet.getString(i)).append(",");
                        expectedBuilder
                                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                                .append(",");
                    }
                    Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
                    cnt++;
                }
                Assert.assertEquals(1, cnt);
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }


    @Test
    public void TestValueFilterTagsQuery() throws ClassNotFoundException {
        String[] retArray = new String[]{"1509465600000,1,1",};

        Class.forName(Config.JDBC_DRIVER_NAME);
        try (Connection connection =
                     DriverManager.getConnection(
                             Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
             Statement statement = connection.createStatement()) {
            statement.execute("select * from Tags(tag5='5' or tag4='4' ) where status = 2");
            try (ResultSet resultSet = statement.getResultSet()) {
                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                List<Integer> actualIndexToExpectedIndexList =
                        checkHeader(
                                resultSetMetaData, "Time," +
                                        "root.ln.wf01.wt04.status," +
                                        "root.ln.wf01.wt05.status,",
                                new int[]{
                                        Types.TIMESTAMP,
                                        Types.INTEGER,
                                        Types.INTEGER,
                                });

                int cnt = 0;
                while (resultSet.next()) {
                    cnt++;
                }
                Assert.assertEquals(0, cnt);
            }


            statement.execute("select * from Tags(tag5='5' or tag4='4' ) where status = 1");
            try (ResultSet resultSet = statement.getResultSet()) {
                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                List<Integer> actualIndexToExpectedIndexList =
                        checkHeader(
                                resultSetMetaData, "Time," +
                                        "root.ln.wf01.wt04.status," +
                                        "root.ln.wf01.wt05.status,",
                                new int[]{
                                        Types.TIMESTAMP,
                                        Types.INTEGER,
                                        Types.INTEGER,
                                });

                int cnt = 0;
                while (resultSet.next()) {
                    String[] expectedStrings = retArray[cnt].split(",");
                    StringBuilder expectedBuilder = new StringBuilder();
                    StringBuilder actualBuilder = new StringBuilder();
                    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                        actualBuilder.append(resultSet.getString(i)).append(",");
                        expectedBuilder
                                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                                .append(",");
                    }
                    Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
                    cnt++;
                }
                Assert.assertEquals(1, cnt);
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }


    @Test
    public void TestTimeFilterTagsQuery() throws ClassNotFoundException {
        String[] retArray = new String[]{"1509465600000,1,1",};

        Class.forName(Config.JDBC_DRIVER_NAME);
        try (Connection connection =
                     DriverManager.getConnection(
                             Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
             Statement statement = connection.createStatement()) {
            statement.execute("select * from Tags(tag5='5' or tag4='4' ) where time > 1509465600000");
            try (ResultSet resultSet = statement.getResultSet()) {
                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                List<Integer> actualIndexToExpectedIndexList =
                        checkHeader(
                                resultSetMetaData, "Time," +
                                        "root.ln.wf01.wt04.status," +
                                        "root.ln.wf01.wt05.status,",
                                new int[]{
                                        Types.TIMESTAMP,
                                        Types.INTEGER,
                                        Types.INTEGER,
                                });

                int cnt = 0;
                while (resultSet.next()) {
                    cnt++;
                }
                Assert.assertEquals(0, cnt);
            }


            statement.execute("select * from Tags(tag5='5' or tag4='4' ) where time = 1509465600000");
            try (ResultSet resultSet = statement.getResultSet()) {
                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                List<Integer> actualIndexToExpectedIndexList =
                        checkHeader(
                                resultSetMetaData, "Time," +
                                        "root.ln.wf01.wt04.status," +
                                        "root.ln.wf01.wt05.status,",
                                new int[]{
                                        Types.TIMESTAMP,
                                        Types.INTEGER,
                                        Types.INTEGER,
                                });

                int cnt = 0;
                while (resultSet.next()) {
                    String[] expectedStrings = retArray[cnt].split(",");
                    StringBuilder expectedBuilder = new StringBuilder();
                    StringBuilder actualBuilder = new StringBuilder();
                    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                        actualBuilder.append(resultSet.getString(i)).append(",");
                        expectedBuilder
                                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                                .append(",");
                    }
                    Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
                    cnt++;
                }
                Assert.assertEquals(1, cnt);
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }


    @Test
    public void TestExpTagsQuery() throws ClassNotFoundException {
        String[] retArray = new String[]{"1509465600000,199.0",};

        Class.forName(Config.JDBC_DRIVER_NAME);
        try (Connection connection =
                     DriverManager.getConnection(
                             Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
             Statement statement = connection.createStatement()) {
            boolean hasResultSet = statement.execute("select plus + plus2 from Tags(tag7='7'  or tag8='8' ) where time > 10 ");
            Assert.assertTrue(hasResultSet);

            try (ResultSet resultSet = statement.getResultSet()) {
                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                List<Integer> actualIndexToExpectedIndexList =
                        checkHeader(
                                resultSetMetaData, "Time,root.ln.wf01.wt07.plus + root.ln.wf01.wt08.plus2,",
                                new int[]{
                                        Types.TIMESTAMP,
                                        Types.DOUBLE,
                                });

                int cnt = 0;
                while (resultSet.next()) {
                    String[] expectedStrings = retArray[cnt].split(",");
                    StringBuilder expectedBuilder = new StringBuilder();
                    StringBuilder actualBuilder = new StringBuilder();
                    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                        actualBuilder.append(resultSet.getString(i)).append(",");
                        expectedBuilder
                                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                                .append(",");
                    }
                    Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
                    cnt++;
                }
                Assert.assertEquals(1, cnt);
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
