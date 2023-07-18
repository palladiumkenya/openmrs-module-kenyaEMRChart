package org.openmrs.module.kenyaemrCharts.fragment.controller;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.Transaction;
import org.hibernate.jdbc.Work;
import org.json.JSONObject;
import org.openmrs.Patient;
import org.openmrs.api.context.Context;
import org.openmrs.api.db.hibernate.DbSessionFactory;
import org.openmrs.module.kenyacore.etl.ETLProcedureBuilder;
import org.openmrs.ui.framework.SimpleObject;
import org.openmrs.ui.framework.UiUtils;
import org.openmrs.ui.framework.fragment.FragmentModel;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * controller for etlManager fragment
 * Handles calls for refreshing and recreation of ETLs
 */
public class EtlManagerFragmentController {
    private final Log log = LogFactory.getLog(getClass());
    private int processCount;

    public void controller(FragmentModel model) {
        DbSessionFactory sf = Context.getRegisteredComponents(DbSessionFactory.class).get(0);

        final String sqlSelectQuery = "SELECT script_name, start_time, stop_time, error FROM kenyaemr_etl.etl_script_status order by start_time desc limit 10;";
        final List<SimpleObject> ret = new ArrayList<SimpleObject>();

        try {
            sf.getCurrentSession().doWork(new Work() {

                @Override
                public void execute(Connection connection) throws SQLException {
                    PreparedStatement statement = connection.prepareStatement(sqlSelectQuery);

                    try {

                        ResultSet resultSet = statement.executeQuery();
                        if (resultSet != null) {
                            ResultSetMetaData metaData = resultSet.getMetaData();
                            while (resultSet.next()) {
                                Object[] row = new Object[metaData.getColumnCount()];
                                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                                    row[i - 1] = resultSet.getObject(i);
                                }
                                ret.add(SimpleObject.create(
                                        "script_name", row[0],
                                        "start_time", row[1] != null ? row[1].toString() : "",
                                        "stop_time", row[2] != null ? row[2].toString() : "",
                                        "status", row[3] != null ? "Pending" : "Success"
                                ));
                            }
                        }
                    } finally {
                        try {
                            if (statement != null) {
                                statement.close();
                            }
                        } catch (Exception e) {
                        }
                    }
                }
            });
        } catch (Exception e) {
            throw new IllegalArgumentException("Unable to execute query", e);
        }

        model.put("logs", ret);
    }

    public JSONObject fetchDataSets() {

        List<Patient> allPatients = Context.getPatientService().getAllPatients();
        JSONObject x = new JSONObject();
        x.put("patients", allPatients);
        return x;
    }

    public SimpleObject refreshTables(UiUtils ui) {
        final List<SimpleObject> ret = new ArrayList<SimpleObject>();
        final List<SimpleObject> status = new ArrayList<SimpleObject>();
        final SimpleObject sampleTypeObject = new SimpleObject();

        DbSessionFactory sf = Context.getRegisteredComponents(DbSessionFactory.class).get(0);
        sf.getCurrentSession().doWork(new Work() {

            @Override
            public void execute(Connection connection) throws SQLException {
                try {
                    Statement stmt = connection.createStatement();
                    ResultSet rs = stmt.executeQuery("SHOW OPEN TABLES WHERE (In_use > 0 AND `Database` = 'kenyaemr_etl');");
                    ResultSetMetaData metaData = rs.getMetaData();

                    while (rs.next()) {
                        Object[] row = new Object[metaData.getColumnCount()];
                        for (int i = 1; i <= metaData.getColumnCount(); i++) {
                            row[i - 1] = rs.getObject(i);
                            rs.getInt("In_use");
                        }
                        if(rs.getInt("In_use") > 0) {
                            status.add(SimpleObject.create(
                                    "process", "locked"
                            ));
                            sampleTypeObject.put("status",status);
                        }
                    }
                } catch (Exception e) {
                    System.out.println("An error has occured"+e);
                    throw new IllegalArgumentException("Unable to execute", e);
                }
            }
        });
        if(sampleTypeObject.isEmpty()) {


            final String sqlSelectQuery = "SELECT script_name, start_time, stop_time, error FROM kenyaemr_etl.etl_script_status order by start_time desc limit 10;";
            Transaction tx = null;
            try {
                Context.openSession();
                tx = sf.getHibernateSessionFactory().getCurrentSession().beginTransaction();
                final Transaction finalTx = tx;
                sf.getCurrentSession().doWork(new Work() {

                    @Override
                    public void execute(Connection connection) throws SQLException {
                        PreparedStatement statement = connection.prepareStatement(sqlSelectQuery);

                        ETLProcedureBuilder procedureBuilder = new ETLProcedureBuilder();
                        procedureBuilder.buildProcedures();

                        List<String> coreIncrementalUpdates = procedureBuilder.getCoreIncrementalUpdatesProcedures();
                        List<String> addonIncrementalUpdates = procedureBuilder.getAddonIncrementalUpdatesProcedures();

                        StringBuilder sb = null;
                        // we want to end up with a string like "{call create_etl_tables}"
                        // we then iterate the various procedures and execute them
                        for (String spName : coreIncrementalUpdates) {
                            sb = new StringBuilder();
                            sb.append("{call ");
                            sb.append(spName).append("}");
                            System.out.println("Core module: currently executing: " + sb);
                            CallableStatement sp = connection.prepareCall(sb.toString());
                            sp.execute();
                        }

                        for (String spName : addonIncrementalUpdates) {
                            sb = new StringBuilder();
                            sb.append("{call ");
                            sb.append(spName).append("}");
                            System.out.println("Addon module: currently executing: " + sb);
                            CallableStatement sp = connection.prepareCall(sb.toString());
                            sp.execute();
                        }

                        System.out.println("Successfully completed refreshing ETL procedures ... ");

                        // get the dataset for the UI
                        try {

                            ResultSet resultSet = statement.executeQuery();
                            if (resultSet != null) {
                                ResultSetMetaData metaData = resultSet.getMetaData();

                                while (resultSet.next()) {
                                    Object[] row = new Object[metaData.getColumnCount()];
                                    for (int i = 1; i <= metaData.getColumnCount(); i++) {
                                        row[i - 1] = resultSet.getObject(i);
                                    }

                                    ret.add(SimpleObject.create(
                                            "script_name", row[0],
                                            "start_time", row[1] != null ? row[1].toString() : "",
                                            "stop_time", row[2] != null ? row[2].toString() : "",
                                            "status", row[3] != null ? "Pending" : "Success"
                                    ));
                                }
                            }
                            finalTx.commit();
                        } finally {
                            try {
                                if (statement != null) {
                                    statement.close();
                                }
                            } catch (Exception e) {
                            }
                        }
                    }
                });
            } catch (Exception e) {
                throw new IllegalArgumentException("Unable to execute query", e);
            } finally {
                Context.closeSession();
            }
            sampleTypeObject.put("data", ret);
        } else {
            return sampleTypeObject;
        }

        return sampleTypeObject;
    }

    public SimpleObject recreateTables(UiUtils ui) {
        final List<SimpleObject> ret = new ArrayList<SimpleObject>();
        final List<SimpleObject> status = new ArrayList<SimpleObject>();
        final SimpleObject sampleTypeObject = new SimpleObject();

        DbSessionFactory sf = Context.getRegisteredComponents(DbSessionFactory.class).get(0);

        sf.getCurrentSession().doWork(new Work() {
            @Override
            public void execute(Connection connection) throws SQLException {
                try {
                    Statement stmt = connection.createStatement();
                    ResultSet rs = stmt.executeQuery("SHOW OPEN TABLES WHERE (In_use > 0 AND `Database` = 'kenyaemr_etl');");
                    ResultSetMetaData metaData = rs.getMetaData();


                    while (rs.next()) {
                        Object[] row = new Object[metaData.getColumnCount()];
                        for (int i = 1; i <= metaData.getColumnCount(); i++) {
                            row[i - 1] = rs.getObject(i);
                            rs.getInt("In_use");
                        }
                        if (rs.getInt("In_use") > 0) {
                            status.add(SimpleObject.create(
                                    "process", "locked"
                            ));
                            sampleTypeObject.put("status", status);
                        }
                    }
                } catch (Exception e) {
                    throw new IllegalArgumentException("Unable to execute", e);
                } finally {
                    Context.closeSession();
                }
            }

        });
        if (sampleTypeObject.isEmpty()) {

            final String sqlSelectQuery = "SELECT script_name, start_time, stop_time, error FROM kenyaemr_etl.etl_script_status order by start_time desc limit 10;";

            Transaction tx = null;
            try {
                Context.openSession();
                tx = sf.getHibernateSessionFactory().getCurrentSession().beginTransaction();
                final Transaction finalTx = tx;
                sf.getCurrentSession().doWork(new Work() {

                    @Override
                    public void execute(Connection connection) throws SQLException {
                        PreparedStatement statement = connection.prepareStatement(sqlSelectQuery);

                        ETLProcedureBuilder procedureBuilder = new ETLProcedureBuilder();
                        procedureBuilder.buildProcedures();

                        List<String> coreDDL = procedureBuilder.getCoreDDlProcedures();
                        List<String> addonDDL = procedureBuilder.getAddonDDlProcedures();

                        List<String> coreDML = procedureBuilder.getCoreDMLProcedures();
                        List<String> addonDML = procedureBuilder.getAddonDMLProcedures();

                        StringBuilder sb = null;
                        // we want to end up with something like "{call create_etl_tables}"
                        // iterate through the various procedures and execute them
                        for (String spName : coreDDL) {
                            sb = new StringBuilder();
                            sb.append("{call ");
                            sb.append(spName).append("}");
                            System.out.println("Core module: currently executing: " + sb);
                            CallableStatement sp = connection.prepareCall(sb.toString());
                            sp.execute();
                        }

                        for (String spName : addonDDL) {
                            sb = new StringBuilder();
                            sb.append("{call ");
                            sb.append(spName).append("}");
                            System.out.println("Addon module: currently executing: " + sb);
                            CallableStatement sp = connection.prepareCall(sb.toString());
                            sp.execute();
                        }

                        for (String spName : coreDML) {
                            sb = new StringBuilder();
                            sb.append("{call ");
                            sb.append(spName).append("}");
                            System.out.println("Core module: currently executing: " + sb);
                            CallableStatement sp = connection.prepareCall(sb.toString());
                            sp.execute();
                        }

                        for (String spName : addonDML) {
                            sb = new StringBuilder();
                            sb.append("{call ");
                            sb.append(spName).append("}");
                            System.out.println("Addon module: currently executing: " + sb);
                            CallableStatement sp = connection.prepareCall(sb.toString());
                            sp.execute();
                        }

                        System.out.println("Successfully completed recreating ETL procedures ... ");

                        try {

                            ResultSet resultSet = statement.executeQuery();
                            if (resultSet != null) {
                                ResultSetMetaData metaData = resultSet.getMetaData();
                                while (resultSet.next()) {
                                    Object[] row = new Object[metaData.getColumnCount()];
                                    for (int i = 1; i <= metaData.getColumnCount(); i++) {
                                        row[i - 1] = resultSet.getObject(i);
                                    }

                                    ret.add(SimpleObject.create(
                                            "script_name", row[0],
                                            "start_time", row[1] != null ? row[1].toString() : "",
                                            "stop_time", row[2] != null ? row[2].toString() : "",
                                            "status", row[3] != null ? "Pending" : "Success"
                                    ));
                                }
                            }
                            finalTx.commit();
                        } finally {
                            try {
                                if (statement != null) {
                                    statement.close();
                                }
                            } catch (Exception e) {
                            }
                        }
                    }
                });
            } catch (Exception e) {
                throw new IllegalArgumentException("Unable to execute query", e);
            } finally {
                Context.closeSession();
            }
            sampleTypeObject.put("data", ret);


        } else {

            return sampleTypeObject;

        }

        return sampleTypeObject;

    }

    public SimpleObject recreateDatatoolsTables(UiUtils ui) {
        final List<SimpleObject> ret = new ArrayList<SimpleObject>();
        final List<SimpleObject> status = new ArrayList<SimpleObject>();
        final SimpleObject sampleTypeObject = new SimpleObject();

        DbSessionFactory sf = Context.getRegisteredComponents(DbSessionFactory.class).get(0);

        sf.getCurrentSession().doWork(new Work() {
            @Override
            public void execute(Connection connection) throws SQLException {
                try {
                    Statement stmt = connection.createStatement();
                    ResultSet rs = stmt.executeQuery("SHOW OPEN TABLES WHERE (In_use > 0 AND `Database` LIKE 'kenyaemr_etl') OR (In_use > 0 AND `Database` LIKE 'kenyaemr_datatools');");
                    ResultSetMetaData metaData = rs.getMetaData();


                    while (rs.next()) {
                        Object[] row = new Object[metaData.getColumnCount()];
                        for (int i = 1; i <= metaData.getColumnCount(); i++) {
                            row[i - 1] = rs.getObject(i);
                            rs.getInt("In_use");
                        }
                        if (rs.getInt("In_use") > 0) {
                            status.add(SimpleObject.create(
                                    "process", "locked"
                            ));
                            sampleTypeObject.put("status", status);
                        }
                    }
                } catch (Exception e) {
                    throw new IllegalArgumentException("Unable to execute", e);
                } finally {
                    Context.closeSession();
                }
            }

        });
        if (sampleTypeObject.isEmpty()) {

            final String sqlSelectQuery = "SELECT script_name, start_time, stop_time, error FROM kenyaemr_etl.etl_script_status order by start_time desc limit 10;";

            Transaction tx = null;
            try {
                Context.openSession();
                tx = sf.getHibernateSessionFactory().getCurrentSession().beginTransaction();
                final Transaction finalTx = tx;
                sf.getCurrentSession().doWork(new Work() {

                    @Override
                    public void execute(Connection connection) throws SQLException {
                        PreparedStatement statement = connection.prepareStatement(sqlSelectQuery);

                        ETLProcedureBuilder procedureBuilder = new ETLProcedureBuilder();
                        procedureBuilder.buildProcedures();

                        List<String> coreDatatools = procedureBuilder.getCoreDatatoolDatabaseProcedures();
                        List<String> addonDatatools = procedureBuilder.getAddonDatatoolDatabaseProcedures();

                        StringBuilder sb = null;
                        // we want to end up with something like "{call create_etl_tables}"
                        // iterate through the various procedures and execute them
                        for (String spName : coreDatatools) {
                            sb = new StringBuilder();
                            sb.append("{call ");
                            sb.append(spName).append("}");
                            System.out.println("Core module: currently executing++++: " + sb);
                            CallableStatement sp = connection.prepareCall(sb.toString());
                            sp.execute();
                        }

                        for (String spName : addonDatatools) {
                            sb = new StringBuilder();
                            sb.append("{call ");
                            sb.append(spName).append("}");
                            System.out.println("Addon module: currently executing: " + sb);
                            CallableStatement sp = connection.prepareCall(sb.toString());
                            sp.execute();
                        }

                        System.out.println("Successfully completed recreating Datatools procedures ... ");

                        try {

                            ResultSet resultSet = statement.executeQuery();
                            if (resultSet != null) {
                                ResultSetMetaData metaData = resultSet.getMetaData();
                                while (resultSet.next()) {
                                    Object[] row = new Object[metaData.getColumnCount()];
                                    for (int i = 1; i <= metaData.getColumnCount(); i++) {
                                        row[i - 1] = resultSet.getObject(i);
                                    }

                                    ret.add(SimpleObject.create(
                                            "script_name", row[0],
                                            "start_time", row[1] != null ? row[1].toString() : "",
                                            "stop_time", row[2] != null ? row[2].toString() : "",
                                            "status", row[3] != null ? "Pending" : "Success"
                                    ));
                                }
                            }
                            finalTx.commit();
                        } finally {
                            try {
                                if (statement != null) {
                                    statement.close();
                                }
                            } catch (Exception e) {
                            }
                        }
                    }
                });
            } catch (Exception e) {
                throw new IllegalArgumentException("Unable to execute query", e);
            } finally {
                Context.closeSession();
            }
            sampleTypeObject.put("data", ret);


        } else {

            return sampleTypeObject;

        }
        return sampleTypeObject;
    }
}
