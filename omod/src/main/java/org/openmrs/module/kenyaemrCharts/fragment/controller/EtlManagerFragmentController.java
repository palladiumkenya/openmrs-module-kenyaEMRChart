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

import liquibase.pro.packaged.et;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * controller for etlManager fragment
 * Handles calls for refreshing and recreation of ETLs
 *
 * This refactor makes the controller tenant-aware. It derives tenant-specific schema names
 * from the runtime DB name (e.g. openmrs_tenant -> etl_tenant, datatools_tenant, dwapi_tenant, facilitywide_tenant)
 * and uses those where applicable. If a tenant-specific schema does not exist the controller
 * will fall back to the original global schema names (kenyaemr_etl, kenyaemr_datatools, dwapi_etl).
 */
public class EtlManagerFragmentController {
    private final Log log = LogFactory.getLog(getClass());

    public void controller(FragmentModel model){
        DbSessionFactory sf = Context.getRegisteredComponents(DbSessionFactory.class).get(0);

        final List<SimpleObject> ret = new ArrayList<SimpleObject>();

        try {
            sf.getCurrentSession().doWork(new Work() {

                @Override
                public void execute(Connection connection) throws SQLException {
                    Map<String, String> schemas = resolveTenantSchemas(connection);
                    String etlSchema = schemas.get("etl");

                    final String sqlSelectQuery = String.format(
                            "SELECT script_name, start_time, stop_time, error FROM %s.etl_script_status order by start_time desc limit 10;",
                            etlSchema);

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
                                        "stop_time", row[2] != null? row[2].toString() : "",
                                        "status", row[3] != null? "Pending": "Success"
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
                    Map<String, String> schemas = resolveTenantSchemas(connection);
                    String etlSchema = schemas.get("etl");

                    Statement stmt = connection.createStatement();
                    String lockQuery = String.format("SHOW OPEN TABLES WHERE (In_use > 0 AND `Database` = '%s');", etlSchema);
                    ResultSet rs = stmt.executeQuery(lockQuery);
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


            Transaction tx = null;
            try {
                Context.openSession();
                tx = sf.getHibernateSessionFactory().getCurrentSession().beginTransaction();
                final Transaction finalTx = tx;
                sf.getCurrentSession().doWork(new Work() {

                    @Override
                    public void execute(Connection connection) throws SQLException {
                        Map<String, String> schemas = resolveTenantSchemas(connection);
                        String etlSchema = schemas.get("etl");

                        final String sqlSelectQuery = String.format(
                                "SELECT script_name, start_time, stop_time, error FROM %s.etl_script_status order by start_time desc limit 10;",
                                etlSchema);
                        PreparedStatement statement = connection.prepareStatement(sqlSelectQuery);

                        ETLProcedureBuilder procedureBuilder = new ETLProcedureBuilder();
                        procedureBuilder.buildProcedures();

                        List<String> coreIncrementalUpdates = procedureBuilder.getCoreIncrementalUpdatesProcedures();
                        List<String> addonIncrementalUpdates = procedureBuilder.getAddonIncrementalUpdatesProcedures();

                        StringBuilder sb = null;
                        // execute core incremental updates in tenant ETL schema
                        // we want to end up with a string like "{call create_etl_tables}"
                        // we then iterate the various procedures and execute them
                        for (String spName : coreIncrementalUpdates) {
                            sb = new StringBuilder();
                            sb.append("{call ");
                            sb.append(etlSchema).append('.').append(spName).append("}");
                            System.out.println("Core module: currently executing: " + sb);
                            CallableStatement sp = connection.prepareCall(sb.toString());
                            sp.execute();
                        }

                        for (String spName : addonIncrementalUpdates) {
                            sb = new StringBuilder();
                            sb.append("{call ");
                            sb.append(etlSchema).append('.').append(spName).append("}");
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
                    Map<String, String> schemas = resolveTenantSchemas(connection);
                    String etlSchema = schemas.get("etl");

                    Statement stmt = connection.createStatement();
                    String lockQuery = String.format("SHOW OPEN TABLES WHERE (In_use > 0 AND `Database` = '%s');", etlSchema);
                    ResultSet rs = stmt.executeQuery(lockQuery);
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

            Transaction tx = null;
            try {
                Context.openSession();
                tx = sf.getHibernateSessionFactory().getCurrentSession().beginTransaction();
                final Transaction finalTx = tx;
                sf.getCurrentSession().doWork(new Work() {

                    @Override
                    public void execute(Connection connection) throws SQLException {
                        Map<String, String> schemas = resolveTenantSchemas(connection);
                        String etlSchema = schemas.get("etl");

                        final String sqlSelectQuery = String.format(
                                "SELECT script_name, start_time, stop_time, error FROM %s.etl_script_status order by start_time desc limit 10;",
                                etlSchema);

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
                            sb.append(etlSchema).append('.').append(spName).append("}");
                            System.out.println("Core module: currently executing: " + sb);
                            CallableStatement sp = connection.prepareCall(sb.toString());
                            sp.execute();
                        }

                        for (String spName : addonDDL) {
                            sb = new StringBuilder();
                            sb.append("{call ");
                            sb.append(etlSchema).append('.').append(spName).append("}");
                            System.out.println("Addon module: currently executing: " + sb);
                            CallableStatement sp = connection.prepareCall(sb.toString());
                            sp.execute();
                        }

                        for (String spName : coreDML) {
                            sb = new StringBuilder();
                            sb.append("{call ");
                            sb.append(etlSchema).append('.').append(spName).append("}");
                            System.out.println("Core module: currently executing: " + sb);
                            CallableStatement sp = connection.prepareCall(sb.toString());
                            sp.execute();
                        }

                        for (String spName : addonDML) {
                            sb = new StringBuilder();
                            sb.append("{call ");
                            sb.append(etlSchema).append('.').append(spName).append("}");
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
                    Map<String, String> schemas = resolveTenantSchemas(connection);
                    String etlSchema = schemas.get("etl");
                    String datatoolsSchema = schemas.get("datatools");

                    Statement stmt = connection.createStatement();
                    String lockQuery = String.format(
                            "SHOW OPEN TABLES WHERE (In_use > 0 AND `Database` = '%s') OR (In_use > 0 AND `Database` = '%s');",
                            etlSchema, datatoolsSchema);

                    ResultSet rs = stmt.executeQuery(lockQuery);
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

            Transaction tx = null;
            try {
                Context.openSession();
                tx = sf.getHibernateSessionFactory().getCurrentSession().beginTransaction();
                final Transaction finalTx = tx;
                sf.getCurrentSession().doWork(new Work() {

                    @Override
                    public void execute(Connection connection) throws SQLException {
                        Map<String, String> schemas = resolveTenantSchemas(connection);
                        String etlSchema = schemas.get("etl");
                        String datatoolsSchema = schemas.get("datatools");

                        final String sqlSelectQuery = String.format(
                                "SELECT script_name, start_time, stop_time, error FROM %s.etl_script_status order by start_time desc limit 10;",
                                etlSchema);
                        PreparedStatement statement = connection.prepareStatement(sqlSelectQuery);

                        ETLProcedureBuilder procedureBuilder = new ETLProcedureBuilder();
                        procedureBuilder.buildProcedures();

                        List<String> coreDatatools = procedureBuilder.getCoreDatatoolDatabaseProcedures();
                        List<String> addonDatatools = procedureBuilder.getAddonDatatoolDatabaseProcedures();

                        StringBuilder sb = null;
                        // iterate through the various procedures and execute them
                        // we want to end up with a string like "{call create_etl_tables}"
                        // we then iterate the various procedures and execute them
                        for (String spName : coreDatatools) {
                            sb = new StringBuilder();
                            sb.append("{call ");
                            sb.append(datatoolsSchema).append('.').append(spName).append("}");
                            System.out.println("Core module: currently executing: " + sb);
                            CallableStatement sp = connection.prepareCall(sb.toString());
                            sp.execute();
                        }

                        for (String spName : addonDatatools) {
                            sb = new StringBuilder();
                            sb.append("{call ");
                            sb.append(datatoolsSchema).append('.').append(spName).append("}");
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

    /**
     * Recreates tables for DWAPI
     * It invokes stored procedures for creating DWAPI related tables
     * @param ui
     * @return
     */
    public SimpleObject recreateDwapiTables(UiUtils ui) {
        final List<SimpleObject> ret = new ArrayList<SimpleObject>();
        final List<SimpleObject> status = new ArrayList<SimpleObject>();
        final SimpleObject sampleTypeObject = new SimpleObject();

        DbSessionFactory sf = Context.getRegisteredComponents(DbSessionFactory.class).get(0);
        sf.getCurrentSession().doWork(new Work() {

            @Override
            public void execute(Connection connection) throws SQLException {
                try {
                    Map<String, String> schemas = resolveTenantSchemas(connection);
                    String etlSchema = schemas.get("etl");
                    String datatoolsSchema = schemas.get("datatools");
                    String dwapiSchema = schemas.get("dwapi");

                    Statement stmt = connection.createStatement();
                    String lockQuery = String.format(
                            "SHOW OPEN TABLES WHERE (In_use > 0 AND `Database` = '%s') OR (In_use > 0 AND `Database` = '%s') OR (In_use > 0 AND `Database` = '%s');",
                            etlSchema, datatoolsSchema, dwapiSchema);
                    ResultSet rs = stmt.executeQuery(lockQuery);
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


            Transaction tx = null;
            try {
                Context.openSession();
                tx = sf.getHibernateSessionFactory().getCurrentSession().beginTransaction();
                final Transaction finalTx = tx;
                sf.getCurrentSession().doWork(new Work() {

                    @Override
                    public void execute(Connection connection) throws SQLException {
                        Map<String, String> schemas = resolveTenantSchemas(connection);
                        String etlSchema = schemas.get("etl");
                        String dwapiSchema = schemas.get("dwapi");

                        PreparedStatement statement = connection.prepareStatement(String.format(
                                "SELECT script_name, start_time, stop_time, error FROM %s.etl_script_status order by start_time desc limit 10;",
                                etlSchema));

                        ETLProcedureBuilder procedureBuilder = new ETLProcedureBuilder();
                        procedureBuilder.buildProcedures();

                        StringBuilder sb = null;
                        String spDwapiDDLName = "create_dwapi_tables()";
                        String spDwapiDMLName = "sp_dwapi_etl_refresh()";
                        // execute DWAPI DDL and DML in the dwapi schema
                        // we want to end up with a string like "{call create_etl_tables}"
                        // we then iterate the various procedures and execute them

                        sb = new StringBuilder();
                        sb.append("{call ");
                        sb.append(dwapiSchema).append('.').append(spDwapiDDLName).append("}");
                        System.out.println("Core module: currently executing: " + spDwapiDDLName);
                        CallableStatement sp = connection.prepareCall(sb.toString());
                        sp.execute();

                        sb = new StringBuilder();
                        sb.append("{call ");
                        sb.append(dwapiSchema).append('.').append(spDwapiDMLName).append("}");
                        System.out.println("Core module: currently executing: " + spDwapiDMLName);
                        CallableStatement spDml = connection.prepareCall(sb.toString());
                        spDml.execute();


                        System.out.println("Successfully completed recreating DWAPI tables ... ");

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

    public SimpleObject recreateFacilitywideTables(UiUtils ui) {
        final List<SimpleObject> ret = new ArrayList<SimpleObject>();
        final List<SimpleObject> status = new ArrayList<SimpleObject>();
        final SimpleObject sampleTypeObject = new SimpleObject();

        DbSessionFactory sf = Context.getRegisteredComponents(DbSessionFactory.class).get(0);
        sf.getCurrentSession().doWork(new Work() {

            @Override
            public void execute(Connection connection) throws SQLException {
                try {
                    Map<String, String> schemas = resolveTenantSchemas(connection);
                    String etlSchema = schemas.get("etl");
                    String datatoolsSchema = schemas.get("datatools");
                    String dwapiSchema = schemas.get("dwapi");

                    Statement stmt = connection.createStatement();
                    String lockQuery = String.format(
                            "SHOW OPEN TABLES WHERE (In_use > 0 AND `Database` = '%s') OR (In_use > 0 AND `Database` = '%s') OR (In_use > 0 AND `Database` = '%s');",
                            etlSchema, datatoolsSchema, dwapiSchema);
                    ResultSet rs = stmt.executeQuery(lockQuery);
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


            Transaction tx = null;
            try {
                Context.openSession();
                tx = sf.getHibernateSessionFactory().getCurrentSession().beginTransaction();
                final Transaction finalTx = tx;
                sf.getCurrentSession().doWork(new Work() {

                    @Override
                    public void execute(Connection connection) throws SQLException {
                        Map<String, String> schemas = resolveTenantSchemas(connection);
                        String etlSchema = schemas.get("etl");

                        PreparedStatement statement = connection.prepareStatement(String.format(
                                "SELECT script_name, start_time, stop_time, error FROM %s.etl_script_status order by start_time desc limit 10;",
                                etlSchema));

                        ETLProcedureBuilder procedureBuilder = new ETLProcedureBuilder();
                        procedureBuilder.buildProcedures();

                        StringBuilder sb = null;
                        String spFacilitywideDDLName = "create_facility_wide_etl_tables()";
                        String spFacilitywideDMLName = "sp_facility_wide_refresh()";
                        // execute facilitywide procs in the facility schema

                        sb = new StringBuilder();
                        sb.append("{call ");
                        sb.append(etlSchema).append('.').append(spFacilitywideDDLName).append("}");
                        System.out.println("Core module: currently executing: " + spFacilitywideDDLName);
                        CallableStatement sp = connection.prepareCall(sb.toString());
                        sp.execute();

                        sb = new StringBuilder();
                        sb.append("{call ");
                        sb.append(etlSchema).append('.').append(spFacilitywideDMLName).append("}");
                        System.out.println("Core module: currently executing: " + spFacilitywideDMLName);
                        CallableStatement spDml = connection.prepareCall(sb.toString());
                        spDml.execute();


                        System.out.println("Successfully completed recreating Facilitywide tables ... ");

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

    /**
     * Helper to derive tenant base name from runtime DB
     * e.g. openmrs_tenant -> tenant
     */
    private String getTenantBaseFromRuntime() {
        String dbName = Context.getRuntimeProperties().getProperty("connection.database");
        if (dbName != null) {
            if (dbName.startsWith("openmrs_")) {
                return dbName.substring("openmrs_".length());
            }
            // if someone configured directly as 'openmrs' or other, return as-is
            if (dbName.startsWith("openmrs")) {
                return dbName;
            }
        }
        return "kenyaemr"; // fallback base
    }

    /**
     * Resolve tenant-specific schemas to use. Checks if tenant-specific candidate schemas exist and
     * falls back to global names if they don't.
     */
    private Map<String, String> resolveTenantSchemas(Connection connection) {
        Map<String, String> map = new HashMap<String, String>();

        String tenantBase = getTenantBaseFromRuntime();

        // Candidate names (following the prefix style used earlier in examples)
        String candidateEtl = "etl_" + tenantBase;          // etl_tenant
        String candidateDatatools = "datatools_" + tenantBase; // datatools_tenant
        String candidateDwapi = "dwapi_" + tenantBase;      // dwapi_tenant
        
        // Global fallbacks (original single-tenant names)
        String fallbackEtl = "kenyaemr_etl";
        String fallbackDatatools = "kenyaemr_datatools";
        String fallbackDwapi = "dwapi_etl";

        try {
            map.put("etl", schemaExists(connection, candidateEtl) ? candidateEtl : fallbackEtl);
            map.put("datatools", schemaExists(connection, candidateDatatools) ? candidateDatatools : fallbackDatatools);
            map.put("dwapi", schemaExists(connection, candidateDwapi) ? candidateDwapi : fallbackDwapi);
        } catch (SQLException e) {
            // If something goes wrong checking information_schema, fall back to global names
            map.put("etl", fallbackEtl);
            map.put("datatools", fallbackDatatools);
            map.put("dwapi", fallbackDwapi);
        }

        return map;
    }

    private boolean schemaExists(Connection connection, String schemaName) throws SQLException {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = connection.prepareStatement("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?");
            ps.setString(1, schemaName);
            rs = ps.executeQuery();
            return rs.next();
        } finally {
            try { if (rs != null) rs.close(); } catch (Exception ex) {}
            try { if (ps != null) ps.close(); } catch (Exception ex) {}
        }
    }
}
