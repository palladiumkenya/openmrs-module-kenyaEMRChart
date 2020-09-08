package org.openmrs.module.kenyaemrCharts.fragment.controller;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.Query;
import org.hibernate.Transaction;
import org.hibernate.jdbc.Work;
import org.json.JSONObject;
import org.openmrs.Patient;
import org.openmrs.api.context.Context;
import org.openmrs.ui.framework.SimpleObject;
import org.openmrs.ui.framework.UiUtils;
import org.openmrs.ui.framework.fragment.FragmentModel;
import org.openmrs.api.db.hibernate.DbSessionFactory;
import org.springframework.web.bind.annotation.RequestParam;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * controller for pivotTableCharts fragment
 */
public class EtlManagerFragmentController {
    private final Log log = LogFactory.getLog(getClass());
    private int processCount;

    public void controller(FragmentModel model){
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
                                        "stop_time", row[2] != null? row[2].toString() : "",
                                        "status", row[3] != null? "Pending": "Success"
                                ));
                            }
                        }
                    }
                    finally {
                        try {
                            if (statement != null) {
                                statement.close();
                            }
                        }
                        catch (Exception e) {}
                    }
                }
            });
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Unable to execute query", e);
        }

        model.put("logs", ret);
    }
    public JSONObject fetchDataSets(){

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
                    ResultSet rs = stmt.executeQuery("SHOW OPEN TABLES WHERE (In_use > 0 AND `Database` LIKE 'kenyaemr_etl') OR (In_use > 0 AND `Database` LIKE 'kenyaemr_datatools');");
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
                        CallableStatement cs = connection.prepareCall("{call sp_scheduled_updates}");
                        CallableStatement dataToolStatement = connection.prepareCall("{CALL create_datatools_tables}");
                        cs.execute();
                        dataToolStatement.execute();
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
                    ResultSet rs = stmt.executeQuery("SHOW OPEN TABLES WHERE (In_use > 0 AND `Database` LIKE 'kenyaemr_etl') OR (In_use > 0 AND `Database` LIKE 'kenyaemr_datatools');");
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
                    throw new IllegalArgumentException("Unable to execute", e);
                } finally {
                    Context.closeSession();
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
                        CallableStatement dropTablesSP = connection.prepareCall("{call create_etl_tables}");
                        CallableStatement populateTableSP = connection.prepareCall("{call sp_first_time_setup}");
                        CallableStatement dataToolStatement = connection.prepareCall("{CALL create_datatools_tables}");
                        dropTablesSP.execute();
                        populateTableSP.execute();
                        dataToolStatement.execute();
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
        }else {

            return sampleTypeObject;

        }

        return sampleTypeObject;

    }
}
