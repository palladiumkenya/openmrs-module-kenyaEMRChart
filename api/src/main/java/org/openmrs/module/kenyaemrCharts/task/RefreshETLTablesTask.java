/**
 * This Source Code Form is subject to the terms of the Mozilla Public License,
 * v. 2.0. If a copy of the MPL was not distributed with this file, You can
 * obtain one at http://mozilla.org/MPL/2.0/. OpenMRS is also distributed under
 * the terms of the Healthcare Disclaimer located at http://openmrs.org/license.
 *
 * Copyright (C) OpenMRS Inc. OpenMRS is a registered trademark and the OpenMRS
 * graphic logo is a trademark of OpenMRS Inc.
 */
package org.openmrs.module.kenyaemrCharts.task;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.Transaction;
import org.hibernate.jdbc.Work;
import org.openmrs.Role;
import org.openmrs.User;
import org.openmrs.api.context.Context;
import org.openmrs.api.db.hibernate.DbSessionFactory;
import org.openmrs.notification.Alert;
import org.openmrs.scheduler.tasks.AbstractTask;
import org.openmrs.ui.framework.SimpleObject;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

/**
 * Periodically refreshes ETL tables
 */
public class RefreshETLTablesTask extends AbstractTask {

	private Log log = LogFactory.getLog(getClass());

	/**
	 * @see AbstractTask#execute()
	 */
	public void execute() {
		Context.openSession();

		DbSessionFactory sf = Context.getRegisteredComponents(DbSessionFactory.class).get(0);

		Transaction tx = null;
		try {

			if (!Context.isAuthenticated()) {
				authenticate();
			}

			tx = sf.getHibernateSessionFactory().getCurrentSession().beginTransaction();
			final Transaction finalTx = tx;
			sf.getCurrentSession().doWork(new Work() {

				@Override
				public void execute(Connection connection) throws SQLException {

			CallableStatement cs = connection.prepareCall("{call sp_scheduled_updates}");
			CallableStatement dataToolStatement = connection.prepareCall("{CALL create_datatools_tables}");
			cs.execute();
			dataToolStatement.execute();

				}
			});
			finalTx.commit();
		}
		catch (Exception e) {
			throw new IllegalArgumentException("Unable to execute query", e);
		} finally {
			Context.closeSession();
		}
	}
	
}
