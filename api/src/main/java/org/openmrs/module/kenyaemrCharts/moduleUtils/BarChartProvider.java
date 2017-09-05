/* ***** BEGIN LICENSE BLOCK *****
 * Version: MPL 1.1/GPL 2.0/LGPL 2.1
 *
 * The contents of this file are subject to the Mozilla Public License Version
 * 1.1 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS IS" basis,
 * WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
 * for the specific language governing rights and limitations under the
 * License.
 *
 * The Original Code is part of dcm4che, an implementation of DICOM(TM) in
 * Java(TM), hosted at https://github.com/gunterze/dcm4che.
 *
 * The Initial Developer of the Original Code is
 * Agfa Healthcare.
 * Portions created by the Initial Developer are Copyright (C) 2012
 * the Initial Developer. All Rights Reserved.
 */

package org.openmrs.module.kenyaemrCharts.moduleUtils;

import java.util.List;

/**
 * Bar chart class one of the chart type
 */
public class BarChartProvider extends AbstractChartProvider {

	private List<String> charts;

	public BarChartProvider(List<String> charts) {
		this.charts = charts;
	}

	//implement the abstract method to return array list of chart types
	@Override
	public List<String> getChartsTypes() {
		if (charts != null) {
			return charts;
		}
		return null;
	}
}
