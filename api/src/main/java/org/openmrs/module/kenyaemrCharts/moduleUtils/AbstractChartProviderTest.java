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

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Test class for AbstractChartProvider
 */
public class AbstractChartProviderTest {

	@org.junit.Test
	public void chartTypes() throws Exception {

		List<String> actualList = Arrays.asList("Histogram", "Bar chart", "Pie chart", "Line chart");
		BarChartProvider expectedList = new BarChartProvider(
				Arrays.asList("Histogram", "Bar chart", "Pie chart", "Line chart"));

		//Test for expected values
		assertEquals(actualList, expectedList.getChartsTypes());

		//		Test if contains expected value
		assertThat(expectedList.getChartsTypes(), hasItems("Histogram"));

		//		Test if it has the expected size

		assertThat(expectedList.getChartsTypes().size(), is(4));

	}

}
