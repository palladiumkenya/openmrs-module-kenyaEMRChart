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

package org.openmrs.module.kenyaemrCharts.fragment.controller;

import org.openmrs.module.kenyaemrCharts.moduleUtils.BarChartProvider;
import org.openmrs.ui.framework.SimpleObject;
import org.openmrs.ui.framework.fragment.FragmentModel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * controller for pivotTableCharts fragments
 */
public class PivotTableChartsFragmentController {
    public void controller(FragmentModel model){
//	    (1) creating a list variable of SimpleObject
	    List<SimpleObject> simpleObjects = new ArrayList<SimpleObject>();

//	    (2) create SimpleObject item
	    simpleObjects.add( SimpleObject.create(
			    "manufacturer", "Trinity Biotech",
			    "Sample_type", "Capillus",
			    "Storage_conditions", "2-8",
			    "Shelf_life" , 43,
			    "kits_distributed","521,000"));

//	    (3) Adding items to the simpleObjects
	    simpleObjects.add( SimpleObject.create(
			    "manufacturer", "TBio-Rad",
			    "Sample_type", "plasma",
			    "Storage_conditions", "15-30",
			    "Shelf_life" , 12,
			    "kits_distributed","633,000"
	    ));

	    simpleObjects.add( SimpleObject.create(
			    "manufacturer", "Biolytical",
			    "Sample_type", "serum",
			    "Storage_conditions", "2-30",
			    "Shelf_life" , 15,
			    "kits_distributed","800,000"
	    ));

	    simpleObjects.add( SimpleObject.create(
			    "manufacturer", "Chembio",
			    "Sample_type", "Capillus",
			    "Storage_conditions", "2-8",
			    "Shelf_life" , 52,
			    "kits_distributed","200,000"
	    ));
	    simpleObjects.add( SimpleObject.create(
			    "manufacturer", "Uni-Gold",
			    "Sample_type", "blood (WB)",
			    "Storage_conditions", "7-11",
			    "Shelf_life" , 52,
			    "kits_distributed","1000,000"
	    ));

//	    (4) Adding the list variable to the model
	    model.put("simpleObjects", simpleObjects);


//	    instantiate the BarChartProvider class to extend the AbstractChartProvider and provide the chart types

	    BarChartProvider barChartProvider = new BarChartProvider(Arrays.asList("Histogram", "Bar chart", "Pie chart",
			    "Line chart"));


//	    add chart types to model
	    model.put("chartTypes",barChartProvider.getChartsTypes());


    }
}
