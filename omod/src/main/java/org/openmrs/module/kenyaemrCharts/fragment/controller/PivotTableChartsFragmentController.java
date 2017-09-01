package org.openmrs.module.kenyaemrCharts.fragment.controller;

import org.openmrs.ui.framework.fragment.FragmentModel;

/**
 * controller for pivotTableCharts fragment
 */
public class PivotTableChartsFragmentController {
    public void controller(FragmentModel model){

        model.put("startDate", "start date");
        model.put("endDate", "end date");

    }
}
