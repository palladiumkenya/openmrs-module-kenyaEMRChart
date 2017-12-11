package org.openmrs.module.kenyaemrCharts.fragment.controller;

import org.codehaus.jackson.map.util.JSONPObject;
import org.json.JSONObject;
import org.openmrs.Patient;
import org.openmrs.api.context.Context;
import org.openmrs.ui.framework.fragment.FragmentModel;

import java.util.List;

/**
 * controller for pivotTableCharts fragment
 */
public class PivotTableChartsFragmentController {
    public void controller(FragmentModel model){


    }
    public JSONObject fetchDataSets(){

        List<Patient> allPatients = Context.getPatientService().getAllPatients();
        JSONObject x = new JSONObject();
        x.put("patients", allPatients);
        return x;
    }
}
