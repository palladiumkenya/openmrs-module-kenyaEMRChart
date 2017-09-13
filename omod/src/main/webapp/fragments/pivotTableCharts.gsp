<%
    ui.includeCss("kenyaemrCharts", "pivot.min.css", 100)
    ui.includeCss("kenyaemrCharts", "c3.min.css", 100)
    ui.includeJavascript("kenyaemrCharts", "d3.min.js", 50)
    ui.includeJavascript("kenyaemrCharts", "c3.min.js",49 )
    ui.includeJavascript("kenyaemrCharts", "pivot.min.js", 48)
    ui.includeJavascript("kenyaemrCharts", "c3_renderers.min.js", 47)
%>
<div class="ke-page-content">
<script type="text/javascript">
    jq = jQuery;
    jq(function () {

        var tpl = jq.pivotUtilities.aggregatorTemplates;
        var derivers = jq.pivotUtilities.derivers;
        var renderers = jq.extend(jq.pivotUtilities.renderers, jq.pivotUtilities.c3_renderers);

        jq("#ChartOutput").pivotUI(
            [
                {
                    "Gender": "F",
                    "AgeGroup": "<1",
                    "StartARTDateEOM": "2004-02-29",
                    "FacilityName": "Kikuyu (PCEA) Hospital",
                    "County": "Kiambu",
                    "SubCounty": "Kikuyu",
                    "ImplementingMechnanism": "CHAP",
                    "Agency": "HHS/CDC",
                    "SiteCode": 10603,
                    "Total": 1
                },
                {
                    "Gender": "F",
                    "AgeGroup": "<1",
                    "StartARTDateEOM": "2004-03-31",
                    "FacilityName": "Matayos Health Centre",
                    "County": "Busia",
                    "SubCounty": "Nambale",
                    "ImplementingMechnanism": "AMPATH plus",
                    "Agency": "USAID",
                    "SiteCode": 16004,
                    "Total": 1
                },
                {
                    "Gender": "F",
                    "AgeGroup": "<1",
                    "StartARTDateEOM": "2004-10-31",
                    "FacilityName": "St Camillus Karungu",
                    "County": "Taita Taveta",
                    "SubCounty": "Taveta",
                    "ImplementingMechnanism": "APHIA plus Pwani",
                    "Agency": "USAID",
                    "SiteCode": 11840,
                    "Total": 7
                },
                {
                    "Gender": "F",
                    "AgeGroup": "<1",
                    "StartARTDateEOM": "2004-11-30",
                    "FacilityName": "St Monica Hospital",
                    "County": "Kisumu",
                    "SubCounty": "Kisumu East",
                    "ImplementingMechnanism": "Kenya Conference of Catholic Bishops (KCCB)",
                    "Agency": "HHS/CDC",
                    "SiteCode": 14120,
                    "Total": 3
                },
                {
                    "Gender": "F",
                    "AgeGroup": "<1",
                    "StartARTDateEOM": "2004-12-31",
                    "FacilityName": "Kikuyu (PCEA) Hospital",
                    "County": "Kiambu",
                    "SubCounty": "Kikuyu",
                    "ImplementingMechnanism": "CHAP",
                    "Agency": "HHS/CDC",
                    "SiteCode": 10603,
                    "Total": 2
                },
                {
                    "Gender": "F",
                    "AgeGroup": "<1",
                    "StartARTDateEOM": "2004-12-31",
                    "FacilityName": "St Camillus Karungu",
                    "County": "Taita Taveta",
                    "SubCounty": "Taveta",
                    "ImplementingMechnanism": "APHIA plus Pwani",
                    "Agency": "USAID",
                    "SiteCode": 11840,
                    "Total": 4
                },
                {
                    "Gender": "F",
                    "AgeGroup": "<1",
                    "StartARTDateEOM": "2005-01-31",
                    "FacilityName": "Homa Bay District Hospital",
                    "County": "Homabay",
                    "SubCounty": "Homa Bay",
                    "ImplementingMechnanism": "EGPAF Timiza",
                    "Agency": "HHS/CDC",
                    "SiteCode": 13608,
                    "Total": 1
                },
                {
                    "Gender": "F",
                    "AgeGroup": "<1",
                    "StartARTDateEOM": "2005-02-28",
                    "FacilityName": "St Monica Hospital",
                    "County": "Kisumu",
                    "SubCounty": "Kisumu East",
                    "ImplementingMechnanism": "Kenya Conference of Catholic Bishops (KCCB)",
                    "Agency": "HHS/CDC",
                    "SiteCode": 14120,
                    "Total": 3
                },
                {
                    "Gender": "F",
                    "AgeGroup": "<1",
                    "StartARTDateEOM": "2005-03-31",
                    "FacilityName": "Kikuyu (PCEA) Hospital",
                    "County": "Kiambu",
                    "SubCounty": "Kikuyu",
                    "ImplementingMechnanism": "CHAP",
                    "Agency": "HHS/CDC",
                    "SiteCode": 10603,
                    "Total": 3
                },
                {
                    "Gender": "F",
                    "AgeGroup": "<1",
                    "StartARTDateEOM": "2005-04-30",
                    "FacilityName": "Kakamega Provincial General Hospital (PGH)",
                    "County": "Kakamega",
                    "SubCounty": "Kakamega Central (Lurambi)",
                    "ImplementingMechnanism": "APHIA plus Nyanza/Western",
                    "Agency": "USAID",
                    "SiteCode": 15915,
                    "Total": 1
                },
                {
                    "Gender": "F",
                    "AgeGroup": "<1",
                    "StartARTDateEOM": "2005-04-30",
                    "FacilityName": "Narok County Referral Hospital",
                    "County": "Narok",
                    "SubCounty": "Narok North",
                    "ImplementingMechnanism": "APHIA plus Rift Valley",
                    "Agency": "USAID",
                    "SiteCode": 15311,
                    "Total": 1
                },
                {
                    "Gender": "F",
                    "AgeGroup": "<1",
                    "StartARTDateEOM": "2005-04-30",
                    "FacilityName": "St Monica Hospital",
                    "County": "Kisumu",
                    "SubCounty": "Kisumu East",
                    "ImplementingMechnanism": "Kenya Conference of Catholic Bishops (KCCB)",
                    "Agency": "HHS/CDC",
                    "SiteCode": 14120,
                    "Total": 1
                },
                {
                    "Gender": "F",
                    "AgeGroup": "<1",
                    "StartARTDateEOM": "2005-07-31",
                    "FacilityName": "Kikuyu (PCEA) Hospital",
                    "County": "Kiambu",
                    "SubCounty": "Kikuyu",
                    "ImplementingMechnanism": "CHAP",
                    "Agency": "HHS/CDC",
                    "SiteCode": 10603,
                    "Total": 2
                },
                {
                    "Gender": "F",
                    "AgeGroup": "<1",
                    "StartARTDateEOM": "2005-07-31",
                    "FacilityName": "St Camillus Karungu",
                    "County": "Taita Taveta",
                    "SubCounty": "Taveta",
                    "ImplementingMechnanism": "APHIA plus Pwani",
                    "Agency": "USAID",
                    "SiteCode": 11840,
                    "Total": 1
                }
            ],
            {
                rows: ["Gender"],
                cols: ["AgeGroup"],
                renderers: renderers,
                rendererName: "Horizontal Bar Chart",
                rowOrder: "value_z_to_a", colOrder: "value_z_to_a"
            }
        );
    });
</script>
<div id="ChartOutput" style="padding-bottom: 5px;"></div>
</div>


