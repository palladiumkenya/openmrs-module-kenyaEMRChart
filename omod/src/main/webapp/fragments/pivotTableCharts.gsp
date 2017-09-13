<%
    ui.includeJavascript("kenyaemr", "controllers/account.js")
    ui.includeCss("kenyaemrCharts", "pivot.min.css", 100)
    ui.includeCss("kenyaemrCharts", "c3.min.css", 100)
    ui.includeJavascript("kenyaemrCharts", "pivot.min.js")
    ui.includeJavascript("kenyaemrCharts", "c3.min.js")
    ui.includeJavascript("kenyaemrCharts", "c3_renderer.min.js")


    def menuItems = [
            [ label: "Back to home", iconProvider: "kenyaui", icon: "buttons/back.png", label: "Back to home", href: ui.pageLink("kenyaemr", "registration/registrationHome") ]
    ]
%>
<div class="ke-page-content">
<script type="text/javascript">
    jQuery(function () {
        var tpl = jQuery.pivotUtilities.aggregatorTemplates;
        var derivers = jQuery.pivotUtilities.derivers;
        var renderers = jQuery.extend(jQuery.pivotUtilities.renderers,
            jQuery.pivotUtilities.c3_renderers);

        jQuery.getJSON(ui.fragmentActionLink('kenyaemrCharts', 'pivotTableCharts', 'fetchDataSets'), function(datim) {
            console.log("Results==>"+datim);
            jQuery("#ChartOutput").pivotUI(datim, {
                hiddenAttributes: ["Total","Id"],
                aggregators: {
                    "Sum":
                        function () { return tpl.sum()(["Total"]) },
                    "Average":
                        function () { return tpl.average()(["Total"]) },
                    "Maximum":
                        function () { return tpl.max()(["Total"]) },
                    "Minimum":
                        function () { return tpl.min()(["Total"]) },
                    "Median":
                        function () { return tpl.median()(["Total"]) }
                },
                renderers: renderers,
                cols: ["County"], rows: ["Gender"],
                rendererName: "Horizontal Bar Chart",
                rowOrder: "value_z_to_a", colOrder: "value_z_to_a"
            });
        });
    });
</script>
<div id="ChartOutput" style="padding-bottom: 5px;"></div>
    <br/><br/>
    <strong>This is new reporting page!!</strong>
</div>


