<script type="text/javascript">
    jq = jQuery;

    jq(function() {
        jq("#showStatus").hide();
        jq('#refresh').click(function() {
            jq("#msgSpan").text("Refreshing ETL Tables");
            jq("#showStatus").show();
            jq("#msg").text("");

            jq("#refresh").prop("disabled", true);
            jq("#recreate").prop("disabled", true);
            jq.getJSON('${ ui.actionLink("refreshTables") }')
                .success(function(data) {
                    if(data.status) {
                        if(data.status[0].process ==="locked") {
                            jq( "#dialog-1" ).dialog( "open" );
                            jq("#showStatus").hide();

                        }
                    }else {
                        jq("#showStatus").hide();
                        jq("#msg").text("ETL tables refreshed successfully");
                        jq("#refresh").prop("disabled", false);
                        jq("#recreate").prop("disabled", false);
                        if(data.data) {
                            var processedData = data.data;
                            for (index in processedData) {
                                jq('#log_table > tbody > tr').remove();
                                var tbody = jq('#log_table > tbody');
                                for (index in processedData) {
                                    var item = processedData[index];
                                    var row = '<tr>';
                                    row += '<td width="35%">' + item.script_name + '</td>';
                                    row += '<td width="20%">' + item.start_time + '</td>';
                                    row += '<td width="20%">' + item.stop_time + '</td>';
                                    row += '<td width="20%">' + item.status + '</td>';
                                    row += '</tr>';
                                    tbody.append(row);
                                }
                            }
                        }

                    }



                })
                .error(function(xhr, status, err) {
                    jq("#showStatus").hide();
                    jq("#msg").text("There was an error refreshing ETL tables. (" + err + ")");
                    jq("#refresh").prop("disabled", false);
                    jq("#recreate").prop("disabled", false);

                    alert('AJAX error ' + err);
                })
        });

        jq('#recreate').click(function() {
            jq("#recreate").attr("disabled", true);
            jq("#msgSpan").text("Recreating ETL Tables");
            jq("#msg").text("");
            jq("#showStatus").show();
            jq("#recreate").prop("disabled", true);
            jq("#refresh").prop("disabled", true);
            jq.getJSON('${ ui.actionLink("recreateTables") }')
                .success(function(data) {
                    if(data.status) {
                        if(data.status[0].process ==="locked") {
                            jq( "#dialog-1" ).dialog( "open" );
                            jq("#showStatus").hide();

                        }

                    }else {
                        jq("#showStatus").hide();
                        jq("#msg").text("ETL tables recreated successfully");
                        jq("#recreate").prop("disabled", false);
                        jq("#refresh").prop("disabled", false);
                        if(data.data) {
                            var processedData = data.data;
                            for (index in processedData) {
                                jq('#log_table > tbody > tr').remove();
                                var tbody = jq('#log_table > tbody');
                                for (index in processedData) {
                                    var item = processedData[index];
                                    var row = '<tr>';
                                    row += '<td width="35%">' + item.script_name + '</td>';
                                    row += '<td width="20%">' + item.start_time + '</td>';
                                    row += '<td width="20%">' + item.stop_time + '</td>';
                                    row += '<td width="20%">' + item.status + '</td>';
                                    row += '</tr>';
                                    tbody.append(row);
                                }
                            }
                        }
                    }
                })
                .error(function(xhr, status, err) {
                    jq("#showStatus").hide();
                    jq("#msg").text("There was an error recreating ETL tables");
                    jq("#recreate").prop("disabled", false);
                    jq("#refresh").prop("disabled", false);
                    alert('AJAX error ' + err);
                })
        });

        jq(function() {
            jq( "#dialog-1" ).dialog({
                autoOpen: false,
            });
            jq( "#opener" ).click(function() {
                jq( "#dialog-1" ).dialog( "open" );
            });
        });

    });
</script>
<style>
table {
    width: 100%;
}

/*
thead tr {
    display: block;
}

thead, tbody {
    display: block;
}
tbody.scrollable {
    height: 400px;
    overflow-y: auto;
}*/
th, td {
    padding: 5px;
    text-align: left;
    height: 30px;
    border-bottom: 1px solid #ddd;
}
tr:nth-child(even) {background-color: #f2f2f2;}
</style>
<hr>
<div>


    <button id="refresh" style="height:43px;width:185px">
        <img src="${ ui.resourceLink("kenyaui", "images/glyphs/switch.png") }" width="32" height="32" /> Refresh Tables
    </button>

    <button id="recreate"  style="height:43px;width:185px">
        <img src="${ ui.resourceLink("kenyaui", "images/buttons/undo.png") }" width="32" height="32" /> Recreate Tables
    </button>
</div>
<br/>
<div id="showStatus">
    <span id="msgSpan"></span> &nbsp;&nbsp;<img src="${ ui.resourceLink("kenyaui", "images/loader_small.gif") }"/>
</div>
<div id="msg"></div>
<div>
    <h3>History of ETL Operations (Last 10 entries)</h3>
    <table id="log_table">
        <thead>
        <tr>
            <th>Procedure</th>
            <th>Start Time</th>
            <th>End Time</th>
            <th>Completion Status</th>
        </tr>
        </thead>
        <tbody class='scrollable'>
        <% if (logs) { %>
        <% logs.each { log -> %>
        <tr>
            <td>${ log.script_name }</td>
            <td>${ log.start_time }</td>
            <td>${ log.stop_time }</td>
            <td>${ log.status }</td>
        </tr>
        <% } %>
        <% } else { %>
        <tr>
            <td colspan="4">No record found. Please refresh for details</td>
        </tr>
        <% } %>
        </tbody>
    </table>
</div>
<div>
    <div id = "dialog-1"
         title = "Warning">Similar request is running! Please wait for the process to complete</div>
</div>



