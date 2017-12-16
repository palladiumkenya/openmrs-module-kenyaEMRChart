<script type="text/javascript">
    jq = jQuery;

    jq(function() {
        jq('#refresh').click(function() {
            jq("#refresh").attr("disabled", true);
            //jq("#recreate").attr("disabled", true);
            jq.getJSON('${ ui.actionLink("refreshTables") }')
                .success(function(data) {
                    for (index in data) {
                        jq('#log_table > tbody > tr').remove();
                        var tbody = jq('#log_table > tbody');
                        for (index in data) {
                            var item = data[index];
                            var row = '<tr>';
                            row += '<td width="35%">' + item.script_name + '</td>';
                            row += '<td width="15%">' + item.start_time + '</td>';
                            row += '<td width="15%">' + item.stop_time + '</td>';
                            row += '<td width="15%">' + item.status + '</td>';
                            row += '</tr>';
                            tbody.append(row);
                        }
                    }
                })
                .error(function(xhr, status, err) {
                    alert('AJAX error ' + err);
                })
            jq("#refresh").attr("disabled", false);
            //jq("#recreate").attr("disabled", false);
        });

        jq('#recreate').click(function() {
            jq("#recreate").attr("disabled", true);
            //jq("#refresh").attr("disabled", true);
            jq.getJSON('${ ui.actionLink("recreateTables") }')
                .success(function(data) {
                    for (index in data) {
                        jq('#log_table > tbody > tr').remove();
                        var tbody = jq('#log_table > tbody');
                        for (index in data) {
                            var item = data[index];
                            var row = '<tr>';
                            row += '<td width="35%">' + item.script_name + '</td>';
                            row += '<td width="15%">' + item.start_time + '</td>';
                            row += '<td width="15%">' + item.stop_time + '</td>';
                            row += '<td width="15%">' + item.status + '</td>';
                            row += '</tr>';
                            tbody.append(row);
                        }
                    }
                })
                .error(function(xhr, status, err) {
                    alert('AJAX error ' + err);
                })
            jq("#recreate").attr("disabled", false);
            //jq("#refresh").attr("disabled", false);
        });

    });
</script>

<br/>
<hr>
<div>

    <button id="refresh">
        <img src="${ ui.resourceLink("kenyaui", "images/glyphs/ok.png") }" /> Refresh Tables
    </button>

    <br/>
    <br/>

    <button id="recreate">
        <img src="${ ui.resourceLink("kenyaui", "images/glyphs/ok.png") }" /> Recreate Tables
    </button>
</div>
<br/>
<br/>
<div id="msg"></div>
<div>
    <h3>Audit Trail</h3>
    <table id="log_table">
        <thead>
        <tr>
            <td width="35%"><b>Procedure</b></td>
            <td width="15%"><b>Start Time</b></td>
            <td width="15%"><b>End Time</b></td>
            <td width="15%"><b>Completion Status</b></td>
        </tr>
        </thead>
        <tbody>
        <% if (logs) { %>
        <% logs.each { log -> %>
        <tr>
            <td width="35%">${ log.script_name }</td>
            <td width="15%">${ log.start_time }</td>
            <td width="15%">${ log.stop_time }</td>
            <td width="15%">${ log.status }</td>
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



