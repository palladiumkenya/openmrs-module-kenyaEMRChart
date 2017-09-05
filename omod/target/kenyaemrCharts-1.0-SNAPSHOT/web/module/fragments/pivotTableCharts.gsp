<!-- CSS goes in the document HEAD or added to your external stylesheet -->
<style type="text/css">
table.gridtable {
    font-family: verdana, arial, sans-serif;
    font-size: 11px;
    color: #333333;
    border-width: 1px;
    border-color: #666666;
    border-collapse: collapse;
}

table.gridtable th {
    border-width: 1px;
    padding: 8px;
    border-style: solid;
    border-color: #666666;
    background-color: #dedede;
}

table.gridtable td {
    border-width: 1px;
    padding: 8px;
    border-style: solid;
    border-color: #666666;
    background-color: #ffffff;
}

</style>

<div>
    <table class="gridtable">
        <h1>Recommended HIV Test Kits</h1>
        <% if (simpleObjects) { %>

        <tr>
            <th>Manufacturer</th>
            <th>Sample type</th>
            <th>Storage conditions</th>
            <th>Shelf life</th>
        </tr>
        <% (simpleObjects).each { %>

        <tr>
            <td>${it.manufacturer}</td>
            <td>${it.Sample_type}</td>
            <td>${it.Storage_conditions}</td>
            <td>${it.Shelf_life}</td>

        </tr>

        <% }
        } else { %>
    </table>

    <div>
        No object to display
    </div>
    <% } %>
</div>

<div>
    <h2>Chart types to choose from</h2>
    <% if (chartTypes) { %>
    <ul>
        <% (chartTypes).each { %>

        <li>${it}</li>
    </ul>

    <% }
    } else { %>
    <div>
        No object to display
    </div>
    <% } %>

</div>






