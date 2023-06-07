function loadServerUnitTimesTable(table_body_id, list_unit_server_times) {
    const unit_server_time_table = document.getElementById(table_body_id)

    for(index in list_unit_server_times) {
        var row = unit_server_time_table.insertRow(-1);
        var unit_time_cell = row.insertCell(0);
        var server_time_cell = row.insertCell(1);

        unit_time_cell.innerHTML = list_unit_server_times[index][0];
        server_time_cell.innerHTML = list_unit_server_times[index][1];
    }
}

function setVisibility(id, visibility) {
    document.getElementById(id).style.display = visibility;
}