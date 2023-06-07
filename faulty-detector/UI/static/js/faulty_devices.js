function loadButtonedTable(table_body_id, list_to_show, list_classified, endpoint){
    const faulty_table = document.getElementById(table_body_id)
    list_to_show = list_to_show.filter(device => !list_classified.includes(device))

    let faulty_count = 1

    for(index in list_to_show) {
        var faulty_row = faulty_table.insertRow(-1);
        var faulty_index_cell = faulty_row.insertCell(0);
        var faulty_device_cell = faulty_row.insertCell(1);

        let faulty_form = document.createElement('form')
        faulty_form.method = 'post'
        faulty_form.action = endpoint
        let faulty_input = document.createElement('input')
        faulty_input.type = "submit"
        faulty_input.value = list_to_show[index]
        faulty_input.id = "deviceId"
        faulty_input.name = "deviceId"
        faulty_form.appendChild(faulty_input)

        faulty_index_cell.innerHTML = faulty_count++;
        faulty_device_cell.appendChild(faulty_form);
    }
}
