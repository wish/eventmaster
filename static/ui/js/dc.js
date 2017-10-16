function getFormData(data) {
    var formData = {};
    for (var i = 0; i < data.length; i++) {
		var key = data[i]["name"];
		var value = data[i]["value"];
		formData[key] = value;
	}
    return formData;
}

function submitDC(form) {
	var data = $(form).serializeArray();
	var formData = getFormData(data);
	$.ajax({
		type: 'POST',
		url: '/v1/dc',
		data: JSON.stringify(formData),
		dataType: "json",
		success: function(data) {
			alert("DC added: " + data["dc_id"]);
			window.location.reload();
		},
		error: function(data) {
			alert("Error adding dc: " + JSON.parse(data.responseText).error);
		}
	});
	return false;
}

function updateDC(form, oldDC) {
    var data = $(form).serializeArray();
    var formData = getFormData(data);
    $.ajax({
        type: 'PUT',
        url: '/v1/dc/' + oldDC,
        data: JSON.stringify(formData),
        dataType: "json",
        success: function(data) {
            alert("DC updated: " + data["dc_id"]);
            window.location.reload();
        },
        error: function(data) {
            alert("Error adding dc: " + JSON.parse(data.responseText).error);
        }
    });
    return false;
}

$(document).ready(function() {
	$.ajax({
		type: 'GET',
		url: '/v1/dc',
		dataType: "json",
		success: function(data) {
			var dcs = data['results'];
			var elem = document.getElementById("dc_list");
			elem.innerHTML = "";
			if (dcs) {
				for (var i = 0; i < dcs.length; i++) {
                    var name = dcs[i]['dc_name'];
                    var dcId = dcs[i]['dc_id'];
					var item = `<div class="panel panel-default">`.concat(
                        `<div class="panel-heading"><h4 class="panel-title"><a data-toggle="collapse" href="#updateForm`, i, `">`,
                        name, '</a></h4></div>',
                    `<div id="updateForm`, i, `" class="collapse">
                        <label>ID:`, dcId, `</label>
                        <form onsubmit="return updateDC(this,'`, name, `')">
                            <div class="form-group">
                                <label for="dc_name">New DC name</label>
                                <input type="text" class="form-control" name="dc_name">
                            </div>
                            <button class="btn btn-default" type="submit">Update</button>
                        </form>
                    </div>
                    </div>`)
					elem.innerHTML += item;
				}
			}
		}
	});
});

