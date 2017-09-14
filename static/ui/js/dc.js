function getFormData(data) {
    var formData = {};
    for (var i = 0; i < data.length; i++) {
		var key = data[i]["name"];
		var value = data[i]["value"];
		formData[key] = value;
	}
    return formData;
}

function submitDc(form) {
	var data = $(form).serializeArray();
	var formData = getFormData(data);
	$.ajax({
		type: 'POST',
		url: '/v1/dc',
		data: JSON.stringify(formData),
		dataType: "json",
		success: function(data) {
			alert("Dc added: " + data["dc_id"]);
			window.location.reload();
		},
		error: function(data) {
			alert("Error adding dc: " + data.responseText);
		}
	});
	return false;
}

function updateDc(form, oldDc) {
    var data = $(form).serializeArray();
    var formData = getFormData(data);
    $.ajax({
        type: 'PUT',
        url: '/v1/dc/' + oldDc,
        data: JSON.stringify(formData),
        dataType: "json",
        success: function(data) {
            alert("Dc updated: " + data["dc_id"]);
            window.location.reload();
        },
        error: function(data) {
            alert("Error adding dc: " + data.responseText);
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
                        <form onsubmit="return updateDc(this,'`, name, `')">
                            <div class="form-group">
                                <label for="dc">New DC name</label>
                                <input type="text" class="form-control" name="dc">
                            </div>
                            <span class="input-group-btn"><button class="btn btn-default" type="submit">Update</button></span>
                        </form>
                    </div>
                    </div>`)
					elem.innerHTML += item;
				}
			}
		}
	});
});

