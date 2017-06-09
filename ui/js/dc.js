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
					var item = `<a class="list-group-item">`.concat(dcs[i], `</a>`)
					elem.innerHTML += item;
				}
			}
		}
	});
});

function submitDc(form) {
	var data = $(form).serializeArray();
	var formData = {};
	for (var i = 0; i < data.length; i++) {
		var key = data[i]["name"];
		var value = data[i]["value"];
		formData[key] = value;
	}
	$.ajax({
		type: 'POST',
		url: '/v1/dc',
		data: JSON.stringify(formData),
		dataType: "json",
		success: function(data) {
			alert("Topic added: " + data["dc_id"])
			window.location.reload();
		},
		error: function(data) {
			alert("Error adding dc: " + data.responseText);
		}
	});
	return false;
}