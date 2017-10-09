$(document).ready(function() {
    $('#datetimepicker').datetimepicker();
});

function submitForm(form) {
	try {
		var data = $(form).serializeArray();
		var eventTime;
		var formData = {};
		for (var i = 0; i < data.length; i++) {
			var key = data[i]["name"];
			var value = data[i]["value"];
			if (value) {
				if (key === "tag_set" || key === "target_host_set") {
					formData[key] = value === "" ? [] : value.split(",")
				} else if (key === "event_time") {
					eventTime = value;
				} else if (key === "data") {
					formData["data"] = JSON.parse(value)
				} else {
					formData[key] = value
				}
			}
		}
		if (eventTime) {
			formData["event_time"] = getTimestamp(eventTime);
		}
	} catch (err) {
		alert(err)
		return false;
	}
	
	$.ajax({
		type: 'POST',
		url: '/v1/event',
		data: JSON.stringify(formData),
		dataType: "json",
		success: function(data) {
			alert("Event added: " + data["event_id"])
		},
		error: function(data) {
			alert("Error adding event: " + JSON.parse(data.responseText).error);
		}
	});
	return false;
}
