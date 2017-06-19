function submitForm(form) {
	try {
		var data = $(form).serializeArray();
		var formData = {};
		for (var i = 0; i < data.length; i++) {
			var key = data[i]["name"];
			var value = data[i]["value"];
			if (value) {
				var date, time;
				if (key === "tag_set" || key === "target_host_set") {
					formData[key] = value === "" ? [] : value.split(",")
				} else if (key === "date") {
					date = value;
				} else if (key === "time") {
					time = value;
				} else if (key === "data") {
					formData["data"] = JSON.parse(value)
				} else {
					formData[key] = value
				}
			}
		}
		var serializedTime = date ? (time ? date + " " + time : date) : "";
		if (serializedTime) {
			formData["event_time"] = new Date(serializedTime).getTime() / 1000;
		}
	} catch (err) {
		alert(err)
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
			alert("Error adding event: " + data.responseText);
		}
	});
	return false;
}