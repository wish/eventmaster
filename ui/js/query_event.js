var params = [];

function updateResults() {
	$.ajax({
		type: "GET",
		url: "/v1/event?"+params.join("&"),
		dataType: "json",
		success: function(data) {
			var elem = document.getElementById("event_table")
			elem.innerHTML = "";
			var results = data["results"];
			if (results) {
				for (var i = 0; i < results.length; i++) {
					var event = results[i];
					var item =
					`<tr>
						<td>`.concat(event['event_id'],`</td>
						<th scope="row">`,event['topic_name'],`</th>
						<td>`,event['dc'],`</td>
						<td>`,event['tag_set'],`</td>
						<td>`,new Date(event['event_time']*1000).toString(),`</td>
						<td>`,event['host'],`</td>
						<td>`,event['target_host_set'],`</td>
						<td>`,event['user'],`</td>
						<td>`,JSON.stringify(event['data']),`</td>
						<td>`,event['parent_event_id'],`</td>
					</tr>`)
					elem.innerHTML += item;
				}
			}
		},
		error: function(data) {
			alert("Error querying events: " + data.responseText);
		}
	});
}

function backgroundUpdate() {
	if (document.getElementById("refreshCheckbox").checked) {
		updateResults()
	}
	setTimeout(backgroundUpdate, 5000)
}


$(document).ready(function() {
    $('#starttimepicker').datetimepicker();
    $('#endtimepicker').datetimepicker();
	backgroundUpdate()
});

function submitQuery(form) {
	var data = $(form).serializeArray();
	var formData = {};
	var startEventTime, endEventTime;
	for (var i = 0; i < data.length; i++) {
		var key = data[i]["name"];
		var value = data[i]["value"];
		if (value) {
			switch(key) {
				case "sort_ascending":
					var sortArr = value === "" ? [] : value.split(",")
					sortArr.map(function(v) {
						if (v.toLowerCase() === "t" || v.toLowerCase() === "true") {
							return true;
						}
						return false;
					})
					formData[key] = sortArr;
					break;
				case "data":
					formData[key] = value
					break;
				case "startEventTime":
					startEventTime = value;
					break;
				case "endEventTime":
					endEventTime = value;
					break;
				default:
					formData[key] = value === "" ? [] : value.split(",");
			}
		}
	}

	if (startEventTime) {
	    formData["start_event_time"] = getTimestamp(startEventTime);
	}
	if (endEventTime) {
	    formData["end_event_time"] = getTimestamp(endEventTime);
	}
	params = [];
	for (var key in formData) {
		if (key === "start_event_time" || key === "end_event_time" || key == "data") {
			if (formData[key] != "") {
				params.push(key + "=" + formData[key])
			}
		} else {
			for (var j = 0; j < formData[key].length; j++) {
				params.push(key + "=" + formData[key][j])
			}
		}
	}
	updateResults()
	return false;
}
