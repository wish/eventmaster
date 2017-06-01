 function submitQuery(form) {
	var data = $(form).serializeArray();
	var formData = {};
	for (var i = 0; i < data.length; i++) {
		var key = data[i]["name"];
		var value = data[i]["value"];
		var startEventDate, startEventTime, endEventDate, endEventTime;
		var startReceivedDate, startReceivedTime, endReceivedDate, endReceivedTime;
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
			case "startEventDate":
				startEventDate = value;
				break;
			case "startEventTime":
				startEventTime = value;
				break;
			case "endEventDate":
				endEventDate = value;
				break;
			case "endEventTime":
				endEventTime = value;
				break;
			case "startReceivedDate":
				startReceivedDate = value;
				break;
			case "startReceivedTime":
				startReceivedTime = value;
				break;
			case "endReceivedDate":
				endReceivedDate = value;
				break;
			case "endReceivedTime":
				endReceivedTime = value;
				break;
			default:
				formData[key] = value === "" ? [] : value.split(",");
		}
	}
	if (startEventDate) {
		var serializedStart = startEventTime ? startEventDate + " " + startEventTime : startEventDate + " 00:00";
		formData["start_event_time"] = new Date(serializedStart).getTime() / 1000;
	}
	if (endEventDate) {
		var serializedEnd = endEventTime ? endEventDate + " " + endEventTime : endEventDate + " 00:00";
		formData["end_event_time"] = new Date(serializedEnd).getTime() / 1000;
	}
	if (startReceivedDate) {
		var serializedStart = startReceivedTime ? startReceivedDate + " " + startReceivedTime : startReceivedDate + " 00:00";
		formData["start_received_time"] = new Date(serializedStart).getTime() / 1000;
	}
	if (endReceivedDate) {
		var serializedEnd = endReceivedTime ? endReceivedDate + " " + endReceivedTime : endReceivedDate + " 00:00";
		formData["end_received_time"] = new Date(serializedEnd).getTime() / 1000;
	}
	params = [];
	for (var key in formData) {
		if (key === "start_event_time" || key === "end_event_time" || key === "start_received_time" || key === "end_received_time" || key === "data") {
			params.push(key + "=" + formData[key])
		} else {
			for (var j = 0; j < formData[key].length; j++) {
				params.push(key + "=" + formData[key][j])
			}
		}
	}
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
						<th scope="row">`.concat(event['topic_name'],`</th>
						<td>`,event['dc'],`</td>
						<td>`,event['tag_set'],`</td>
						<td>`,new Date(event['event_time']*1000).toUTCString(),`</td>
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
	return false;
}