var params = [];
var sortFields = 0;
var querySuccess = true;

function updateResults() {
	$.ajax({
		type: "GET",
		url: "/v1/event?"+params.join("&"),
		dataType: "json",
		success: function(data) {
            querySuccess = true;
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
						<td>`,event['parent_event_id'],`</td>
					</tr>
                    <tr>
                        <td colspan="9"><pre>Data: `,JSON.stringify(event['data'],null,4),`</pre></td>
                    </tr>`)
					elem.innerHTML += item;
                    $("td[colspan=9]").find("pre").hide();
				}
			}
		},
		error: function(data) {
            querySuccess = false;
			alert("Error querying events: " + data.responseText);
		}
	});
}

function backgroundUpdate() {
	if (querySuccess && document.getElementById("refreshCheckbox").checked) {
		updateResults()
	}
	setTimeout(backgroundUpdate, 5000)
}


$(document).ready(function() {
    $('#starttimepicker').datetimepicker();
    $('#endtimepicker').datetimepicker();
	backgroundUpdate()
});

$("#menu-toggle").click(function(e) {
	e.preventDefault();
	$("#wrapper").toggleClass("toggled");
});

$(function() {
    $("tbody").click(function(event) {
        event.stopPropagation();
        document.getElementById("refreshCheckbox").checked = false;
        var $target = $(event.target);
        if ( $target.closest("td").attr("colspan") > 1 ) {
            $target.slideUp();
        } else {
            $target.closest("tr").next().find("pre").slideToggle();
        }
    });
});

function submitQuery(form) {
	var data = $(form).serializeArray();
	var formData = {};
    var sortFields = [];
    var sortAscending = [];
	var startEventTime, endEventTime;
	for (var i = 0; i < data.length; i++) {
		var key = data[i]["name"];
		var value = data[i]["value"];
		if (value) {
            if (key.startsWith("sort_field")) {
                sortFields.push(value);
            } else if (key.startsWith("sort_ascending")) {
                if (value.toLowerCase() === "t" || value.toLowerCase() === "true") {
                    sortAscending.push("true")
                } else {
                    sortAscending.push("false")
                }
            } else {
                switch(key) {
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
	}

    if (sortFields.length > 0) {
        formData["sort_field"] = sortFields;
        formData["sort_ascending"] = sortAscending;
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

function addSortField() {
    var newSortField = document.createElement('div');
    var html = `<div class="col-sm-6"><select name="sort_field` + sortFields + `" class="form-control" style="display: inline-block;">
            <option value=""></option>
            <option value="topic">topic</option>
            <option value="dc">dc</option>
            <option value="host">host</option>
            <option value="target_host_set">target host set</option>
            <option value="user">user</option>
            <option value="tag_set">tag</option>
            <option value="parent_event_id">parent event id</option>
            <option value="event_time">event time</option>
        </select></div>
        <div class="col-sm-6"><select name="sort_ascending` + sortFields + `" class="form-control" style="display: inline-block;">
            <option value="true">ascending</option>
            <option value="false">descending</option>
        </select></div>`;
    newSortField.innerHTML = html;
    sortFields++;
    document.getElementById("sortFields").appendChild(newSortField);
}
