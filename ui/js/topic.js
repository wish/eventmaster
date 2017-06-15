$(document).ready(function() {
	$.ajax({
		type: 'GET',
		url: '/v1/topic',
		dataType: "json",
		success: function(data) {
			var results = data['results'];
			var elem = document.getElementById("topic_list")
			elem.innerHTML = "";
			if (results) {
				for (var i = 0; i < results.length; i++) {
					topicName = results[i]['topic_name'];
					schema = JSON.stringify(results[i]['data_schema']);
					var inner = `
					<div class="panel panel-default">
					    <div class="panel-heading">`.concat(topicName, `</div>
					    <div class="panel-body">`, schema, `</div>
				    </div>`);
					elem.innerHTML += inner;
				}
			}

		}
	});
});

function submitTopic(form) {
	var data = $(form).serializeArray();
	var formData = {};
	for (var i = 0; i < data.length; i++) {
		var key = data[i]["name"];
		var value = data[i]["value"];
		if (key === "data_schema") {
            if (value) {
			    formData[key] = JSON.parse(value)
            } else {
                formData[key] = {}
            }
		} else {
			formData[key] = value;
		}
	}
	$.ajax({
		type: 'POST',
		url: '/v1/topic',
		data: JSON.stringify(formData),
		dataType: "json",
		success: function(data) {
			alert("Topic added: " + data["topic_id"])
			window.location.reload();
		},
		error: function(data) {
			alert("Error adding topic: " + data.responseText);
		}
	});
	return false;
}
