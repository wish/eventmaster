function getFormData(data) {
    var formData = {};
    for (var i = 0; i < data.length; i++) {
        var key = data[i]["name"];
	    var value = data[i]["value"];
		if (value) {
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
    }
    return formData;
}

function submitTopic(form) {
    var formData = {};
	try {
		var data = $(form).serializeArray();
        formData = getFormData(data);
	} catch (err) {
		alert(err);
		return false;
	}

	$.ajax({
		type: 'POST',
		url: '/v1/topic',
		data: JSON.stringify(formData),
		dataType: "json",
		success: function(data) {
			alert("Topic added: " + data["topic_id"]);
			window.location.reload();
		},
		error: function(data) {
			alert("Error adding topic: " + data.responseText);
		}
	});

	return false;
}

function updateTopic(form, oldTopicName) {
    var formData = {};
    try {
        var data = $(form).serializeArray();
        formData = getFormData(data);
    } catch (err) {
        alert(err);
        return false;
    }

    $.ajax({
        type: 'PUT',
        url: 'v1/topic/' + oldTopicName,
        data: JSON.stringify(formData),
        dataType: 'json',
        success: function(data) {
            alert("Topic updated: " + data["topic_id"]);
            window.location.reload();
        },
        error: function(data) {
            alert("Error updating topic: " + data.responseText);
        }
    });
}

function deleteTopic(topicName) {
    $.ajax({
        type: 'DELETE',
        url: 'v1/topic/' + topicName,
        success: function(data) {
            alert("Topic deleted: " + topicName);
            window.location.reload();
        },
        error: function(data) {
            alert("Error deleting topic: " + data.responseText);
        }
    });
}

$(document).ready(function() {
	$.ajax({
		type: 'GET',
		url: '/v1/topic',
		dataType: "json",
		success: function(data) {
			var results = data['results'];
			var elem = document.getElementById("topic_list");
			elem.innerHTML = "";
			if (results) {
				for (var i = 0; i < results.length; i++) {
					topicName = results[i]['topic_name'];
					schema = JSON.stringify(results[i]['data_schema'], null, 2);
					var inner = `
					<div class="panel panel-default">`.concat(
                        `<div class="panel-heading"><h4 class="panel-title"><a data-toggle="collapse" href="#updateForm`, i, `">`,
                            topicName, '</a></h4></div>',
                        `<div id="updateForm`, i, `" class="collapse">
                            <label>ID: `, results[i]['topic_id'],`</label>
                            <form onsubmit="return updateTopic(this,'`, topicName, `')">
                                <div class="form-group">
                                    <label for="topic_name">New Topic Name</label>
                                    <input type="text" class="form-control" name="topic_name">
                                 </div>
                                 <div class="form-group">
                                    <label for="data_schema">Topic Schema</label>
                                    <textarea name="data_schema" class="form-control">`, schema, `</textarea>
                                </div>
                                <span class="input-group-btn"><button class="btn btn-default" type="submit">Update</button></span>
                            </form>
                            <span class="input-group-btn">
                                <button class="btn btn-danger" data-toggle="modal" data-target="#confirm-delete" data-topic-name="`, topicName, `">Delete</button>
                            </span>
                        </div>
				    </div>`);
					elem.innerHTML += inner;
				}
			}
		}
	});

    $('#confirm-delete').on('show.bs.modal', function(e) {
        var name = $(e.relatedTarget).data('topic-name');
        $('#confirm-delete').on('click', '.btn-danger', function(e){
            return deleteTopic(name);
        })
    })

});

