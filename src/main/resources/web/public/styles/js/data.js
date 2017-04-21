function loadTopics() {
	var xhttp = new XMLHttpRequest();
	xhttp.open("GET", "/api/topics", false);
	xhttp.setRequestHeader("Content-type", "application/json");
	xhttp.send();
	var topics = JSON.parse(xhttp.responseText);
	var panel = document.getElementById('topicsPanel');
	var html = '';
	for ( var topic in topics) {
		html = html + '<button type="button" class="list-group-item">' + topics[topic]
				+ '</li>'

	}
	panel.innerHTML = html;
}

loadTopics();