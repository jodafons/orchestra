$(document).ready(function () {

    const mainLog = document.getElementById('mainLog');
    const mainSource = new EventSource("/logs/main", {withCredentials: true});

    mainSource.onmessage = function (event) {
        
        // Parsing data
        const data = JSON.parse(event.data).message;
        
        // Getting old data
        var text = mainLog.innerHTML;

        // Appending messages
        text = data + text;

        // Setting text
        mainLog.innerHTML = text;
    }
});