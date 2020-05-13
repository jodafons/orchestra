$(document).ready(function () {
    setTimeout(function(){
        const mainLog = document.getElementById('mainLog');
        const mainSource = new EventSource("/logs/main", {withCredentials: true});

        const apiLog = document.getElementById('apiLog');
        const apiSource = new EventSource("/logs/api", {withCredentials: true});

        const webLog = document.getElementById('webLog');
        const webSource = new EventSource("/logs/web", {withCredentials: true});

        mainSource.onmessage = function (event) {
            // Parsing data
            const data = JSON.parse(event.data).message;
            // Getting old data
            var text = mainLog.innerHTML;
            // Appending messages
            text = data + "<br>" + text;
            // Setting text
            mainLog.innerHTML = text;
        }

        apiSource.onmessage = function (event) {
            // Parsing data
            const data = JSON.parse(event.data).message;
            // Getting old data
            var text = apiLog.innerHTML;
            // Appending messages
            text = data + "<br>" + text;
            // Setting text
            apiLog.innerHTML = text;
        }

        webSource.onmessage = function (event) {
            // Parsing data
            const data = JSON.parse(event.data).message;
            // Getting old data
            var text = webLog.innerHTML;
            // Appending messages
            text = data + "<br>" + text;
            // Setting text
            webLog.innerHTML = text;
        }
    }, 2000);
});