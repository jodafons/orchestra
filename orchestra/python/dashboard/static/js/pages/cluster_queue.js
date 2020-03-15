$(document).ready(function () {

    const queueDiv = document.getElementById('queueDiv');
    const histoDiv = document.getElementById('historyDiv');

    const source = new EventSource("/queue", {withCredentials: true});

    source.onmessage = function (event) {
        
        // Parsing data
        const data = JSON.parse(event.data);
        const queue = data.queue;
        const history = data.history;
        
        // Building HTML string
        var textQueue = `
        <style type="text/css">
        .tg  {border-collapse:collapse;border-spacing:0;align:center;text-align:center;}
        .tg td{font-family:Arial, sans-serif;font-size:14px;padding:10px 5px;border-style:solid;border-width:1px;overflow:hidden;word-break:normal;border-color:black;}
        .tg th{font-family:Arial, sans-serif;font-size:14px;font-weight:normal;padding:10px 5px;border-style:solid;border-width:1px;overflow:hidden;word-break:normal;border-color:black;}
        .tg .tg-fp8u{background-color:#87ca87;color:#000000;border-color:#9b9b9b;text-align:center;vertical-align:top}
        .tg .tg-6jfo{font-weight:bold;background-color:#e0e0e0;color:#000000;border-color:#9b9b9b;text-align:center;vertical-align:top}
        .tg .tg-0eos{background-color:#f4f4f4;color:#000000;border-color:#9b9b9b;text-align:center;vertical-align:top}
        .tg .tg-7b6l{background-color:#d97b7b;color:#000000;border-color:#9b9b9b;text-align:center;vertical-align:top}
        .tg .tg-c4ex{font-weight:bold;background-color:#e0e0e0;color:#d08a00;border-color:#9b9b9b;text-align:center;vertical-align:top}
        .tg .tg-amri{font-weight:bold;background-color:#e0e0e0;color:#3166ff;border-color:#9b9b9b;text-align:center;vertical-align:top}
        .tg .tg-1n22{font-weight:bold;background-color:#e0e0e0;color:#00ac00;border-color:#9b9b9b;text-align:center;vertical-align:top}
        .tg .tg-b6e5{font-weight:bold;background-color:#e0e0e0;color:#fe0000;border-color:#9b9b9b;text-align:center;vertical-align:top}
        </style>
        <table class="tg" style="table-layout: fixed; width: 100%;">
          <tr>
            <th class="tg-6jfo" style="width: 60%;">TaskName</th>
            <th class="tg-6jfo">Status</th>
            <th class="tg-6jfo">Jobs</th>
            <th class="tg-amri">Registered</th>
            <th class="tg-amri">Assigned</th>
            <th class="tg-c4ex">Testing</th>
            <th class="tg-1n22">Running</th>
            <th class="tg-1n22">Done</th>
            <th class="tg-b6e5">Failed</th>
          </tr>       
        `
        // Appending queue items
        for (var i = 0; i < queue.length; i++) {
            var statusText = "";
            if (queue[i].status == 'registered') {
                statusText = '<font color="#3166ff">registered</style>';
            }
            else if (queue[i].status == 'assigned') {
                statusText = '<font color="#3166ff">assigned</style>';
            }
            else if (queue[i].status == 'testing') {
                statusText = '<font color="#d08a00">testing</style>';
            }
            else if (queue[i].status == 'running') {
                statusText = '<font color="#00ac00">running</style>';
            }
            else if (queue[i].status == 'broken') {
                statusText = '<font color="#fe0000">broken</style>';
            }
            else {
                statusText = queue[i].status;
            }
            textQueue += `
            <tr>
            <td class="tg-0eos">` + queue[i].name + `</td>
            <td class="tg-0eos">` + statusText + `</td>
            <td class="tg-0eos">` + queue[i].n_jobs + `</td>
            <td class="tg-0eos">` + queue[i].n_regs + `</td>
            <td class="tg-0eos">` + queue[i].n_asgn + `</td>
            <td class="tg-0eos">` + queue[i].n_test + `</td>
            <td class="tg-0eos">` + queue[i].n_runn + `</td>
            <td class="tg-fp8u">` + queue[i].n_done + `</td>
            <td class="tg-7b6l">` + queue[i].n_fail + `</td>
            </tr>        
            `;
        }
        textQueue += "</table>"

        // Building HTML string
        var textHisto = `
        <style type="text/css">
        .tg  {border-collapse:collapse;border-spacing:0;align:center;text-align:center;}
        .tg td{font-family:Arial, sans-serif;font-size:14px;padding:10px 5px;border-style:solid;border-width:1px;overflow:hidden;word-break:normal;border-color:black;}
        .tg th{font-family:Arial, sans-serif;font-size:14px;font-weight:normal;padding:10px 5px;border-style:solid;border-width:1px;overflow:hidden;word-break:normal;border-color:black;}
        .tg .tg-fp8u{background-color:#87ca87;color:#000000;border-color:#9b9b9b;text-align:center;vertical-align:top}
        .tg .tg-6jfo{font-weight:bold;background-color:#e0e0e0;color:#000000;border-color:#9b9b9b;text-align:center;vertical-align:top}
        .tg .tg-0eos{background-color:#f4f4f4;color:#000000;border-color:#9b9b9b;text-align:center;vertical-align:top}
        .tg .tg-7b6l{background-color:#d97b7b;color:#000000;border-color:#9b9b9b;text-align:center;vertical-align:top}
        .tg .tg-c4ex{font-weight:bold;background-color:#e0e0e0;color:#d08a00;border-color:#9b9b9b;text-align:center;vertical-align:top}
        .tg .tg-amri{font-weight:bold;background-color:#e0e0e0;color:#3166ff;border-color:#9b9b9b;text-align:center;vertical-align:top}
        .tg .tg-1n22{font-weight:bold;background-color:#e0e0e0;color:#00ac00;border-color:#9b9b9b;text-align:center;vertical-align:top}
        .tg .tg-b6e5{font-weight:bold;background-color:#e0e0e0;color:#fe0000;border-color:#9b9b9b;text-align:center;vertical-align:top}
        </style>
        <table class="tg" style="width: 100%;">
          <tr>
            <th class="tg-6jfo" style="">TaskName</th>
            <th class="tg-6jfo">Status</th>
          </tr>
        `;
        // Appending history items
        for (var i = 0; i < history.length; i++) {
            var statusText = "";
            if (history[i].status == 'registered') {
                statusText = '<font color="#3166ff">registered</style>';
            }
            else if (history[i].status == 'assigned') {
                statusText = '<font color="#3166ff">assigned</style>';
            }
            else if (history[i].status == 'testing') {
                statusText = '<font color="#d08a00">testing</style>';
            }
            else if (history[i].status == 'running') {
                statusText = '<font color="#00ac00">running</style>';
            }
            else if (history[i].status == 'done') {
                statusText = '<font color="#00ac00">done</style>';
            }
            else if (history[i].status == 'broken') {
                statusText = '<font color="#fe0000">broken</style>';
            }
            else {
                statusText = history[i].status;
            }
            textHisto += `
            <tr>
            <td class="tg-0eos">` + history[i].name + `</td>
            <td class="tg-0eos">` + statusText + `</td>
            </tr>        
            `;
        }
        textHisto += "</table>"

        // Setting text
        queueDiv.innerHTML = textQueue;
        histoDiv.innerHTML = textHisto;
    }
});
