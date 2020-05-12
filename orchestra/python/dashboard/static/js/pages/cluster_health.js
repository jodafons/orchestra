$(document).ready(function () {
  
  var textWeb = document.getElementById("health_web");
  var textApi = document.getElementById("health_api");
  var textMain= document.getElementById("health_main");
   
  const source = new EventSource("/health", {withCredentials: true});

  source.onmessage = function (event) {
    // Parsing data
    const data = JSON.parse(event.data);

    if (data.web) {
      textWeb.innerHTML = '<span style="color:MediumSeaGreen;"> healthy</span>'
    }
    else {
      textWeb.innerHTML = '<span style="color:Tomato;"> unhealthy</span>'
    }

    if (data.api) {
      textApi.innerHTML = '<span style="color:MediumSeaGreen;"> healthy</span>'
    }
    else {
      textApi.innerHTML = '<span style="color:Tomato;"> unhealthy</span>'
    }

    if (data.main) {
      textMain.innerHTML = '<span style="color:MediumSeaGreen;"> healthy</span>'
    }
    else {
      textMain.innerHTML = '<span style="color:Tomato;"> unhealthy</span>'
    }
    
  }
  
});
  
