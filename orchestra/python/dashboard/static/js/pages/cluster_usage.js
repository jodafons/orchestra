$(document).ready(function () {

    Chart.pluginService.register({
          beforeDraw: function (chart) {
              if (chart.config.options.elements.center) {
          //Get ctx from string
          var ctx = chart.chart.ctx;
          
                  //Get options from the center object in options
          var centerConfig = chart.config.options.elements.center;
            var fontStyle = centerConfig.fontStyle || 'Arial';
                  var txt = centerConfig.text;
          var color = centerConfig.color || '#000';
          var sidePadding = centerConfig.sidePadding || 20;
          var sidePaddingCalculated = (sidePadding/100) * (chart.innerRadius * 2)
          //Start with a base font of 30px
          ctx.font = "30px " + fontStyle;
          
                  //Get the width of the string and also the width of the element minus 10 to give it 5px side padding
          var stringWidth = ctx.measureText(txt).width;
          var elementWidth = (chart.innerRadius * 2) - sidePaddingCalculated;
  
          // Find out how much the font can grow in width.
          var widthRatio = elementWidth / stringWidth;
          var newFontSize = Math.floor(30 * widthRatio);
          var elementHeight = (chart.innerRadius * 2);
  
          // Pick a new font size so it will not be larger than the height of label.
          var fontSizeToUse = Math.min(newFontSize, elementHeight);
  
                  //Set font settings to draw it correctly.
          ctx.textAlign = 'center';
          ctx.textBaseline = 'middle';
          var centerX = ((chart.chartArea.left + chart.chartArea.right) / 2);
          var centerY = ((chart.chartArea.top + chart.chartArea.bottom) / 2);
          ctx.font = fontSizeToUse+"px " + fontStyle;
          ctx.fillStyle = color;
          
          //Draw text in center
          ctx.fillText(txt, centerX, centerY);
              }
          }
      });
  
    var configMem = {
      type: 'doughnut',
      data: {
        labels: [
          "Memory Usage",
          "Free memory"
        ],
        datasets: [{
          data: [],
          backgroundColor: [
            "#0073b7",
            "rgba(0,0,0,0)",
          ],
          hoverBackgroundColor: [
            "#0a7dc1",
            "rgba(0,0,0,0)",
          ]
        }],
      },

      options: {
        legend: {
          display: false,
        },
      }
    };
  
    var configCPU = {
      type: 'doughnut',
      data: {
        labels: [
          "CPU Usage",
          "Free CPU"
        ],
        datasets: [{
          data: [],
          backgroundColor: [
            "#0073b7",
            "rgba(0,0,0,0)",
          ],
          hoverBackgroundColor: [
            "#0a7dc1",
            "rgba(0,0,0,0)",
          ]
        }],
      },
      options: {
        legend: {
          display: false,
        },
      }
    };
  
    var contextCPU = document.getElementById("cpuUsage").getContext("2d");
    var contextMem = document.getElementById("memUsage").getContext("2d");
    var textCPU = document.getElementById("cpuUsageNumber");
    var textMem = document.getElementById("memUsageNumber"); 
 
    var chartCPU = new Chart(contextCPU, configCPU);
    var chartMem = new Chart(contextMem, configMem);
  
    const source = new EventSource("/usage", {withCredentials: true});
  
    source.onmessage = function (event) {
      // Parsing data
      const data = JSON.parse(event.data);
  
      configCPU.data.datasets[0].data = [data.cpu, 100 - data.cpu];
      configMem.data.datasets[0].data = [data.mem, 100 - data.mem];
      textCPU.innerHTML = data.cpu.toFixed(2) + '%';
      textMem.innerHTML = data.mem.toFixed(2) + '%';
  
      chartCPU.update();
      chartMem.update();
      
    }
  
  });
  
