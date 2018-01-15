// example data from server
window.addEventListener("load", function () {
    // Crée l'instance WebSocket
    mySocket = new WebSocket("ws://localhost:9000");
    // Ecoute pour les messages arrivant
    mySocket.onmessage = function (event) {

        let res = JSON.parse(event.data);

        console.log('res', res);
        console.log(d3.keys(res));

        var country_codes2 = d3.keys(res);
        var series = []
        country_codes2.forEach(function(d) {
          series.push([d, parseFloat(res[d])])
        });

        refreshMap(series);

        console.log(series);

    };
});

function refreshMap(series){

    d3.selectAll(".datamap").remove();
    // Datamaps expect data in format:
    // { "USA": { "fillColor": "#42a844", numberOfWhatever: 75},
    //   "FRA": { "fillColor": "#8dc386", numberOfWhatever: 43 } }
    var dataset = {};

    // We need to colorize every country based on "numberOfWhatever"
    // colors should be uniq for every value.
    // For this purpose we create palette(using min/max series-value)
    var onlyValues = series.map(function(obj){ return obj[1]; });

    var minValue = 0.0001;
    var maxValue = d3.max(onlyValues);

    // create color palette function
    // color can be whatever you wish
    var paletteScale = d3.scale.log()
            .domain([minValue,maxValue])
            .range(["#EFEFFF","#02386F"]); // blue color

    // fill dataset in appropriate format
    series.forEach(function(item){ //
        // item example value ["USA", 70]
        var iso = item[0],
                value = item[1];
        dataset[iso] = { numberOfThings: value, fillColor: paletteScale(value) };
    });

    // render map
    new Datamap({
        element: document.getElementById('containerMap'),
        done: function(datamap) {
          var startInput = document.getElementById('dateStart');
          var endInput = document.getElementById('dateEnd');
          var startDate = startInput.value;
          var endDate = endInput.value;
          startInput.addEventListener("change", function(){
            startDate = this.value;
            console.log('start date: ', this.value);
          })

          endInput.addEventListener("change", function(){
            endDate = this.value;
            console.log('end date: ', this.value);
          })

            datamap.svg.selectAll('.datamaps-subunit').on('click', function(geography) {
                console.log(geography.id);
                mySocket.send(JSON.stringify({country: geography.id, start: startDate, end: endDate}));

            });
        },
        projection: 'mercator', // big world map
        // countries don't listed in dataset will be painted with this color
        fills: { defaultFill: '#F5F5F5' },
        data: dataset,
        geographyConfig: {
            borderColor: '#1a1a00',
            highlightBorderWidth: 2,
            // don't change color on mouse hover
            highlightFillColor: function(geo) {
                return geo['fillColor'] || '#F5F5F5';
            },
            // only change border
            highlightBorderColor: '#B7B7B7',
            // show desired information in tooltip
            popupTemplate: function(geo, data) {
                // don't show tooltip if country don't present in dataset
                if (!data) { return ; }
                // tooltip content
                return ['<div class="hoverinfo">',
                    '<strong>', geo.properties.name, '</strong>',
                    '<br>Count: <strong>', data.numberOfThings, '</strong>',
                    '</div>'].join('');
            }
        }
    });

}

d3.csv("/home/bud/Documents/s2/nosql/project/NoSql/src/JSvisualisation/frequencies.csv", function(data) {
  series = [];
  console.log(data);
  data.forEach(function(d) {
    series.push([d.Country, parseFloat(d.Frequency)])
  });

  refreshMap(series);

});
