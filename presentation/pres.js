
// More info https://github.com/hakimel/reveal.js#configuration
Reveal.initialize({
    controls: true,
    progress: true,
    history: true,
    center: true,

    transition: 'slide', // none/fade/slide/convex/concave/zoom

    // More info https://github.com/hakimel/reveal.js#dependencies
    dependencies: [
        { src: './reveal.js/lib/js/classList.js', condition: function() { return !document.body.classList; } },
        { src: './reveal.js/plugin/markdown/marked.js', condition: function() { return !!document.querySelector( '[data-markdown]' ); } },
        { src: './reveal.js/plugin/markdown/markdown.js', condition: function() { return !!document.querySelector( '[data-markdown]' ); } },
        { src: './reveal.js/plugin/highlight/highlight.js', async: true, callback: function() { hljs.initHighlightingOnLoad(); } },
        { src: './reveal.js/plugin/search/search.js', async: true },
        { src: './reveal.js/plugin/zoom-js/zoom.js', async: true },
        { src: './reveal.js/plugin/notes/notes.js', async: true }
    ]
});


Reveal.addEventListener('ready', function( event ) {
    // event.currentSlide, event.indexh, event.indexv
    mySocket = new WebSocket("ws://localhost:9000");
    // Ecoute pour les messages arrivant
    mySocket.onmessage = function (event) {

        let res = JSON.parse(event.data);

        var country_codes2 = d3.keys(res);
        var series = []
        country_codes2.forEach(function(d) {
            series.push([d, parseFloat(res[d])])
        });

        refreshMap(series);

        console.log(series);

    };

    d3.csv("frequencies.csv", function(data) {
        series = [];
        console.log(data);
        data.forEach(function(d) {
            series.push([d.Country, parseFloat(d.Frequency)])
        });

        refreshMap(series);

    });
} );


// Define the div for the tooltip
var divTooltip = d3.select("#slideMap").append("div")
    .attr("id", "divTooltip")
    .attr("class", "tooltip")
    .style("opacity", 0);

// // example data from server
// window.addEventListener("load", function () {
//     // Crée l'instance WebSocket
//     mySocket = new WebSocket("ws://localhost:9000");
//     // Ecoute pour les messages arrivant
//     mySocket.onmessage = function (event) {
//
//         let res = JSON.parse(event.data);
//
//         var country_codes2 = d3.keys(res);
//         var series = []
//         country_codes2.forEach(function(d) {
//             series.push([d, parseFloat(res[d])])
//         });
//
//         refreshMap(series);
//
//         console.log(series);
//
//     };
// });

function refreshMap(series){

    d3.selectAll(".datamap").remove();
    d3.selectAll("divTooltip").remove();
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
    let mymap = new Datamap({
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

            datamap.svg.selectAll('.datamaps-subunit').on('mouseover', function(geography) {
                let id = geography.id;
                let d = dataset[geography.id] ? dataset[geography.id].numberOfThings : 0;
                //console.log(id, d);
                divTooltip.transition()
                    .duration(200)
                    .style("opacity", .9);
                divTooltip.html(id + " : "  + d)
                    .style("left", (d3.event.pageX) + "px")
                    .style("top", (d3.event.pageY - 28) + "px");
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
            },
            popupOnHover: false,
        }
    });

    console.log(mymap);
    // console.log(mymap.options.element.outerHTML)
}

// d3.csv("frequencies.csv", function(data) {
//     series = [];
//     console.log(data);
//     data.forEach(function(d) {
//         series.push([d.Country, parseFloat(d.Frequency)])
//     });
//
//     refreshMap(series);
//
// });
