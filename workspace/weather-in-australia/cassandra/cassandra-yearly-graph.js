
exports.handleRequest = async function (req, res) {
    const cassandra = require('cassandra-driver');
    const client = new cassandra.Client({
        contactPoints: ['cassandra'],
        localDataCenter: 'datacenter1'
    });

    res.writeHead(200, {'Content-Type': 'text/html; charset=utf-8'})

    const filters = {city: 'Albury', year: "2018"}
    const data = retrieveData(client, filters)


    console.info("data : ", data)

    page(res,
        `Le temps en Australie : Graphes annuels`,
        `${filters.city} en ${filters.year}`,
        `Évolution des températures minimales, maximales et pluviométrie à ${filters.city} en ${filters.year}`,
        data
    )

    res.end()
}

function retrieveData(client, filters) {
    const data = []

    const tmp = [
        [new Date(2008, 12, 1),     13.4, 22.9, 0.6],
        [new Date(2008, 12, 2),     7.4, 25.1, 0],
        [new Date(2008, 12, 3),     12.9, 25.7, 0],
        [new Date(2008, 12, 4),     9.2, 28, 0],
        [new Date(2008, 12, 5),     17.5 ,32.3, 1],
        [new Date(2008, 12, 6),     14.6, 29.7, 0.2],
        [new Date(2008, 12, 7),     14.3, 25, 0],
        [new Date(2008, 12, 8),     7.7, 26.7, 0],
        [new Date(2008, 12, 9),     9.7, 31.9, 0],
        [new Date(2008, 12, 10),    13.1, 30.1, 1.4],
        [new Date(2008, 12, 11),    13.4, 30.4, 0],
        [new Date(2008, 12, 12),    15.9, 21.7, 2.2]
    ]
    tmp.forEach(value => {
        data.push({
            at: value[0],
            minTemp: value[1],
            maxTemp: value[2],
            rainfall: value[3]
        })
    })

    return data
}


/*
 *
 * Formattage : les fonction ci-dessous sont des fonctions d'affichage, elle ne sont pas à roprement parler intéressante
 * pour le cours, mais, si vous souhaitez comprendre le fonctionnement du script... allez-y !
 *
 */

function page(res, title, header, resume, data) {

    res.write(
        `
        <?xml version="1.0" encoding="UTF-8" ?>
        <!DOCTYPE html>
        `
    )
    res.write(
        `
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="fr" lang="fr" dir="ltr">
        <head>
            <title>${title}</title>
            <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/3.4.1/css/bootstrap.min.css" integrity="sha384-HSMxcRTRxnN+Bdg0JdbxYKrThecOKuH5zCYotlSAcp1+c8xmyTe9GYg1l9a69psu" crossorigin="anonymous">
            <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
            
            <script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
            <script type="text/javascript">
              google.charts.load('current', {'packages':['line', 'corechart']});
              google.charts.setOnLoadCallback(drawChart);
              
              function drawChart() {
                  
                  var data = new google.visualization.DataTable();
                  data.addColumn('date', 'Jour');
                  data.addColumn('number', "T° Minmale");
                  data.addColumn('number', "T° Maximale");
                  data.addColumn('number', "Pluviométrie");
            
                  data.addRows([`
    )

    const formattedData = []
    data.forEach(datum => {
        formattedData.push(`[new Date(${datum.at.getYear()}, ${datum.at.getMonth()}, ${datum.at.getDate()}), ${datum.minTemp}, ${datum.maxTemp}, ${datum.rainfall}]`)
    })
    res.write(formattedData.join(", "))

    res.write(
            `
                  ]);
            
                  var materialOptions = {
                    chart: {
                      title: '${resume}'
                    },
                    width: 900,
                    height: 500,
                    series: {
                      // Gives each series an axis name that matches the Y-axis below.
                      0: {axis: 'Temp'},
                      1: {axis: 'Temp'},
                      2: {axis: 'Rainfall'}
                    },
                    axes: {
                      // Adds labels to each axis; they don't have to match the axis names.
                      y: {
                        Temps: {label: 'Temps (°C)'},
                        Rainfall: {label: 'Pluviométrie (mm)'}
                      }
                    } 
                  };
                  
                  var materialChart = new google.charts.Line(chartDiv);
                  materialChart.draw(data, materialOptions);
              }
            </script>
        </head>
        <body>
            <div class="container">
                <div class="row"><h1 class="display-4 text-center">${header}</h1></div>
                <div class="row">
                    <div id="chartDiv" style="width: 900px; height: 500px"></div>
                </div>
            </div>
        </body>
        </html>    
        `)
}
