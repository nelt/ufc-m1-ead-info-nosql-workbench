
exports.handleRequest = async function (req, res) {
    var url = require('url');
    const parsedQuery = url.parse(req.url, true);

    const filters = {
        city: parsedQuery.query.city ? parsedQuery.query.city : 'Albury',
        year: parsedQuery.query.year ? parsedQuery.query.year : "2008"
    }

    const redis = require("redis")
    const client = redis.createClient({host: "redis"})

    const bucket = `${filters.city}-${filters.year}`


    client.zrange(["year_labels", 0, -1], function (error, years) {
        client.smembers(["city_labels"], function (error, cities) {
            client.zrange([bucket, 0, -1], function (error, result) {

                const data = formatData(result)

                res.writeHead(200, {'Content-Type': 'text/html; charset=utf-8'})
                page(res,
                    `Le temps en Australie : Graphes annuels`,
                    `${filters.city} en ${filters.year}`,
                    `Évolution des températures minimales, maximales et pluviométrie à ${filters.city} en ${filters.year}`,
                    data,
                    filters,
                    cities,
                    years
                )

                client.quit(function () {
                    console.info("bye...")
                    res.end()
                })
            })
        })
    })

}

function formatData(result) {
    const data = []
    result.forEach(json => {
        const datum = JSON.parse(json)
        datum.at = new Date(datum.at)
        data.push(datum)
    })

    return data
}


/*
 *
 * Formattage : les fonction ci-dessous sont des fonctions d'affichage, elle ne sont pas à roprement parler intéressante
 * pour le cours, mais, si vous souhaitez comprendre le fonctionnement du script... allez-y !
 *
 */

function page(res, title, header, resume, data, filters, cities, years) {

    const formattedData = []
    data.forEach(datum => {
        try {
            /*
            On doit formatter une chaîne à partir du datum qui sera exécutée en tant que javascript par le navigateur
             */
            formattedData.push(`[new Date(${datum.at.getTime()}), ${datum.minTemp}, ${datum.maxTemp}, ${datum.rainfall}]`)
        } catch (e) {
            console.error("failed formatting datum : ", datum, e)
        }

    })

    try {
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

                      data.addRows([${formattedData.join(", ")}]);

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

                    <nav class="navbar navbar-light bg-light">

                      <form class="form-inline" action="">
                        <select class="form-control" name="city">
            `)
        cities.sort()
        cities.forEach(city => {
            res.write(`<option ${city === filters.city ? 'selected' : ''}>${city}</option>`)
        })
        res.write(`
                        </select>
                        <select class="form-control" name="year">
            `)
        years.forEach(year => {
            res.write(`<option ${year === filters.year ? 'selected' : ''}>${year}</option>`)
        })
        res.write(`
                        </select>

                        <button class="btn btn-outline-success my-2 my-sm-0" type="submit">Search</button>
                      </form>

                    </nav>

                    </div>
                    <div class="row">
                        <div id="chartDiv" style="width: 900px; height: 500px"></div>
                    </div>
                </div>
            </body>
            </html>
            `)
    } catch (e) {
        console.error("error formatting data", e)
    }
}
