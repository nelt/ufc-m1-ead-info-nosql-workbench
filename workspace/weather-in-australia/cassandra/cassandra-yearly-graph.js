
exports.handleRequest = async function (req, res) {
    var url = require('url');
    const parsedQuery = url.parse(req.url, true);

    const cassandra = require('cassandra-driver');
    const client = new cassandra.Client({
        contactPoints: ['cassandra'],
        localDataCenter: 'datacenter1'
    });

    /*
    On calcule la valeur courante du filtre depuis la requête.
    Par défaut, on prend Sydney en 2017.
     */
    const queryFilter = parsedQuery.query.filter ? JSON.parse(decodeURIComponent(parsedQuery.query.filter)) : undefined;
    console.log("query filter: " , queryFilter)
    const filters = {
        city: queryFilter ? queryFilter.city : 'Sydney',
        year: queryFilter ? queryFilter.year : "2017"
    }

    /*
    On commence par récupérer la liste des valeurs de filtre possible en groupant par le couple (location, year).
     */
    client.execute("SELECT location, year FROM ufcead.weather_data GROUP BY location, year", [], {prepare: true}, function (error, resultSet) {
        filterRange = resultSet.rows.map(row => {
            return {
                city: row.location,
                year: row.year
            }
        })
        filterRange.sort((a, b) => {
            if (a.city == b.city) {
                return a.year.localeCompare(b.year)
            } else {
                return a.city.localeCompare(b.city)
            }
        })

        /*
        On exécute la requête sur les données pour le couple ville / année sélectionné.
         */
        client.execute("SELECT * FROM ufcead.weather_data WHERE location = ? AND year = ?",
            [filters.city, filters.year],
            {prepare: true},
            function (error, resultSet) {
                /*
                Le nom des colonnes est en minuscule, on rétablit la casse.
                 */
                const data = resultSet.rows.map(row => {
                    row.minTemp = row.mintemp
                    row.maxTemp = row.maxtemp
                    return row
                })

                /*
                Gestion de l'affichage.
                 */
                res.writeHead(200, {'Content-Type': 'text/html; charset=utf-8'})
                page(res,
                    `Le temps en Australie : Graphes annuels`,
                    `${filters.city} en ${filters.year}`,
                    `Évolution des températures minimales, maximales et pluviométrie à ${filters.city} en ${filters.year}`,
                    data,
                    filters,
                    filterRange
                )

                res.end()
            }
        );
    })
}


/*
 *
 * Formatage : les fonctions ci-dessous sont des fonctions d'affichage, elle ne sont pas à proprement parler intéressantes
 * pour le cours, mais, si vous souhaitez comprendre le fonctionnement du script... allez-y !
 *
 */

function page(res, title, header, resume, data, filters, filterRange) {

    const formattedData = []
    data.forEach(datum => {
        try {
            /*
            On doit formater une chaîne à partir du datum qui sera exécutée en tant que JavaScript par le navigateur.
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
                        <select class="form-control" name="filter">
            `)
        filterRange.forEach(filter => {
            const encodedFilter = encodeURIComponent(JSON.stringify(filter))
            if(filter.city === filters.city && filter.year === filters.year) {
                res.write(`<option value="${encodedFilter}" selected>${filter.city} - ${filter.year}</option>`)
            } else {
                res.write(`<option value="${encodedFilter}">${filter.city} - ${filter.year}</option>`)
            }
        })
        res.write(`
                        </select>

                        <button class="btn btn-outline-success my-2 my-sm-0" type="submit">Changer de ville / années</button>
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
