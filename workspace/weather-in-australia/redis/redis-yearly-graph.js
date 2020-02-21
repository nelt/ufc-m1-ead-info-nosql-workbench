
exports.handleRequest = async function (req, res) {
    var url = require('url');
    const parsedQuery = url.parse(req.url, true);

    const redis = require("redis")
    const client = redis.createClient({host: "redis"})

    /*
    On calcule la valeur courante du filtre depuis la requête.
    Par défaut, on prend Sydney en 2017
     */
    const queryFilter = parsedQuery.query.filter ? JSON.parse(decodeURIComponent(parsedQuery.query.filter)) : undefined;
    console.log("query filter: " , queryFilter)
    const filters = {
        city: queryFilter ? queryFilter.city : 'Sydney',
        year: queryFilter ? queryFilter.year : "2017"
    }

    /*
    On commence par récupérer la liste des valeurs de filtre possible.
    La commande smembers permet de récupérer toutes les valeurs d'un Set
     */
    client.smembers(["filter_range"], function (error, filterRange) {
        try {
            filterRange = filterRange.map(json => JSON.parse(json))
            filterRange.sort((a, b) => {
                if (a.city == b.city) {
                    return a.year.localeCompare(b.year)
                } else {
                    return a.city.localeCompare(b.city)
                }
            })
        } catch (e) {
            console.error("error parsing filte ranges", e)
            filterRange = []
        }
        /*
        Le bucket contenant les valeurs est déterminer par la valeur du filtre
        La commande zrange permet de récupérer un range de valeurs d'un SortedSet.
        Ici, on demande le range [0, -1], ce qui correspond à récupérer toutes les valeurs. On aurait pus limiter le nombre
        de valeurs. Par exemple, [0, 9] aurait permis de récupérer les 10 premières valeurs
         */
        const bucket = `${filters.city}-${filters.year}`
        client.zrange([bucket, 0, -1], function (error, result) {

            /*
            Les valeurs stockées sont des chaîne de caractère encodant en JSON les échantillons.
            On le désérialise et on transforme le timestamp at en Date
             */
            const data = result.map(json => {
                const datum = JSON.parse(json)
                datum.at = new Date(datum.at)
                return datum;
            })

            /*
            Gestion de l'affichage
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

            client.quit(function () {
                console.info("bye...")
                res.end()
            })
        })
    })
}


/*
 *
 * Formattage : les fonction ci-dessous sont des fonctions d'affichage, elle ne sont pas à roprement parler intéressante
 * pour le cours, mais, si vous souhaitez comprendre le fonctionnement du script... allez-y !
 *
 */

function page(res, title, header, resume, data, filters, filterRange) {

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
