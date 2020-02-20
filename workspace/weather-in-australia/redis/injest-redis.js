exports.run = async function (args) {
    const parse = require('csv-parse')
    const fs = require('fs')

    const redis = require("redis")
    const client = redis.createClient({host: "redis"})


    const start = Date.now();
    let readCount = 0;

    let max = -1;
    if(args.length > 0) {
        max = parseInt(args[0])
        console.info("will read " + max + " records from weatherAUS.csv")
    } else {
        console.info("will read all records from weatherAUS.csv")
    }

    /*
    Ouverture d'un flux pour lire le fichier avec la librairie fs.
    Le flux est ensuite passé (methode pipe) à la librairie csv-parse qui implémente un mécanisme de lecture asynchrone
    du fichier.
     */
    let stream = fs.createReadStream('./workspace/weather-in-australia/data-set/weatherAUS.csv');
    await stream
        .pipe(parse({
            delimiter: ',',
            skip_lines_with_error: true,
            columns: true
        }))
        .on('readable', async function(){
            /*
            Cette fonction est appelée lors que des lignes du fchier CSV sont prètes à être traitées
             */
            let row
            let lapStart = Date.now()
            while (row = this.read()) {
                /*
                Insertion de d'un échantillon
                 */
                try {
                    /*
                        Date: '2017-06-23',
                        Location: 'Uluru',
                        MinTemp: '5.4',
                        MaxTemp: '26.9',
                        Rainfall: '0'
                     */
                    if(row.MinTemp != 'NA' && row.MaxTemp != 'NA' && row.Rainfall != 'NA') {
                        /*
                        On insère la données dans le bon bucket
                        On veut un bucket par ville et par an, on construit donc le nom du bucket en concaténant
                        ces deux valeurs.
                         */
                        const year = row.Date.substring(0,4);
                        const bucket = row.Location + "-" + year
                        /*
                        On va utiliser comme implémentation de bucket dans redis un "SortedSet", cf. Etude de cas.
                        La commande ZADD ajoute un élément dans un sorted set avec comme ragument :
                        - la clé du set, ici, le nom du bucket pour avoir un set par ville / année
                        - le score de l'élément, c'est lui qui assure l'ordre dans le set, ici, on se sert du timestamp
                        de la date de l'échantillon : il est unique et ordonne nos valeur comme nous souhaitons les afficher
                        - la donnée, ici, nous encodons les données en json
                         */

                        client.zadd(bucket, Date.parse(row.Date), `{"at": ${Date.parse(row.Date)}, "minTemp": ${row.MinTemp},"maxTemp": ${row.MaxTemp},"rainfall": ${row.Rainfall}}`)
                        client.zadd("year_labels", parseFloat(year), year)
                        client.sadd("city_labels", row.Location)
                    }
                } catch (e) {
                    console.error("error indexing document : ", row)
                    console.error("error was : ", e)
                }

                readCount++
                if(readCount % 5000 == 0) {
                    const elapsed = (Date.now() - lapStart) / 1000
                    lapStart = Date.now()
                    console.info(readCount + " rows read, last 5000 in " + elapsed + "s.")
                }
                if(readCount == max) {
                    console.info("max read reached")
                    this.end()
                    return
                }
            }
        })
        .on('end', function () {
            const elapsed = (Date.now() - start) / 1000;
            client.quit(function () {
                console.info("Read " + readCount + " rows data-set in " + elapsed + "s.")
            })
        })

}