exports.run = async function (args) {
    const parse = require('csv-parse')
    const fs = require('fs')

    const cassandra = require('cassandra-driver');

    const client = new cassandra.Client({
        contactPoints: ['cassandra'],
        localDataCenter: 'datacenter1'
    });


    /*
    On créé notre keyspace s'il n'existe pas
     */
    await client.execute("CREATE KEYSPACE IF NOT EXISTS ufcead WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 1 }");

    /*
    On créé une table pour nos données. On utilise les champs du fichier csv suivant :
        Date: '2017-06-23',
        Location: 'Uluru',
        MinTemp: '5.4',
        MaxTemp: '26.9',
        Rainfall: '0'
     */
    await client.execute(`
        CREATE TABLE IF NOT EXISTS ufcead.weather_data (
            location text,
            year text,
            at timestamp,
            minTemp double,
            maxTemp double,
            rainfall double,
            PRIMARY KEY ((location, year), at)
        )
    `);

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
                    const query = 'INSERT INTO ufcead.weather_data(location, year, at, minTemp, maxTemp, rainfall) VALUES (?, ?, ?, ?, ?, ?)';
                    /*
                        Date: '2017-06-23',
                        Location: 'Uluru',
                        MinTemp: '5.4',
                        MaxTemp: '26.9',
                        Rainfall: '0'
                     */
                    if(row.MinTemp != 'NA' && row.MaxTemp != 'NA' && row.Rainfall != 'NA') {
                        const result = await client.execute(query, [
                            row.Location,
                            row.Date.substring(0,4),
                            Date.parse(row.Date),
                            row.MinTemp,
                            row.MaxTemp,
                            row.Rainfall
                        ], {prepare: true});
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
            console.info("Read " + readCount + " rows data-set in " + elapsed + "s.")
        })

}