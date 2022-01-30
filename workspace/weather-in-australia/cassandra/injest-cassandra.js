exports.run = async function (args) {
    const fs = require('fs')
    const csv = require('fast-csv');

    const cassandra = require('cassandra-driver');
    const client = new cassandra.Client({
        contactPoints: ['cassandra'],
        localDataCenter: 'datacenter1',
        pooling: {maxRequestsPerConnection: 10000}
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
    let options = { headers: true }
    if(max != -1) {
        options.maxRows = max
    }
    let rowStoragePromises = []
    let counter = {processed: 0, start: new Date(), end: false}
    await stream
        .pipe(csv.parse(options))
        .on('error', error => {
            console.error(error)
        })
        .on('data', row => {
            rowStoragePromises.push(storeRow(row, client, counter))
        })
        .on('end', async function(rowCount) {
            await Promise.all(rowStoragePromises)
            const elapsed = (Date.now() - counter.start) / 1000
            console.info("Read " + counter.processed + " rows data-set in " + elapsed + "s.")
            counter.end = true
            await client.shutdown()
        })

    showCounter(counter)

}

async function storeRow(row, client, counter) {
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
    counter.processed++
}

function showCounter(counter) {
    if(counter.end) return

    const elapsed = (Date.now() - counter.start) / 1000
    console.info("processed " + counter.processed + " rows in " + Math.floor(elapsed) + "s.")
    setTimeout(() => showCounter(counter), 10 * 1000)
}