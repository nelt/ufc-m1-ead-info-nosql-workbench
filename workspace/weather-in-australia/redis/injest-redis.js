exports.run = async function (args) {
    const fs = require('fs')
    const csv = require('fast-csv')

    const redis = require("redis")
    const client = redis.createClient({url: 'redis://redis:6379'})
    await client.connect()

    let max = -1
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
    stream
        .pipe(csv.parse(options))
        .on('error', error => {
            console.error(error)
        })
        .on('data', row => {
            // stream.pause()
            rowStoragePromises.push(storeRow(row, client, counter)
                // .then(() => stream.resume())
            )
        })
        .on('end', async function(rowCount) {
            await Promise.all(rowStoragePromises)
            const elapsed = (Date.now() - counter.start) / 1000
            console.info("Read " + counter.processed + " rows data-set in " + elapsed + "s.")
            counter.end = true
            await client.quit();
        })

    showCounter(counter)
}

async function storeRow(row, client, counter) {
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
            La commande ZADD ajoute un élément dans un sorted set avec comme argument :
            - la clé du set, ici, le nom du bucket pour avoir un set par ville / année
            - le score de l'élément, c'est lui qui assure l'ordre dans le set, ici, on se sert du timestamp
            de la date de l'échantillon : il est unique et ordonne nos valeur comme nous souhaitons les afficher
            - la donnée, ici, nous encodons les données en json
             */
            await client.zAdd(bucket, {
                score: Date.parse(row.Date),
                value: JSON.stringify({"at": Date.parse(row.Date), "minTemp": row.MinTemp,"maxTemp": row.MaxTemp,"rainfall": row.Rainfall})
            })

            /*
            On utilise bucket de type Set (sans relation d'ordre) pour dresser la liste des couple city / year
             */
            await client.sAdd("filter_range",JSON.stringify({"city":row.Location,"year":year}))
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
