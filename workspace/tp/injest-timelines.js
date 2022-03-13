exports.run = async function (args) {
    console.info("Starting injestion...")
    const fs = require('fs')
    const csv = require('fast-csv');

    const MongoClient = require('mongodb').MongoClient
    const mongo = await MongoClient.connect('mongodb://mongo:27017', { useUnifiedTopology: true })

    const redis = require("redis")
    const redisClient = redis.createClient({url: 'redis://redis:6379'})
    await redisClient.connect()

    /*
    Création de vos index de collection si nécessaire
     */





    /*
    Le code d'insertion commence ici,
     */
    const start = Date.now();
    let readCount = 0;

    let max = -1;
    if(args.length > 0) {
        max = parseInt(args[0])
        console.info("will read " + max + " records from FIFA.csv")
    } else {
        console.info("will read all records from FIFA.csv")
    }

    /*
    Ouverture d'un flux pour lire le fichier avec la librairie fs.
    Le flux est ensuite passé (methode pipe) à la librairie fast-csv qui implémente un mécanisme de lecture asynchrone
    du fichier.
     */
    let stream = fs.createReadStream('./workspace/fifa-2018-tweets/data-set/FIFA.csv')
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
            rowStoragePromises.push(storeRow(row, mongo, redisClient, counter))
        })
        .on('end', async function(rowCount) {
            await Promise.all(rowStoragePromises)
            const elapsed = (Date.now() - counter.start) / 1000
            console.info("Read " + counter.processed + " rows data-set in " + elapsed + "s.")
            counter.end = true
            mongo.close()
            await redisClient.quit();
        })

    showCounter(counter)
}


async function storeRow(row, mongo, redisClient, counter) {
    /*
    * Méthode appelée pour la lecture de chaque ligne du fichier.
    *
    * La variable row contient la donnée lue, i.e., les données d'un tweet.
    * La variable client est le client mongodb, il vous permet de récupérer votre base de donnée et d'interagir avec vos collections.
    * La variable redicsClient est le client redis connecté
    */

    /*
     * Ajouter le code pour :
     * - stocker le tweet en stockage primaire (mongo)
     * - stocker dfans redis les données nécessaire à la construction des timelines
     */


    counter.processed++
}

function showCounter(counter) {
    if(counter.end) return

    const elapsed = (Date.now() - counter.start) / 1000
    console.info("processed " + counter.processed + " rows in " + Math.floor(elapsed) + "s.")
    setTimeout(() => showCounter(counter), 10 * 1000)
}