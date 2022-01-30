exports.run = async function (args) {
    const fs = require('fs')
    const csv = require('fast-csv');
    const MongoClient = require('mongodb').MongoClient

    const mongoClient = await MongoClient.connect('mongodb://mongo:27017', { useUnifiedTopology: true })

    /*
    Création de la base de données et des collections :

    Pas de commande de création explicite, il suffit d'accéder à une base / collection pour
    qu'elles soient créées
     */
    const db = mongoClient.db("fifa_tweets")
    const tweets = db.collection('tweets')

    /*
    Création des indexes
     */
    await db.createIndex('tweets', 'id')
    await db.createIndex('tweets', 'date')
    /*
    Pour l'index sur le champ "tweet", on spécifie son type, le type text.
    https://docs.mongodb.com/manual/text-search/
    */
    await db.createIndex( 'tweets',  { tweet: "text"} )

    /*
    Création de la collection "hashtags" et de son index
     */
    const hashtags = db.collection('hashtags')
    await db.createIndex('hashtags', 'tag')
    
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
            rowStoragePromises.push(storeRow(row, tweets, hashtags, counter))
        })
        .on('end', async function(rowCount) {
            await Promise.all(rowStoragePromises)
            const elapsed = (Date.now() - counter.start) / 1000
            console.info("Read " + counter.processed + " rows data-set in " + elapsed + "s.")
            counter.end = true
            mongoClient.close()
        })

    showCounter(counter)
}

async function storeRow(row, tweets, hashtags, counter) {
    let tags = row.Hashtags ? row.Hashtags.split(',') : []
    /*
    Insertion de d'un tweet :
        - la variable row contient une ligne du fichier CSV
        - on utilise la méthode updateOne pour insérer / mettre à jour le tweet avec l'option upsert (cf. étude de cas)
     */
    await tweets.updateOne(
        {id: row.ID},
        {$set: {
                id: row.ID,
                lang: row.lang,
                date: row.Date,
                source: row.Source,
                tweet: row.Orig_Tweet,
                likes: row.Likes,
                rts: row.RTs,
                hashtags: tags,
                mentionNames: row.UserMentionNames ? row.UserMentionNames.split(',') : [],
                mentionIds: row.UserMentionID ? row.UserMentionID.split(',') : [],
                authorName: row.Name,
                authorPLace: row.Place,
                authorFollowers: row.Followers,
                authorFiends: row.Friends
            }},
        {upsert: true}
    )
    for (var i = 0; i < tags.length; i++) {
        /*
        On construit en parrallèle la collection "hashtags" qui contient les compteurs.
        Là aussi, on utilise l'option upsert pour insèrer / mettre à jour les compteurs.
        On utilise également l'opérateur $inc qui incrémente la valeur d'un champs, ici, le champ "count".
         */
        await hashtags.updateOne(
            {tag: tags[i]},
            {$inc: {count: 1}},
            {upsert: true}
        )
    }
    counter.processed++
}

function showCounter(counter) {
    if(counter.end) return

    const elapsed = (Date.now() - counter.start) / 1000
    console.info("processed " + counter.processed + " rows in " + Math.floor(elapsed) + "s.")
    setTimeout(() => showCounter(counter), 10 * 1000)
}