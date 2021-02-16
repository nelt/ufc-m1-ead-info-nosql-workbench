exports.run = async function (args) {
    const parse = require('csv-parse')
    const fs = require('fs')
    const MongoClient = require('mongodb').MongoClient

    mogoClient = await MongoClient.connect('mongodb://mongo:27017', { useUnifiedTopology: true })

    /*
    Création de la base de données et des collections :

    Pas de commande de création explicite, il suffit d'accéder à une base / collection pour
    qu'elles soient créées
     */
    const db = mogoClient.db("fifa_tweets")
    const tweets = db.collection('tweets')

    /*
    Création des indexes
     */
    db.createIndex('tweets', 'id')
    db.createIndex('tweets', 'date')
    /*
    Pour l'index sur le champ "tweet", on spécifie son type, le type text.
    https://docs.mongodb.com/manual/text-search/
    */
    db.createIndex( 'tweets',  { tweet: "text"} )

    /*
    Création de la collection "hastags" et de son index
     */
    const hashtags = db.collection('hashtags')
    db.createIndex('hashtags', 'tag')
    
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
    Le flux est ensuite passé (méthode pipe) à la librairie csv-parse qui implémente un mécanisme de lecture asynchrone
    du fichier.
     */
    let stream = fs.createReadStream('./workspace/fifa-2018-tweets/data-set/FIFA.csv');
    await stream
        .pipe(parse({
            delimiter: ',',
            skip_lines_with_error: true,
            columns: true
        }))
        .on('readable', async function(){
            /*
            Cette fonction est appelée lorsque des lignes du fichier CSV sont prêtes à être traitées
             */
            let row
            let lapStart = Date.now()
            while (row = this.read()) {
                let tags = row.Hashtags ? row.Hashtags.split(',') : [];

                /*
                Insertion d'un tweet :
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
                    On construit en parallèle la collection "hashtags" qui contient les compteurs.
                    Là aussi, on utilise l'option upsert pour insérer / mettre à jour les compteurs.
                    On utilise également l'opérateur $inc qui incrémente la valeur d'un champs, ici, le champ "count".
                     */
                    await hashtags.updateOne(
                        {tag: tags[i]},
                        {$inc: {count: 1}},
                        {upsert: true}
                    )
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
            mogoClient.close()
        })

}