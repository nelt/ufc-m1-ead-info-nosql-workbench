exports.run = async function (args) {
    const fs = require('fs')
    /*
    On n'utilise pas la même csv librairie que pour les autres bases. ES ne peut pas indexer autant de document
    au fil de l'eau, on doit utiliser l'API batch (cette API permet une indexation très rapide)
     */
    const csvBatch = require('csv-batch');
    const {Client} = require('@elastic/elasticsearch')

    const esClient = new Client({node: 'http://elasticsearch:9200'})

    /*
    Création de l'index et de son mapping. Fonction en fin de fichier
     */
    await manageIndex(esClient)

    let max = -1;
    if(args.length > 0) {
        max = parseInt(args[0])
        console.info("will read " + max + " records from FIFA.csv")
    } else {
        console.info("will read all records from FIFA.csv")
    }

    /*
    Ouverture d'un flux pour lire le fichier avec la librairie fs.
    Le flux est ensuite passé (methode pipe) à la librairie csv-parse qui implémente un mécanisme de lecture asynchrone
    du fichier.
     */

    let stream = fs.createReadStream('./workspace/fifa-2018-tweets/data-set/FIFA.csv');
    let rowStoragePromises = []
    let counter = {processed: 0, start: new Date(), end: false}
    csvBatch(stream, {
        batch: true,
        batchSize: 100,
        batchExecution: batch => rowStoragePromises.push(storeRowBatch(batch, esClient, counter))
    }).then( async function(results) {
        await Promise.all(rowStoragePromises)
        console.log(`Processed ${results.totalRecords}`);
        const elapsed = (Date.now() - counter.start) / 1000
        console.info("Read " + counter.processed + " rows data-set in " + elapsed + "s.")
        counter.end = true
    })

    showCounter(counter)
}

async function storeRowBatch(batch, esClient, counter) {
    let indexActions = []
    batch.forEach(row => {
        let tags = row.Hashtags ? row.Hashtags.split(',') : [];
        /*
        Insertion de d'un tweet :
            - la variable row contient une ligne du fichier CSV
            - on indexe le tweet dans l'index "tweets" en utilisant row.ID comme identifiant, de cette manière
              la première passe du script créera le document dans l'index, une autre passe entraînera sa mise à
              jour
         */
        const doc = {
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
        };
        /*
        En utilisant l'API Bulk, on doit construire un tableau dont les éléments vont par deux :
        1. l'action à réaliser, ici, une action d'indexation dans l'index  'tweets'
        2. la donnée à utiliser pour réaliser cette action, ici, le document à indexer
         */
        indexActions.push({index: { _index: 'tweets' }}, doc)
    })

    /*
    Une fois le tableau d'action à réaliser construit, on le transmet à l'API Bulk :
     */
    const { body: bulkResponse } = await esClient.bulk({
        refresh: true,
        // body: body
        body: indexActions
    })

    counter.processed = counter.processed + batch.length
}

async function manageIndex(esClient) {
    /*
    On exécute une première requête pour savoir si l'index existe déjà
     */
    const existsResp = await esClient.indices.exists({index: 'tweets'})

    /*
    Si l'index n'existe pas, la requête ne renvoie pas de body (on pourrait tester le statut HTTP qui est à 404 dans ce cas)
     */
    if (!existsResp.body) {
        /*
        Si l'index n'existe pas, on le crée
         */
        await esClient.indices.create({index: 'tweets'})
        console.log("created tweets index")
    } else {
        console.log("tweets index exists")
    }
    try {
        /*
        Ensuite on affecte à l'index son mapping.
        On défnit le type de deux champs :
        - le champ date est indexé en tant que date en parsant son contenu à partir des trois expression fournies
        - le champ hashtags contient des mots clés, celà permettrat de les aggréger pour connaître le nombre de documents les contenant
        Le type des autres champs sera déterminé par le moteur. Le plus souvent, il s'agira d'une indexation textuelle.
         */
        await esClient.indices.putMapping({
            index: 'tweets',
            body: {
                properties: {
                    "date": {
                        type: "date",
                        format: "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
                    },
                    "hashtags": {
                        type: "keyword"
                    }
                }
            }
        })
    } catch (e) {
        console.error("error managing tweet index", e)
    }

    /*
    Ce code commenté permet d'afficher le mapping de l'index
     */
    // try {
    //     mapping = await esClient.indices.getMapping({index: 'tweets'})
    //     console.log("mapping ", mapping.body.tweets.mappings)
    // } catch (e) {
    //     console.error("error managing tweet index", e)
    // }
}

function showCounter(counter) {
    if(counter.end) return

    const elapsed = (Date.now() - counter.start) / 1000
    console.info("processed " + counter.processed + " rows in " + Math.floor(elapsed) + "s.")
    setTimeout(() => showCounter(counter), 10 * 1000)
}