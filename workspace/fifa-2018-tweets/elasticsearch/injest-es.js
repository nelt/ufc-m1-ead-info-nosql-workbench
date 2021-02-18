exports.run = async function (args) {
    const parse = require('csv-parse')
    const fs = require('fs')
    const {Client} = require('@elastic/elasticsearch')

    const esClient = new Client({node: 'http://elasticsearch:9200'})

    /*
    Création de l'index et de son mapping. Fonction en fin de fichier
     */
    await manageIndex(esClient);

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
    Ouverture d'un flux pour lire le fichier avec la bibliothèque fs.
    Le flux est ensuite passé (méthode pipe) à la bibliothèque csv-parse qui implémente un mécanisme de lecture asynchrone
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
                    - on indexe le tweet dans l'index "tweets" en utilisant row.ID comme identifiant, de cette manière
                      la première passe du script créera le document dans l'index, une autre passe entraînera sa mise à
                      jour
                 */
                try {
                    await esClient.index({
                        index: 'tweets',
                        id: row.ID,
                        body: {
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
                        }
                    });
                } catch (e) {
                    console.error("error indexing document : ", e)
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
        On définit le type de deux champs :
        - le champ date est indexé en tant que date en parsant son contenu à partir des trois expression fournies
        - le champ hashtags contient des mots clés, cela permettra de les agréger pour connaître le nombre de documents les contenant
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