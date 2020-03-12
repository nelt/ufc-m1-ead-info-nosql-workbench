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
    Création de vos tables ICI
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
    Le flux est ensuite passé (methode pipe) à la librairie csv-parse qui implémente un mécanisme de lecture asynchrone
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
            Cette fonction est appelée lors que des lignes du fchier CSV sont prètes à être traitées
             */
            let row
            let lapStart = Date.now()
            while (row = this.read()) {
                try {
                    /*
                    Insertion de d'un échantillon ICI
                     */












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