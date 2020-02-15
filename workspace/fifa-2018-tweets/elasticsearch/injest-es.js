

exports.run = async function (args) {
    const parse = require('csv-parse')
    const fs = require('fs')
    const {Client} = require('@elastic/elasticsearch')

    const esClient = new Client({node: 'http://elasticsearch:9200'})
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

    let stream = fs.createReadStream('./workspace/fifa-2018-tweets/data-set/FIFA.csv');
    await stream
        .pipe(parse({
            delimiter: ',',
            skip_lines_with_error: true,
            columns: true
        }))
        .on('readable', async function(){
            let row
            let lapStart = Date.now()
            while (row = this.read()) {
                let tags = row.Hashtags ? row.Hashtags.split(',') : [];

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
    const existsResp = await esClient.indices.exists({index: 'tweets'})

    if (!existsResp.body) {
        await esClient.indices.create({index: 'tweets'})
        console.log("created tweets index")
    } else {
        console.log("tweets index exists")
    }
    try {
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

    // try {
    //     mapping = await esClient.indices.getMapping({index: 'tweets'})
    //     console.log("mapping ", mapping.body.tweets.mappings)
    // } catch (e) {
    //     console.error("error managing tweet index", e)
    // }
}