exports.run = async function (args) {
    const parse = require('csv-parse')
    const fs = require('fs')
    const MongoClient = require('mongodb').MongoClient

    mogoClient = await MongoClient.connect('mongodb://mongo:27017', { useUnifiedTopology: true })
    const db = mogoClient.db("fifa_tweets")

    const tweets = db.collection('tweets')
    db.createIndex('tweets', 'id')
    db.createIndex('tweets', 'date')
    db.createIndex( 'tweets',  { tweet: "text"} )

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