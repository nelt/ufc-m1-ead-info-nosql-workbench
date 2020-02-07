exports.handleRequest = async function(req, res) {

    const parse = require('csv-parse');
    const fs = require('fs');

    const cassandra = require('cassandra-driver');

    const client = new cassandra.Client({
        contactPoints: ['cassandra'],
        localDataCenter: 'datacenter1'
    });
    await client.connect();
    await createTable(client);

    const start = Date.now();
    await fs.createReadStream('./workspace/rain-in-australia/data-set/weatherAUS-20200122-tiny.csv')
        .pipe(parse({
            delimiter: ',',
            skip_lines_with_error: true,
            columns: true
        }))
        .on('data', function(row) {
            storeRow(row, client);
        })
        .on('end',function() {
            const elapsed = (Date.now() - start) / 1000;
            console.info("Read data-set took : " + elapsed + "s.")
        });


    res.write("Read data-set started at " + start);
};

function storeRow(row, client) {
    console.log("date : %s ; location: %s ; min : %s ; max : %s ; rainfall : %s", row.Date, row.Location, row.MinTemp, row.MaxTemp, row.Rainfall);
}

async function createTable(client) {
    const statement = "CREATE TABLE IF NOT EXISTS\n" +
    "ufcead.weather_data (\n" +
    "sensor text,\n" +
    "week text,\n" +
    "at timestamp,\n" +
    "temperature double,\n" +
    "hygrometry double,\n" +
    "PRIMARY KEY ((sensor, week), at)\n" +
    ")";

    await client.execute(statement);

}