
exports.handleRequest = async function(req, res) {
    res.writeHead(200, {'Content-Type': 'text/html; charset=utf-8'});
    pageStart(res, "NoSQL Workbench : status");

    await mongodbStatus(res);
    await elasticsearchStatus(res);
    await cassandraStatus(res);

    pageEnd(res);
}

async function mongodbStatus(res) {
    const MongoClient = require('mongodb').MongoClient;
    const url = 'mongodb://mongo:27017';

    try {
        mogoClient = await MongoClient.connect(url);
        console.log("Connected successfully to mongo server");
        dbStatusStatement(res, 'MongoDB', 'success');
        mogoClient.close();
    } catch (err) {
        console.log("Can't connect to mongo server !!");
        dbStatusStatement(res, 'MongoDB', 'danger',
            '<p>La base semble être arrêtée.</p>\n' +
            '<p>Pour lancer la base exécuter :</p>\n' +
            '<pre>docker-compose up -d mongo</pre>'
        );
    }
}

async function elasticsearchStatus(res) {
    const {Client} = require('@elastic/elasticsearch')
    const esClient = new Client({node: 'http://elasticsearch:9200'})

    try {
        await esClient.ping();
        console.log("Connected successfully to elasticsearch server");
        dbStatusStatement(res, 'ElasticSearch', 'success');
        esClient.close();
    } catch (err) {
        console.log("Can't connect to elasticsearch server !! " + err.stack);
        dbStatusStatement(res, 'ElasticSearch', 'danger',
            '<p>La base semble être arrêtée.</p>\n' +
            '<p>Pour lancer la base exécuter :</p>\n' +
            '<pre>docker-compose up -d elasticsearch</pre>'
        );
    }
}

async function cassandraStatus(res) {
    const cassandra = require('cassandra-driver');

    const client = new cassandra.Client({
        contactPoints: ['cassandra'],
        localDataCenter: 'datacenter1'
    });

    try {
        await client.connect();
        console.log("Connected successfully to cassandra server");
        dbStatusStatement(res, 'Cassandra', 'success');
    } catch(err) {
        console.log("Can't connect to cassandra server !! " + err.stack);
        dbStatusStatement(res, 'Cassandra', 'danger',
            '<p>La base semble être arrêtée.</p>\n' +
            '<p>Pour lancer la base exécuter :</p>\n' +
            '<pre>docker-compose up -d cassandra</pre>'
        );
    }
}

function pageStart(res, title) {
    res.write(
        `<?xml version="1.0" encoding="UTF-8" ?>
<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="fr" lang="fr" dir="ltr">
  <head>
    <title>${title}</title>
    <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/3.4.1/css/bootstrap.min.css" integrity="sha384-HSMxcRTRxnN+Bdg0JdbxYKrThecOKuH5zCYotlSAcp1+c8xmyTe9GYg1l9a69psu" crossorigin="anonymous">  </head>
  <body class="container">
`
    );
}

function dbStatusStatement(res, db, status, message) {
    res.write(
        `
<article>
  <h3><span class="label label-${status}">${db}</span></h3>
${message ? message : ''}
</article>
`
    );
}

function pageEnd(res) {
    res.write(
        '  </body>\n' +
        '</html>');
}
